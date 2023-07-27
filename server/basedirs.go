/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package server

import (
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-ssg/wrstat/v4/basedirs"
	ifs "github.com/wtsi-ssg/wrstat/v4/internal/fs"
	"github.com/wtsi-ssg/wrstat/v4/watch"
)

const ErrBadBasedirsQuery = gas.Error("bad query; check id and basedir")

// LoadBasedirsDB loads the given basedirs.db file (as produced by
// basedirs.CreateDatabase()) and makes use of the given owners file (a
// gid,owner csv) and adds the following GET endpoints to the REST API:
//
// /rest/v1/basedirs/usage/groups
// /rest/v1/basedirs/usage/users
// /rest/v1/basedirs/subdirs/group
// /rest/v1/basedirs/subdirs/user
// /rest/v1/basedirs/history
//
// If you call EnableAuth() first, then these endpoints will be secured and be
// available at /rest/v1/auth/basedirs/*.
//
// The subdir endpoints require id (gid or uid) and basedir parameters.
// The history endpoint requires a gid and basedir (can be basedir, actually a
// mountpoint) parameter.
func (s *Server) LoadBasedirsDB(dbPath, ownersPath string) error {
	s.basedirsMutex.Lock()
	defer s.basedirsMutex.Unlock()

	bd, err := basedirs.NewReader(dbPath, ownersPath)
	if err != nil {
		return err
	}

	s.basedirs = bd
	s.basedirsPath = dbPath
	s.ownersPath = ownersPath

	authGroup := s.AuthRouter()

	if authGroup == nil {
		s.Router().GET(EndPointBasedirUsageGroup, s.getBasedirsGroupUsage)
		s.Router().GET(EndPointBasedirUsageUser, s.getBasedirsUserUsage)
		s.Router().GET(EndPointBasedirSubdirGroup, s.getBasedirsGroupSubdirs)
		s.Router().GET(EndPointBasedirSubdirUser, s.getBasedirsUserSubdirs)
		s.Router().GET(EndPointBasedirHistory, s.getBasedirsHistory)
	} else {
		authGroup.GET(basedirsGroupUsagePath, s.getBasedirsGroupUsage)
		authGroup.GET(basedirsUserUsagePath, s.getBasedirsUserUsage)
		authGroup.GET(basedirsGroupSubdirPath, s.getBasedirsGroupSubdirs)
		authGroup.GET(basedirsUserSubdirPath, s.getBasedirsUserSubdirs)
		authGroup.GET(basedirsHistoryPath, s.getBasedirsHistory)
	}

	return nil
}

func (s *Server) getBasedirsGroupUsage(c *gin.Context) {
	s.getBasedirs(c, func() (any, error) {
		return s.basedirs.GroupUsage()
	})
}

// getBasedirs responds with the output of your callback in JSON format.
// LoadBasedirsDB() must already have been called.
//
// This is called when there is a GET on /rest/v1/basedirs/* or
// /rest/v1/authbasedirs/*.
func (s *Server) getBasedirs(c *gin.Context, cb func() (any, error)) {
	s.basedirsMutex.RLock()
	defer s.basedirsMutex.RUnlock()

	result, err := cb()
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.IndentedJSON(http.StatusOK, result)
}

func (s *Server) getBasedirsUserUsage(c *gin.Context) {
	s.getBasedirs(c, func() (any, error) {
		return s.basedirs.UserUsage()
	})
}

func (s *Server) getBasedirsGroupSubdirs(c *gin.Context) {
	allowedGIDs, err := s.allowedGIDs(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	id, basedir, ok := getSubdirsArgs(c)
	if !ok {
		return
	}

	if areDisjoint(allowedGIDs, []uint32{uint32(id)}) {
		io.WriteString(c.Writer, "[]") //nolint:errcheck

		return
	}

	s.getBasedirs(c, func() (any, error) {
		return s.basedirs.GroupSubDirs(uint32(id), basedir)
	})
}

func getSubdirsArgs(c *gin.Context) (int, string, bool) {
	idStr := c.Query("id")
	basedir := c.Query("basedir")

	if idStr == "" || basedir == "" {
		c.AbortWithError(http.StatusBadRequest, ErrBadBasedirsQuery) //nolint:errcheck

		return 0, "", false
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, ErrBadBasedirsQuery) //nolint:errcheck

		return 0, "", false
	}

	return id, basedir, true
}

func (s *Server) getBasedirsUserSubdirs(c *gin.Context) {
	id, basedir, ok := getSubdirsArgs(c)
	if !ok {
		return
	}

	if !s.isUserAuthedToReadPath(c, basedir) {
		io.WriteString(c.Writer, "[]") //nolint:errcheck

		return
	}

	s.getBasedirs(c, func() (any, error) {
		return s.basedirs.UserSubDirs(uint32(id), basedir)
	})
}

func (s *Server) isUserAuthedToReadPath(c *gin.Context, path string) bool {
	s.treeMutex.RLock()
	defer s.treeMutex.RUnlock()

	di, err := s.tree.DirInfo(path, nil)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return false
	}

	allowedGIDs, err := s.allowedGIDs(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return false
	}

	return !areDisjoint(allowedGIDs, di.Current.GIDs)
}

func (s *Server) getBasedirsHistory(c *gin.Context) {
	id, basedir, ok := getSubdirsArgs(c)
	if !ok {
		return
	}

	s.getBasedirs(c, func() (any, error) {
		return s.basedirs.History(uint32(id), basedir)
	})
}

// EnableBasedirDBReloading will wait for changes to the file at watchPath, then:
//  1. close any previously loaded basedirs database file
//  2. find the latest file in the given directory with the given suffix
//  3. set the basedirs.db directory path to that and load it
//  4. delete the old basedirs.db file
//
// It will only return an error if trying to watch watchPath immediately fails.
// Other errors (eg. reloading or deleting files) will be logged.
func (s *Server) EnableBasedirDBReloading(watchPath, dir, suffix string, pollFrequency time.Duration) error {
	s.basedirsMutex.Lock()
	defer s.basedirsMutex.Unlock()

	cb := func(_ time.Time) {
		s.reloadBasedirsDB(dir, suffix)
	}

	watcher, err := watch.New(watchPath, cb, pollFrequency)
	if err != nil {
		return err
	}

	s.basedirsWatcher = watcher

	return nil
}

// reloadBasedirsDB closes the database file previously loaded during
// LoadBasedirsDB(), looks for the latest file in the given directory that has
// the given suffix, and loads that as our new basedirsPath.
//
// On success, deletes the previous basedirsPath.
//
// Logs any errors.
func (s *Server) reloadBasedirsDB(dir, suffix string) {
	s.basedirsMutex.Lock()
	defer s.basedirsMutex.Unlock()

	if s.basedirs != nil {
		s.basedirs.Close()
	}

	oldPath := s.basedirsPath

	err := s.findNewBasedirsPath(dir, suffix)
	if err != nil {
		s.Logger.Printf("reloading basedirs db failed: %s", err)

		return
	}

	if s.basedirsPath == oldPath {
		return
	}

	s.loadNewBasedirsDBAndDeleteOld(oldPath)
}

// findNewBasedirsPath finds the latest file in dir that has the given suffix,
// then sets our basedirsPath to the result.
func (s *Server) findNewBasedirsPath(dir, suffix string) error {
	path, err := FindLatestBasedirsDB(dir, suffix)
	if err != nil {
		return err
	}

	s.basedirsPath = path

	return nil
}

// FindLatestBasedirsDB finds the latest file in dir that has the given suffix.
func FindLatestBasedirsDB(dir, suffix string) (string, error) {
	return ifs.FindLatestDirectoryEntry(dir, suffix)
}

func (s *Server) loadNewBasedirsDBAndDeleteOld(oldPath string) {
	s.Logger.Printf("reloading basedirs db from %s", s.basedirsPath)

	var err error

	s.basedirs, err = basedirs.NewReader(s.basedirsPath, s.ownersPath)
	if err != nil {
		s.Logger.Printf("reloading basedirs db failed: %s", err)

		return
	}

	s.Logger.Printf("server ready again after reloading dgut dbs")

	err = os.Remove(oldPath)
	if err != nil {
		s.Logger.Printf("deletion of old basedirs db after reload failed: %s", err)
	}
}
