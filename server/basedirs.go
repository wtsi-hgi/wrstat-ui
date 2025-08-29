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
	"strconv"

	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	db "github.com/wtsi-hgi/wrstat-ui/db"
)

const ErrBadBasedirsQuery = gas.Error("bad query; check id and basedir")

func (s *Server) addBaseDirRoutes() {
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
}

func (s *Server) getBasedirsGroupUsage(c *gin.Context) {
	s.getBasedirs(c, func() (any, error) {
		results := make([]*basedirs.Usage, 0)

		for _, age := range db.DirGUTAges {
			result, err := s.basedirs.GroupUsage(age)
			if err != nil {
				return nil, err
			}

			results = append(results, result...)
		}

		return results, nil
	})
}

// getBasedirs responds with the output of your callback in JSON format.
// LoadBasedirsDB() must already have been called.
//
// This is called when there is a GET on /rest/v1/basedirs/* or
// /rest/v1/authbasedirs/*.
func (s *Server) getBasedirs(c *gin.Context, cb func() (any, error)) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result, err := cb()
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.IndentedJSON(http.StatusOK, result)
}

func (s *Server) getBasedirsUserUsage(c *gin.Context) {
	s.getBasedirs(c, func() (any, error) {
		results := make([]*basedirs.Usage, 0)

		for _, age := range db.DirGUTAges {
			result, err := s.basedirs.UserUsage(age)
			if err != nil {
				return nil, err
			}

			results = append(results, result...)
		}

		return results, nil
	})
}

func (s *Server) getBasedirsGroupSubdirs(c *gin.Context) {
	allowedGIDs, err := s.allowedGIDs(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	id, basedir, age, ok := getSubdirsArgs(c)
	if !ok {
		return
	}

	if areDisjoint(allowedGIDs, []uint32{uint32(id)}) {
		io.WriteString(c.Writer, "[]") //nolint:errcheck

		return
	}

	s.getBasedirs(c, func() (any, error) {
		var results []*basedirs.SubDir

		result, err := s.basedirs.GroupSubDirs(uint32(id), basedir, age)
		if err != nil {
			return nil, err
		}

		results = append(results, result...)

		return results, nil
	})
}

func getSubdirsArgs(c *gin.Context) (int, string, db.DirGUTAge, bool) {
	idStr := c.Query("id")
	basedir := c.Query("basedir")
	ageStr := c.Query("age")

	if idStr == "" || basedir == "" {
		c.AbortWithError(http.StatusBadRequest, ErrBadBasedirsQuery) //nolint:errcheck

		return 0, "", db.DGUTAgeAll, false
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, ErrBadBasedirsQuery) //nolint:errcheck

		return 0, "", db.DGUTAgeAll, false
	}

	if ageStr == "" {
		ageStr = "0"
	}

	age, err := db.AgeStringToDirGUTAge(ageStr)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, ErrBadBasedirsQuery) //nolint:errcheck

		return 0, "", db.DGUTAgeAll, false
	}

	return id, basedir, age, true
}

func (s *Server) getBasedirsUserSubdirs(c *gin.Context) {
	id, basedir, age, ok := getSubdirsArgs(c)
	if !ok {
		return
	}

	if !s.isUserAuthedToReadPath(c, basedir) {
		io.WriteString(c.Writer, "[]") //nolint:errcheck

		return
	}

	s.getBasedirs(c, func() (any, error) {
		var results []*basedirs.SubDir

		result, err := s.basedirs.UserSubDirs(uint32(id), basedir, age)
		if err != nil {
			return nil, err
		}

		results = append(results, result...)

		return results, nil
	})
}

func (s *Server) isUserAuthedToReadPath(c *gin.Context, path string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
	id, basedir, _, ok := getSubdirsArgs(c)
	if !ok {
		return
	}

	s.getBasedirs(c, func() (any, error) {
		return s.basedirs.History(uint32(id), basedir)
	})
}
