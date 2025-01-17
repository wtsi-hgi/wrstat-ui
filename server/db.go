/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const ErrNoPaths = basedirs.Error("no db paths found")

// LoadDBs loads dirguta and basedir databases from the given
// directory/directories, and adds the relevant endpoints to the REST API.
//
// For the dirguta databases (as produced by one or more invocations of
// dguta.DB.Store()) it adds the /rest/v1/where GET endpoint to the REST API. If
// you call EnableAuth() first, then this endpoint will be secured and be
// available at /rest/v1/auth/where.
//
// The where endpoint can take the dir, splits, groups, users and types
// parameters, which correspond to arguments that dguta.Tree.Where() takes.
//
// For the basedirs database (as produced by basedirs.CreateDatabase()), in
// combination with an owners file (a gid,owner csv), it adds the following GET
// endpoints to the REST API:
//
// /rest/v1/basedirs/usage/groups /rest/v1/basedirs/usage/users
// /rest/v1/basedirs/subdirs/group /rest/v1/basedirs/subdirs/user
// /rest/v1/basedirs/history
//
// If you call EnableAuth() first, then these endpoints will be secured and be
// available at /rest/v1/auth/basedirs/*.
//
// The subdir endpoints require id (gid or uid) and basedir parameters. The
// history endpoint requires a gid and basedir (can be basedir, actually a
// mountpoint) parameter.
func (s *Server) LoadDBs(basePaths []string, dgutaDBsSuffix, basedirBasename, ownersPath string) error { //nolint:funlen
	dirgutaPaths, baseDirPaths := makeDBPaths(basePaths, dgutaDBsSuffix, basedirBasename)

	mt, err := s.getLatestTimestamp(dirgutaPaths, baseDirPaths)
	if err != nil {
		return err
	}

	tree, err := db.NewTree(dirgutaPaths...)
	if err != nil {
		return err
	}

	bd, err := basedirs.OpenMulti(ownersPath, baseDirPaths...)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	loaded := s.basedirs != nil
	s.basedirs = bd
	s.tree = tree
	s.dataTimeStamp = mt

	if !loaded {
		s.addBaseDGUTARoutes()
		s.addBaseDirRoutes()
	}

	return nil
}

func (s *Server) getLatestTimestamp(a, b []string) (time.Time, error) {
	mt, err := s.getLatestTimestampFromPaths(a)
	if err != nil {
		return mt, err
	}

	nt, err := s.getLatestTimestampFromPaths(b)
	if err != nil {
		return mt, err
	}

	if nt.After(mt) {
		return nt, nil
	}

	return mt, nil
}

func (s *Server) getLatestTimestampFromPaths(paths []string) (time.Time, error) {
	var lt time.Time

	for _, path := range paths {
		st, err := os.Stat(path)
		if err != nil {
			return lt, err
		}

		mt := st.ModTime()
		if mt.After(lt) {
			lt = mt
		}
	}

	return lt, nil
}

func (s *Server) EnableDBReloading(basepath, dgutaDBsSuffix, basedirBasename, ownersPath string,
	sentinelPollFrequency time.Duration, removeOldPaths bool) error {
	dbPaths, toDelete, err := findDBDirs(basepath, dgutaDBsSuffix, basedirBasename)
	if err != nil {
		return err
	} else if len(dbPaths) == 0 {
		return ErrNoPaths
	}

	if err := s.LoadDBs(dbPaths, dgutaDBsSuffix, basedirBasename, ownersPath); err != nil {
		return err
	}

	if removeOldPaths {
		if err := removeAll(basepath, toDelete); err != nil {
			return err
		}
	}

	go s.reloadLoop(basepath, dgutaDBsSuffix, basedirBasename, ownersPath,
		sentinelPollFrequency, removeOldPaths, dbPaths)

	return nil
}

func (s *Server) reloadLoop(basepath, dgutaDBsSuffix, basedirBasename, ownersPath string, //nolint:gocognit,gocyclo
	sentinelPollFrequency time.Duration, removeOldPaths bool, dbPaths []string) {
	for {
		select {
		case <-time.After(sentinelPollFrequency):
		case <-s.stopCh:
			return
		}

		newDBPaths, toDelete, err := findDBDirs(basepath, dgutaDBsSuffix, basedirBasename)
		if err != nil {
			s.Logger.Printf("finding new database directories failed: %s", err)

			continue
		}

		if slices.Equal(newDBPaths, dbPaths) {
			continue
		}

		if s.reloadDBs(dgutaDBsSuffix, basedirBasename, ownersPath, newDBPaths) { //nolint:nestif
			dbPaths = newDBPaths

			if removeOldPaths {
				if err := removeAll(basepath, toDelete); err != nil {
					s.Logger.Printf("deleting old database failed: %s", err)
				}
			}
		}
	}
}

func (s *Server) reloadDBs(dgutaDBsSuffix, basedirBasename, //nolint:funlen
	ownersPath string, dbPaths []string) bool {
	dirgutaPaths, baseDirPaths := makeDBPaths(dbPaths, dgutaDBsSuffix, basedirBasename)

	mt, err := s.getLatestTimestamp(dirgutaPaths, baseDirPaths)
	if err != nil {
		return s.logReloadError("reloading dbs failed: %s", err)
	}

	s.Logger.Printf("reloading dirguta db from %v", dirgutaPaths)

	tree, err := db.NewTree(dirgutaPaths...)
	if err != nil {
		return s.logReloadError("reloading dirguta db failed: %s", err)
	}

	s.Logger.Printf("reloading basedirs db from %v", baseDirPaths)

	bd, err := basedirs.OpenMulti(ownersPath, baseDirPaths...)
	if err != nil {
		return s.logReloadError("reloading basedirs db failed: %s", err)
	}

	s.Logger.Printf("server ready again after reloading")

	s.mu.Lock()
	s.tree = tree
	s.basedirs = bd
	s.dataTimeStamp = mt
	s.mu.Unlock()

	return true
}

func (s *Server) logReloadError(format string, v ...any) bool {
	s.Logger.Printf(format, v...)

	return false
}

// FindDBDirs finds the latest dirguta and basedir databases in the given base
// directory, returning the paths to the dirguta dbs and basedir dbs for each
// key/mountpoint.
func FindDBDirs(basepath, dgutaDBsSuffix, basedirBasename string) ([]string, []string, error) {
	dbPaths, _, err := findDBDirs(basepath, dgutaDBsSuffix, basedirBasename)
	dirgutaPaths, basedirPaths := makeDBPaths(dbPaths, dgutaDBsSuffix, basedirBasename)

	return dirgutaPaths, basedirPaths, err
}

func makeDBPaths(dbPaths []string, dgutaDBsSuffix, basedirBasename string) ([]string, []string) {
	dirgutaPaths := make([]string, len(dbPaths))
	baseDirPaths := make([]string, len(dbPaths))

	for n, path := range dbPaths {
		dirgutaPaths[n] = filepath.Join(path, dgutaDBsSuffix)
		baseDirPaths[n] = filepath.Join(path, basedirBasename)
	}

	return dirgutaPaths, baseDirPaths
}

type nameTime struct {
	name string
	time int64
}

func findDBDirs(basepath, dgutaDBsSuffix, basedirBasename string) ([]string, []string, error) {
	entries, err := os.ReadDir(basepath)
	if err != nil {
		return nil, nil, err
	}

	var toDelete []string

	latest := make(map[string]nameTime)

	for _, entry := range entries {
		if !isValidDBDir(entry, basepath, dgutaDBsSuffix, basedirBasename) {
			continue
		}

		toDelete = addEntryToMap(entry, latest, toDelete)
	}

	dirs := make([]string, 0, len(latest))

	for _, nt := range latest {
		dirs = append(dirs, filepath.Join(basepath, nt.name))
	}

	slices.Sort(dirs)

	return dirs, toDelete, nil
}

var validDBDir = regexp.MustCompile(`^\d+_.`)

func isValidDBDir(entry fs.DirEntry, basepath, dgutaDBsSuffix, basedirBasename string) bool {
	name := entry.Name()

	return entry.IsDir() && validDBDir.MatchString(name) &&
		entryExists(filepath.Join(basepath, name, dgutaDBsSuffix)) &&
		entryExists(filepath.Join(basepath, name, basedirBasename))
}

func entryExists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

func addEntryToMap(entry fs.DirEntry, latest map[string]nameTime, toDelete []string) []string {
	parts := strings.SplitN(entry.Name(), "_", 2) //nolint:mnd
	key := parts[1]

	timestamp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return toDelete
	}

	if previous, ok := latest[key]; previous.time > timestamp { //nolint:nestif
		toDelete = append(toDelete, key)
	} else {
		if ok {
			toDelete = append(toDelete, previous.name)
		}

		latest[key] = nameTime{name: entry.Name(), time: timestamp}
	}

	return toDelete
}

func removeAll(baseDirectory string, toDelete []string) error {
	for _, path := range toDelete {
		if err := os.RemoveAll(filepath.Join(baseDirectory, path)); err != nil {
			return err
		}
	}

	return nil
}
