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
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	ErrNoPaths    = basedirs.Error("no db paths found")
	numDBDirParts = 2
	keyPart       = 1
)

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
func (s *Server) LoadDBs(basePaths []string, dgutaDBName, basedirDBName, ownersPath string, mounts ...string) error { //nolint:funlen,lll
	dirgutaPaths, baseDirPaths := JoinDBPaths(basePaths, dgutaDBName, basedirDBName)

	mts, err := s.getDBTimestamps(baseDirPaths)
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

	if len(mounts) > 0 {
		bd.SetMountPoints(mounts)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	err = s.prewarmCaches(bd)
	if err != nil {
		return err
	}

	loaded := s.basedirs != nil
	s.basedirs = bd
	s.tree = tree
	s.dataTimeStamp = mts

	if !loaded {
		s.addBaseDGUTARoutes()
		s.addBaseDirRoutes()
	}

	return nil
}

func (s *Server) getDBTimestamps(paths []string) (map[string]int64, error) {
	timestamps := make(map[string]int64, len(paths))

	for _, path := range paths {
		dbDir := filepath.Dir(path)

		st, err := os.Stat(dbDir)
		if err != nil {
			return nil, err
		}

		timestamps[strings.SplitN(filepath.Base(dbDir), "_", numDBDirParts)[keyPart]] = st.ModTime().Unix()
	}

	return timestamps, nil
}

// EnableDBReloading will periodically scan the basepath for the latest
// directories that contains dirguta and basedir databases and update the server
// to use the new databases.
//
// The scan will looks for directories within the base path with names that
// match the following regular expression:
//
// ^\d+_.
//
// The directory name consists of two parts, a numeric version identifier and a
// mountpoint/key, separated by an underscore.
//
// The directory must contain entries with names equal to both the dgutaDBName
// and basedirDBName to be considered valid.
//
// For each mountpoint/key, the databases in the directory with the greatest
// (numerical) version will be considered the most up-to-date version and those
// that will be loaded.
//
// You can set the removeOldPaths to true to cause valid, but older directories
// to be removed from the base path after each reload.
func (s *Server) EnableDBReloading(basepath, dgutaDBName, basedirDBName, ownersPath string,
	pollFrequency time.Duration, removeOldPaths bool, mounts ...string) error {
	dbPaths, toDelete, err := findDBDirs(basepath, dgutaDBName, basedirDBName)
	if err != nil {
		return err
	} else if len(dbPaths) == 0 {
		return ErrNoPaths
	}

	if err := s.LoadDBs(dbPaths, dgutaDBName, basedirDBName, ownersPath, mounts...); err != nil {
		return err
	}

	if removeOldPaths {
		if err := removeAll(basepath, toDelete); err != nil {
			return err
		}
	}

	go s.reloadLoop(basepath, dgutaDBName, basedirDBName, ownersPath,
		pollFrequency, removeOldPaths, dbPaths, mounts)

	return nil
}

func (s *Server) reloadLoop(basepath, dgutaDBName, basedirDBName, ownersPath string, //nolint:gocognit,gocyclo
	pollFrequency time.Duration, removeOldPaths bool, dbPaths, mounts []string) {
	for {
		select {
		case <-time.After(pollFrequency):
		case <-s.stopCh:
			return
		}

		newDBPaths, toDelete, err := findDBDirs(basepath, dgutaDBName, basedirDBName)
		if err != nil {
			s.Logger.Printf("finding new database directories failed: %s", err)

			continue
		}

		if slices.Equal(newDBPaths, dbPaths) {
			continue
		}

		if s.reloadDBs(dgutaDBName, basedirDBName, newDBPaths, mounts, toDelete) { //nolint:nestif
			dbPaths = newDBPaths

			if removeOldPaths {
				if err := removeAll(basepath, toDelete); err != nil {
					s.Logger.Printf("deleting old database failed: %s", err)
				}
			}
		}
	}
}

// reloadDBs performs a full or incremental reload of the DirGUTAge tree and
// basedirs MultiReader. It updates only new or changed databases while closing
// obsolete ones and prewarming caches.
func (s *Server) reloadDBs(dgutaDBName, basedirDBName string, dbPaths, mounts, toDelete []string) bool { //nolint:funlen,lll
	dirgutaPaths, baseDirPaths := JoinDBPaths(dbPaths, dgutaDBName, basedirDBName)

	mt, err := s.getDBTimestamps(baseDirPaths)
	if err != nil {
		return s.logReloadError("reloading dbs failed: %s", err)
	}

	s.mu.RLock()
	oldTree := s.tree
	oldBD := s.basedirs
	s.mu.RUnlock()

	newTree, err := s.reloadTreeIncrementally(oldTree, dirgutaPaths, toDelete)
	if err != nil {
		return s.logReloadError("%s", err)
	}

	newBD, err := s.reloadBaseDirsIncrementally(oldBD, baseDirPaths, toDelete)
	if err != nil {
		return s.logReloadError("%s", err)
	}

	if len(mounts) > 0 {
		newBD.SetMountPoints(mounts)
	}

	if err := s.prewarmCaches(newBD); err != nil {
		return s.logReloadError("prewarming caches failed: %s", err)
	}

	s.mu.Lock()
	s.tree = newTree
	s.basedirs = newBD
	s.dataTimeStamp = mt
	s.mu.Unlock()

	if err := s.closeObsoleteDBs(oldTree, oldBD, toDelete); err != nil {
		return s.logReloadError("%s", err)
	}

	s.Logger.Printf("server ready again after reloading")

	return true
}

// reloadTreeIncrementally attempts to reload an existing db.Tree incrementally
// using the provided new paths and paths to remove. Returns an error if it
// fails.
func (s *Server) reloadTreeIncrementally(oldTree *db.Tree, dirgutaPaths, toDelete []string) (*db.Tree, error) {
	newTree, err := oldTree.OpenFrom(dirgutaPaths, toDelete)
	if err != nil {
		s.Logger.Printf("incremental tree reload failed, falling back: %s", err)

		return nil, err
	}

	return newTree, nil
}

// reloadBaseDirsIncrementally attempts to reload an existing
// basedirs.MultiReader incrementally using the provided new paths and paths to
// remove. Returns an error if it fail.
func (s *Server) reloadBaseDirsIncrementally(oldBD basedirs.MultiReader,
	paths, toDelete []string) (basedirs.MultiReader, error) {
	newBD, err := oldBD.OpenFrom(paths, toDelete)
	if err != nil {
		s.Logger.Printf("incremental basedirs reload failed, falling back: %s", err)

		return nil, err
	}

	return newBD, nil
}

// closeObsoleteDBs closes databases that are no longer needed after a reload.
func (s *Server) closeObsoleteDBs(oldTree *db.Tree, oldBD basedirs.MultiReader, toDelete []string) error {
	if oldTree != nil {
		if err := oldTree.CloseOnly(toDelete); err != nil {
			s.Logger.Printf("error closing obsolete tree dbs: %s", err)

			return err
		}
	}

	if oldBD != nil {
		if err := oldBD.CloseOnly(toDelete); err != nil {
			s.Logger.Printf("error closing obsolete basedirs dbs: %s", err)

			return err
		}
	}

	return nil
}

func (s *Server) logReloadError(format string, v ...any) bool {
	s.Logger.Printf(format, v...)

	return false
}

// FindDBDirs finds the latest dirguta and basedir databases in the given base
// directory, returning the paths to the dirguta dbs and basedir dbs for each
// key/mountpoint.
func FindDBDirs(basepath string, required ...string) ([]string, error) {
	dbPaths, _, err := findDBDirs(basepath, required...)

	return dbPaths, err
}

// JoinDBPaths produces a list of a dgutaDB paths and basedirDB paths from the
// provided base dbPaths and the basenames of the DBs.
func JoinDBPaths(dbPaths []string, dgutaDBName, basedirDBName string) ([]string, []string) {
	dirgutaPaths := make([]string, len(dbPaths))
	baseDirPaths := make([]string, len(dbPaths))

	for n, path := range dbPaths {
		dirgutaPaths[n] = filepath.Join(path, dgutaDBName)
		baseDirPaths[n] = filepath.Join(path, basedirDBName)
	}

	return dirgutaPaths, baseDirPaths
}

type nameVersion struct {
	name    string
	version string
}

func findDBDirs(basepath string, required ...string) ([]string, []string, error) {
	entries, err := os.ReadDir(basepath)
	if err != nil {
		return nil, nil, err
	}
	var toDelete []string

	latest := make(map[string]nameVersion)

	for _, entry := range entries {
		if !IsValidDBDir(entry, basepath, required...) {
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

var validDBDir = regexp.MustCompile(`^[^.][^_]*_.`)

// IsValidDBDir returns true if the given entry is a directory named with the
// correct format and containing the required files.
func IsValidDBDir(entry fs.DirEntry, basepath string, required ...string) bool {
	name := entry.Name()

	if !entry.IsDir() || !validDBDir.MatchString(name) {
		return false
	}

	for _, entry := range required {
		if !entryExists(filepath.Join(basepath, name, entry)) {
			return false
		}
	}

	return true
}

func entryExists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

func addEntryToMap(entry fs.DirEntry, latest map[string]nameVersion, toDelete []string) []string {
	parts := strings.SplitN(entry.Name(), "_", numDBDirParts)
	key := parts[1]

	version := parts[0]

	if previous, ok := latest[key]; previous.version > version { //nolint:nestif
		toDelete = append(toDelete, key)
	} else {
		if ok {
			toDelete = append(toDelete, previous.name)
		}

		latest[key] = nameVersion{name: entry.Name(), version: version}
	}

	return toDelete
}

func removeAll(baseDirectory string, toDelete []string) error {
	for _, path := range toDelete {
		// Create marker to avoid the watch subcommand re-running a summarise.
		f, err := os.Create(filepath.Join(baseDirectory, "."+path))
		if err != nil {
			return err
		}

		if err := f.Close(); err != nil {
			return err
		}

		if err := os.RemoveAll(filepath.Join(baseDirectory, path)); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) dbUpdateTimestamps(c *gin.Context) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c.JSON(http.StatusOK, s.dataTimeStamp)
}
