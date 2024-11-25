/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"io/fs"
	"os"
	"path/filepath"
	"time"

	ifs "github.com/wtsi-hgi/wrstat-ui/internal/fs"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
	"github.com/wtsi-hgi/wrstat-ui/watch"
)

// LoadDGUTADBs loads the given dguta.db directories (as produced by one or more
// invocations of dguta.DB.Store()) and adds the /rest/v1/where GET endpoint to
// the REST API. If you call EnableAuth() first, then this endpoint will be
// secured and be available at /rest/v1/auth/where.
//
// The where endpoint can take the dir, splits, groups, users and types
// parameters, which correspond to arguments that dguta.Tree.Where() takes.
func (s *Server) LoadDGUTADBs(paths ...string) error {
	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	tree, err := dirguta.NewTree(paths...)
	if err != nil {
		return err
	}

	s.tree = tree
	s.dgutaPaths = paths

	authGroup := s.AuthRouter()

	if authGroup == nil {
		s.Router().GET(EndPointWhere, s.getWhere)
	} else {
		authGroup.GET(wherePath, s.getWhere)
	}

	return nil
}

// EnableDGUTADBReloading will wait for changes to the file at watchPath, then:
//  1. close any previously loaded dguta database files
//  2. find the latest sub-directory in the given directory with the given suffix
//  3. set the dguta.db directory paths to children of 2) and load those
//  4. delete the old dguta.db directory paths to save space, and their parent
//     dir if now empty
//  5. update the server's data-creation date to the mtime of the watchPath file
//
// It will also do 5) immediately on calling this method.
//
// It will only return an error if trying to watch watchPath immediately fails.
// Other errors (eg. reloading or deleting files) will be logged.
func (s *Server) EnableDGUTADBReloading(watchPath, dir, suffix string, pollFrequency time.Duration) error {
	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	cb := func(mtime time.Time) {
		s.reloadDGUTADBs(dir, suffix, mtime)
	}

	watcher, err := watch.New(watchPath, cb, pollFrequency)
	if err != nil {
		return err
	}

	s.dataTimeStamp = watcher.Mtime()

	s.dgutaWatcher = watcher

	return nil
}

// reloadDGUTADBs closes database files previously loaded during LoadDGUTADBs(),
// looks for the latest subdirectory of the given directory that has the given
// suffix, and loads the children of that as our new dgutaPaths.
//
// On success, deletes the previous dgutaPaths and updates our dataTimestamp.
//
// Logs any errors.
func (s *Server) reloadDGUTADBs(dir, suffix string, mtime time.Time) {
	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	if s.tree != nil {
		s.tree.Close()
	}

	oldPaths := s.dgutaPaths

	err := s.findNewDgutaPaths(dir, suffix)
	if err != nil {
		s.Logger.Printf("reloading dguta dbs failed: %s", err)

		return
	}

	s.Logger.Printf("reloading dguta dbs from %s", s.dgutaPaths)

	s.tree, err = dirguta.NewTree(s.dgutaPaths...)
	if err != nil {
		s.Logger.Printf("reloading dguta dbs failed: %s", err)

		return
	}

	s.Logger.Printf("server ready again after reloading dguta dbs")

	s.deleteDirs(oldPaths)

	s.dataTimeStamp = mtime
}

// findNewDgutaPaths finds the latest subdirectory of dir that has the given
// suffix, then sets our dgutaPaths to the result's children.
func (s *Server) findNewDgutaPaths(dir, suffix string) error {
	paths, err := FindLatestDgutaDirs(dir, suffix)
	if err != nil {
		return err
	}

	s.dgutaPaths = paths

	return nil
}

// FindLatestDgutaDirs finds the latest subdirectory of dir that has the given
// suffix, then returns that result's child directories.
func FindLatestDgutaDirs(dir, suffix string) ([]string, error) {
	latest, err := ifs.FindLatestDirectoryEntry(dir, suffix)
	if err != nil {
		return nil, err
	}

	return getChildDirectories(latest)
}

// getChildDirectories returns the child directories of the given dir.
func getChildDirectories(dir string) ([]string, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var paths []string

	for _, de := range des {
		if de.IsDir() || de.Type()&fs.ModeSymlink != 0 {
			paths = append(paths, filepath.Join(dir, de.Name()))
		}
	}

	if len(paths) == 0 {
		return nil, ifs.ErrNoDirEntryFound
	}

	return paths, nil
}

// deleteDirs deletes the given directories. Logs any errors. Also tries to
// delete their parent directory which will work if now empty. Does not delete
// any directory that's a current db directory.
func (s *Server) deleteDirs(dirs []string) {
	current := make(map[string]bool)
	for _, dir := range s.dgutaPaths {
		current[dir] = true
	}

	for _, dir := range dirs {
		if current[dir] {
			s.Logger.Printf("skipping deletion of dguta db dir since still current: %s", dir)

			continue
		}

		if err := os.RemoveAll(dir); err != nil {
			s.Logger.Printf("deleting dguta dbs failed: %s", err)
		}
	}

	parent := filepath.Dir(dirs[0])

	if err := os.Remove(parent); err != nil {
		s.Logger.Printf("deleting dguta dbs parent dir failed: %s", err)
	}
}
