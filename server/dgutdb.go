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
	"sort"
	"strings"
	"time"

	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/watch"
)

const ErrNoDgutDBDirFound = gas.Error("dgut database directory not found")

// LoadDGUTDBs loads the given dgut.db directories (as produced by one or more
// invocations of dgut.DB.Store()) and adds the /rest/v1/where GET endpoint to
// the REST API. If you call EnableAuth() first, then this endpoint will be
// secured and be available at /rest/v1/auth/where.
//
// The where endpoint can take the dir, splits, groups, users and types
// parameters, which correspond to arguments that dgut.Tree.Where() takes.
func (s *Server) LoadDGUTDBs(paths ...string) error {
	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	tree, err := dgut.NewTree(paths...)
	if err != nil {
		return err
	}

	s.tree = tree
	s.dgutPaths = paths

	authGroup := s.AuthRouter()

	if authGroup == nil {
		s.Router().GET(EndPointWhere, s.getWhere)
	} else {
		authGroup.GET(wherePath, s.getWhere)
	}

	return nil
}

// EnableDGUTDBReloading will wait for changes to the file at watchPath, then:
//  1. close any previously loaded dgut database files
//  2. find the latest sub-directory in the given directory with the given suffix
//  3. set the dgut.db directory paths to children of 2) and load those
//  4. delete the old dgut.db directory paths to save space, and their parent
//     dir if now empty
//  5. update the server's data-creation date to the mtime of the watchPath file
//
// It will also do 5) immediately on calling this method.
//
// It will only return an error if trying to watch watchPath immediately fails.
// Other errors (eg. reloading or deleting files) will be logged.
func (s *Server) EnableDGUTDBReloading(watchPath, dir, suffix string, pollFrequency time.Duration) error {
	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	cb := func(mtime time.Time) {
		s.reloadDGUTDBs(dir, suffix, mtime)
	}

	watcher, err := watch.New(watchPath, cb, pollFrequency)
	if err != nil {
		return err
	}

	s.dataTimeStamp = watcher.Mtime()

	s.dgutWatcher = watcher

	return nil
}

// reloadDGUTDBs closes database files previously loaded during LoadDGUTDBs(),
// looks for the latest subdirectory of the given directory that has the given
// suffix, and loads the children of that as our new dgutPaths.
//
// On success, deletes the previous dgutPaths and updates our dataTimestamp.
//
// Logs any errors.
func (s *Server) reloadDGUTDBs(dir, suffix string, mtime time.Time) {
	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	if s.tree != nil {
		s.tree.Close()
	}

	oldPaths := s.dgutPaths

	err := s.findNewDgutPaths(dir, suffix)
	if err != nil {
		s.Logger.Printf("reloading dgut dbs failed: %s", err)

		return
	}

	s.Logger.Printf("reloading dgut dbs from %s", s.dgutPaths)

	s.tree, err = dgut.NewTree(s.dgutPaths...)
	if err != nil {
		s.Logger.Printf("reloading dgut dbs failed: %s", err)

		return
	}

	s.Logger.Printf("server ready again after reloading dgut dbs")

	s.deleteDirs(oldPaths)

	s.dataTimeStamp = mtime
}

// findNewDgutPaths finds the latest subdirectory of dir that has the given
// suffix, then sets our dgutPaths to the result's children.
func (s *Server) findNewDgutPaths(dir, suffix string) error {
	paths, err := FindLatestDgutDirs(dir, suffix)
	if err != nil {
		return err
	}

	s.dgutPaths = paths

	return nil
}

// FindLatestDgutDirs finds the latest subdirectory of dir that has the given
// suffix, then returns that result's child directories.
func FindLatestDgutDirs(dir, suffix string) ([]string, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	sort.Slice(des, func(i, j int) bool {
		return dirEntryModTime(des[i]).After(dirEntryModTime(des[j]))
	})

	for _, de := range des {
		if strings.HasSuffix(de.Name(), "."+suffix) {
			return getChildDirectories(filepath.Join(dir, de.Name()))
		}
	}

	return nil, ErrNoDgutDBDirFound
}

// dirEntryModTime returns the ModTime of the given DirEntry, treating errors as
// time 0.
func dirEntryModTime(de os.DirEntry) time.Time {
	info, err := de.Info()
	if err != nil {
		return time.Time{}
	}

	return info.ModTime()
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
		return nil, ErrNoDgutDBDirFound
	}

	return paths, nil
}

// deleteDirs deletes the given directories. Logs any errors. Also tries to
// delete their parent directory which will work if now empty.
func (s *Server) deleteDirs(dirs []string) {
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			s.Logger.Printf("deleting dgut dbs failed: %s", err)
		}
	}

	parent := filepath.Dir(dirs[0])

	if err := os.Remove(parent); err != nil {
		s.Logger.Printf("deleting dgut dbs parent dir failed: %s", err)
	}
}
