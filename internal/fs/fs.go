/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
 *	- Michael Woolnough <mw31@sanger.ac.uk>
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

package fs

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type noDirEntryFoundError struct{}

func (noDirEntryFoundError) Error() string {
	return "file not found in directory"
}

var ErrNoDirEntryFound noDirEntryFoundError

const DirPerms = 0755

// FindLatestDirectoryEntry finds the latest entry in dir that has the given
// suffix and returns its path.
func FindLatestDirectoryEntry(dir, suffix string) (string, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	sort.Slice(des, func(i, j int) bool {
		return DirEntryModTime(des[i]).After(DirEntryModTime(des[j]))
	})

	for _, de := range des {
		if strings.HasSuffix(de.Name(), "."+suffix) {
			return filepath.Join(dir, de.Name()), nil
		}
	}

	return "", ErrNoDirEntryFound
}

// DirEntryModTime returns the ModTime of the given DirEntry, treating errors as
// time 0.
func DirEntryModTime(de os.DirEntry) time.Time {
	info, err := de.Info()
	if err != nil {
		return time.Time{}
	}

	return info.ModTime()
}

// ModTime returns the ModTime for the given path, treating errors as time 0.
func ModTime(path string) time.Time {
	fi, err := os.Lstat(path)
	if err != nil {
		return time.Time{}
	}

	return fi.ModTime()
}

// Touch updates the modtime and access time of the specified path to the
// specified time.
func Touch(path string, t time.Time) error {
	return os.Chtimes(path, t, t)
}

type pathTime struct {
	path    string
	modtime time.Time
}

// FindLatestCombinedOutput finds the entry in dir that contains the newest
// watchFile.
func FindLatestCombinedOutput(dir, watchFile string) (string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*", watchFile))
	if err != nil {
		return "", err
	}

	if len(files) == 0 {
		return "", ErrNoDirEntryFound
	}

	de, err := filesToLatestPathTime(files)
	if err != nil {
		return "", err
	}

	return filepath.Dir(de.path), nil
}

func filesToLatestPathTime(files []string) (pathTime, error) {
	des := make([]pathTime, len(files))

	for n, file := range files {
		fi, err := os.Lstat(file)
		if err != nil {
			return pathTime{}, err
		}

		des[n] = pathTime{
			path:    file,
			modtime: fi.ModTime(),
		}
	}

	sort.Slice(des, func(i, j int) bool {
		return des[i].modtime.After(des[j].modtime)
	})

	return des[0], nil
}

// CopyPreservingTimestamp copies the source to the dest, recursively,
// preserving the modification and access times.
func CopyPreservingTimestamp(source, dest string) error {
	fi, err := os.Lstat(source)
	if err != nil {
		return err
	}

	t := fi.ModTime()

	if fi.IsDir() {
		err = CopyDirectoryPreservingTimestamp(source, dest)
	} else {
		err = CopyFile(source, dest)
	}

	if err != nil {
		return err
	}

	return os.Chtimes(dest, t, t)
}

// CopyDirectoryPreservingTimestamp copies the source directory to the
// destination, preserving the modification and access times.
func CopyDirectoryPreservingTimestamp(source, dest string) error {
	if err := os.MkdirAll(dest, DirPerms); err != nil {
		return err
	}

	matches, err := filepath.Glob(filepath.Join(source, "*"))
	if err != nil {
		return err
	}

	for _, match := range matches {
		if err := CopyPreservingTimestamp(match, filepath.Join(dest, filepath.Base(match))); err != nil {
			return err
		}
	}

	return nil
}

// CopyFile copies the source file to the destination, preserving the
// modification and access times.
func CopyFile(source, dest string) (err error) {
	var f, d *os.File

	if f, err = os.Open(source); err != nil {
		return err
	}

	defer f.Close()

	if d, err = os.Create(dest); err != nil {
		return err
	}

	defer func() {
		if errr := d.Close(); err == nil {
			err = errr
		}
	}()

	_, err = io.Copy(d, f)

	return err
}

// RemoveFromDirWhenOlderThan removes all children of the given directory if
// their modification time is before the time specified.
func RemoveFromDirWhenOlderThan(dir string, before time.Time) error {
	matches, err := filepath.Glob(dir + "/*")
	if err != nil {
		return err
	}

	for _, match := range matches {
		fi, err := os.Lstat(match)
		if err != nil {
			return err
		} else if !fi.ModTime().Before(before) {
			continue
		}

		if err := os.RemoveAll(match); err != nil {
			return err
		}
	}

	return nil
}
