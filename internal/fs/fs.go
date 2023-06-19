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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	gas "github.com/wtsi-hgi/go-authserver"
)

const ErrNoDirEntryFound = gas.Error("file not found in directory")

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
