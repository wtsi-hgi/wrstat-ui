/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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
package backups

import (
	"encoding/json"
	"io"
	"maps"
	"math"
	"slices"
	"strings"
	"unsafe"

	"github.com/wtsi-hgi/wrstat-ui/summary"
	"vimagination.zapto.org/rwcount"
)

type rootUserAction struct {
	*projectRootData
	userID uint32
	action action
}

func (r rootUserAction) compare(s rootUserAction) int { //nolint:gocognit,gocyclo
	if r.projectRootData == s.projectRootData {
		if s.userID == r.userID {
			return int(r.action) - int(s.action)
		}

		return int(r.userID) - int(s.userID)
	}

	if r.projectData == nil { //nolint:nestif
		if s.projectData == nil {
			return strings.Compare(r.Root, s.Root)
		}

		return -1
	} else if s.projectData == nil {
		return 1
	}

	if r.Faculty == s.Faculty { //nolint:nestif
		if r.Name == s.Name {
			if r.Requestor == s.Requestor {
				return strings.Compare(r.Root, s.Root)
			}

			return strings.Compare(r.Requestor, s.Requestor)
		}

		return strings.Compare(r.Name, s.Name)
	}

	return strings.Compare(r.Faculty, s.Faculty)
}

type Summary map[rootUserAction]*RootSummary

func (s Summary) addFile(file *summary.FileInfo, group *projectAction) {
	key := rootUserAction{
		projectRootData: group.projectRootData,
		userID:          file.UID,
		action:          group.action,
	}

	root := s[key]
	if root == nil {
		root = &RootSummary{
			Action:          group.action,
			UserID:          file.UID,
			projectRootData: group.projectRootData,
			OldestMTime:     math.MaxInt64,
		}
		s[key] = root
	}

	root.Add(file)
}

func (s Summary) WriteTo(w io.Writer) (int64, error) { //nolint:unparam
	sw := &rwcount.Writer{Writer: w}
	e := json.NewEncoder(sw)
	first := true
	keys := slices.Collect(maps.Keys(s))

	slices.SortFunc(keys, rootUserAction.compare)

	io.WriteString(sw, "[") //nolint:errcheck

	var tmpPath [maxPathLength + maxFilenameLength]byte

	for _, rua := range keys {
		if userRoot := s[rua]; userRoot != nil { //nolint:nestif
			if first {
				first = false
			} else {
				io.WriteString(sw, ",") //nolint:errcheck
			}

			path := userRoot.base.AppendTo(tmpPath[:0])
			userRoot.Base = unsafe.String(unsafe.SliceData(path), len(path))

			e.Encode(userRoot) //nolint:errcheck,errchkjson
		}
	}

	io.WriteString(sw, "]") //nolint:errcheck

	return sw.Count, sw.Err
}

type RootSummary struct {
	*projectRootData
	Action      action
	UserID      uint32
	base        *summary.DirectoryPath
	Base        string
	Size        int64
	Count       int64
	OldestMTime int64
	NewestMTime int64
}

func (r *RootSummary) Add(file *summary.FileInfo) {
	if r.base == nil {
		r.base = file.Path
	} else {
		r.base = findCommonParent(r.base, file.Path)
	}

	r.Count++
	r.Size += file.Size
	r.OldestMTime = min(r.OldestMTime, file.MTime)
	r.NewestMTime = max(r.NewestMTime, file.MTime)
}

func findCommonParent(a, b *summary.DirectoryPath) *summary.DirectoryPath {
	for a.Depth > b.Depth {
		a = a.Parent
	}

	for b.Depth > a.Depth {
		b = b.Parent
	}

	for a != b {
		a = a.Parent
		b = b.Parent
	}

	return a
}
