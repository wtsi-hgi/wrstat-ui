/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
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

package usergroup

import (
	"fmt"
	"io"
	"sort"

	"github.com/wtsi-hgi/wrstat-ui/summary"
)

// userGroup is used to summarise file stats by user and group.
type userGroup struct {
	root      *rootUserGroup
	summaries map[summary.GroupUserID]*summary.Summary
	thisDir   *summary.DirectoryPath
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will break path in to
// its directories and add the file size and increment the file count to each,
// summed for the info's user and group. If path is a directory, it is ignored.
func (u *userGroup) Add(info *summary.FileInfo) error {
	if info.IsDir() {
		if u.thisDir == nil {
			u.thisDir = info.Path
		}

		return nil
	}

	id := summary.NewGroupUserID(info.GID, info.UID)

	s, ok := u.summaries[id]
	if !ok {
		s = new(summary.Summary)
		u.summaries[id] = s
	}

	s.Add(info.Size)

	return nil
}

func (u *userGroup) Output() error {
	u.root.addToStore(u)

	u.thisDir = nil

	for k := range u.summaries {
		delete(u.summaries, k)
	}

	return nil
}

// NewByUserGroup returns a Usergroup.
func NewByUserGroup(w io.WriteCloser) summary.OperationGenerator {
	root := &rootUserGroup{
		w:              w,
		uidLookupCache: make(map[uint32]string),
		gidLookupCache: make(map[uint32]string),
		userGroup: userGroup{
			summaries: make(map[summary.GroupUserID]*summary.Summary),
		},
	}

	root.userGroup.root = root
	first := true

	return func() summary.Operation {
		if first {
			first = false

			return root
		}

		return &userGroup{
			root:      root,
			summaries: make(map[summary.GroupUserID]*summary.Summary),
		}
	}
}

type userGroupDirectory struct {
	Group, User string
	Directory   *summary.DirectoryPath
	*summary.Summary
}

type userGroupDirectories []userGroupDirectory

func (u userGroupDirectories) Len() int {
	return len(u)
}

func (u userGroupDirectories) Less(i, j int) bool {
	if u[i].User < u[j].User {
		return true
	}

	if u[i].User > u[j].User {
		return false
	}

	if u[i].Group < u[j].Group {
		return true
	}

	if u[i].Group > u[j].Group {
		return false
	}

	return u[i].Directory.Less(u[j].Directory)
}

func (u userGroupDirectories) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

type rootUserGroup struct {
	w              io.WriteCloser
	store          userGroupDirectories
	uidLookupCache map[uint32]string
	gidLookupCache map[uint32]string
	userGroup
}

func (r *rootUserGroup) addToStore(u *userGroup) {
	for id, s := range u.summaries {
		r.store = append(r.store, userGroupDirectory{
			Group:     summary.GIDToName(id.GID(), r.gidLookupCache),
			User:      summary.UIDToName(id.UID(), r.uidLookupCache),
			Directory: u.thisDir,
			Summary:   s,
		})
	}
}

// Output will write summary information for all the paths previously added. The
// format is (tab separated):
//
// username group directory filecount filesize
//
// usernames, groups and directories are sorted.
//
// Returns an error on failure to write, or if username or group can't be
// determined from the uids and gids in the added file info. output is closed
// on completion.
func (r *rootUserGroup) Output() error {
	r.addToStore(&r.userGroup)

	sort.Sort(r.store)

	path := make([]byte, 0, summary.MaxPathLen)

	for _, row := range r.store {
		rowPath := row.Directory.AppendTo(path)

		if _, err := fmt.Fprintf(r.w, "%s\t%s\t%q\t%d\t%d\n",
			row.User, row.Group, rowPath, row.Count, row.Size); err != nil {
			return err
		}
	}

	return r.w.Close()
}
