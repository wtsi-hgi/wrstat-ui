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

package summary

import (
	"fmt"
	"io"
	"os/user"
	"sort"
	"strconv"
)

type dirSummary struct {
	*DirectoryPath
	*summary
}

// gidToName converts gid to group name, using the given cache to avoid lookups.
func gidToName(gid uint32, cache map[uint32]string) string {
	return cachedIDToName(gid, cache, getGroupName)
}

func cachedIDToName(id uint32, cache map[uint32]string, lookup func(uint32) string) string {
	if name, ok := cache[id]; ok {
		return name
	}

	name := lookup(id)

	cache[id] = name

	return name
}

// getGroupName returns the name of the group given gid. If the lookup fails,
// returns "idxxx", where xxx is the given id as a string.
func getGroupName(id uint32) string {
	sid := strconv.Itoa(int(id))

	g, err := user.LookupGroupId(sid)
	if err != nil {
		return "id" + sid
	}

	return g.Name
}

// getUserName returns the username of the given uid. If the lookup fails,
// returns "idxxx", where xxx is the given id as a string.
func getUserName(id uint32) string {
	sid := strconv.Itoa(int(id))

	u, err := user.LookupId(sid)
	if err != nil {
		return "id" + sid
	}

	return u.Username
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
			Group:     gidToName(id.GID(), r.gidLookupCache),
			User:      uidToName(id.UID(), r.uidLookupCache),
			Directory: u.thisDir,
			summary:   s,
		})
	}
}

type directorySummaryStore map[*DirectoryPath]*summary

func (d directorySummaryStore) Get(p *DirectoryPath) *summary {
	s, ok := d[p]
	if !ok {
		s = new(summary)
		d[p] = s
	}

	return s
}

// userGroup is used to summarise file stats by user and group.
type userGroup struct {
	root      *rootUserGroup
	summaries map[groupUserID]*summary
	thisDir   *DirectoryPath
}

// NewByUserGroup returns a Usergroup.
func NewByUserGroup(w io.WriteCloser) OperationGenerator {
	root := &rootUserGroup{
		w:              w,
		uidLookupCache: make(map[uint32]string),
		gidLookupCache: make(map[uint32]string),
		userGroup: userGroup{
			summaries: make(map[groupUserID]*summary),
		},
	}

	root.userGroup.root = root
	first := true

	return func() Operation {
		if first {
			first = false

			return root
		}

		return &userGroup{
			root:      root,
			summaries: make(map[groupUserID]*summary),
		}
	}
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will break path in to
// its directories and add the file size and increment the file count to each,
// summed for the info's user and group. If path is a directory, it is ignored.
func (u *userGroup) Add(info *FileInfo) error {
	if info.IsDir() {
		if u.thisDir == nil {
			u.thisDir = info.Path
		}

		return nil
	}

	id := newGroupUserID(info.GID, info.UID)

	s, ok := u.summaries[id]
	if !ok {
		s = new(summary)
		u.summaries[id] = s
	}

	s.add(info.Size)

	return nil
}

type userGroupDirectory struct {
	Group, User string
	Directory   *DirectoryPath
	*summary
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

	path := make([]byte, 0, maxPathLen)

	for _, row := range r.store {
		rowPath := row.Directory.appendTo(path)

		if _, err := fmt.Fprintf(r.w, "%s\t%s\t%q\t%d\t%d\n",
			row.Group, row.User, rowPath, row.count, row.size); err != nil {
			return err
		}
	}

	return r.w.Close()
}

func (u *userGroup) Output() error {
	u.root.addToStore(u)

	u.thisDir = nil

	for k := range u.summaries {
		delete(u.summaries, k)
	}

	return nil
}
