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
	"bytes"
	"fmt"
	"io"
	"os/user"
	"sort"
	"strconv"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/stats"
)

type directoryPath struct {
	Name   string
	Depth  int
	Parent *directoryPath
}

func (d *directoryPath) Cwd(path []byte) *directoryPath {
	depth := bytes.Count(path, slash)

	for d.Depth >= depth {
		d = d.Parent
	}

	name := path[bytes.LastIndexByte(path[:len(path)-1], '/')+1:]

	return &directoryPath{
		Name:   string(name),
		Depth:  depth,
		Parent: d,
	}
}

func (d *directoryPath) appendTo(p []byte) []byte {
	if d.Parent != nil {
		p = d.Parent.appendTo(p)
	}

	return append(p, d.Name...)
}

func (d *directoryPath) Less(e *directoryPath) bool {
	if d.Depth < e.Depth {
		return d.compare(e.getDepth(d.Depth)) != 1
	} else if d.Depth > e.Depth {
		return d.getDepth(e.Depth).compare(e) == -1
	}

	return d.compare(e) == -1
}

func (d *directoryPath) getDepth(n int) *directoryPath {
	for d.Depth != n {
		d = d.Parent
	}

	return d
}

func (d *directoryPath) compare(e *directoryPath) int {
	if d == e {
		return 0
	}

	cmp := d.Parent.compare(e.Parent)

	if cmp == 0 {
		return strings.Compare(d.Name[:len(d.Name)-1], e.Name[:len(e.Name)-1])
	}

	return cmp
}

type dirSummary struct {
	*directoryPath
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
	w io.WriteCloser
	userGroup
}

type directorySummaryStore map[*directoryPath]*summary

func (d directorySummaryStore) Get(p *directoryPath) *summary {
	s, ok := d[p]
	if !ok {
		s = new(summary)
		d[p] = s
	}

	return s
}

type userGroupStore map[groupUserID]directorySummaryStore

func (u userGroupStore) Get(id groupUserID, p *directoryPath) *summary {
	d, ok := u[id]
	if !ok {
		d = make(directorySummaryStore)
		u[id] = d
	}

	return d.Get(p)
}

// userGroup is used to summarise file stats by user and group.
type userGroup struct {
	store            userGroupStore
	currentDirectory **directoryPath
	thisDir          *directoryPath
}

// NewByUserGroup returns a Usergroup.
func NewByUserGroup(w io.WriteCloser) OperationGenerator {
	store := make(userGroupStore)
	first := true

	var currentDirectory *directoryPath

	return func() Operation {
		if first {
			first = false

			return &rootUserGroup{
				w: w,
				userGroup: userGroup{
					store:            store,
					currentDirectory: &currentDirectory,
				},
			}
		}

		return &userGroup{
			store:            store,
			currentDirectory: &currentDirectory,
		}
	}
}

func (r *rootUserGroup) Add(info *stats.FileInfo) error {
	if info.IsDir() {
		if *r.currentDirectory == nil {
			r.thisDir = &directoryPath{
				Name:  string(info.Path),
				Depth: bytes.Count(info.Path, slash),
			}
			*r.currentDirectory = r.thisDir
		}

		return nil
	}

	return r.userGroup.Add(info)
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will break path in to
// its directories and add the file size and increment the file count to each,
// summed for the info's user and group. If path is a directory, it is ignored.
func (u *userGroup) Add(info *stats.FileInfo) error {
	if info.IsDir() {
		if u.thisDir == nil {
			*u.currentDirectory = (*u.currentDirectory).Cwd(info.Path)
			u.thisDir = *u.currentDirectory
		}

		return nil
	}

	u.store.Get(newGroupUserID(info.GID, info.UID), u.thisDir).add(info.Size)

	return nil
}

type userGroupDirectory struct {
	Group, User string
	Directory   *directoryPath
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
	uidLookupCache := make(map[uint32]string)
	gidLookupCache := make(map[uint32]string)

	data := make(userGroupDirectories, 0, len(r.store))

	for gu, ds := range r.store {
		for d, s := range ds {
			data = append(data, userGroupDirectory{
				Group:     gidToName(gu.GID(), gidLookupCache),
				User:      uidToName(gu.UID(), uidLookupCache),
				Directory: d,
				summary:   s,
			})
		}
	}

	sort.Sort(data)

	path := make([]byte, 0, maxPathLen)

	for _, row := range data {
		rowPath := row.Directory.appendTo(path)

		if _, err := fmt.Fprintf(r.w, "%s\t%s\t%q\t%d\t%d\n",
			row.Group, row.User, rowPath, row.count, row.size); err != nil {
			return err
		}
	}

	return r.w.Close()
}

func (u *userGroup) Output() error {
	u.thisDir = nil

	return nil
}
