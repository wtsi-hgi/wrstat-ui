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

// package summary lets you summarise file stats.

package summary

import (
	"os/user"
	"strconv"
)

// Summary holds count and size and lets you accumulate count and size as you
// add more things with a size.
type Summary struct {
	Count int64
	Size  int64
}

// add will increment our count and add the given size to our size.
func (s *Summary) Add(size int64) {
	s.Count++
	s.Size += size
}

// SummaryWithTimes is like summary, but also holds the reference time, oldest
// atime, newest mtime add()ed.
type SummaryWithTimes struct {
	Summary
	Atime int64 // seconds since Unix epoch
	Mtime int64 // seconds since Unix epoch
}

// add will increment our count and add the given size to our size. It also
// stores the given atime if it is older than our current one, and the given
// mtime if it is newer than our current one.
func (s *SummaryWithTimes) Add(size int64, atime int64, mtime int64) {
	s.Summary.Add(size)

	if atime > 0 && (s.Atime == 0 || atime < s.Atime) {
		s.Atime = atime
	}

	if mtime > 0 && (s.Mtime == 0 || mtime > s.Mtime) {
		s.Mtime = mtime
	}
}

type GroupUserID uint64

func NewGroupUserID(gid, uid uint32) GroupUserID {
	return GroupUserID(gid)<<32 | GroupUserID(uid)
}

func (g GroupUserID) GID() uint32 {
	return uint32(g >> 32)
}

func (g GroupUserID) UID() uint32 {
	return uint32(g)
}

// GIDToName converts gid to group name, using the given cache to avoid lookups.
func GIDToName(gid uint32, cache map[uint32]string) string {
	return cachedIDToName(gid, cache, getGroupName)
}

// UIDToName converts uid to username, using the given cache to avoid lookups.
func UIDToName(uid uint32, cache map[uint32]string) string {
	return cachedIDToName(uid, cache, getUserName)
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
