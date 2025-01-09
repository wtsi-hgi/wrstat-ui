/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

package basedirs

import (
	"iter"
	"os/user"
	"strconv"
	"sync"
)

func (b *BaseDirReader) SetCachedGroup(gid uint32, name string) {
	b.groupCache.mu.Lock()
	defer b.groupCache.mu.Unlock()

	b.groupCache.data[gid] = name
}

type GroupCache struct {
	mu   sync.RWMutex
	data map[uint32]string
}

func NewGroupCache() *GroupCache {
	return &GroupCache{
		data: make(map[uint32]string),
	}
}

func (g *GroupCache) GroupName(gid uint32) string {
	g.mu.RLock()
	groupName, ok := g.data[gid]
	g.mu.RUnlock()

	if ok {
		return groupName
	}

	groupStr := strconv.FormatUint(uint64(gid), 10)

	group, err := user.LookupGroupId(groupStr)
	if err == nil {
		groupStr = group.Name
	}

	g.mu.Lock()
	g.data[gid] = groupStr
	g.mu.Unlock()

	return groupStr
}

func (g *GroupCache) Iter(yield func(k uint32, v string) bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for k, v := range g.data {
		if !yield(k, v) {
			return
		}
	}
}

func (b *BaseDirReader) IterCachedGroups() iter.Seq2[uint32, string] {
	return b.groupCache.Iter
}

func (b *BaseDirReader) SetCachedUser(uid uint32, name string) {
	b.userCache.mu.Lock()
	defer b.userCache.mu.Unlock()

	b.userCache.data[uid] = name
}

type UserCache struct {
	mu   sync.RWMutex
	data map[uint32]string
}

func NewUserCache() *UserCache {
	return &UserCache{
		data: make(map[uint32]string),
	}
}

func (u *UserCache) UserName(uid uint32) string {
	u.mu.RLock()
	userName, ok := u.data[uid]
	u.mu.RUnlock()

	if ok {
		return userName
	}

	userStr := strconv.FormatUint(uint64(uid), 10)

	uu, err := user.LookupId(userStr)
	if err == nil {
		userStr = uu.Username
	}

	u.mu.Lock()
	u.data[uid] = userStr
	u.mu.Unlock()

	return userStr
}
func (u *UserCache) Iter(yield func(k uint32, v string) bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	for k, v := range u.data {
		if !yield(k, v) {
			return
		}
	}
}

func (b *BaseDirReader) IterCachedUsers() iter.Seq2[uint32, string] {
	return b.userCache.Iter
}
