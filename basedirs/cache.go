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
	"os/user"
	"strconv"
	"sync"
)

// GroupCache caches the names associated with GIDs.
type GroupCache struct {
	mu   sync.RWMutex
	data map[uint32]string
}

// NewGroupCache creates a new GroupCache.
func NewGroupCache() *GroupCache {
	return &GroupCache{
		data: make(map[uint32]string),
	}
}

// GroupName retrieves the name of a given GID.
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

// SetCached sets the cached name for a given GID.
func (g *GroupCache) SetCached(gid uint32, name string) {
	g.mu.Lock()
	g.data[gid] = name
	g.mu.Unlock()
}

// Iter iterates over the cached groups.
func (g *GroupCache) Iter(yield func(k uint32, v string) bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for k, v := range g.data {
		if !yield(k, v) {
			return
		}
	}
}

// UserCache caches the names associated with UIDs.
type UserCache struct {
	mu   sync.RWMutex
	data map[uint32]string
}

// NewUserCache creates a new UserCache.
func NewUserCache() *UserCache {
	return &UserCache{
		data: make(map[uint32]string),
	}
}

// UserName retrieves the name of a given UID.
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

// SetCached sets the cached name for a given UID.
func (u *UserCache) SetCached(uid uint32, name string) {
	u.mu.Lock()
	u.data[uid] = name
	u.mu.Unlock()
}

// Iter iterates over the cached users.
func (u *UserCache) Iter(yield func(k uint32, v string) bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	for k, v := range u.data {
		if !yield(k, v) {
			return
		}
	}
}
