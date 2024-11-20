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
	"io/fs"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
)

type Error string

func (e Error) Error() string { return string(e) }

const errNotUnix = Error("file info Sys() was not a *syscall.Stat_t; only unix is supported")

// dirStore is a sortable map with directory paths as keys and summaries as
// values.
type dirStore map[string]*summary

// addForEachDir breaks path into each directory and calls add() on it.
func (store dirStore) addForEachDir(path string, size int64) {
	dir := filepath.Dir(path)

	for {
		store.add(dir, size)

		if dir == "/" || dir == "." {
			return
		}

		dir = filepath.Dir(dir)
	}
}

// add will auto-vivify a summary for the given directory path and call
// add(size) on it.
func (store dirStore) add(path string, size int64) {
	s, ok := store[path]
	if !ok {
		s = &summary{}
		store[path] = s
	}

	s.add(size)
}

// sort returns a slice of our summary values, sorted by our directory path
// keys which are also returned.
func (store dirStore) sort() ([]string, []*summary) {
	return sortSummaryStore(store)
}

// sortSummaryStore returns a slice of the store's values, sorted by the store's
// keys which are also returned.
func sortSummaryStore[T any](store map[string]*T) ([]string, []*T) {
	keys := make([]string, len(store))
	i := 0

	for k := range store {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]*T, len(store))

	for i, k := range keys {
		s[i] = store[k]
	}

	return keys, s
}

// groupStore is a sortable map of gid to dirStore.
type groupStore map[uint32]dirStore

// getDirStore auto-vivifies a dirStore for the given gid and returns it.
func (store groupStore) getDirStore(gid uint32) dirStore {
	dStore, ok := store[gid]
	if !ok {
		dStore = make(dirStore)
		store[gid] = dStore
	}

	return dStore
}

// sort returns a slice of our dirStore values, sorted by our gid keys converted
// to group names, which are also returned.
//
// If a gid is invalid, the name will be id[gid].
//
// If you will be sorting multiple different groupStores, supply them all the
// same gidLookupCache which is used to minimise gid to name lookups.
func (store groupStore) sort(gidLookupCache map[uint32]string) ([]string, []dirStore) {
	byGroupName := make(map[string]dirStore)

	for gid, dStore := range store {
		byGroupName[gidToName(gid, gidLookupCache)] = dStore
	}

	keys := make([]string, len(byGroupName))
	i := 0

	for k := range byGroupName {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]dirStore, len(byGroupName))

	for i, k := range keys {
		s[i] = byGroupName[k]
	}

	return keys, s
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

// userStore is a sortable map of uid to groupStore.
type userStore map[uint32]groupStore

// DirStore auto-vivifies an entry in our store for the given uid and gid and
// returns it.
func (store userStore) DirStore(uid, gid uint32) dirStore {
	return store.getGroupStore(uid).getDirStore(gid)
}

// getGroupStore auto-vivifies a groupStore for the given uid and returns it.
func (store userStore) getGroupStore(uid uint32) groupStore {
	gStore, ok := store[uid]
	if !ok {
		gStore = make(groupStore)
		store[uid] = gStore
	}

	return gStore
}

// sort returns a slice of our groupStore values, sorted by our uid keys
// converted to user names, which are also returned. If uid has no user name,
// user name will be id[uid].
func (store userStore) sort() ([]string, []groupStore) {
	byUserName := make(map[string]groupStore)

	for uid, gids := range store {
		byUserName[getUserName(uid)] = gids
	}

	keys := make([]string, len(byUserName))
	i := 0

	for k := range byUserName {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]groupStore, len(byUserName))

	for i, k := range keys {
		s[i] = byUserName[k]
	}

	return keys, s
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

// Usergroup is used to summarise file stats by user and group.
type Usergroup struct {
	store userStore
}

// NewByUserGroup returns a Usergroup.
func NewByUserGroup() *Usergroup {
	return &Usergroup{
		store: make(userStore),
	}
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will break path in to
// its directories and add the file size and increment the file count to each,
// summed for the info's user and group. If path is a directory, it is ignored.
func (u *Usergroup) Add(path string, info fs.FileInfo) error {
	if info.IsDir() {
		return nil
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errNotUnix
	}

	dStore := u.store.DirStore(stat.Uid, stat.Gid)

	dStore.addForEachDir(path, info.Size())

	return nil
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
func (u *Usergroup) Output(output io.WriteCloser) error {
	users, gStores := u.store.sort()

	gidLookupCache := make(map[uint32]string)

	for i, username := range users {
		if err := outputGroupDirectorySummariesForUser(output, username, gStores[i], gidLookupCache); err != nil {
			return err
		}
	}

	return output.Close()
}

// outputGroupDirectorySummariesForUser sortes the groups for this user and
// calls outputDirectorySummariesForGroup.
func outputGroupDirectorySummariesForUser(output io.WriteCloser, username string,
	gStore groupStore, gidLookupCache map[uint32]string,
) error {
	groupnames, dStores := gStore.sort(gidLookupCache)

	for i, groupname := range groupnames {
		if err := outputDirectorySummariesForGroup(output, username, groupname, dStores[i]); err != nil {
			return err
		}
	}

	return nil
}

// outputDirectorySummariesForGroup sorts the directories for this group and
// does the actual output of all the summary information.
func outputDirectorySummariesForGroup(output io.WriteCloser, username, groupname string, dStore dirStore) error {
	dirs, summaries := dStore.sort()

	for i, s := range summaries {
		_, errw := fmt.Fprintf(output, "%s\t%s\t%s\t%d\t%d\n",
			username, groupname, strconv.Quote(dirs[i]), s.count, s.size)
		if errw != nil {
			return errw
		}
	}

	return nil
}
