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
	"sort"
	"syscall"
)

// userToSummary is a sortable map with uids as keys and summaries as values.
type userToSummaryStore map[uint32]*summary

// add will auto-vivify a summary for the given uid and call add(size) on it.
func (store userToSummaryStore) add(uid uint32, size int64) {
	s, ok := store[uid]
	if !ok {
		s = &summary{}
		store[uid] = s
	}

	s.add(size)
}

// sort returns a slice of our summary values, sorted by our uid keys converted
// to user names, which are also returned.
//
// If uid is invalid, user name will be id[uid].
//
// If you will be sorting multiple different userToSummaryStores, supply them
// all the same uidLookupCache which is used to minimise uid to name lookups.
func (store userToSummaryStore) sort(uidLookupCache map[uint32]string) ([]string, []*summary) {
	byUserName := make(map[string]*summary)

	for uid, summary := range store {
		byUserName[uidToName(uid, uidLookupCache)] = summary
	}

	keys := make([]string, len(byUserName))
	i := 0

	for k := range byUserName {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]*summary, len(byUserName))

	for i, k := range keys {
		s[i] = byUserName[k]
	}

	return keys, s
}

// uidToName converts uid to username, using the given cache to avoid lookups.
func uidToName(uid uint32, cache map[uint32]string) string {
	return cachedIDToName(uid, cache, getUserName)
}

// groupToUserStore is a sortable map of gid to userToSummaryStore.
type groupToUserStore map[uint32]userToSummaryStore

// getUserToSummaryStore auto-vivifies a userToSummaryStore for the given gid
// and returns it.
func (store groupToUserStore) getUserToSummaryStore(gid uint32) userToSummaryStore {
	uStore, ok := store[gid]
	if !ok {
		uStore = make(userToSummaryStore)
		store[gid] = uStore
	}

	return uStore
}

// sort returns a slice of our userToSummaryStore values, sorted by our gid keys
// converted to unix group names, which are also returned. If gid has no group
// name, name becomes id[gid].
func (store groupToUserStore) sort() ([]string, []userToSummaryStore) {
	byGroupName := make(map[string]userToSummaryStore)

	for gid, uStore := range store {
		byGroupName[getGroupName(gid)] = uStore
	}

	keys := make([]string, len(byGroupName))
	i := 0

	for k := range byGroupName {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]userToSummaryStore, len(byGroupName))

	for i, k := range keys {
		s[i] = byGroupName[k]
	}

	return keys, s
}

// GroupUser is used to summarise file stats by group and user.
type GroupUser struct {
	store groupToUserStore
}

// NewByGroupUser returns a GroupUser.
func NewByGroupUser() *GroupUser {
	return &GroupUser{
		store: make(groupToUserStore),
	}
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will add the file size
// and increment the file count summed for the info's group and user. If path is
// a directory, it is ignored.
func (g *GroupUser) Add(_ string, info fs.FileInfo) error {
	if info.IsDir() {
		return nil
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errNotUnix
	}

	g.store.getUserToSummaryStore(stat.Gid).add(stat.Uid, info.Size())

	return nil
}

// Output will write summary information for all the paths previously added. The
// format is (tab separated):
//
// group username filecount filesize
//
// group and username are sorted, and there is a special username "all" to give
// total filecount and filesize for all users that wrote files in that group.
//
// Returns an error on failure to write, or if username or group can't be
// determined from the uids and gids in the added file info. output is closed
// on completion.
func (g *GroupUser) Output(output io.WriteCloser) error {
	groups, uStores := g.store.sort()

	uidLookupCache := make(map[uint32]string)

	for i, groupname := range groups {
		if err := outputUserSummariesForGroup(output, groupname, uStores[i], uidLookupCache); err != nil {
			return err
		}
	}

	return output.Close()
}

// outputUserSummariesForGroup sorts the users for this group and outputs the
// summary information.
func outputUserSummariesForGroup(output io.WriteCloser, groupname string,
	uStore userToSummaryStore, uidLookupCache map[uint32]string) error {
	usernames, summaries := uStore.sort(uidLookupCache)

	for i, s := range summaries {
		if _, err := fmt.Fprintf(output, "%s\t%s\t%d\t%d\n",
			groupname, usernames[i], s.count, s.size); err != nil {
			return err
		}
	}

	return nil
}
