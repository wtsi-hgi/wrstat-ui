/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package server

import (
	"fmt"
	"os/user"
	"sort"
	"time"

	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/summary"
)

// DirSummary holds nested file count, size and atime information on a
// directory. It also holds which users and groups own files nested under the
// directory, and their file types. It differs from dgut.DirSummary in having
// string names for users, groups and types, instead of ids.
type DirSummary struct {
	Dir       string
	Count     uint64
	Size      uint64
	Atime     time.Time
	Users     []string
	Groups    []string
	FileTypes []string
}

// dcssToSummaries converts the given DCSs to our own DirSummary, the difference
// being we change the UIDs to usernames and the GIDs to group names. On failure
// to convert, the name will skipped.
func (s *Server) dcssToSummaries(dcss dgut.DCSs) []*DirSummary {
	summaries := make([]*DirSummary, len(dcss))

	for i, dds := range dcss {
		summaries[i] = s.dgutDStoSummary(dds)
	}

	return summaries
}

// dgutDStoSummary converts the given dgut.DirSummary to one of our DirSummary,
// basically just converting the *IDs to names.
func (s *Server) dgutDStoSummary(dds *dgut.DirSummary) *DirSummary {
	return &DirSummary{
		Dir:       dds.Dir,
		Count:     dds.Count,
		Size:      dds.Size,
		Atime:     dds.Atime,
		Users:     s.uidsToUsernames(dds.UIDs),
		Groups:    s.gidsToNames(dds.GIDs),
		FileTypes: s.ftsToNames(dds.FTs),
	}
}

// uidsToUsernames converts the given user IDs to usernames, sorted on the
// names.
func (s *Server) uidsToUsernames(uids []uint32) []string {
	return idsToSortedNames(uids, s.uidToNameCache, func(uid string) (string, error) {
		u, err := user.LookupId(uid)
		if err != nil {
			return "", err
		}

		return u.Username, nil
	})
}

// idsToSortedNames uses the given callback to convert the given ids to names
// (skipping if the cb errors), and sorts them. It caches results in the given
// map, avoiding the use of the cb if we already have the answer.
func idsToSortedNames(ids []uint32, cache map[uint32]string, cb func(string) (string, error)) []string {
	names := make([]string, len(ids))

	for i, id := range ids {
		name, found := cache[id]
		if found {
			names[i] = name

			continue
		}

		name, err := cb(fmt.Sprintf("%d", id))
		if err != nil {
			names[i] = unknown
		} else {
			names[i] = name
		}

		cache[id] = names[i]
	}

	names = removeUnknown(names)

	sort.Strings(names)

	return names
}

// removeUnknown does a no-allocation filter of slice to remove unknown entries.
func removeUnknown(slice []string) []string {
	filtered := slice[:0]

	for _, item := range slice {
		if item != unknown {
			filtered = append(filtered, item)
		}
	}

	return filtered
}

// gidsToNames converts the given unix group IDs to group names, sorted
// on the names.
func (s *Server) gidsToNames(gids []uint32) []string {
	return idsToSortedNames(gids, s.gidToNameCache, func(gid string) (string, error) {
		g, err := user.LookupGroupId(gid)
		if err != nil {
			return "", err
		}

		return g.Name, nil
	})
}

// ftsToNames converts the given file types to their names, sorted on the names.
func (s *Server) ftsToNames(fts []summary.DirGUTFileType) []string {
	names := make([]string, len(fts))

	for i, ft := range fts {
		names[i] = ft.String()
	}

	sort.Strings(names)

	return names
}
