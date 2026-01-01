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

package groupuser

import (
	"fmt"
	"io"
	"sort"

	"github.com/wtsi-hgi/wrstat-ui/summary"
)

type groupUserSummary struct {
	Group, User string
	*summary.Summary
}

type groupUserSummaries []groupUserSummary

func (g groupUserSummaries) Len() int {
	return len(g)
}

func (g groupUserSummaries) Less(i, j int) bool {
	if g[i].Group < g[j].Group {
		return true
	}

	if g[i].Group > g[j].Group {
		return false
	}

	return g[i].User < g[j].User
}

func (g groupUserSummaries) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

// GroupUser is used to summarise file stats by group and user.
type GroupUser struct {
	w     io.WriteCloser
	store map[summary.GroupUserID]*summary.Summary
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will add the file size
// and increment the file count summed for the info's group and user. If path is
// a directory, it is ignored.
func (g *GroupUser) Add(info *summary.FileInfo) error {
	if info.IsDir() {
		return nil
	}

	id := summary.NewGroupUserID(info.GID, info.UID)

	ss, ok := g.store[id]
	if !ok {
		ss = new(summary.Summary)
		g.store[id] = ss
	}

	ss.Add(info.Size)

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
func (g *GroupUser) Output() error {
	uidLookupCache := make(map[uint32]string)
	gidLookupCache := make(map[uint32]string)

	data := make(groupUserSummaries, 0, len(g.store))

	for gu, s := range g.store {
		data = append(data, groupUserSummary{
			Group:   summary.GIDToName(gu.GID(), gidLookupCache),
			User:    summary.UIDToName(gu.UID(), uidLookupCache),
			Summary: s,
		})
	}

	sort.Sort(data)

	for _, row := range data {
		if _, err := fmt.Fprintf(g.w, "%s\t%s\t%d\t%d\n",
			row.Group, row.User, row.Count, row.Size); err != nil {
			return err
		}
	}

	return g.w.Close()
}

// NewByGroupUser returns a GroupUser.
func NewByGroupUser(w io.WriteCloser) summary.OperationGenerator {
	return func() summary.Operation {
		return &GroupUser{
			w:     w,
			store: make(map[summary.GroupUserID]*summary.Summary),
		}
	}
}
