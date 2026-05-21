/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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
	"io"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	treeBatchRoot       = "/root"
	treeBatchRootChildA = "/root/a"
	treeBatchRootChildB = "/root/b"
)

type batchHasChildrenTestDB struct {
	hasChildren           map[string]bool
	childrenCalls         int
	dirsHaveChildrenCalls [][]string
}

func (d *batchHasChildrenTestDB) DirInfo(dir string, _ *db.Filter) (*db.DirSummary, error) {
	return &db.DirSummary{Dir: dir, Count: 1}, nil
}

func (d *batchHasChildrenTestDB) Children(string) ([]string, error) {
	d.childrenCalls++

	return []string{}, nil
}

func (d *batchHasChildrenTestDB) DirsHaveChildren(dirs []string, _ *db.Filter) (map[string]bool, error) {
	d.dirsHaveChildrenCalls = append(d.dirsHaveChildrenCalls, dirs)

	result := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		result[dir] = d.hasChildren[dir]
	}

	return result, nil
}

func (d *batchHasChildrenTestDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *batchHasChildrenTestDB) Close() error {
	return nil
}

func TestTreeElementUsesBatchedChildExistence(t *testing.T) {
	Convey("DiskTree conversion batches child has_children checks", t, func() {
		database := &batchHasChildrenTestDB{
			hasChildren: map[string]bool{
				treeBatchRootChildA: true,
				treeBatchRootChildB: false,
			},
		}

		s := New(io.Discard)
		s.tree = db.NewTree(database)

		element := s.diToTreeElement(&db.DirInfo{
			Current: &db.DirSummary{Dir: treeBatchRoot, Count: 3},
			Children: []*db.DirSummary{
				{Dir: treeBatchRootChildA, Count: 2},
				{Dir: treeBatchRootChildB, Count: 1},
			},
		}, nil, nil, treeBatchRoot)

		So(element.Children, ShouldHaveLength, 2)
		So(element.Children[0].HasChildren, ShouldBeTrue)
		So(element.Children[1].HasChildren, ShouldBeFalse)
		So(database.dirsHaveChildrenCalls, ShouldResemble, [][]string{{
			treeBatchRootChildA, treeBatchRootChildB,
		}})
		So(database.childrenCalls, ShouldBeZeroValue)
	})
}
