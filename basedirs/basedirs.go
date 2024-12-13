/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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

// package basedirs is used to summarise disk usage information by base
// directory, storing and retrieving the information from an embedded database.

package basedirs

import (
	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// BaseDirs is used to summarise disk usage information by base directory and
// group or user.
type BaseDirs struct {
	dbPath      string
	config      Config
	quotas      *Quotas
	ch          codec.Handle
	mountPoints mountPoints
}

// NewCreator returns a BaseDirs that lets you create a database summarising
// usage information by base directory, taken from the given tree and quotas.
//
// Choose splits and minDirs based on how many directories deep you expect data
// for different groups/users to appear. Eg. if your file structure is
// `/mounts/[group name]`, that's 2 directories deep and splits 1, minDirs 2
// might work well. If it's 5 directories deep, splits 4, minDirs 4 might work
// well.
func NewCreator(dbPath string, c Config, quotas *Quotas) (*BaseDirs, error) {
	mp, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	return &BaseDirs{
		dbPath:      dbPath,
		config:      c,
		quotas:      quotas,
		ch:          new(codec.BincHandle),
		mountPoints: mp,
	}, nil
}

// SetMountPoints can be used to manually set your mountpoints, if the automatic
// discovery of mountpoints on your system doesn't work.
func (b *BaseDirs) SetMountPoints(mountpoints []string) {
	b.mountPoints = mountpoints
}

type SummaryWithChildren struct {
	db.DirSummary
	Children []*SubDir
}

type AgeDirs [len(db.DirGUTAges)][]SummaryWithChildren

type IDAgeDirs map[uint32]*AgeDirs

func (i IDAgeDirs) Get(id uint32) *AgeDirs {
	ap := i[id]

	if ap == nil {
		ap = new(AgeDirs)

		i[id] = ap
	}

	return ap
}
