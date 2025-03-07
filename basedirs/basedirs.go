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
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// BaseDirs is used to summarise disk usage information by base directory and
// group or user.
type BaseDirs struct {
	dbPath      string
	quotas      *Quotas
	ch          codec.Handle
	mountPoints mountPoints
	modTime     time.Time
}

// NewCreator returns a BaseDirs that lets you create a database summarising
// usage information by base directory, taken from the given quotas.
func NewCreator(dbPath string, quotas *Quotas) (*BaseDirs, error) {
	mp, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	return &BaseDirs{
		dbPath:      dbPath,
		quotas:      quotas,
		ch:          new(codec.BincHandle),
		mountPoints: mp,
		modTime:     time.Now(),
	}, nil
}

// SetMountPoints can be used to manually set your mountpoints, if the automatic
// discovery of mountpoints on your system doesn't work.
func (b *BaseDirs) SetMountPoints(mountpoints []string) {
	b.mountPoints = validateMountPoints(mountpoints)
}

// SetModTime sets the time used for the new History date. Defaults to
// time.Now() if not set.
func (b *BaseDirs) SetModTime(t time.Time) {
	b.modTime = t
}

// SummaryWithChildren contains all of the information for a basedirectory and
// it's direct children.
type SummaryWithChildren struct {
	db.DirSummary
	Children []*SubDir
}

// AgeDirs contains all of the basedirectory information for a particular user
// or group at various time intervals.
type AgeDirs [len(db.DirGUTAges)][]SummaryWithChildren

// IDAgeDirs is a map of all of the user or group base directories at various
// time intervals.
type IDAgeDirs map[uint32]*AgeDirs

// Get retrieves the base directory information for a given user or group ID.
func (i IDAgeDirs) Get(id uint32) *AgeDirs {
	ap := i[id]

	if ap == nil {
		ap = new(AgeDirs)

		i[id] = ap
	}

	return ap
}
