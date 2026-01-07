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
)

// BaseDirs is used to summarise disk usage information by base directory and
// group or user.
type BaseDirs struct {
	store       Store
	quotas      *Quotas
	mountPoints MountPoints
	modTime     time.Time
}

// NewCreator returns a BaseDirs that lets you create a database summarising
// usage information by base directory, taken from the given quotas.
func NewCreator(store Store, quotas *Quotas) (*BaseDirs, error) {
	mp, err := GetMountPoints()
	if err != nil {
		// Some environments (eg. restricted CI containers) may not expose mount
		// information. Fall back to a single root mountpoint.
		mp = ValidateMountPoints([]string{"/"})
	}

	return &BaseDirs{
		store:       store,
		quotas:      quotas,
		mountPoints: mp,
		modTime:     time.Now(),
	}, nil
}

// SetMountPoints can be used to manually set your mountpoints, if the automatic
// discovery of mountpoints on your system doesn't work.
func (b *BaseDirs) SetMountPoints(mountpoints []string) {
	b.mountPoints = ValidateMountPoints(mountpoints)
}

// SetModTime sets the time used for the new History date. Defaults to
// time.Now() if not set.
func (b *BaseDirs) SetModTime(t time.Time) {
	b.modTime = t
}
