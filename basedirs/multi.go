/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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
	"errors"

	"github.com/hashicorp/go-multierror"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

type MultiReader []*BaseDirReader

// OpenMulti opens a BaseDirReader for each path specified.
func OpenMulti(ownersPath string, paths ...string) (MultiReader, error) { //nolint:funlen
	mp, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	owners, err := parseOwners(ownersPath)
	if err != nil {
		return nil, err
	}

	mr := make(MultiReader, len(paths))
	ch := new(codec.BincHandle)
	groupCache, userCache := NewGroupCache(), NewUserCache()

	for n, path := range paths {
		db, err := OpenDBRO(path)
		if err != nil {
			return nil, err
		}

		mr[n] = &BaseDirReader{
			db:          db,
			ch:          ch,
			mountPoints: mp,
			groupCache:  groupCache,
			userCache:   userCache,
			owners:      owners,
		}
	}

	return mr, nil
}

// Close closes each database.
func (m MultiReader) Close() (err error) {
	for _, r := range m {
		if errr := r.Close(); err != nil {
			err = multierror.Append(err, errr)
		}
	}

	return err
}

// GroupUsage returns the usage for every GID-BaseDir combination in the
// databases.
func (m MultiReader) GroupUsage(age db.DirGUTAge) ([]*Usage, error) {
	return m.usage(GroupUsageBucket, age)
}

func (m MultiReader) usage(bucket string, age db.DirGUTAge) ([]*Usage, error) {
	var usage []*Usage

	for _, r := range m {
		u, err := r.usage(bucket, age)
		if err != nil {
			return nil, err
		}

		usage = append(usage, u...)
	}

	return usage, nil
}

// UserUsage returns the usage for every UID-BaseDir combination in the
// databases.
func (m MultiReader) UserUsage(age db.DirGUTAge) ([]*Usage, error) {
	return m.usage(UserUsageBucket, age)
}

// GroupSubDirs returns a slice of SubDir, one for each subdirectory of the
// given basedir, owned by the given group. If basedir directly contains files,
// one of the SubDirs will be for ".".
func (m MultiReader) GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*SubDir, error) {
	return m.subDirs(GroupSubDirsBucket, gid, basedir, age)
}

func (m MultiReader) subDirs(bucket string, id uint32, basedir string, age db.DirGUTAge) ([]*SubDir, error) {
	for _, r := range m {
		s, err := r.subDirs(bucket, id, basedir, age)
		if err != nil {
			return nil, err
		} else if s != nil {
			return s, nil
		}
	}

	return nil, nil
}

// UserSubDirs returns a slice of SubDir, one for each subdirectory of the
// given basedir, owned by the given user. If basedir directly contains files,
// one of the SubDirs will be for ".".
func (m MultiReader) UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*SubDir, error) {
	return m.subDirs(UserSubDirsBucket, uid, basedir, age)
}

// SetMountPoints can be used to manually set your mountpoints, if the automatic
// discovery of mountpoints on your system doesn't work.
func (m MultiReader) SetMountPoints(mountpoints []string) {
	for _, r := range m {
		r.mountPoints = mountpoints
	}
}

// History returns a slice of History values for the given gid and path, one
// value per Date the information was calculated.
func (m MultiReader) History(gid uint32, path string) ([]History, error) {
	for _, r := range m {
		h, err := r.History(gid, path)

		switch {
		case errors.Is(err, ErrNoBaseDirHistory):
		case err != nil:
			return nil, err
		case h != nil:
			return h, nil
		}
	}

	return nil, ErrNoBaseDirHistory
}

// SetCachedGroup sets the name of a specified GID.
func (m MultiReader) SetCachedGroup(gid uint32, name string) {
	if len(m) > 0 {
		m[0].SetCachedGroup(gid, name)
	}
}

// SetCachedUser sets the name of a specified UID.
func (m MultiReader) SetCachedUser(uid uint32, name string) {
	if len(m) > 0 {
		m[0].SetCachedUser(uid, name)
	}
}
