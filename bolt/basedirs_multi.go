/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

package bolt

import (
	"errors"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

type multiBaseDirsReader []basedirs.Reader

func (m multiBaseDirsReader) GroupUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	return m.usage(func(r basedirs.Reader) ([]*basedirs.Usage, error) { return r.GroupUsage(age) })
}

func (m multiBaseDirsReader) UserUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	return m.usage(func(r basedirs.Reader) ([]*basedirs.Usage, error) { return r.UserUsage(age) })
}

func (m multiBaseDirsReader) usage(fn func(basedirs.Reader) ([]*basedirs.Usage, error)) ([]*basedirs.Usage, error) {
	var (
		out  []*basedirs.Usage
		merr error
	)

	for _, r := range m {
		vals, err := fn(r)
		if err != nil {
			merr = multierror.Append(merr, err)

			continue
		}

		out = append(out, vals...)
	}

	return out, merr
}

func (m multiBaseDirsReader) GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return m.subDirs(func(r basedirs.Reader) ([]*basedirs.SubDir, error) { return r.GroupSubDirs(gid, basedir, age) })
}

func (m multiBaseDirsReader) UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return m.subDirs(func(r basedirs.Reader) ([]*basedirs.SubDir, error) { return r.UserSubDirs(uid, basedir, age) })
}

func (m multiBaseDirsReader) subDirs(fn func(basedirs.Reader) ([]*basedirs.SubDir, error)) ([]*basedirs.SubDir, error) {
	for _, r := range m {
		vals, err := fn(r)
		if err == nil {
			return vals, nil
		}

		if errors.Is(err, basedirs.ErrNoSuchUserOrGroup) {
			continue
		}

		return nil, err
	}

	return nil, nil
}

func (m multiBaseDirsReader) History(gid uint32, path string) ([]basedirs.History, error) {
	for _, r := range m {
		h, err := r.History(gid, path)
		if err == nil {
			return h, nil
		}

		if errors.Is(err, basedirs.ErrNoBaseDirHistory) {
			continue
		}

		return nil, err
	}

	return nil, basedirs.ErrNoBaseDirHistory
}

func (m multiBaseDirsReader) SetMountPoints(mountpoints []string) {
	for _, r := range m {
		r.SetMountPoints(mountpoints)
	}
}

func (m multiBaseDirsReader) SetCachedGroup(gid uint32, name string) {
	for _, r := range m {
		r.SetCachedGroup(gid, name)
	}
}

func (m multiBaseDirsReader) SetCachedUser(uid uint32, name string) {
	for _, r := range m {
		r.SetCachedUser(uid, name)
	}
}

func (m multiBaseDirsReader) MountTimestamps() (map[string]time.Time, error) {
	out := make(map[string]time.Time)

	var merr error

	for _, r := range m {
		ts, err := r.MountTimestamps()
		if err != nil {
			merr = multierror.Append(merr, err)

			continue
		}

		for k, v := range ts {
			if v.After(out[k]) {
				out[k] = v
			}
		}
	}

	return out, merr
}

func (m multiBaseDirsReader) Info() (*basedirs.DBInfo, error) {
	out := new(basedirs.DBInfo)

	var merr error

	for _, r := range m {
		info, err := r.Info()
		if err != nil {
			merr = multierror.Append(merr, err)

			continue
		}

		if info == nil {
			continue
		}

		out.GroupDirCombos += info.GroupDirCombos
		out.GroupMountCombos += info.GroupMountCombos
		out.GroupHistories += info.GroupHistories
		out.GroupSubDirCombos += info.GroupSubDirCombos
		out.GroupSubDirs += info.GroupSubDirs
		out.UserDirCombos += info.UserDirCombos
		out.UserSubDirCombos += info.UserSubDirCombos
		out.UserSubDirs += info.UserSubDirs
	}

	return out, merr
}

func (m multiBaseDirsReader) Close() (err error) {
	for _, r := range m {
		if errr := r.Close(); errr != nil {
			err = multierror.Append(err, errr)
		}
	}

	return err
}
