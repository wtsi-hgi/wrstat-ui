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
	var sawNoSuch bool

	for _, r := range m {
		vals, err := fn(r)
		if err == nil {
			return vals, nil
		}

		if errors.Is(err, basedirs.ErrNoSuchUserOrGroup) {
			sawNoSuch = true

			continue
		}

		return nil, err
	}

	// If none of the sources have an entry for this (id, basedir, age), treat it
	// as "not found" rather than an error.
	if sawNoSuch {
		return nil, nil
	}

	return nil, nil
}

func (m multiBaseDirsReader) History(gid uint32, path string) ([]basedirs.History, error) {
	var sawNoHistory bool

	for _, r := range m {
		h, err := r.History(gid, path)
		if err == nil {
			return h, nil
		}

		if errors.Is(err, basedirs.ErrNoBaseDirHistory) {
			sawNoHistory = true

			continue
		}

		return nil, err
	}

	if sawNoHistory {
		return nil, basedirs.ErrNoBaseDirHistory
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
