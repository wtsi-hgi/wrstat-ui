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

package basedirs

import "github.com/wtsi-hgi/wrstat-ui/db"

func forEachSummaryWithChildren(dcss *AgeDirs, fn func(dcs SummaryWithChildren) error) error {
	if dcss == nil {
		return nil
	}

	for _, adcs := range dcss {
		for i := range adcs {
			if err := fn(adcs[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

// Output persists a database containing usage information for each of our
// groups and users by calculated base directory.
func (b *BaseDirs) Output(users, groups IDAgeDirs) error {
	if b.store == nil {
		return Error("store not set")
	}

	steps := []func() error{
		b.store.Reset,
		func() error { return b.storeGroupUsage(groups) },
		func() error { return b.storeUserUsage(users) },
		func() error { return b.storeGroupHistories(groups) },
		func() error { return b.storeGroupSubDirs(groups) },
		func() error { return b.storeUserSubDirs(users) },
		b.store.Finalise,
	}

	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}

	return nil
}

func (b *BaseDirs) storeGroupUsage(gidBase IDAgeDirs) error {
	for gid, dcss := range gidBase {
		if err := forEachSummaryWithChildren(dcss, func(dcs SummaryWithChildren) error {
			quotaSize, quotaInode := b.quotas.Get(gid, dcs.Dir)

			u := &Usage{
				GID:         gid,
				UIDs:        dcs.UIDs,
				BaseDir:     dcs.Dir,
				UsageSize:   dcs.Size,
				QuotaSize:   quotaSize,
				UsageInodes: dcs.Count,
				QuotaInodes: quotaInode,
				Mtime:       dcs.Mtime,
				Age:         dcs.Age,
			}

			return b.store.PutGroupUsage(u)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (b *BaseDirs) storeUserUsage(uidBase IDAgeDirs) error {
	for uid, dcss := range uidBase {
		if err := forEachSummaryWithChildren(dcss, func(dcs SummaryWithChildren) error {
			u := &Usage{
				UID:         uid,
				GIDs:        dcs.GIDs,
				BaseDir:     dcs.Dir,
				UsageSize:   dcs.Size,
				UsageInodes: dcs.Count,
				Mtime:       dcs.Mtime,
				Age:         dcs.Age,
			}

			return b.store.PutUserUsage(u)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (b *BaseDirs) storeGroupHistories(gidBase IDAgeDirs) error {
	gidMounts := b.gidsToMountpoints(gidBase)

	for gid, mounts := range gidMounts {
		for mount, ds := range mounts {
			quotaSize, quotaInode := b.quotas.Get(gid, mount)

			key := HistoryKey{GID: gid, MountPath: mount}
			point := History{
				Date:        b.modTime,
				UsageSize:   ds.Size,
				UsageInodes: ds.Count,
				QuotaSize:   quotaSize,
				QuotaInodes: quotaInode,
			}

			if err := b.store.AppendGroupHistory(key, point); err != nil {
				return err
			}
		}
	}

	return nil
}

type gidMountsMap map[uint32]map[string]db.DirSummary

func (b *BaseDirs) gidsToMountpoints(gidBase IDAgeDirs) gidMountsMap {
	gidMounts := make(gidMountsMap, len(gidBase))

	for gid, dcss := range gidBase {
		gidMounts[gid] = b.dcssToMountPoints(dcss)
	}

	return gidMounts
}

func (b *BaseDirs) dcssToMountPoints(dcss *AgeDirs) map[string]db.DirSummary {
	mounts := make(map[string]db.DirSummary)

	for _, dcs := range dcss[0] {
		mp := b.mountPoints.PrefixOf(dcs.Dir)
		if mp == "" {
			continue
		}

		ds := mounts[mp]
		ds.Count += dcs.Count

		ds.Size += dcs.Size
		if dcs.Modtime.After(ds.Modtime) {
			ds.Modtime = dcs.Modtime
		}

		mounts[mp] = ds
	}

	return mounts
}

func (b *BaseDirs) storeGroupSubDirs(gidBase IDAgeDirs) error {
	for gid, dcss := range gidBase {
		if err := forEachSummaryWithChildren(dcss, func(dcs SummaryWithChildren) error {
			key := SubDirKey{ID: gid, BaseDir: dcs.Dir, Age: dcs.Age}

			return b.store.PutGroupSubDirs(key, dcs.Children)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (b *BaseDirs) storeUserSubDirs(uidBase IDAgeDirs) error {
	for uid, dcss := range uidBase {
		if err := forEachSummaryWithChildren(dcss, func(dcs SummaryWithChildren) error {
			key := SubDirKey{ID: uid, BaseDir: dcs.Dir, Age: dcs.Age}

			return b.store.PutUserSubDirs(key, dcs.Children)
		}); err != nil {
			return err
		}
	}

	return nil
}
