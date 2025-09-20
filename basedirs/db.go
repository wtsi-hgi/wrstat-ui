/*
*******************************************************************************
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
*******************************************************************************
 */

package basedirs

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	bucketKeySeparator     = "-"
	bucketKeySeparatorByte = '-'
	gBytes                 = 1024 * 1024 * 1024

	GroupUsageBucket      = "groupUsage"
	GroupHistoricalBucket = "groupHistorical"
	GroupSubDirsBucket    = "groupSubDirs"
	UserUsageBucket       = "userUsage"
	UserSubDirsBucket     = "userSubDirs"

	sizeOfUint32         = 4
	sizeOfUint16         = 2
	sizeOfKeyWithoutPath = sizeOfUint32 + sizeOfUint16 + 2
)

var bucketKeySeparatorByteSlice = []byte{bucketKeySeparatorByte} //nolint:gochecknoglobals

// Usage holds information summarising usage by a particular GID/UID-BaseDir.
//
// Only one of GID or UID will be set, and Owner will always be blank when UID
// is set. If GID is set, then UIDs will be set, showing which users own files
// in the BaseDir. If UID is set, then GIDs will be set, showing which groups
// own files in the BaseDir.
type Usage struct {
	GID         uint32
	UID         uint32
	GIDs        []uint32
	UIDs        []uint32
	Name        string // the group or user name
	Owner       string
	BaseDir     string
	UsageSize   uint64
	QuotaSize   uint64
	UsageInodes uint64
	QuotaInodes uint64
	Mtime       time.Time
	// DateNoSpace is an estimate of when there will be no space quota left.
	DateNoSpace time.Time
	// DateNoFiles is an estimate of when there will be no inode quota left.
	DateNoFiles time.Time
	Age         db.DirGUTAge
}

// Output creates a database containing usage information for each of
// our groups and users by calculated base directory.
func (b *BaseDirs) Output(users, groups IDAgeDirs) error {
	if err := b.store.Update(b.updateDatabase(users, groups)); err != nil {
		return err
	}
	return b.store.Update(b.storeDateQuotasFill())
}

func (b *BaseDirs) updateDatabase(users, groups IDAgeDirs) func(Writer) error {
	return func(w Writer) error {
		if errc := b.calculateUsage(w, groups, users); errc != nil {
			return errc
		}
		if errc := b.updateHistories(w, groups); errc != nil {
			return errc
		}
		return b.calculateSubDirUsage(w, groups, users)
	}
}

func (b *BaseDirs) calculateUsage(w Writer, gidBase IDAgeDirs, uidBase IDAgeDirs) error {
	if errc := b.storeGIDBaseDirs(w, gidBase); errc != nil {
		return errc
	}

	return b.storeUIDBaseDirs(w, uidBase)
}

func (b *BaseDirs) storeGIDBaseDirs(w Writer, gidBase IDAgeDirs) error { //nolint:gocognit
	for gid, dcss := range gidBase {
		for _, adcs := range dcss {
			for _, dcs := range adcs {
				quotaSize, quotaInode := b.quotas.Get(gid, dcs.Dir)
				uwm := &Usage{
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
				if err := w.PutGroupUsage(uwm); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (b *BaseDirs) storeUIDBaseDirs(w Writer, uidBase IDAgeDirs) error { //nolint:gocognit

	for uid, dcss := range uidBase {
		for _, adcs := range dcss {
			for _, dcs := range adcs {
				uwm := &Usage{
					UID:         uid,
					GIDs:        dcs.GIDs,
					BaseDir:     dcs.Dir,
					UsageSize:   dcs.Size,
					UsageInodes: dcs.Count,
					Mtime:       dcs.Mtime,
					Age:         dcs.Age,
				}

				if err := w.PutUserUsage(uwm); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *BaseDirs) updateHistories(w Writer, gidBase IDAgeDirs) error {

	gidMounts := b.gidsToMountpoints(gidBase)

	for gid, mounts := range gidMounts {
		if err := b.updateGroupHistories(w, gid, mounts); err != nil {
			return err
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
		mp := b.mountPoints.prefixOf(dcs.Dir)
		if mp == "" {
			continue
		}

		ds := mounts[mp]

		ds.Count += dcs.Count
		ds.Size += dcs.Size

		mounts[mp] = ds
	}

	return mounts
}

func (b *BaseDirs) updateGroupHistories(w Writer, gid uint32,
	mounts map[string]db.DirSummary,
) error {
	for mount, ds := range mounts {
		quotaSize, quotaInode := b.quotas.Get(gid, mount)

		// Merge new history with existing: append only if newer than last entry
		existing, err := w.History(gid, mount)
		if err != nil {
			return err
		}
		if len(existing) > 0 && !b.modTime.After(existing[len(existing)-1].Date) {
			// existing history already up-to-date for this timestamp or newer; skip
			continue
		}
		updated := append(existing, History{
			Date:        b.modTime,
			UsageSize:   ds.Size,
			UsageInodes: ds.Count,
			QuotaSize:   quotaSize,
			QuotaInodes: quotaInode,
		})
		if err = w.PutHistory(gid, mount, updated); err != nil {
			return err
		}
	}

	return nil
}

// updateHistory and decode/encode helpers removed; storage encoding is handled by the backend adapter.

// UsageBreakdownByType is a map of file type to total size of files in bytes
// with that type.
type UsageBreakdownByType map[db.DirGUTAFileType]uint64

func (u UsageBreakdownByType) String() string {
	var sb strings.Builder

	types := make([]db.DirGUTAFileType, 0, len(u))

	for ft := range u {
		types = append(types, ft)
	}

	sort.Slice(types, func(i, j int) bool {
		return types[i] < types[j]
	})

	for n, ft := range types {
		if n > 0 {
			sb.WriteByte(' ')
		}

		fmt.Fprintf(&sb, "%s: %.2f", ft, float64(u[ft])/gBytes)
	}

	return sb.String()
}

// SubDir contains information about a sub-directory of a base directory.
type SubDir struct {
	SubDir       string
	NumFiles     uint64
	SizeFiles    uint64
	LastModified time.Time
	FileUsage    UsageBreakdownByType
}

func (b *BaseDirs) calculateSubDirUsage(w Writer, gidBase, uidBase IDAgeDirs) error {
	if errc := b.storeGIDSubDirs(w, gidBase); errc != nil {
		return errc
	}

	return b.storeUIDSubDirs(w, uidBase)
}

func (b *BaseDirs) storeGIDSubDirs(w Writer, gidBase IDAgeDirs) error { //nolint:gocognit

	for gid, dcss := range gidBase {
		for _, adcs := range dcss {
			for _, dcs := range adcs {
				if err := b.storeSubDirs(w, GroupSubDirsBucket, gid, dcs); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *BaseDirs) storeSubDirs(w Writer, bucket string, id uint32, dcs SummaryWithChildren) error {
	if bucket == GroupSubDirsBucket {
		return w.PutGroupSubDirs(id, dcs.Dir, uint16(dcs.Age), dcs.Children)
	}
	return w.PutUserSubDirs(id, dcs.Dir, uint16(dcs.Age), dcs.Children)
}

func (b *BaseDirs) storeUIDSubDirs(w Writer, uidBase IDAgeDirs) error { //nolint:gocognit

	for uid, dcss := range uidBase {
		for _, adcs := range dcss {
			for _, dcs := range adcs {
				if err := b.storeSubDirs(w, UserSubDirsBucket, uid, dcs); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// storeDateQuotasFill goes through all our stored group usage and histories and
// stores the date quota will be full on the group Usage.
//
// This needs to be pre-calculated and stored in the db because it's too slow to
// do for all group-basedirs every time the reader gets all of them.
//
// This is done as a separate transaction to updateDatabase() so we have access
// to the latest stored history, without having to have all histories in memory.
func (b *BaseDirs) storeDateQuotasFill() func(Writer) error {
	return func(w Writer) error {
		return w.ForEachGroupUsage(func(gu *Usage) error {
			if gu.Age != db.DGUTAgeAll {
				return nil
			}
			mp := b.mountPoints.prefixOf(gu.BaseDir)
			if mp == "" {
				return nil
			}
			h, err := w.History(gu.GID, mp)
			if err != nil {
				return err
			}
			sizeExceedDate, inodeExceedDate := DateQuotaFull(h)
			gu.DateNoSpace = sizeExceedDate
			gu.DateNoFiles = inodeExceedDate
			return w.PutGroupUsage(gu)
		})
	}
}

// mustDecodeHistories removed.

// Merge and open-helper functions are backend-specific and have been removed from this
// backend-agnostic package. Implementations should live alongside the backend adapters.
