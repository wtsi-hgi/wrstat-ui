/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
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

package basedirs

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/moby/sys/mountinfo"
	db "github.com/wtsi-hgi/wrstat-ui/db"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrInvalidBasePath  = errors.New("invalid base path")
	ErrNoBaseDirHistory = errors.New("no base dir history found")
)

const fiveYearsInHours = 24 * 365 * 5

// History contains actual usage and quota max information for a particular
// point in time.
type History struct {
	Date        time.Time
	UsageSize   uint64
	QuotaSize   uint64
	UsageInodes uint64
	QuotaInodes uint64
}

// History returns a slice of History values for the given gid and path, one
// value per Date the information was calculated.
func (b *BaseDirReader) History(gid uint32, path string) ([]History, error) {
	mp := b.mountPoints.prefixOf(path)
	if mp == "" {
		return nil, ErrInvalidBasePath
	}

	var history []History

	if err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(groupHistoricalBucket))
		key := historyKey(gid, mp)

		data := bucket.Get(key)
		if data == nil {
			return ErrNoBaseDirHistory
		}

		return b.decodeFromBytes(data, &history)
	}); err != nil {
		return nil, err
	}

	return history, nil
}

func historyKey(gid uint32, mountPoint string) []byte {
	return keyName(gid, mountPoint, db.DGUTAgeAll)
}

type mountPoints []string

// SetMountPoints can be used to manually set your mountpoints, if the automatic
// discovery of mountpoints on your system doesn't work.
func (b *BaseDirReader) SetMountPoints(mountpoints []string) {
	b.mountPoints = mountpoints
}

func getMountPoints() (mountPoints, error) {
	mounts, err := mountinfo.GetMounts(nil)
	if err != nil {
		return nil, err
	}

	mountList := make(mountPoints, len(mounts))

	for n, mp := range mounts {
		if !strings.HasSuffix(mp.Mountpoint, "/") {
			mp.Mountpoint += "/"
		}

		mountList[n] = mp.Mountpoint
	}

	sort.Slice(mountList, func(i, j int) bool {
		return len(mountList[i]) > len(mountList[j])
	})

	return mountList, nil
}

func (m mountPoints) prefixOf(basedir string) string {
	for _, mount := range m {
		if strings.HasPrefix(basedir, mount) {
			return mount
		}
	}

	return ""
}

// DateQuotaFull returns our estimate of when the quota will fill based on the
// history of usage over time. Returns date when size full, and date when inodes
// full.
//
// Returns a zero time value if the estimate is infinite.
func DateQuotaFull(history []History) (time.Time, time.Time) {
	var oldest History

	switch len(history) {
	case 0:
		return time.Time{}, time.Time{}
	case 1, 2: //nolint:gomnd
		oldest = history[0]
	default:
		oldest = history[len(history)-3]
	}

	latest := history[len(history)-1]

	untilSize := calculateTrend(latest.QuotaSize, latest.Date, oldest.Date, latest.UsageSize, oldest.UsageSize)
	untilInodes := calculateTrend(latest.QuotaInodes, latest.Date, oldest.Date, latest.UsageInodes, oldest.UsageInodes)

	return untilSize, untilInodes
}

func calculateTrend(max uint64, latestTime, oldestTime time.Time, latestValue, oldestValue uint64) time.Time {
	if latestValue >= max {
		return latestTime
	}

	if latestTime.Equal(oldestTime) || latestValue <= oldestValue {
		return time.Time{}
	}

	latestSecs := float64(latestTime.Unix())
	oldestSecs := float64(oldestTime.Unix())

	dt := latestSecs - oldestSecs

	dy := float64(latestValue - oldestValue)

	c := float64(latestValue) - latestSecs*dy/dt

	secs := (float64(max) - c) * dt / dy

	t := time.Unix(int64(secs), 0)

	if t.After(time.Now().Add(fiveYearsInHours * time.Hour)) {
		return time.Time{}
	}

	return t
}
