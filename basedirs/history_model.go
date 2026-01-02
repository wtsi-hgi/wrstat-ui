package basedirs

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/moby/sys/mountinfo"
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
	case 1, 2:
		oldest = history[0]
	default:
		oldest = history[len(history)-3]
	}

	latest := history[len(history)-1]

	untilSize := calculateTrend(latest.QuotaSize, latest.Date, oldest.Date, latest.UsageSize, oldest.UsageSize)
	untilInodes := calculateTrend(latest.QuotaInodes, latest.Date, oldest.Date, latest.UsageInodes, oldest.UsageInodes)

	return untilSize, untilInodes
}

func calculateTrend(maxV uint64, latestTime, oldestTime time.Time, latestValue, oldestValue uint64) time.Time {
	if latestValue >= maxV {
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

	secs := (float64(maxV) - c) * dt / dy

	t := time.Unix(int64(secs), 0)

	if t.After(time.Now().Add(fiveYearsInHours * time.Hour)) {
		return time.Time{}
	}

	return t
}

// MountPoints is a list of mountpoint paths, sorted by descending length.
// Paths are normalized to end with '/'.
type MountPoints []string

// ValidateMountPoints normalizes mountpoints and sorts them for longest-prefix matching.
func ValidateMountPoints(mountpoints []string) MountPoints {
	mps := make(MountPoints, len(mountpoints))

	for n, mp := range mountpoints {
		if !strings.HasSuffix(mp, "/") {
			mp += "/"
		}

		mps[n] = mp
	}

	sort.Slice(mps, func(i, j int) bool { return len(mps[i]) > len(mps[j]) })

	return mps
}

// GetMountPoints returns mountpoints auto-discovered from the OS.
func GetMountPoints() (MountPoints, error) {
	mounts, err := mountinfo.GetMounts(nil)
	if err != nil {
		return nil, err
	}

	mountList := make(MountPoints, len(mounts))

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

// PrefixOf returns the longest mountpoint that prefixes basedir.
func (m MountPoints) PrefixOf(basedir string) string {
	if !strings.HasSuffix(basedir, "/") {
		basedir += "/"
	}

	for _, mount := range m {
		if strings.HasPrefix(basedir, mount) {
			return mount
		}
	}

	return ""
}
