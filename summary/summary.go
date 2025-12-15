/*******************************************************************************
 * Copyright (c) 2021, 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *         Michael Woolnough <mw31@sanger.ac.uk>
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

// package summary lets you summarise file stats.

package summary

import (
	"os/user"
	"strconv"
)

const (
	Range7Years AgeRange = iota
	Range5Years
	Range3Years
	Range2Years
	Range1Year
	Range6Months
	Range2Months
	Range1Month
	RangeLess1Month
)

const (
	month = int64(30 * 24 * 3600)
	year  = int64(365 * 24 * 3600)
)

// Summary holds count and size and lets you accumulate count and size as you
// add more things with a size.
type Summary struct {
	Count int64
	Size  int64
}

// Add will increment our count and add the given size to our size.
func (s *Summary) Add(size int64) {
	s.Count++
	s.Size += size
}

// SummaryWithTimes is like summary, but also holds the reference time, oldest
// atime, newest mtime add()ed.
type SummaryWithTimes struct { //nolint:revive
	Summary
	Atime        int64 // seconds since Unix epoch
	Mtime        int64 // seconds since Unix epoch
	AtimeBuckets AgeBuckets
	MtimeBuckets AgeBuckets
}

// AgeRange represents a time-based bucket index for file ages.
// It is used to index into age bucket arrays.
type AgeRange uint8

// AgeBuckets stores counts per AgeRange.
// The index corresponds directly to the AgeRange constants.
type AgeBuckets [9]uint64

// bucketForAge returns the correct age bucket for a given file age in seconds.
// The checks go from oldest to newest. The first matching range is returned.
// Ages under one month always fall into the "less than 1 month" bucket.
func bucketForAge(ageSeconds int64) AgeRange { //nolint:gocyclo
	switch {
	case ageSeconds >= 7*year:
		return Range7Years
	case ageSeconds >= 5*year:
		return Range5Years
	case ageSeconds >= 3*year:
		return Range3Years
	case ageSeconds >= 2*year:
		return Range2Years
	case ageSeconds >= year:
		return Range1Year
	case ageSeconds >= 6*month:
		return Range6Months
	case ageSeconds >= 2*month:
		return Range2Months
	case ageSeconds >= month:
		return Range1Month
	default:
		return RangeLess1Month
	}
}

// Add will increment our count and add the given size to our size. It also
// stores the given atime if it is older than our current one, and the given
// mtime if it is newer than our current one. It also updates corresponding
// access-time and modified-time buckets based on the file's age.
func (s *SummaryWithTimes) Add(size int64, atime int64, mtime int64, now int64) {
	s.Summary.Add(size)

	s.updateAtime(atime)
	s.updateMtime(mtime)
	s.addAtimeBucket(atime, now)
	s.addMtimeBucket(mtime, now)
}

func (s *SummaryWithTimes) updateAtime(atime int64) {
	if atime > 0 && (s.Atime == 0 || atime < s.Atime) {
		s.Atime = atime
	}
}

func (s *SummaryWithTimes) updateMtime(mtime int64) {
	if mtime > 0 && (s.Mtime == 0 || mtime > s.Mtime) {
		s.Mtime = mtime
	}
}

func (s *SummaryWithTimes) addAtimeBucket(atime int64, now int64) {
	if atime > 0 {
		age := now - atime
		s.AtimeBuckets[bucketForAge(age)]++
	}
}

func (s *SummaryWithTimes) addMtimeBucket(mtime int64, now int64) {
	if mtime > 0 {
		age := now - mtime
		s.MtimeBuckets[bucketForAge(age)]++
	}
}

// AddSummary add the data in the passed SummaryWithTimes to the existing
// SummaryWithTimes. It also merges the access-time and modified-time
// bucket counts from the provided summary.
func (s *SummaryWithTimes) AddSummary(t *SummaryWithTimes) {
	s.Count += t.Count
	s.Size += t.Size

	if t.Atime > 0 && (s.Atime == 0 || t.Atime < s.Atime) {
		s.Atime = t.Atime
	}

	if t.Mtime > 0 && (s.Mtime == 0 || t.Mtime > s.Mtime) {
		s.Mtime = t.Mtime
	}

	for i := range t.AtimeBuckets {
		s.AtimeBuckets[i] += t.AtimeBuckets[i]
		s.MtimeBuckets[i] += t.MtimeBuckets[i]
	}
}

// MostCommonBucket returns the index of the bucket with the highest count.
// If multiple buckets have the same count, the later (higher-index) bucket
// is chosen. This matches the expected tie-breaking behaviour.
func MostCommonBucket(ranges AgeBuckets) AgeRange {
	var bestIdx AgeRange

	bestCount := ranges[0]

	for i := 1; i < len(ranges); i++ {
		if ranges[i] >= bestCount {
			bestIdx = AgeRange(i) //nolint:gosec
			bestCount = ranges[i]
		}
	}

	return bestIdx
}

// GroupUserID is a combined GID and UID.
type GroupUserID uint64

// NewGroupUserID create a new GroupUserID.
func NewGroupUserID(gid, uid uint32) GroupUserID {
	return GroupUserID(gid)<<32 | GroupUserID(uid)
}

// GID returns the GID.
func (g GroupUserID) GID() uint32 {
	return uint32(g >> 32) //nolint:mnd,gosec
}

// UID returns the UID.
func (g GroupUserID) UID() uint32 {
	return uint32(g) //nolint:gosec
}

// GIDToName converts gid to group name, using the given cache to avoid lookups.
func GIDToName(gid uint32, cache map[uint32]string) string {
	return cachedIDToName(gid, cache, getGroupName)
}

// UIDToName converts uid to username, using the given cache to avoid lookups.
func UIDToName(uid uint32, cache map[uint32]string) string {
	return cachedIDToName(uid, cache, getUserName)
}

func cachedIDToName(id uint32, cache map[uint32]string, lookup func(uint32) string) string {
	if name, ok := cache[id]; ok {
		return name
	}

	name := lookup(id)

	cache[id] = name

	return name
}

// getGroupName returns the name of the group given gid. If the lookup fails,
// returns "idxxx", where xxx is the given id as a string.
func getGroupName(id uint32) string {
	sid := strconv.Itoa(int(id))

	g, err := user.LookupGroupId(sid)
	if err != nil {
		return "id" + sid
	}

	return g.Name
}

// getUserName returns the username of the given uid. If the lookup fails,
// returns "idxxx", where xxx is the given id as a string.
func getUserName(id uint32) string {
	sid := strconv.Itoa(int(id))

	u, err := user.LookupId(sid)
	if err != nil {
		return "id" + sid
	}

	return u.Username
}
