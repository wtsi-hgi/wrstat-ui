/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package db

import (
	"sort"
	"time"

	"golang.org/x/exp/constraints"
	"vimagination.zapto.org/byteio"
)

// GUTA handles group,user,type,age,count,size information.
type GUTA struct {
	GID        uint32
	UID        uint32
	FT         DirGUTAFileType
	Age        DirGUTAge
	Count      uint64
	Size       uint64
	Atime      int64 // seconds since Unix epoch
	Mtime      int64 // seconds since Unix epoch
	updateTime time.Time
}

func (g GUTA) writeTo(w byteio.StickyEndianWriter) {
	w.WriteUintX(uint64(g.GID))
	w.WriteUintX(uint64(g.UID))
	w.WriteUint16(uint16(g.FT))
	w.WriteUint8(uint8(g.Age))
	w.WriteUintX(g.Count)
	w.WriteUintX(g.Size)
	w.WriteIntX(g.Atime)
	w.WriteIntX(g.Mtime)
}

func (g *GUTA) readFrom(r byteio.StickyEndianReader) {
	g.GID = uint32(r.ReadUintX()) //nolint:gosec
	g.UID = uint32(r.ReadUintX()) //nolint:gosec
	g.FT = DirGUTAFileType(r.ReadUint16())
	g.Age = DirGUTAge(r.ReadUint8())
	g.Count = r.ReadUintX()
	g.Size = r.ReadUintX()
	g.Atime = r.ReadIntX()
	g.Mtime = r.ReadIntX()
}

// Filter can be applied to a GUTA to see if it has one of the specified GIDs,
// UIDs and FTs or has the specified Age, in which case it passes the filter.
//
// If the Filter has one of those properties set to nil, or the whole Filter is
// nil, a GUTA will be considered to pass the filter.
//
// The exeception to this is when FTs != []{DGUTFileTypeTemp}, and the GUTA has
// an FT of DGUTAFileTypeTemp. A GUTA for a temporary file will always fail to
// pass the filter unless filtering specifically for temporary files, because
// other GUTA objects will represent the same file on disk but with another file
// type, and you won't want to double-count.
type Filter struct {
	GIDs []uint32
	UIDs []uint32
	FT   DirGUTAFileType
	Age  DirGUTAge
}

// PassesFilter checks to see if this GUTA has a GID in the filter's GIDs
// (considered true if GIDs is nil), and has a UID in the filter's UIDs
// (considered true if UIDs is nil), and an Age the same as the filter's Age,
// and has an FT in the filter's FTs (considered true if FTs is nil).
func (g *GUTA) PassesFilter(filter *Filter) bool {
	if !g.passesGIDFilter(filter) || !g.passesUIDFilter(filter) || !g.passesAgeFilter(filter) {
		return false
	}

	return g.passesFTFilter(filter)
}

// passesGIDFilter tells you if our GID is in the filter's GIDs. Also returns
// true if filter or filter.GIDs in nil.
func (g *GUTA) passesGIDFilter(filter *Filter) bool {
	if filter == nil || filter.GIDs == nil {
		return true
	}

	for _, gid := range filter.GIDs {
		if gid == g.GID {
			return true
		}
	}

	return false
}

// passesUIDFilter tells you if our UID is in the filter's UIDs. Also returns
// true if filter or filter.UIDs in nil.
func (g *GUTA) passesUIDFilter(filter *Filter) bool {
	if filter == nil || filter.UIDs == nil {
		return true
	}

	for _, uid := range filter.UIDs {
		if uid == g.UID {
			return true
		}
	}

	return false
}

// passesFTFilter returns true if this GUTA's file type matches the filter's file types.
func (g *GUTA) passesFTFilter(filter *Filter) bool {
	if filter == nil || filter.FT == 0 {
		return true
	}

	return g.FT&filter.FT > 0
}

// passesAgeFilter tells you if our age is the same as the filter's Age. Also
// returns true if filter is nil.
func (g *GUTA) passesAgeFilter(filter *Filter) bool {
	if filter == nil {
		return true
	}

	return filter.Age == g.Age
}

// GUTAs is a slice of *GUTA, offering ways to filter and summarise the
// information in our *GUTAs.
type GUTAs []*GUTA

func (g GUTAs) writeTo(w byteio.StickyEndianWriter) {
	w.WriteUintX(uint64(len(g)))

	for _, guta := range g {
		guta.writeTo(w)
	}
}

func (g *GUTAs) readFrom(r byteio.StickyEndianReader) {
	*g = make(GUTAs, r.ReadUintX())

	for n := range *g {
		(*g)[n] = new(GUTA)

		(*g)[n].readFrom(r)
	}
}

// Summary sums the count and size of all our GUTA elements and returns the
// results, along with the oldest atime and newset mtime (in seconds since Unix
// epoch) and lists of the unique UIDs, GIDs and FTs in our GUTA elements.
//
// Provide a Filter to ignore GUTA elements that do not match one of the
// specified GIDs, one of the UIDs, one of the FTs, and the specified Age. If
// one of those properties is nil, does not filter on that property.
//
// Provide nil to do no filtering, but providing Age: summary.DGUTAgeAll is
// recommended.
//
// Note that FT 1 is "temp" files, and because a file can be both temporary and
// another type, if your Filter's FTs slice doesn't contain just
// DGUTAFileTypeTemp, any GUTA with FT DGUTAFileTypeTemp is always ignored. (But
// the FTs list will still indicate if you had temp files that passed other
// filters.)
func (g GUTAs) Summary(filter *Filter) *DirSummary { //nolint:funlen
	var (
		count, size  uint64
		atime, mtime int64
		updateTime   time.Time
		age          DirGUTAge
	)

	if filter != nil {
		age = filter.Age
	}

	uniqueUIDs := make(map[uint32]bool)
	uniqueGIDs := make(map[uint32]bool)

	var fileType DirGUTAFileType

	for _, guta := range g {
		passes := guta.PassesFilter(filter)
		if !passes {
			continue
		}

		fileType |= guta.FT

		addGUTAToSummary(guta, &count, &size, &atime, &mtime, &updateTime, uniqueUIDs, uniqueGIDs)
	}

	if count == 0 {
		return nil
	}

	return &DirSummary{
		Count: count,
		Size:  size,
		Atime: time.Unix(atime, 0),
		Mtime: time.Unix(mtime, 0),
		UIDs:  boolMapToSortedKeys(uniqueUIDs),
		GIDs:  boolMapToSortedKeys(uniqueGIDs),
		FT:    fileType,
		Age:   age,
	}
}

// addGUTAToSummary alters the incoming arg summary values based on the gut.
func addGUTAToSummary(guta *GUTA, count, size *uint64, atime, mtime *int64,
	updateTime *time.Time, uniqueUIDs, uniqueGIDs map[uint32]bool) {
	*count += guta.Count
	*size += guta.Size

	if (*atime == 0 || guta.Atime < *atime) && guta.Atime != 0 {
		*atime = guta.Atime
	}

	if *mtime == 0 || guta.Mtime > *mtime {
		*mtime = guta.Mtime
	}

	if guta.updateTime.After(*updateTime) {
		*updateTime = guta.updateTime
	}

	uniqueUIDs[guta.UID] = true
	uniqueGIDs[guta.GID] = true
}

// boolMapToSortedKeys returns a sorted slice of the given keys.
func boolMapToSortedKeys[T constraints.Ordered](m map[T]bool) []T {
	keys := make([]T, len(m))
	i := 0

	for key := range m {
		keys[i] = key
		i++
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	return keys
}
