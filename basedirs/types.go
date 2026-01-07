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

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	gBytes = 1024 * 1024 * 1024

	GroupUsageBucket      = "groupUsage"
	GroupHistoricalBucket = "groupHistorical"
	GroupSubDirsBucket    = "groupSubDirs"
	UserUsageBucket       = "userUsage"
	UserSubDirsBucket     = "userSubDirs"
)

// UsageBreakdownByType is a map of file type to total size of files in bytes
// with that type.
type UsageBreakdownByType map[db.DirGUTAFileType]uint64

func (u UsageBreakdownByType) String() string {
	var sb strings.Builder

	types := make([]db.DirGUTAFileType, 0, len(u))
	for ft := range u {
		types = append(types, ft)
	}

	sort.Slice(types, func(i, j int) bool { return types[i] < types[j] })

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

// SummaryWithChildren contains all of the information for a basedirectory and
// its direct children.
type SummaryWithChildren struct {
	db.DirSummary
	Children []*SubDir
}

// AgeDirs contains all of the basedirectory information for a particular user
// or group at various time intervals.
type AgeDirs [len(db.DirGUTAges)][]SummaryWithChildren

// IDAgeDirs is a map of all of the user or group base directories at various
// time intervals.
type IDAgeDirs map[uint32]*AgeDirs

// Get retrieves the base directory information for a given user or group ID.
func (i IDAgeDirs) Get(id uint32) *AgeDirs {
	ap := i[id]
	if ap == nil {
		ap = new(AgeDirs)
		i[id] = ap
	}

	return ap
}

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
