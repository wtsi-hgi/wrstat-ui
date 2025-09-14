/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

package server

import (
	"context"
	"errors"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
)

// Backend captures the operations the server needs from a data backend.
//
// Implementations may be backed by Bolt (db.Tree + basedirs.MultiReader)
// or ClickHouse. This allows the server to switch between storage engines.
type Backend interface {
	// Tree-like queries
	DirInfo(dir string, filter *db.Filter) (*db.DirInfo, error)
	DirHasChildren(dir string, filter *db.Filter) bool
	Where(dir string, filter *db.Filter, recurseCount split.SplitFn) (db.DCSs, error)

	// Basedirs queries
	GroupUsage(age db.DirGUTAge) ([]*basedirs.Usage, error)
	UserUsage(age db.DirGUTAge) ([]*basedirs.Usage, error)
	GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error)
	UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error)
	History(gid uint32, basedir string) ([]basedirs.History, error)
}

// NotImplemented is returned by Backend methods that aren't implemented for
// a particular backend yet.
var ErrNotImplemented = errors.New("not implemented")

// ClickHouseBackend is a Backend backed by ClickHouse.
//
// For Phase 0, only the methods needed by tree summaries will be implemented
// later; other methods return NotImplemented for now.
type ClickHouseBackend struct {
	ch *clickhouse.Clickhouse
}

// NewClickHouseBackend creates a new ClickHouse-backed Backend.
// Caller is responsible for managing the ClickHouse connection lifecycle.
func NewClickHouseBackend(ch *clickhouse.Clickhouse) *ClickHouseBackend {
	return &ClickHouseBackend{ch: ch}
}

// DirInfo returns a directory summary and its immediate children summaries.
// Phase 1: Implemented using ClickHouse SubtreeSummary and ChildrenSummaries
func (b *ClickHouseBackend) DirInfo(dir string, filter *db.Filter) (*db.DirInfo, error) {
	// Convert db.Filter to clickhouse.Filters
	chFilters := convertFilters(filter)

	// Use the Gin context to allow for cancellation
	ctx := context.Background()

	// Get the summary for the directory
	sum, err := b.ch.SubtreeSummary(ctx, dir, chFilters)
	if err != nil {
		return nil, err
	}

	// Convert the ClickHouse summary to a db.DirSummary
	current := &db.DirSummary{
		Dir:   dir,
		Count: sum.FileCount,
		Size:  sum.TotalSize,
		Atime: sum.OldestATime,     // Oldest atime matches Bolt semantics
		Mtime: sum.MostRecentMTime, // Most recent mtime matches Bolt semantics
		UIDs:  sum.UIDs,
		GIDs:  sum.GIDs,
		FTs:   convertFTypesToDirGUTAFileTypes(sum.FTypes),
		Age:   db.DirGUTAge(sum.Age),
	}

	// Get the children summaries
	children, err := b.ch.ChildrenSummaries(ctx, dir, chFilters)
	if err != nil {
		return nil, err
	}

	// Convert the ClickHouse child summaries to db.DirSummary slice
	childSummaries := make([]*db.DirSummary, len(children))
	for i, child := range children {
		childSummaries[i] = &db.DirSummary{
			Dir:   child.Path,
			Count: child.Summary.FileCount,
			Size:  child.Summary.TotalSize,
			Atime: child.Summary.OldestATime,
			Mtime: child.Summary.MostRecentMTime,
			UIDs:  child.Summary.UIDs,
			GIDs:  child.Summary.GIDs,
			FTs:   convertFTypesToDirGUTAFileTypes(child.Summary.FTypes),
			Age:   db.DirGUTAge(child.Summary.Age),
		}
	}

	return &db.DirInfo{
		Current:  current,
		Children: childSummaries,
	}, nil
}

// DirHasChildren reports if dir has any child dirs with matching files.
// Phase 1: Implemented to check if a directory has any children
func (b *ClickHouseBackend) DirHasChildren(dir string, filter *db.Filter) bool {
	// Convert db.Filter to clickhouse.Filters
	chFilters := convertFilters(filter)

	// Use the Gin context to allow for cancellation
	ctx := context.Background()

	// Get the children summaries
	children, err := b.ch.ChildrenSummaries(ctx, dir, chFilters)
	if err != nil {
		return false
	}

	// Return true if there are any children
	return len(children) > 0
}

// convertFilters converts a db.Filter to clickhouse.Filters
func convertFilters(filter *db.Filter) clickhouse.Filters {
	if filter == nil {
		return clickhouse.Filters{}
	}

	var aTimeBucket, mTimeBucket string

	// Convert age filter to appropriate bucket
	switch filter.Age {
	case db.DGUTAgeA1M:
		aTimeBucket = ">1m"
	case db.DGUTAgeA2M:
		aTimeBucket = ">2m"
	case db.DGUTAgeA6M:
		aTimeBucket = ">6m"
	case db.DGUTAgeA1Y:
		aTimeBucket = ">1y"
	case db.DGUTAgeA2Y:
		aTimeBucket = ">2y"
	case db.DGUTAgeA3Y:
		aTimeBucket = ">3y"
	case db.DGUTAgeA5Y:
		aTimeBucket = ">5y"
	case db.DGUTAgeA7Y:
		aTimeBucket = ">7y"
	case db.DGUTAgeM1M:
		mTimeBucket = ">1m"
	case db.DGUTAgeM2M:
		mTimeBucket = ">2m"
	case db.DGUTAgeM6M:
		mTimeBucket = ">6m"
	case db.DGUTAgeM1Y:
		mTimeBucket = ">1y"
	case db.DGUTAgeM2Y:
		mTimeBucket = ">2y"
	case db.DGUTAgeM3Y:
		mTimeBucket = ">3y"
	case db.DGUTAgeM5Y:
		mTimeBucket = ">5y"
	case db.DGUTAgeM7Y:
		mTimeBucket = ">7y"
	}

	// Build and return the ClickHouse filters
	return clickhouse.Filters{
		GIDs:        filter.GIDs,
		UIDs:        filter.UIDs,
		Exts:        convertDGUTAFileTypesToExts(filter.FTs),
		ATimeBucket: aTimeBucket,
		MTimeBucket: mTimeBucket,
	}
}

// convertFTypesToDirGUTAFileTypes converts ClickHouse FTypes to db.DirGUTAFileType slice
func convertFTypesToDirGUTAFileTypes(ftypes []uint8) []db.DirGUTAFileType {
	// If no file types, return nil
	if len(ftypes) == 0 {
		return nil
	}

	// Map to db.DirGUTAFileType values
	result := make([]db.DirGUTAFileType, 0, len(ftypes))

	// This is a simplified conversion - we'll need to expand this
	for _, ft := range ftypes {
		// ClickHouse file types to db.DirGUTAFileType conversion logic
		// We'll map based on our understanding of the file types
		var dgft db.DirGUTAFileType

		if ft == 2 { // clickhouse.FileTypeDir (uint8 value is 2)
			dgft = 15 // Value of db.DirGUTAFileTypeDir
			result = append(result, dgft)
		}
		// For non-directory types, we skip as they're handled via extensions
	}

	return result
}

// convertDGUTAFileTypesToExts converts db.DirGUTAFileType slice to extensions for filtering
func convertDGUTAFileTypesToExts(fts []db.DirGUTAFileType) []string {
	if len(fts) == 0 {
		return nil
	}

	// Create a map to avoid duplicates
	extMap := make(map[string]struct{})

	// This mapping should align with extToDGUTA in server/tree.go
	for _, ft := range fts {
		switch ft {
		case 2: // db.DirGUTAFileTypeVCF
			extMap["vcf"] = struct{}{}
		case 3: // db.DirGUTAFileTypeVCFGz
			extMap["vcf.gz"] = struct{}{}
		case 4: // db.DirGUTAFileTypeBCF
			extMap["bcf"] = struct{}{}
		case 5: // db.DirGUTAFileTypeSam
			extMap["sam"] = struct{}{}
		case 6: // db.DirGUTAFileTypeBam
			extMap["bam"] = struct{}{}
		case 7: // db.DirGUTAFileTypeCram
			extMap["cram"] = struct{}{}
		case 8: // db.DirGUTAFileTypeFasta
			extMap["fa"] = struct{}{}
			extMap["fasta"] = struct{}{}
		case 9: // db.DirGUTAFileTypeFastq
			extMap["fastq"] = struct{}{}
			extMap["fq"] = struct{}{}
		case 10: // db.DirGUTAFileTypeFastqGz
			extMap["fastq.gz"] = struct{}{}
			extMap["fq.gz"] = struct{}{}
		case 11: // db.DirGUTAFileTypePedBed
			extMap["ped"] = struct{}{}
			extMap["bed"] = struct{}{}
			extMap["bim"] = struct{}{}
			extMap["fam"] = struct{}{}
			extMap["map"] = struct{}{}
		case 13: // db.DirGUTAFileTypeText
			extMap["csv"] = struct{}{}
			extMap["dat"] = struct{}{}
			extMap["md"] = struct{}{}
			extMap["readme"] = struct{}{}
			extMap["text"] = struct{}{}
			extMap["txt"] = struct{}{}
			extMap["tsv"] = struct{}{}
		case 14: // db.DirGUTAFileTypeLog
			extMap["log"] = struct{}{}
			extMap["err"] = struct{}{}
			extMap["e"] = struct{}{}
			extMap["oe"] = struct{}{}
		case 12: // db.DirGUTAFileTypeCompressed
			extMap["gz"] = struct{}{}
			extMap["bz2"] = struct{}{}
			extMap["xz"] = struct{}{}
			extMap["zip"] = struct{}{}
			extMap["tgz"] = struct{}{}
			extMap["bzip2"] = struct{}{}
			extMap["bgz"] = struct{}{}
			extMap["zst"] = struct{}{}
			extMap["lz4"] = struct{}{}
			extMap["lz"] = struct{}{}
			extMap["br"] = struct{}{}
		}
		// Skip db.DirGUTAFileTypeDir (15) and db.DirGUTAFileTypeTemp (1) - these are handled separately
	}

	// Convert the map to a slice
	exts := make([]string, 0, len(extMap))
	for ext := range extMap {
		exts = append(exts, ext)
	}

	return exts
}

// Where implements where-splits style aggregation.
func (b *ClickHouseBackend) Where(_ string, _ *db.Filter, _ split.SplitFn) (db.DCSs, error) {
	return nil, ErrNotImplemented
}

// GroupUsage returns usage grouped by group.
func (b *ClickHouseBackend) GroupUsage(_ db.DirGUTAge) ([]*basedirs.Usage, error) {
	return nil, ErrNotImplemented
}

// UserUsage returns usage grouped by user.
func (b *ClickHouseBackend) UserUsage(_ db.DirGUTAge) ([]*basedirs.Usage, error) {
	return nil, ErrNotImplemented
}

// GroupSubDirs returns subdirectory usage for a group.
func (b *ClickHouseBackend) GroupSubDirs(_ uint32, _ string, _ db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return nil, ErrNotImplemented
}

// UserSubDirs returns subdirectory usage for a user.
func (b *ClickHouseBackend) UserSubDirs(_ uint32, _ string, _ db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return nil, ErrNotImplemented
}

// History returns historical usage for a group basedir.
func (b *ClickHouseBackend) History(_ uint32, _ string) ([]basedirs.History, error) {
	return nil, ErrNotImplemented
}
