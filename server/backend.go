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

// extMapping maps db.DirGUTAFileType to file extensions used for filtering.
// getExtMapping returns the extension mapping used by convertDGUTAFileTypesToExts.
func getExtMapping() map[db.DirGUTAFileType][]string {
	return map[db.DirGUTAFileType][]string{
		db.DGUTAFileTypeVCF:        {"vcf"},
		db.DGUTAFileTypeVCFGz:      {"vcf.gz"},
		db.DGUTAFileTypeBCF:        {"bcf"},
		db.DGUTAFileTypeSam:        {"sam"},
		db.DGUTAFileTypeBam:        {"bam"},
		db.DGUTAFileTypeCram:       {"cram"},
		db.DGUTAFileTypeFasta:      {"fa", "fasta"},
		db.DGUTAFileTypeFastq:      {"fastq", "fq"},
		db.DGUTAFileTypeFastqGz:    {"fastq.gz", "fq.gz"},
		db.DGUTAFileTypePedBed:     {"ped", "bed", "bim", "fam", "map"},
		db.DGUTAFileTypeText:       {"csv", "dat", "md", "readme", "text", "txt", "tsv"},
		db.DGUTAFileTypeLog:        {"log", "err", "e", "oe"},
		db.DGUTAFileTypeCompressed: {"gz", "bz2", "xz", "zip", "tgz", "bzip2", "bgz", "zst", "lz4", "lz", "br"},
	}
}

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
// Phase 1: Implemented using ClickHouse SubtreeSummary and ChildrenSummaries.
func (b *ClickHouseBackend) DirInfo(
	dir string,
	filter *db.Filter,
) (*db.DirInfo, error) {
	// Convert db.Filter to clickhouse.Filters
	chFilters := convertFilters(filter)

	// Use context to allow for cancellation
	ctx := context.Background()

	// Get the summary for the directory
	sum, err := b.ch.SubtreeSummary(ctx, dir, chFilters)
	if err != nil {
		return nil, err
	}

	// Convert the ClickHouse summary to a db.DirSummary
	current := b.convertSummaryToDirSummary(sum, dir)

	// Get the children summaries
	children, err := b.ch.ChildrenSummaries(ctx, dir, chFilters)
	if err != nil {
		return nil, err
	}

	// Convert the ClickHouse child summaries to db.DirSummary slice
	childSummaries := b.convertChildrenToDirSummaries(children)

	return &db.DirInfo{
		Current:  current,
		Children: childSummaries,
	}, nil
}

// convertSummaryToDirSummary converts a ClickHouse summary to db.DirSummary.
func (b *ClickHouseBackend) convertSummaryToDirSummary(sum clickhouse.Summary, dir string) *db.DirSummary {
	return &db.DirSummary{
		Dir:   dir,
		Count: sum.FileCount,
		Size:  sum.TotalSize,
		Atime: sum.OldestATime,
		Mtime: sum.MostRecentMTime,
		UIDs:  sum.UIDs,
		GIDs:  sum.GIDs,
		FTs:   convertFTypesToDirGUTAFileTypes(sum.FTypes),
		Age:   db.DirGUTAge(sum.Age),
	}
}

// convertChildrenToDirSummaries converts ClickHouse child summaries to []*db.DirSummary.
func (b *ClickHouseBackend) convertChildrenToDirSummaries(children []clickhouse.SummaryChild) []*db.DirSummary {
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

	return childSummaries
}

// DirHasChildren reports if dir has any child dirs with matching files.
func (b *ClickHouseBackend) DirHasChildren(dir string, filter *db.Filter) bool {
	chFilters := convertFilters(filter)
	ctx := context.Background()

	children, err := b.ch.ChildrenSummaries(ctx, dir, chFilters)
	if err != nil {
		return false
	}

	return len(children) > 0
}

// convertFilters converts a db.Filter to clickhouse.Filters.
func convertFilters(filter *db.Filter) clickhouse.Filters {
	if filter == nil {
		return clickhouse.Filters{}
	}

	aTimeBucket, mTimeBucket := convertAgeToBuckets(filter.Age)

	// Build and return the ClickHouse filters
	return clickhouse.Filters{
		GIDs:        filter.GIDs,
		UIDs:        filter.UIDs,
		Exts:        convertDGUTAFileTypesToExts(filter.FTs),
		ATimeBucket: aTimeBucket,
		MTimeBucket: mTimeBucket,
	}
}

// convertAgeToBuckets converts a db.DirGUTAge to appropriate time bucket strings.
func convertAgeToBuckets(age db.DirGUTAge) (aTimeBucket, mTimeBucket string) {
	aTimeMap := map[db.DirGUTAge]string{
		db.DGUTAgeA1M: ">1m",
		db.DGUTAgeA2M: ">2m",
		db.DGUTAgeA6M: ">6m",
		db.DGUTAgeA1Y: ">1y",
		db.DGUTAgeA2Y: ">2y",
		db.DGUTAgeA3Y: ">3y",
		db.DGUTAgeA5Y: ">5y",
		db.DGUTAgeA7Y: ">7y",
	}

	mTimeMap := map[db.DirGUTAge]string{
		db.DGUTAgeM1M: ">1m",
		db.DGUTAgeM2M: ">2m",
		db.DGUTAgeM6M: ">6m",
		db.DGUTAgeM1Y: ">1y",
		db.DGUTAgeM2Y: ">2y",
		db.DGUTAgeM3Y: ">3y",
		db.DGUTAgeM5Y: ">5y",
		db.DGUTAgeM7Y: ">7y",
	}

	return aTimeMap[age], mTimeMap[age]
}

// convertFTypesToDirGUTAFileTypes converts ClickHouse FTypes to db.DirGUTAFileType slice.
func convertFTypesToDirGUTAFileTypes(ftypes []uint8) []db.DirGUTAFileType {
	if len(ftypes) == 0 {
		return nil
	}

	// Map to db.DirGUTAFileType values
	result := make([]db.DirGUTAFileType, 0, len(ftypes))

	for _, ft := range ftypes {
		// FileTypeDir (2) in ClickHouse maps to 15 (DirGUTAFileTypeDir) in db
		if ft == uint8(clickhouse.FileTypeDir) {
			result = append(result, db.DGUTAFileTypeDir) // DirGUTAFileTypeDir value
		}
		// For non-directory types, we skip as they're handled via extensions
	}

	return result
}

// convertDGUTAFileTypesToExts converts db.DirGUTAFileType slice to extensions for filtering.
func convertDGUTAFileTypesToExts(fts []db.DirGUTAFileType) []string {
	if len(fts) == 0 {
		return nil
	}

	extMap := make(map[string]struct{})

	mapping := getExtMapping()
	for _, ft := range fts {
		if exts, ok := mapping[ft]; ok {
			for _, e := range exts {
				extMap[e] = struct{}{}
			}
		}
	}

	res := make([]string, 0, len(extMap))
	for e := range extMap {
		res = append(res, e)
	}

	return res
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
