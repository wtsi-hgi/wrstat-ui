/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	insertParentFactsQuery = "INSERT INTO wrstat_parent_facts " +
		"(mount_path, snapshot_id, parent_dir, dir, updated_at, all_count, all_size, " +
		"all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, " +
		"all_uids, all_gids, all_ft, " +
		"file_count, file_size, file_atime_min, file_mtime_max, " +
		"file_atime_buckets, file_mtime_buckets, file_uids, file_gids, file_ft, " +
		"gids, uids, fts, ages, counts, sizes, atime_mins, mtime_maxs, " +
		"atime_buckets, mtime_buckets, child_count, has_children, refreshed_at) VALUES " +
		"(?, toUUID(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	parentFactsChildSummariesQueryScope = "FROM wrstat_parent_facts " +
		"PREWHERE mount_path = ? AND snapshot_id = ? AND parent_dir = ? " +
		"ORDER BY dir"

	parentFactsAllChildSummariesQuery = "SELECT dir, updated_at, all_count, all_size, " +
		"all_atime_min, all_mtime_max, all_atime_buckets, all_mtime_buckets, all_uids, all_gids, all_ft, " +
		"child_count, has_children " +
		parentFactsChildSummariesQueryScope

	parentFactsFileChildSummariesQuery = "SELECT dir, updated_at, file_count, file_size, " +
		"file_atime_min, file_mtime_max, file_atime_buckets, file_mtime_buckets, file_uids, file_gids, file_ft, " +
		"child_count, has_children " +
		parentFactsChildSummariesQueryScope

	parentFactsVectorChildSummariesQuery = "SELECT dir, updated_at, " +
		"gids, uids, fts, ages, counts, sizes, atime_mins, mtime_maxs, " +
		"atime_buckets, mtime_buckets, child_count, has_children " +
		parentFactsChildSummariesQueryScope
)

var errParentFactsBatchNotPrepared = errors.New("clickhouse: parent facts batch is not prepared")

const parentFactsFallbackRoute = "parent_facts_fallback"

var parentFactsFallbackRouteCounter atomic.Uint64 //nolint:gochecknoglobals

// NavigationObject identifies the Disktree navigation storage shape selected
// by the bounded C1 decision gate.
type NavigationObject string

const (
	// NavigationObjectParentFacts is the default parent-ordered navigation
	// table selected unless projection or child facts meet C1 evidence.
	NavigationObjectParentFacts NavigationObject = "wrstat_parent_facts"

	// NavigationObjectChildFacts identifies a physical child/navigation facts
	// candidate such as wrstat_tree_nav_facts.
	NavigationObjectChildFacts NavigationObject = "wrstat_tree_nav_facts"
)

type parentFactReadMode uint8

const (
	parentFactReadAll parentFactReadMode = iota
	parentFactReadFiles
	parentFactReadVector
)

// DefaultNavigationObject returns the C1 default navigation object.
func DefaultNavigationObject() NavigationObject {
	return NavigationObjectParentFacts
}

// ChooseNavigationObject selects the implemented navigation object from the
// measured C1 evidence booleans.
func ChooseNavigationObject(projectionAccepted, childFactsAccepted bool) NavigationObject {
	if projectionAccepted {
		return NavigationObjectProjection
	}

	if childFactsAccepted {
		return NavigationObjectChildFacts
	}

	return DefaultNavigationObject()
}

func parentFactReadModeForFilter(filter *db.Filter) parentFactReadMode {
	if filter == nil {
		return parentFactReadAll
	}

	mode, ok := mountDirSummaryModeForFilter(filter)
	if !ok {
		return parentFactReadVector
	}

	if mode == mountDirSummaryFiles {
		return parentFactReadFiles
	}

	return parentFactReadAll
}

func parentFactChildSummaries(
	ctx context.Context,
	conn parentFactsQueryer,
	mount activeMount,
	parentDir string,
	filter *db.Filter,
) ([]parentFactChildSummary, error) {
	mode := parentFactReadModeForFilter(filter)

	rows, err := queryParentFactChildSummaryRows(ctx, conn, mount, parentDir, mode)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query parent facts child summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanParentFactChildSummaryRows(rows, filter, mode)
}

func parentFactDirInfoChildSummaries(
	ctx context.Context,
	conn parentFactsQueryer,
	mount activeMount,
	parentDir string,
	filter *db.Filter,
) ([]parentFactChildSummary, error) {
	mode := parentFactReadModeForFilter(filter)

	rows, err := queryParentFactChildSummaryRows(ctx, conn, mount, parentDir, mode)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query parent facts child summaries: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanParentFactDirInfoChildSummaryRows(rows, filter, mode)
}

func queryParentFactChildSummaryRows(
	ctx context.Context,
	conn parentFactsQueryer,
	mount activeMount,
	parentDir string,
	mode parentFactReadMode,
) (driver.Rows, error) {
	return conn.Query(
		ctx,
		parentFactsChildSummariesQueryForMode(mode),
		mount.mountPath,
		mount.snapshotID,
		ensureTrailingSlash(parentDir),
	)
}

func parentFactsChildSummariesQueryForMode(mode parentFactReadMode) string {
	switch mode {
	case parentFactReadFiles:
		return parentFactsFileChildSummariesQuery
	case parentFactReadVector:
		return parentFactsVectorChildSummariesQuery
	default:
		return parentFactsAllChildSummariesQuery
	}
}

func scanParentFactDirInfoChildSummaryRows(
	rows rowsScanner,
	filter *db.Filter,
	mode parentFactReadMode,
) ([]parentFactChildSummary, error) {
	summaries := make([]parentFactChildSummary, 0)

	for rows.Next() {
		summary, err := scanParentFactDirInfoChildSummaryRow(rows, filter, mode)
		if err != nil {
			return nil, err
		}

		if parentFactChildSummaryMatchesFilter(summary, filter) {
			summaries = append(summaries, summary)
		}
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: parent facts child summary iteration error: %w", err)
	}

	return summaries, nil
}

func scanParentFactDirInfoChildSummaryRow(
	rows rowsScanner,
	filter *db.Filter,
	mode parentFactReadMode,
) (parentFactChildSummary, error) {
	var row parentFactRowScanned
	if err := row.scanFrom(rows, mode); err != nil {
		return parentFactChildSummary{}, err
	}

	summary, err := row.dirInfoSummary(filter, mode)
	if err != nil {
		return parentFactChildSummary{}, err
	}

	return parentFactChildSummary{
		Dir:         row.dir,
		Summary:     summary,
		HasChildren: row.hasChildren > 0,
		ChildCount:  row.childCount,
	}, nil
}

func parentFactChildSummaryMatchesFilter(
	summary parentFactChildSummary,
	filter *db.Filter,
) bool {
	return filter == nil || summary.Summary != nil || summary.HasChildren
}

func scanParentFactChildSummaryRows(
	rows rowsScanner,
	filter *db.Filter,
	mode parentFactReadMode,
) ([]parentFactChildSummary, error) {
	summaries := make([]parentFactChildSummary, 0)

	for rows.Next() {
		summary, err := scanParentFactChildSummaryRow(rows, filter, mode)
		if err != nil {
			return nil, err
		}

		summaries = append(summaries, summary)
	}

	if err := rowsErr(rows); err != nil {
		return nil, fmt.Errorf("clickhouse: parent facts child summary iteration error: %w", err)
	}

	return summaries, nil
}

func scanParentFactChildSummaryRow(
	rows rowsScanner,
	filter *db.Filter,
	mode parentFactReadMode,
) (parentFactChildSummary, error) {
	var row parentFactRowScanned
	if err := row.scanFrom(rows, mode); err != nil {
		return parentFactChildSummary{}, err
	}

	summary, err := row.summary(filter, mode)
	if err != nil {
		return parentFactChildSummary{}, err
	}

	return parentFactChildSummary{
		Dir:         row.dir,
		Summary:     summary,
		HasChildren: row.hasChildren > 0,
		ChildCount:  row.childCount,
	}, nil
}

type parentFactsWriter struct {
	conn ch.Conn

	batch    driver.Batch
	openedAt time.Time
	batchNow func() time.Time

	batchSize   int
	refreshedAt time.Time
	writeErr    error
}

func newParentFactsWriter(conn ch.Conn, batchSize int, refreshedAt time.Time) *parentFactsWriter {
	writer := &parentFactsWriter{
		conn:        conn,
		batchSize:   batchSize,
		refreshedAt: refreshedAt,
	}
	if writer.refreshedAt.IsZero() {
		writer.refreshedAt = writer.importBatchNow().UTC()
	}

	return writer
}

func (w *parentFactsWriter) appendRecord(
	ctx context.Context,
	mount activeMount,
	dir string,
	gutas db.GUTAs,
	_ []string,
	childCount uint64,
	_ []db.DirGUTAge,
) error {
	return w.blockWriter().append(ctx, func(batch driver.Batch) error {
		return appendParentFactRecordValues(
			batch,
			mount,
			parentFactsParentDir(dir),
			dir,
			gutas,
			childCount,
			w.refreshedAt,
		)
	})
}

func appendParentFactRecordValues(
	batch driver.Batch,
	mount activeMount,
	parentDir string,
	dir string,
	gutas db.GUTAs,
	childCount uint64,
	refreshedAt time.Time,
) error {
	allSummary, fileSummary := parentFactRecordSummaries(gutas)
	parts := parentFactRecordValueParts{
		mount:       mount,
		parentDir:   parentDir,
		dir:         dir,
		allSummary:  allSummary,
		fileSummary: fileSummary,
		columns:     mountDirProjectionVectorColumnsFor(gutas),
		childCount:  childCount,
		refreshedAt: refreshedAt,
	}

	if err := batch.Append(parts.values()...); err != nil {
		return fmt.Errorf("clickhouse: failed to append parent facts row: %w", err)
	}

	return nil
}

func parentFactsParentDir(dir string) string {
	dir = ensureTrailingSlash(dir)
	if dir == "/" {
		return "/"
	}

	trimmed := strings.TrimSuffix(dir, "/")

	idx := strings.LastIndex(trimmed, "/")
	if idx <= 0 {
		return "/"
	}

	return trimmed[:idx+1]
}

func (w *parentFactsWriter) flush(context.Context) error {
	return w.blockWriter().close()
}

func (w *parentFactsWriter) abort() error {
	return w.blockWriter().abort()
}

func (w *parentFactsWriter) importPhase() string {
	return importPhaseParentFactsInsert
}

func (w *parentFactsWriter) blockWriter() *importBlockWriter {
	return &importBlockWriter{
		conn:        w.conn,
		query:       insertParentFactsQuery,
		name:        "parent facts",
		batch:       &w.batch,
		openedAt:    &w.openedAt,
		writeErr:    &w.writeErr,
		batchSize:   w.batchSize,
		notPrepared: errParentFactsBatchNotPrepared,
		now:         w.importBatchNow,
	}
}

func (w *parentFactsWriter) importBatchNow() time.Time {
	if w != nil && w.batchNow != nil {
		return w.batchNow()
	}

	return time.Now()
}

type parentFactRecordValueParts struct {
	mount       activeMount
	parentDir   string
	dir         string
	allSummary  *mountDirRecordSummary
	fileSummary *mountDirRecordSummary
	columns     mountDirProjectionVectorColumns
	childCount  uint64
	refreshedAt time.Time
}

func (p parentFactRecordValueParts) values() []any {
	values := p.identityValues()
	values = append(values, p.allSummaryValues()...)
	values = append(values, p.fileSummaryValues()...)
	values = append(values, p.vectorValues()...)

	return append(values, p.childCount, parentFactsHasChildrenValue(p.childCount), p.refreshedAt)
}

func parentFactsHasChildrenValue(childCount uint64) uint8 {
	if childCount > 0 {
		return 1
	}

	return 0
}

func (p parentFactRecordValueParts) identityValues() []any {
	return []any{
		p.mount.mountPath,
		p.mount.snapshotID,
		p.parentDir,
		p.dir,
		p.mount.updatedAt,
	}
}

func (p parentFactRecordValueParts) allSummaryValues() []any {
	return []any{
		p.allSummary.count,
		p.allSummary.size,
		p.allSummary.atimeMin,
		p.allSummary.mtimeMax,
		ageBucketsSlice(&p.allSummary.atimeBuckets),
		ageBucketsSlice(&p.allSummary.mtimeBuckets),
		p.allSummary.sortedUIDs(),
		p.allSummary.sortedGIDs(),
		uint16(p.allSummary.ft),
	}
}

func (p parentFactRecordValueParts) fileSummaryValues() []any {
	return []any{
		recordSummaryCount(p.fileSummary),
		recordSummarySize(p.fileSummary),
		recordSummaryAtimeMin(p.fileSummary),
		recordSummaryMtimeMax(p.fileSummary),
		recordSummaryATimeBuckets(p.fileSummary),
		recordSummaryMTimeBuckets(p.fileSummary),
		recordSummaryUIDs(p.fileSummary),
		recordSummaryGIDs(p.fileSummary),
		recordSummaryFT(p.fileSummary),
	}
}

func (p parentFactRecordValueParts) vectorValues() []any {
	return []any{
		p.columns.gids,
		p.columns.uids,
		p.columns.fts,
		p.columns.ages,
		p.columns.counts,
		p.columns.sizes,
		p.columns.atimeMins,
		p.columns.mtimeMaxs,
		p.columns.atimeBuckets,
		p.columns.mtimeBuckets,
	}
}

type parentFactsQueryer interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

type parentFactChildSummary struct {
	Dir         string
	Summary     *db.DirSummary
	HasChildren bool
	ChildCount  uint64
}

func cloneParentFactChildSummaries(in []parentFactChildSummary) []parentFactChildSummary {
	if len(in) == 0 {
		return nil
	}

	out := make([]parentFactChildSummary, len(in))
	for i, fact := range in {
		out[i] = fact
		out[i].Summary = cloneDirSummary(fact.Summary)
	}

	return out
}

type parentFactSummaryScalars struct {
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets []uint64
	mtimeBuckets []uint64
	uids         []uint32
	gids         []uint32
	ft           uint16
}

func (s parentFactSummaryScalars) summary(
	age db.DirGUTAge,
	updatedAt time.Time,
) *db.DirSummary {
	if s.count == 0 {
		return nil
	}

	return &db.DirSummary{
		Count:       s.count,
		Size:        s.size,
		Atime:       time.Unix(s.atimeMin, 0),
		CommonATime: summary.MostCommonBucket(sliceToAgeBuckets(s.atimeBuckets)),
		Mtime:       time.Unix(s.mtimeMax, 0),
		CommonMTime: summary.MostCommonBucket(sliceToAgeBuckets(s.mtimeBuckets)),
		UIDs:        s.uids,
		GIDs:        s.gids,
		FT:          db.DirGUTAFileType(s.ft),
		Age:         age,
		Modtime:     updatedAt.UTC(),
	}
}

type parentFactRowScanned struct {
	dir         string
	updatedAt   time.Time
	all         parentFactSummaryScalars
	file        parentFactSummaryScalars
	vector      dgutaVectorColumns
	childCount  uint64
	hasChildren uint8
}

func (s *parentFactRowScanned) scanFrom(rows rowsScanner, mode parentFactReadMode) error {
	if err := rows.Scan(s.scanDestinations(mode)...); err != nil {
		return fmt.Errorf("clickhouse: failed to scan parent facts child summary: %w", err)
	}

	return nil
}

func (s *parentFactRowScanned) scanDestinations(mode parentFactReadMode) []any {
	destinations := s.metadataDestinations()

	switch mode {
	case parentFactReadFiles:
		destinations = append(destinations, parentFactSummaryDestinations(&s.file)...)
	case parentFactReadVector:
		destinations = append(destinations, parentFactVectorDestinations(&s.vector)...)
	default:
		destinations = append(destinations, parentFactSummaryDestinations(&s.all)...)
	}

	return append(destinations, &s.childCount, &s.hasChildren)
}

func parentFactSummaryDestinations(summary *parentFactSummaryScalars) []any {
	return []any{
		&summary.count,
		&summary.size,
		&summary.atimeMin,
		&summary.mtimeMax,
		&summary.atimeBuckets,
		&summary.mtimeBuckets,
		&summary.uids,
		&summary.gids,
		&summary.ft,
	}
}

func parentFactVectorDestinations(vector *dgutaVectorColumns) []any {
	return []any{
		&vector.gids,
		&vector.uids,
		&vector.fts,
		&vector.ages,
		&vector.counts,
		&vector.sizes,
		&vector.atimeMins,
		&vector.mtimeMaxs,
		&vector.atimeBuckets,
		&vector.mtimeBuckets,
	}
}

func (s *parentFactRowScanned) metadataDestinations() []any {
	return []any{
		&s.dir,
		&s.updatedAt,
	}
}

func (s *parentFactRowScanned) summary(
	filter *db.Filter,
	mode parentFactReadMode,
) (*db.DirSummary, error) {
	switch mode {
	case parentFactReadFiles:
		return s.file.summary(parentFactFilterAge(filter), s.updatedAt), nil
	case parentFactReadVector:
		gutas, err := s.vector.gutas("dir", s.dir)
		if err != nil {
			return nil, err
		}

		return dirSummaryWithModtime(gutas, filter, s.updatedAt.UTC()), nil
	default:
		return s.all.summary(parentFactFilterAge(filter), s.updatedAt), nil
	}
}

func parentFactFilterAge(filter *db.Filter) db.DirGUTAge {
	if filter == nil {
		return db.DGUTAgeAll
	}

	return filter.Age
}

func (s *parentFactRowScanned) dirInfoSummary(
	filter *db.Filter,
	mode parentFactReadMode,
) (*db.DirSummary, error) {
	return s.summary(filter, mode)
}

func parentFactsFallbackRouteName() string {
	return parentFactsFallbackRoute
}

func recordParentFactsFallbackRoute() {
	parentFactsFallbackRouteCounter.Add(1)
}

func parentFactsFallbackRoutes() uint64 {
	return parentFactsFallbackRouteCounter.Load()
}

func resetParentFactsFallbackRoutesForTest() {
	parentFactsFallbackRouteCounter.Store(0)
}

func parentFactRecordSummaries(gutas db.GUTAs) (*mountDirRecordSummary, *mountDirRecordSummary) {
	allSummary, fileSummary := mountDirRecordSummaries(gutas)

	if allSummary == nil {
		allSummary = newMountDirRecordSummary()
	}

	return allSummary, fileSummary
}
