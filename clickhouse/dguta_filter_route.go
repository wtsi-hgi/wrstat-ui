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
	"errors"
	"fmt"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	dgutaTupleGIDPosition        = 1
	dgutaTupleUIDPosition        = 2
	dgutaTupleFTPosition         = 3
	dgutaTupleAgePosition        = 4
	dgutaVectorChildrenFixedArgs = 8
	dgutaVectorSubtreeRangeArgs  = 2

	dgutaVectorTupleColumns = "gids, uids, fts, ages, counts, sizes, " +
		"atime_mins, mtime_maxs, atime_buckets, mtime_buckets"

	dgutaVectorMappedColumns = "dir, summary_dir_id, child_count, gutas, " +
		"arrayMap(g -> tupleElement(g, 1), gutas) AS gids, " +
		"arrayMap(g -> tupleElement(g, 2), gutas) AS uids, " +
		"arrayMap(g -> tupleElement(g, 3), gutas) AS fts, " +
		"arrayMap(g -> tupleElement(g, 5), gutas) AS counts, " +
		"arrayMap(g -> tupleElement(g, 6), gutas) AS sizes, " +
		"arrayFilter(v -> v != 0, arrayMap(g -> tupleElement(g, 7), gutas)) AS nonzero_atime_mins, " +
		"arrayMap(g -> tupleElement(g, 8), gutas) AS mtime_maxs, " +
		"arrayMap(g -> tupleElement(g, 9), gutas) AS atime_buckets, " +
		"arrayMap(g -> tupleElement(g, 10), gutas) AS mtime_buckets"

	dgutaVectorSummaryColumns = "dir, length(gutas) AS raw_rows, " +
		"arrayReduce('sum', counts) AS total_count, " +
		"arrayReduce('sum', sizes) AS total_size, " +
		"if(empty(nonzero_atime_mins), toInt64(0), arrayReduce('min', nonzero_atime_mins)) AS atime_min, " +
		"if(empty(mtime_maxs), toInt64(0), arrayReduce('max', mtime_maxs)) AS mtime_max, " +
		"arrayReduce('sumForEach', atime_buckets) AS atime_buckets, " +
		"arrayReduce('sumForEach', mtime_buckets) AS mtime_buckets, " +
		"arraySort(arrayDistinct(uids)) AS uids, " +
		"arraySort(arrayDistinct(gids)) AS gids, " +
		"arrayReduce('groupBitOr', fts) AS file_types"

	dgutaVectorChildSummaryColumns = "summary_dir_id, child_count, gutas, " + dgutaVectorSummaryColumns

	dgutaFilterMaterialisedExactQuery = "SELECT c.full_path AS dir, count() AS raw_rows, " +
		"sum(f.count) AS total_count, sum(f.size) AS total_size, " +
		"minIf(f.atime_min, f.atime_min != 0) AS atime_min, max(f.mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(f.atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(f.mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(f.uid)) AS uids, arraySort(groupUniqArray(f.gid)) AS gids, " +
		"groupBitOr(f.ft) AS file_types " +
		"FROM wrstat_dir_filter_all AS f " +
		"INNER JOIN wrstat_dirs AS c " +
		"ON c.mount_path = f.mount_path AND c.snapshot_id = f.snapshot_id AND c.dir_id = f.dir_id " +
		"WHERE f.mount_path = ? AND f.snapshot_id = toUUID(?) AND f.age = ? %s AND %s " +
		"GROUP BY c.full_path ORDER BY c.full_path"

	dgutaFilterMaterialisedChildrenQuery = "SELECT c.full_path AS dir, count() AS raw_rows, " +
		"sum(f.count) AS total_count, sum(f.size) AS total_size, " +
		"minIf(f.atime_min, f.atime_min != 0) AS atime_min, max(f.mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(f.atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(f.mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(f.uid)) AS uids, arraySort(groupUniqArray(f.gid)) AS gids, " +
		"groupBitOr(f.ft) AS file_types, max(f.filter_child_count) AS filter_child_count, " +
		"max(f.child_count) AS child_count, max(f.has_filter_children) AS has_filter_children, " +
		"max(f.has_children) AS has_children " +
		"FROM wrstat_child_filter_all AS f " +
		"INNER JOIN wrstat_dirs AS c " +
		"ON c.mount_path = f.mount_path AND c.snapshot_id = f.snapshot_id AND c.dir_id = f.dir_id " +
		"WHERE f.mount_path = ? AND f.snapshot_id = toUUID(?) AND f.parent_id = ? AND f.age = ? %s " +
		"GROUP BY c.full_path ORDER BY c.full_path"

	dgutaFilterMaterialisedSubtreeQuery = "SELECT c.full_path AS dir, count() AS raw_rows, " +
		"sum(f.count) AS total_count, sum(f.size) AS total_size, " +
		"minIf(f.atime_min, f.atime_min != 0) AS atime_min, max(f.mtime_max) AS mtime_max, " +
		"arrayReduce('sumForEach', groupArray(f.atime_buckets)) AS atime_buckets, " +
		"arrayReduce('sumForEach', groupArray(f.mtime_buckets)) AS mtime_buckets, " +
		"arraySort(groupUniqArray(f.uid)) AS uids, arraySort(groupUniqArray(f.gid)) AS gids, " +
		"groupBitOr(f.ft) AS file_types " +
		"FROM wrstat_dir_filter_all AS f " +
		"INNER JOIN wrstat_dirs AS c " +
		"ON c.mount_path = f.mount_path AND c.snapshot_id = f.snapshot_id AND c.dir_id = f.dir_id " +
		"WHERE f.mount_path = ? AND f.snapshot_id = toUUID(?) AND f.age = ? %s " +
		"AND f.dir_id >= ? AND f.dir_id < ? GROUP BY c.full_path ORDER BY c.full_path"
)

var errDGUTAFilterCollapseUnproven = errors.New("clickhouse: unproven D4 filter materialisation collapse")

type dgutaFilterRoute uint8

const (
	dgutaFilterRouteMaterialised dgutaFilterRoute = iota
	dgutaFilterRouteVector
)

type dgutaFilterPattern string

const (
	dgutaFilterPatternExact    dgutaFilterPattern = "filtered_exact"
	dgutaFilterPatternChildren dgutaFilterPattern = "filtered_children"
	dgutaFilterPatternSubtree  dgutaFilterPattern = "filtered_subtree"

	dgutaFilterMaterialisationChildAll = "wrstat_child_filter_all"
	dgutaFilterMaterialisationDirAll   = "wrstat_dir_filter_all"
	dgutaFilterMaterialisationAgeAll   = "wrstat_dir_filter_ageall"

	dgutaFilterCollapseReasonNoMeasurement = "no Phase 7 D4 measurement; materialisation retained"
	dgutaFilterCollapseReasonUnreproduced  = "bounded dataset could not reproduce pattern; materialisation retained"
)

func dgutaFilterExactSummariesQueryForRoute(
	route dgutaFilterRoute,
	mountPath, snapshotID string,
	dirIDs []uint32,
	filter *db.Filter,
) (string, []any) {
	if route == dgutaFilterRouteVector {
		return dgutaVectorExactSummariesQuery(mountPath, snapshotID, dirIDs, filter)
	}

	return dgutaMaterialisedExactSummariesQuery(mountPath, snapshotID, dirIDs, filter)
}

func dgutaVectorExactSummariesQuery(
	mountPath, snapshotID string,
	dirIDs []uint32,
	filter *db.Filter,
) (string, []any) {
	source, filterArgs := dgutaVectorRowsSource(
		"d",
		filter,
		"WHERE d.mount_path = ? AND d.snapshot_id = toUUID(?) AND "+
			dgutaFilterIDPredicate("d", "dir_id", len(dirIDs)),
	)
	query := dgutaVectorSummaryQuery(source)
	args := make([]any, 0, len(filterArgs)+queryScopeArgs+len(dirIDs))
	args = append(args, filterArgs...)
	args = append(args, mountPath, snapshotID)
	args = appendUint32Args(args, dirIDs)

	return query, args
}

func dgutaVectorRowsSource(alias string, filter *db.Filter, where string) (string, []any) {
	filterExpr, filterArgs := dgutaVectorFilterExpression("g", filter)
	query := "SELECT c.full_path AS dir, " + alias + ".dir_id AS summary_dir_id, " +
		alias + ".child_count AS child_count, " +
		"arrayFilter(g -> " + filterExpr + ", " + dgutaVectorZipExpr(alias) + ") AS gutas " +
		"FROM wrstat_dir_facts AS " + alias + " " +
		"INNER JOIN wrstat_dirs AS c " +
		"ON c.mount_path = " + alias + ".mount_path AND c.snapshot_id = " + alias + ".snapshot_id " +
		"AND c.dir_id = " + alias + ".dir_id " + where

	return query, filterArgs
}

func dgutaVectorFilterExpression(tuple string, filter *db.Filter) (string, []any) {
	if filter == nil {
		return "1", nil
	}

	if isEmptyIDFilter(filter.GIDs) || isEmptyIDFilter(filter.UIDs) {
		return "0", nil
	}

	clauses := make([]string, 0, dirSummaryFilterClauseInitialCap)
	args := make([]any, 0, dgutaVectorFilterArgCap(filter))

	appendTupleIDMembershipClause(&clauses, &args, tuple, dgutaTupleGIDPosition, filter.GIDs)
	appendTupleIDMembershipClause(&clauses, &args, tuple, dgutaTupleUIDPosition, filter.UIDs)
	appendTupleFTMembershipClause(&clauses, &args, tuple, filter.FT)

	clauses = append(clauses, fmt.Sprintf("tupleElement(%s, %d) = ?", tuple, dgutaTupleAgePosition))
	args = append(args, uint8(filter.Age))

	return strings.Join(clauses, " AND "), args
}

func dgutaVectorFilterArgCap(filter *db.Filter) int {
	if filter == nil {
		return 0
	}

	capacity := len(filter.GIDs) + len(filter.UIDs) + 1
	if filter.FT != 0 {
		capacity++
	}

	return capacity
}

func appendTupleIDMembershipClause(
	clauses *[]string,
	args *[]any,
	tuple string,
	position int,
	values []uint32,
) {
	if values == nil {
		return
	}

	*clauses = append(
		*clauses,
		fmt.Sprintf("tupleElement(%s, %d) IN (%s)", tuple, position, placeholders(len(values))),
	)
	appendUint32ArgsToPointer(args, values)
}

func appendUint32ArgsToPointer(args *[]any, values []uint32) {
	for _, value := range values {
		*args = append(*args, value)
	}
}

func appendTupleFTMembershipClause(
	clauses *[]string,
	args *[]any,
	tuple string,
	ft db.DirGUTAFileType,
) {
	if ft == 0 {
		return
	}

	*clauses = append(*clauses, fmt.Sprintf("bitAnd(tupleElement(%s, %d), ?) > 0", tuple, dgutaTupleFTPosition))
	*args = append(*args, uint16(ft))
}

func dgutaVectorZipExpr(alias string) string {
	columns := strings.Split(dgutaVectorTupleColumns, ", ")
	for i, column := range columns {
		columns[i] = alias + "." + column
	}

	return "arrayZip(" + strings.Join(columns, ", ") + ")"
}

func dgutaFilterIDPredicate(alias, column string, count int) string {
	if count == 0 {
		return "0"
	}

	return alias + "." + column + " IN (" + placeholders(count) + ")"
}

func dgutaVectorSummaryQuery(source string) string {
	return "SELECT dir, raw_rows, total_count, total_size, atime_min, mtime_max, " +
		"atime_buckets, mtime_buckets, uids, gids, file_types FROM (" +
		"SELECT " + dgutaVectorSummaryColumns + " FROM (" +
		dgutaVectorMappedRowsQuery(source) +
		")) WHERE raw_rows > 0 ORDER BY dir"
}

func dgutaVectorMappedRowsQuery(source string) string {
	return "SELECT " + dgutaVectorMappedColumns + " FROM (" + source + ")"
}

func appendUint32Args(args []any, values []uint32) []any {
	for _, value := range values {
		args = append(args, value)
	}

	return args
}

func dgutaMaterialisedExactSummariesQuery(
	mountPath, snapshotID string,
	dirIDs []uint32,
	filter *db.Filter,
) (string, []any) {
	clauses, filterArgs := fullFilterOptionalClauses(filter)
	idPredicate := dgutaFilterIDPredicate("f", "dir_id", len(dirIDs))
	query := fmt.Sprintf(dgutaFilterMaterialisedExactQuery, clauses, idPredicate)
	args := make([]any, 0, queryScopeArgs+1+len(filterArgs)+len(dirIDs))
	args = append(args, mountPath, snapshotID, uint8(filter.Age))
	args = append(args, filterArgs...)
	args = appendUint32Args(args, dirIDs)

	return query, args
}

func dgutaFilterChildrenSummariesQueryForRoute(
	route dgutaFilterRoute,
	mountPath, snapshotID string,
	parentID uint32,
	filter *db.Filter,
) (string, []any) {
	if route == dgutaFilterRouteVector {
		return dgutaVectorChildrenSummariesQuery(mountPath, snapshotID, parentID, filter)
	}

	return dgutaMaterialisedChildrenSummariesQuery(mountPath, snapshotID, parentID, filter)
}

func dgutaVectorChildrenSummariesQuery(
	mountPath, snapshotID string,
	parentID uint32,
	filter *db.Filter,
) (string, []any) {
	source, sourceFilterArgs := dgutaVectorRowsSource(
		"d",
		filter,
		"WHERE d.mount_path = ? AND d.snapshot_id = toUUID(?) AND d.parent_id = ?",
	)
	childCounts, childFilterArgs := dgutaVectorFilteredChildCountsQuery(filter)
	query := "WITH filtered_child_counts AS (" + childCounts + ") " +
		dgutaVectorChildSummaryQuery(source)

	args := make([]any, 0, len(childFilterArgs)+len(sourceFilterArgs)+dgutaVectorChildrenFixedArgs)
	args = append(args, childFilterArgs...)
	args = append(args, mountPath, snapshotID, mountPath, snapshotID, parentID)
	args = append(args, sourceFilterArgs...)
	args = append(args, mountPath, snapshotID, parentID)

	return query, args
}

func dgutaVectorFilteredChildCountsQuery(filter *db.Filter) (string, []any) {
	filterExpr, filterArgs := dgutaVectorFilterExpression("g", filter)
	query := "SELECT summary_dir_id, gid, uid, ft, age, uniqExact(dir_id) AS filter_child_count " +
		"FROM (" +
		"SELECT parent_id AS summary_dir_id, dir_id, tupleElement(g, 1) AS gid, " +
		"tupleElement(g, 2) AS uid, tupleElement(g, 3) AS ft, tupleElement(g, 4) AS age " +
		"FROM (" +
		"SELECT gd.parent_id, gd.dir_id, arrayJoin(arrayFilter(g -> " + filterExpr + ", " +
		dgutaVectorZipExpr("gd") + ")) AS g " +
		"FROM wrstat_dir_facts AS gd " +
		"WHERE gd.mount_path = ? AND gd.snapshot_id = toUUID(?) AND gd.parent_id IN (" +
		"SELECT cd.dir_id FROM wrstat_dir_facts AS cd " +
		"WHERE cd.mount_path = ? AND cd.snapshot_id = toUUID(?) AND cd.parent_id = ?" +
		"))" +
		") GROUP BY summary_dir_id, gid, uid, ft, age"

	return query, filterArgs
}

func dgutaVectorChildSummaryQuery(source string) string {
	return "SELECT s.dir, s.raw_rows, s.total_count, s.total_size, s.atime_min, s.mtime_max, " +
		"s.atime_buckets, s.mtime_buckets, s.uids, s.gids, s.file_types, " +
		"max(ifNull(f.filter_child_count, toUInt64(0))) AS filter_child_count, " +
		"max(s.child_count) AS child_count, " +
		"toUInt8(if(filter_child_count > 0, 1, 0)) AS has_filter_children, " +
		"toUInt8(if(child_count > 0, 1, 0)) AS has_children " +
		"FROM (" +
		"SELECT " + dgutaVectorChildSummaryColumns + " FROM (" +
		dgutaVectorMappedRowsQuery(source) +
		")) AS s " +
		"LEFT JOIN filtered_child_counts AS f " +
		"ON f.summary_dir_id = s.summary_dir_id AND " +
		"arrayExists(g -> tupleElement(g, 1) = f.gid AND tupleElement(g, 2) = f.uid AND " +
		"tupleElement(g, 3) = f.ft AND tupleElement(g, 4) = f.age, s.gutas) " +
		"WHERE s.raw_rows > 0 " +
		"GROUP BY s.dir, s.raw_rows, s.total_count, s.total_size, s.atime_min, s.mtime_max, " +
		"s.atime_buckets, s.mtime_buckets, s.uids, s.gids, s.file_types " +
		"ORDER BY s.dir"
}

func dgutaMaterialisedChildrenSummariesQuery(
	mountPath, snapshotID string,
	parentID uint32,
	filter *db.Filter,
) (string, []any) {
	clauses, filterArgs := fullFilterOptionalClauses(filter)
	query := fmt.Sprintf(dgutaFilterMaterialisedChildrenQuery, clauses)
	args := make([]any, 0, queryScopeArgs+2+len(filterArgs))
	args = append(args, mountPath, snapshotID, parentID, uint8(filter.Age))
	args = append(args, filterArgs...)

	return query, args
}

func dgutaFilterSubtreeSummariesQueryForRoute(
	route dgutaFilterRoute,
	mountPath, snapshotID string,
	dirID, subtreeEnd uint32,
	filter *db.Filter,
) (string, []any) {
	if route == dgutaFilterRouteVector {
		return dgutaVectorSubtreeSummariesQuery(mountPath, snapshotID, dirID, subtreeEnd, filter)
	}

	return dgutaMaterialisedSubtreeSummariesQuery(mountPath, snapshotID, dirID, subtreeEnd, filter)
}

func dgutaVectorSubtreeSummariesQuery(
	mountPath, snapshotID string,
	dirID, subtreeEnd uint32,
	filter *db.Filter,
) (string, []any) {
	source, filterArgs := dgutaVectorRowsSource(
		"d",
		filter,
		"WHERE d.mount_path = ? AND d.snapshot_id = toUUID(?) AND d.dir_id >= ? AND d.dir_id < ?",
	)
	query := dgutaVectorSummaryQuery(source)
	args := make([]any, 0, len(filterArgs)+queryScopeArgs+dgutaVectorSubtreeRangeArgs)
	args = append(args, filterArgs...)
	args = append(args, mountPath, snapshotID, dirID, subtreeEnd)

	return query, args
}

func dgutaMaterialisedSubtreeSummariesQuery(
	mountPath, snapshotID string,
	dirID, subtreeEnd uint32,
	filter *db.Filter,
) (string, []any) {
	clauses, filterArgs := fullFilterOptionalClauses(filter)
	query := fmt.Sprintf(dgutaFilterMaterialisedSubtreeQuery, clauses)
	args := make([]any, 0, queryScopeArgs+3+len(filterArgs))
	args = append(args, mountPath, snapshotID, uint8(filter.Age))
	args = append(args, filterArgs...)
	args = append(args, dirID, subtreeEnd)

	return query, args
}

func defaultDGUTAFilterCollapseDecisions() []dgutaFilterCollapseDecision {
	patterns := []dgutaFilterPattern{
		dgutaFilterPatternExact,
		dgutaFilterPatternChildren,
		dgutaFilterPatternSubtree,
	}
	decisions := make([]dgutaFilterCollapseDecision, len(patterns))

	for i, pattern := range patterns {
		decisions[i] = retainedDGUTAFilterCollapseDecision(pattern, dgutaFilterCollapseReasonNoMeasurement)
	}

	return decisions
}

func retainedDGUTAFilterCollapseDecision(
	pattern dgutaFilterPattern,
	reason string,
) dgutaFilterCollapseDecision {
	return dgutaFilterCollapseDecision{
		Pattern:         pattern,
		Route:           dgutaFilterRouteMaterialised,
		Materialisation: dgutaFilterMaterialisationForPattern(pattern),
		Reason:          reason,
	}
}

func dgutaFilterMaterialisationForPattern(pattern dgutaFilterPattern) string {
	switch pattern {
	case dgutaFilterPatternChildren:
		return dgutaFilterMaterialisationChildAll
	case dgutaFilterPatternExact, dgutaFilterPatternSubtree:
		return dgutaFilterMaterialisationDirAll
	default:
		return ""
	}
}

type dgutaFilterCollapseMeasurement struct {
	Pattern        dgutaFilterPattern
	Dataset        string
	Fanout         string
	Citation       string
	InQueryP95MS   float64
	LatencyGateMS  float64
	ParityProven   bool
	PatternCovered bool
}

func validateDGUTAFilterMeasurementIdentity(
	request dgutaFilterCollapseRequest,
	measurement *dgutaFilterCollapseMeasurement,
) error {
	if measurement.Pattern != request.Pattern {
		return fmt.Errorf(
			"%w: %s measurement supplied for %s",
			errDGUTAFilterCollapseUnproven,
			measurement.Pattern,
			request.Pattern,
		)
	}

	return nil
}

func validateDGUTAFilterMeasurementProof(
	request dgutaFilterCollapseRequest,
	measurement *dgutaFilterCollapseMeasurement,
) error {
	if !measurement.PatternCovered {
		return fmt.Errorf(
			"%w: %s was not reproduced by the measured dataset",
			errDGUTAFilterCollapseUnproven,
			request.Pattern,
		)
	}

	if !measurement.ParityProven {
		return fmt.Errorf("%w: %s lacks materialised/vector parity proof", errDGUTAFilterCollapseUnproven, request.Pattern)
	}

	if strings.TrimSpace(measurement.Citation) == "" {
		return fmt.Errorf("%w: %s lacks cited Phase 7 measurement", errDGUTAFilterCollapseUnproven, request.Pattern)
	}

	return nil
}

func validateDGUTAFilterMeasurementLatency(
	request dgutaFilterCollapseRequest,
	measurement *dgutaFilterCollapseMeasurement,
) error {
	if measurement.InQueryP95MS <= 0 || measurement.LatencyGateMS <= 0 {
		return fmt.Errorf("%w: %s has invalid latency measurement", errDGUTAFilterCollapseUnproven, request.Pattern)
	}

	if measurement.InQueryP95MS > measurement.LatencyGateMS {
		return fmt.Errorf("%w: %s in-query p95 exceeds latency gate", errDGUTAFilterCollapseUnproven, request.Pattern)
	}

	return nil
}

type dgutaFilterCollapseRequest struct {
	Pattern     dgutaFilterPattern
	Collapse    bool
	Measurement *dgutaFilterCollapseMeasurement
}

func validateDGUTAFilterCollapseMeasurement(request dgutaFilterCollapseRequest) error {
	measurement := request.Measurement
	if measurement == nil {
		return fmt.Errorf("%w: %s has no Phase 7 measurement", errDGUTAFilterCollapseUnproven, request.Pattern)
	}

	if err := validateDGUTAFilterMeasurementIdentity(request, measurement); err != nil {
		return err
	}

	if err := validateDGUTAFilterMeasurementProof(request, measurement); err != nil {
		return err
	}

	return validateDGUTAFilterMeasurementLatency(request, measurement)
}

type dgutaFilterCollapseDecision struct {
	Pattern         dgutaFilterPattern
	Route           dgutaFilterRoute
	Materialisation string
	Collapsed       bool
	Citation        string
	Reason          string
	InQueryP95MS    float64
	LatencyGateMS   float64
}

func dgutaFilterCollapseDecisionFor(
	request dgutaFilterCollapseRequest,
) (dgutaFilterCollapseDecision, error) {
	if !request.Collapse {
		reason := dgutaFilterCollapseReasonNoMeasurement
		if request.Measurement != nil && !request.Measurement.PatternCovered {
			reason = dgutaFilterCollapseReasonUnreproduced
		}

		return retainedDGUTAFilterCollapseDecision(request.Pattern, reason), nil
	}

	decision := retainedDGUTAFilterCollapseDecision(request.Pattern, dgutaFilterCollapseReasonNoMeasurement)
	if err := validateDGUTAFilterCollapseMeasurement(request); err != nil {
		return decision, err
	}

	measurement := request.Measurement
	decision.Route = dgutaFilterRouteVector
	decision.Collapsed = true
	decision.Citation = strings.TrimSpace(measurement.Citation)
	decision.Reason = "Phase 7 D4 measurement proves in-query route meets latency gate"
	decision.InQueryP95MS = measurement.InQueryP95MS
	decision.LatencyGateMS = measurement.LatencyGateMS

	return decision, nil
}
