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

package chperf

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const (
	queryInputQueryTypeKey    = "query_type"
	queryInputQueryVariantKey = "query_variant"

	j4MissingOperation = "missing before/after operation"

	j4QueryTypeExactDirectory = "Exact directory"
	j4QueryTypeBatchDirectory = "Batch directory"
	j4QueryTypeChildren       = "Children/presence"
	j4QueryTypeSubtree        = "Subtree/recursive"
	j4QueryTypeDisktree       = "Disktree"
	j4QueryTypeFileAPI        = "File API"
	j4QueryTypeGlob           = "Glob/full-text"
	j4QueryTypeVirtual        = "Virtual/active"
	j4QueryTypeBasedirs       = "Basedirs/quota"
	j4QueryTypeMaintenance    = "Maintenance"
)

var errJ4MatrixCoverage = errors.New("j4 matrix coverage failed")

func j4Inputs(queryType string, variant string, inputs map[string]any) map[string]any {
	if inputs == nil {
		inputs = map[string]any{}
	}

	inputs[queryInputQueryTypeKey] = queryType
	inputs[queryInputQueryVariantKey] = variant

	return inputs
}

type j4MatrixDelta struct {
	QueryType      string
	Operation      string
	BaselineP50MS  float64
	CandidateP50MS float64
	DeltaP50MS     float64
	BaselineP95MS  float64
	CandidateP95MS float64
	DeltaP95MS     float64
	BaselineP99MS  float64
	CandidateP99MS float64
	DeltaP99MS     float64
}

func j4MatrixDeltas(
	baseline []perfreport.Report,
	candidate []perfreport.Report,
) ([]j4MatrixDelta, error) {
	if reason := j4MatrixCoverageFailure(baseline, candidate); reason != "" {
		return nil, fmt.Errorf("%w: %s", errJ4MatrixCoverage, reason)
	}

	if reason := j4MatrixCorrectnessFailure(baseline, candidate); reason != "" {
		return nil, fmt.Errorf("%w: %s", errJ4MatrixCoverage, reason)
	}

	deltas := make([]j4MatrixDelta, 0, len(j4CanonicalQueryTypes()))
	for _, queryType := range j4CanonicalQueryTypes() {
		before, _ := j4FirstOperationOfType(baseline, queryType)
		after, _ := j4FirstOperationOfType(candidate, queryType)
		deltas = append(deltas, j4MatrixDelta{
			QueryType:      queryType,
			Operation:      after.Name,
			BaselineP50MS:  before.P50MS,
			CandidateP50MS: after.P50MS,
			DeltaP50MS:     after.P50MS - before.P50MS,
			BaselineP95MS:  before.P95MS,
			CandidateP95MS: after.P95MS,
			DeltaP95MS:     after.P95MS - before.P95MS,
			BaselineP99MS:  before.P99MS,
			CandidateP99MS: after.P99MS,
			DeltaP99MS:     after.P99MS - before.P99MS,
		})
	}

	return deltas, nil
}

func j4MatrixCoverageFailure(baseline []perfreport.Report, candidate []perfreport.Report) string {
	for _, queryType := range j4CanonicalQueryTypes() {
		if _, ok, beforeReason := j4FirstValidOperationOfType(baseline, queryType); !ok {
			return fmt.Sprintf("%s baseline %s", queryType, beforeReason)
		}

		if _, ok, afterReason := j4FirstValidOperationOfType(candidate, queryType); !ok {
			return fmt.Sprintf("%s candidate %s", queryType, afterReason)
		}
	}

	return ""
}

func j4CanonicalQueryTypes() []string {
	return []string{
		j4QueryTypeExactDirectory,
		j4QueryTypeBatchDirectory,
		j4QueryTypeChildren,
		j4QueryTypeSubtree,
		j4QueryTypeDisktree,
		j4QueryTypeFileAPI,
		j4QueryTypeGlob,
		j4QueryTypeVirtual,
		j4QueryTypeBasedirs,
		j4QueryTypeMaintenance,
	}
}

func j4FirstValidOperationOfType(
	reports []perfreport.Report,
	queryType string,
) (perfreport.Operation, bool, string) {
	ops := j4OperationsOfType(reports, queryType)
	firstReason := j4MissingOperation

	for _, op := range ops {
		reason := j4OperationMetricsFailure(op)
		if reason == "" {
			return op, true, ""
		}

		if firstReason == j4MissingOperation {
			firstReason = reason
		}
	}

	return perfreport.Operation{}, false, firstReason
}

func j4OperationsOfType(reports []perfreport.Report, queryType string) []perfreport.Operation {
	var ops []perfreport.Operation

	for _, report := range reports {
		for _, op := range report.Operations {
			if stringInput(op.Inputs, queryInputQueryTypeKey) == queryType {
				ops = append(ops, op)
			}
		}
	}

	return ops
}

func j4OperationMetricsFailure(op perfreport.Operation) string {
	checks := []struct {
		reason  string
		missing bool
	}{
		{"missing duration samples", len(op.DurationsMS) == 0},
		{"missing ReadRows", len(op.ReadRows) == 0},
		{"missing ReadBytes", len(op.ReadBytes) == 0},
		{"missing ReadMarks", len(op.ReadMarks) == 0},
		{"missing result rows", len(op.ResultCount) == 0},
		{"missing result digest", stringInput(op.Inputs, queryInputResultDigest) == ""},
		{"missing p50/p95/p99", op.P50MS <= 0 || op.P95MS <= 0 || op.P99MS <= 0},
	}

	for _, check := range checks {
		if check.missing {
			return check.reason
		}
	}

	return ""
}

func j4MatrixCorrectnessFailure(baseline []perfreport.Report, candidate []perfreport.Report) string {
	for _, queryType := range j4CanonicalQueryTypes() {
		before, _ := j4FirstOperationOfType(baseline, queryType)
		after, _ := j4FirstOperationOfType(candidate, queryType)

		if !j4ResultCountsEquivalent(before.ResultCount, after.ResultCount) {
			return queryType + " result rows mismatch"
		}

		beforeDigest := stringInput(before.Inputs, queryInputResultDigest)

		afterDigest := stringInput(after.Inputs, queryInputResultDigest)
		if beforeDigest != afterDigest {
			return queryType + " result digest mismatch"
		}
	}

	return ""
}

func j4FirstOperationOfType(
	reports []perfreport.Report,
	queryType string,
) (perfreport.Operation, bool) {
	op, ok, _ := j4FirstValidOperationOfType(reports, queryType)

	return op, ok
}

func j4ResultCountsEquivalent(before []uint64, after []uint64) bool {
	if len(before) == 0 || len(after) == 0 {
		return false
	}

	beforeCount := before[0]

	afterCount := after[0]
	if beforeCount != afterCount {
		return false
	}

	return allUint64Equal(before, beforeCount) && allUint64Equal(after, afterCount)
}

func allUint64Equal(values []uint64, want uint64) bool {
	for _, value := range values {
		if value != want {
			return false
		}
	}

	return true
}

// ExplainFindByGlobAvoidsFilePathScan reports whether a full-path glob plan
// uses the directory catalog and reads files by dir_id rather than file path
// text.
func ExplainFindByGlobAvoidsFilePathScan(explain string) bool {
	normal := strings.ToLower(strings.Join(strings.Fields(explain), " "))
	if !strings.Contains(normal, "wrstat_dirs") {
		return false
	}

	filesReadByDirID := strings.Contains(normal, "wrstat_files") &&
		strings.Contains(normal, "keys: mount_path snapshot_id dir_id")
	if !filesReadByDirID {
		return false
	}

	filePathNeedles := []string{
		"f.path",
		"wrstat_files.path",
		"wrstat_files.full_path",
		"match(path",
	}

	return !containsAny(normal, filePathNeedles)
}

func containsAny(value string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}

	return false
}
