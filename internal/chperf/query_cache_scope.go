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
	"slices"

	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const j5MissingOperation = "missing before/after operation"

var errJ5CacheScopeCoverage = errors.New("j5 cache scope coverage failed")

type j5CacheScopeDelta struct {
	CacheScope                    string
	BaselineOperation             string
	CandidateOperation            string
	BaselineP95MS                 float64
	CandidateP95MS                float64
	DeltaP95MS                    float64
	BaselineRepeats               int
	CandidateRepeats              int
	BaselineHighFanoutChildCount  uint64
	CandidateHighFanoutChildCount uint64
}

func j5DeltaForScope(
	scope string,
	before perfreport.Operation,
	after perfreport.Operation,
) j5CacheScopeDelta {
	return j5CacheScopeDelta{
		CacheScope:                    scope,
		BaselineOperation:             before.Name,
		CandidateOperation:            after.Name,
		BaselineP95MS:                 before.P95MS,
		CandidateP95MS:                after.P95MS,
		DeltaP95MS:                    after.P95MS - before.P95MS,
		BaselineRepeats:               len(before.DurationsMS),
		CandidateRepeats:              len(after.DurationsMS),
		BaselineHighFanoutChildCount:  j5HighFanoutChildCount(before),
		CandidateHighFanoutChildCount: j5HighFanoutChildCount(after),
	}
}

func j5CacheScopeDeltas(
	baseline []perfreport.Report,
	candidate []perfreport.Report,
) ([]j5CacheScopeDelta, error) {
	if reason := j5CacheScopeCoverageFailure(baseline, candidate); reason != "" {
		return nil, fmt.Errorf("%w: %s", errJ5CacheScopeCoverage, reason)
	}

	deltas := make([]j5CacheScopeDelta, 0, len(j5RequiredCacheScopes()))
	for _, scope := range j5RequiredCacheScopes() {
		before, _ := j5FirstScopeOperation(baseline, scope)
		after, _ := j5FirstScopeOperation(candidate, scope)
		deltas = append(deltas, j5DeltaForScope(scope, before, after))
	}

	return deltas, nil
}

func j5CacheScopeCoverageFailure(baseline []perfreport.Report, candidate []perfreport.Report) string {
	for _, group := range []struct {
		name    string
		reports []perfreport.Report
	}{
		{"baseline", baseline},
		{"candidate", candidate},
	} {
		for _, scope := range j5RequiredCacheScopes() {
			if _, ok, reason := j5FirstValidScopeOperation(group.reports, scope); !ok {
				return fmt.Sprintf("%s %s %s", group.name, scope, reason)
			}
		}

		if reason := j5HighFanoutRepeatedReadFailure(group.reports); reason != "" {
			return group.name + " " + reason
		}
	}

	return ""
}

func j5RequiredCacheScopes() []string {
	return []string{
		queryScopeFreshProvider,
		queryScopeColdProvider,
		queryScopeSameProviderCold,
		queryScopeSameQueryClient,
		queryScopeAncestorDirs,
		queryScopeNewDirEachRepeat,
		queryScopeVisibleChildDirs,
		queryScopeStartupAudit,
		queryScopeProviderUpdateCold,
		queryScopeSameProviderDir,
	}
}

func j5FirstValidScopeOperation(
	reports []perfreport.Report,
	scope string,
) (perfreport.Operation, bool, string) {
	ops := j5OperationsOfScope(reports, scope)
	firstReason := j5MissingOperation

	for _, op := range ops {
		reason := j5OperationMetricsFailure(op, scope)
		if reason == "" {
			return op, true, ""
		}

		if firstReason == j5MissingOperation {
			firstReason = reason
		}
	}

	return perfreport.Operation{}, false, firstReason
}

func j5OperationsOfScope(reports []perfreport.Report, scope string) []perfreport.Operation {
	var ops []perfreport.Operation

	for _, report := range reports {
		for _, op := range report.Operations {
			if stringInput(op.Inputs, queryInputCacheScope) == scope {
				ops = append(ops, op)
			}
		}
	}

	return ops
}

func j5OperationMetricsFailure(op perfreport.Operation, scope string) string {
	checks := []struct {
		reason  string
		missing bool
	}{
		{"missing duration samples", len(op.DurationsMS) == 0},
		{"missing result rows", len(op.ResultCount) == 0},
		{"unstable result rows", !j5ResultCountsStable(op.ResultCount)},
		{"missing repeated samples", scope != queryScopeStartupAudit && len(op.DurationsMS) < 2},
	}

	for _, check := range checks {
		if check.missing {
			return check.reason
		}
	}

	return ""
}

func j5ResultCountsStable(counts []uint64) bool {
	return len(counts) > 0 && allUint64Equal(counts, counts[0])
}

func j5HighFanoutRepeatedReadFailure(reports []perfreport.Report) string {
	for _, op := range j5AllOperations(reports) {
		scope := stringInput(op.Inputs, queryInputCacheScope)
		if !slices.Contains(j5RepeatedReadCacheScopes(), scope) {
			continue
		}

		if j5HighFanoutChildCount(op) == 0 {
			continue
		}

		if j5OperationMetricsFailure(op, scope) == "" {
			return ""
		}
	}

	return "missing high-fanout repeated-read evidence"
}

func j5AllOperations(reports []perfreport.Report) []perfreport.Operation {
	var ops []perfreport.Operation

	for _, report := range reports {
		ops = append(ops, report.Operations...)
	}

	return ops
}

func j5RepeatedReadCacheScopes() []string {
	return []string{
		queryScopeFreshProvider,
		queryScopeColdProvider,
		queryScopeSameProviderCold,
		queryScopeSameQueryClient,
		queryScopeAncestorDirs,
		queryScopeNewDirEachRepeat,
		queryScopeVisibleChildDirs,
		queryScopeProviderUpdateCold,
		queryScopeSameProviderDir,
	}
}

func j5HighFanoutChildCount(op perfreport.Operation) uint64 {
	return uint64Input(op.Inputs, queryInputHighFanoutChildCount)
}

func j5FirstScopeOperation(reports []perfreport.Report, scope string) (perfreport.Operation, bool) {
	op, ok, _ := j5FirstValidScopeOperation(reports, scope)

	return op, ok
}
