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
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
)

const f3GlobCatalogProofExplain = "ReadFromMergeTree wrstat_dirs cd full_path ngrambf_v1 " +
	"ReadFromMergeTree wrstat_files PrimaryKey Keys: mount_path snapshot_id dir_id"

func TestF3GlobProof(t *testing.T) {
	Convey("full-path glob proof requires catalog path matching and dir-id file reads", t, func() {
		So(ExplainFindByGlobAvoidsFilePathScan(f3GlobCatalogProofExplain), ShouldBeTrue)
		So(ExplainFindByGlobAvoidsFilePathScan("wrstat_dirs full_path wrstat_files match(f.path, ?)"), ShouldBeFalse)
		So(ExplainFindByGlobAvoidsFilePathScan("wrstat_files f dir_id"), ShouldBeFalse)
		So(ExplainFindByGlobAvoidsFilePathScan("wrstat_dirs cd full_path"), ShouldBeFalse)
	})
}

type queryMatrixBaseDirsReader struct {
	groups  []*basedirs.Usage
	users   []*basedirs.Usage
	history []basedirs.History
}

func newQueryMatrixBaseDirsReader() *queryMatrixBaseDirsReader {
	now := time.Unix(1, 0).UTC()

	return &queryMatrixBaseDirsReader{
		groups: []*basedirs.Usage{{
			GID:       7,
			BaseDir:   queryOpTestRootDir,
			UsageSize: 100,
			Age:       db.DGUTAgeAll,
		}},
		users: []*basedirs.Usage{{
			UID:       9,
			BaseDir:   queryOpTestRootDir,
			UsageSize: 50,
			Age:       db.DGUTAgeAll,
		}},
		history: []basedirs.History{{
			Date:        now,
			UsageSize:   100,
			QuotaSize:   200,
			UsageInodes: 10,
			QuotaInodes: 20,
		}},
	}
}

func (r *queryMatrixBaseDirsReader) GroupUsage(db.DirGUTAge) ([]*basedirs.Usage, error) {
	return append([]*basedirs.Usage(nil), r.groups...), nil
}

func (r *queryMatrixBaseDirsReader) UserUsage(db.DirGUTAge) ([]*basedirs.Usage, error) {
	return append([]*basedirs.Usage(nil), r.users...), nil
}

func (*queryMatrixBaseDirsReader) GroupSubDirs(uint32, string, db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return []*basedirs.SubDir{{SubDir: queryOpTestChildADir, NumFiles: 2, SizeFiles: 20}}, nil
}

func (*queryMatrixBaseDirsReader) UserSubDirs(uint32, string, db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return []*basedirs.SubDir{{SubDir: queryOpTestChildBDir, NumFiles: 1, SizeFiles: 10}}, nil
}

func (r *queryMatrixBaseDirsReader) History(uint32, string) ([]basedirs.History, error) {
	return append([]basedirs.History(nil), r.history...), nil
}

func (*queryMatrixBaseDirsReader) SetMountPoints([]string) {}

func (*queryMatrixBaseDirsReader) SetCachedGroup(uint32, string) {}

func (*queryMatrixBaseDirsReader) SetCachedUser(uint32, string) {}

func (*queryMatrixBaseDirsReader) Info() (*basedirs.DBInfo, error) {
	return &basedirs.DBInfo{GroupDirCombos: 1, UserDirCombos: 1, GroupHistories: 1}, nil
}

func (*queryMatrixBaseDirsReader) MountTimestamps() (map[string]time.Time, error) {
	return map[string]time.Time{
		"／root": time.Unix(1, 0).UTC(),
	}, nil
}

func (*queryMatrixBaseDirsReader) Close() error {
	return nil
}

func queryMatrixTestContext() queryContext {
	database := newQueryOpTestDB()
	database.children["/"] = []string{queryOpTestRootDir}
	database.summaries["/"] = &db.DirSummary{Dir: "/", Count: 3}

	return queryContext{
		provider: fakeMountTimestampsProvider{
			tree: db.NewTree(database),
			bd:   newQueryMatrixBaseDirsReader(),
		},
		client: &fakeQueryClient{
			rows: []QueryRow{
				{Path: queryOpTestRootDir + "file.bam", Ext: queryTestBamExt, EntryType: 'f'},
				{Path: queryOpTestChildADir, EntryType: 'd'},
			},
		},
		inspector: fakeQueryInspector{
			measure: func(ctx context.Context, run func(context.Context) error) (*QueryMetrics, error) {
				if err := run(ctx); err != nil {
					return nil, err
				}

				return &QueryMetrics{
					DurationMs:  3,
					ReadRows:    4,
					ReadBytes:   5,
					ReadMarks:   6,
					ResultRows:  7,
					ResultBytes: 8,
				}, nil
			},
		},
		dir: queryOpTestRootDir,
	}
}

func TestJ4CanonicalMatrix(t *testing.T) {
	Convey("buildOps labels every canonical J4 query type", t, func() {
		ops := buildOps(queryMatrixTestContext(), queryMatrixTestOptions(), func(string, ...any) {})
		covered := make(map[string]int)
		required := make(map[j4RequiredMatrixOperation]bool)
		matrixRows := j4RequiredMatrixOperationSet()

		for _, op := range ops {
			queryType := stringInput(op.inputs, queryInputQueryTypeKey)
			if queryType != "" {
				covered[queryType]++
			}

			spec := j4RequiredMatrixOperation{
				QueryType:    queryType,
				Operation:    op.name,
				QueryVariant: stringInput(op.inputs, queryInputQueryVariantKey),
			}
			required[spec] = true
		}

		for _, queryType := range j4CanonicalQueryTypes() {
			So(covered[queryType], ShouldBeGreaterThan, 0)
		}

		for _, spec := range j4RequiredMatrixOperations() {
			So(required[spec], ShouldBeTrue)
		}

		for _, spec := range []j4RequiredMatrixOperation{
			{
				QueryType:    j4QueryTypeVirtual,
				Operation:    queryOpVirtualActivePrefixRollupName,
				QueryVariant: "active-prefix rollups",
			},
			{
				QueryType:    j4QueryTypeMaintenance,
				Operation:    queryOpImportReadinessPublishName,
				QueryVariant: "import readiness/publish",
			},
			{
				QueryType:    j4QueryTypeMaintenance,
				Operation:    queryOpActiveSnapshotCleanupName,
				QueryVariant: "active-snapshot cleanup",
			},
		} {
			So(matrixRows[spec], ShouldBeTrue)
			So(required[spec], ShouldBeTrue)
		}

		fullPathSpec := j4RequiredMatrixOperation{
			QueryType:    j4QueryTypeGlob,
			Operation:    queryOpGlobFullPathName,
			QueryVariant: "FindByGlob full-path",
		}
		So(required[fullPathSpec], ShouldBeTrue)

		for _, spec := range j4RequiredMatrixOperations() {
			So(spec, ShouldNotResemble, fullPathSpec)
		}
	})

	Convey("runSuite records metrics, result rows, and digest for selected J4 operations", t, func() {
		report := perfreport.NewReport("clickhouse", "", 1, 0)
		err := runSuite(
			&report,
			queryMatrixTestContext(),
			QueryOptions{
				Repeat: 1,
				Splits: 1,
				Ops:    queryMatrixRepresentativeOps(),
			},
			func(string, ...any) {},
		)

		So(err, ShouldBeNil)

		for _, name := range queryMatrixRepresentativeOps() {
			op, ok := j4FirstOperationNamed([]perfreport.Report{report}, name)
			So(ok, ShouldBeTrue)
			So(op.DurationsMS, ShouldHaveLength, 1)
			So(op.ReadRows, ShouldHaveLength, 1)
			So(op.ReadBytes, ShouldHaveLength, 1)
			So(op.ReadMarks, ShouldHaveLength, 1)
			So(op.ResultCount, ShouldNotBeEmpty)
			So(stringInput(op.Inputs, queryInputResultDigest), ShouldNotBeBlank)
		}
	})

	Convey("matrix validation fails when a type or required metric is missing", t, func() {
		report := queryMatrixCompleteReport()

		missingType := cloneJ4Report(report)
		missingType.Operations = missingType.Operations[:len(missingType.Operations)-1]
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingType}),
			ShouldContainSubstring,
			j4QueryTypeMaintenance,
		)

		missingCounters := cloneJ4Report(report)
		missingCounters.Operations[0].ReadRows = nil
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingCounters}),
			ShouldContainSubstring,
			"ReadRows",
		)

		missingDurationSamples := cloneJ4Report(report)
		missingDurationSamples.Operations[0].DurationsMS = nil
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingDurationSamples}),
			ShouldContainSubstring,
			"duration samples",
		)

		missingDigest := cloneJ4Report(report)
		delete(missingDigest.Operations[1].Inputs, queryInputResultDigest)
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingDigest}),
			ShouldContainSubstring,
			"result digest",
		)

		missingStatPath := cloneJ4Report(report)
		removeJ4ReportOperation(&missingStatPath, queryOpFilesStatPathName)
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingStatPath}),
			ShouldContainSubstring,
			queryOpFilesStatPathName,
		)

		missingFilteredDirInfo := cloneJ4Report(report)
		removeJ4ReportOperation(&missingFilteredDirInfo, queryOpDirInfoFilteredName)
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingFilteredDirInfo}),
			ShouldContainSubstring,
			queryOpDirInfoFilteredName,
		)

		missingActivePrefixRollup := cloneJ4Report(report)
		removeJ4ReportOperation(&missingActivePrefixRollup, queryOpVirtualActivePrefixRollupName)
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingActivePrefixRollup}),
			ShouldContainSubstring,
			queryOpVirtualActivePrefixRollupName,
		)

		missingImportReadinessPublish := cloneJ4Report(report)
		removeJ4ReportOperation(&missingImportReadinessPublish, queryOpImportReadinessPublishName)
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingImportReadinessPublish}),
			ShouldContainSubstring,
			queryOpImportReadinessPublishName,
		)

		missingActiveSnapshotCleanup := cloneJ4Report(report)
		removeJ4ReportOperation(&missingActiveSnapshotCleanup, queryOpActiveSnapshotCleanupName)
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingActiveSnapshotCleanup}),
			ShouldContainSubstring,
			queryOpActiveSnapshotCleanupName,
		)
	})

	Convey("matrix validation rejects synthetic virtual and maintenance evidence", t, func() {
		report := queryMatrixCompleteReport()

		wallDuration := cloneJ4Report(report)
		mutateJ4ReportOperation(&wallDuration, queryOpImportReadinessPublishName, func(op *perfreport.Operation) {
			op.Inputs[queryInputDurationSource] = querySourceWall
		})
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{wallDuration}),
			ShouldContainSubstring,
			"duration_source",
		)

		missingAuditCounts := cloneJ4Report(report)
		mutateJ4ReportOperation(&missingAuditCounts, queryOpActiveSnapshotCleanupName, func(op *perfreport.Operation) {
			delete(op.Inputs, queryInputAuditCounts)
		})
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingAuditCounts}),
			ShouldContainSubstring,
			queryInputAuditCounts,
		)

		missingAuditSurface := cloneJ4Report(report)
		mutateJ4ReportOperation(&missingAuditSurface, queryOpImportReadinessPublishName, func(op *perfreport.Operation) {
			op.Inputs[queryInputAuditSurfaces] = []string{
				tableSchema3SnapshotSets,
				queryAuditSurfaceActiveVirtualReady,
				tableActivePrefixRollupSets,
			}
		})
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{missingAuditSurface}),
			ShouldContainSubstring,
			queryAuditSurfaceMountEventsPublish,
		)

		unobservedRoute := cloneJ4Report(report)
		mutateJ4ReportOperation(&unobservedRoute, queryOpVirtualActivePrefixRollupName, func(op *perfreport.Operation) {
			op.Inputs[queryInputActivePrefixRouteProof] = queryActivePrefixRouteProofUnobserved
		})
		So(
			j4MatrixCoverageFailure([]perfreport.Report{report}, []perfreport.Report{unobservedRoute}),
			ShouldContainSubstring,
			queryInputActivePrefixRouteProof,
		)
	})

	Convey("matrix deltas report before/after p95 differences by operation variant", t, func() {
		baseline := queryMatrixCompleteReport()
		candidate := queryMatrixCompleteReport()
		candidate.Operations[0].P95MS = baseline.Operations[0].P95MS + 7

		deltas, err := j4MatrixDeltas([]perfreport.Report{baseline}, []perfreport.Report{candidate})

		So(err, ShouldBeNil)
		So(deltas, ShouldHaveLength, len(j4RequiredMatrixOperations()))
		So(deltas[0].QueryType, ShouldEqual, j4QueryTypeExactDirectory)
		So(deltas[0].Operation, ShouldEqual, queryOpTreeDirInfoName)
		So(deltas[0].QueryVariant, ShouldEqual, "DirInfo selected directory")
		So(deltas[0].DeltaP95MS, ShouldEqual, float64(7))
	})

	Convey("matrix deltas fail when result counts or digests differ", t, func() {
		baseline := queryMatrixCompleteReport()

		differentCount := queryMatrixCompleteReport()
		differentCount.Operations[0].ResultCount = []uint64{99, 99}
		_, err := j4MatrixDeltas([]perfreport.Report{baseline}, []perfreport.Report{differentCount})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "result rows mismatch")

		differentDigest := queryMatrixCompleteReport()
		differentDigest.Operations[0].Inputs[queryInputResultDigest] = "sha256:different"
		_, err = j4MatrixDeltas([]perfreport.Report{baseline}, []perfreport.Report{differentDigest})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "result digest mismatch")
	})

	Convey("active snapshot cleanup evidence accepts baseline and current schema cleanup surfaces", t, func() {
		baseline := queryMatrixCompleteReport()
		candidate := queryMatrixCompleteReport()

		queryMatrixSetActiveSnapshotCleanupEvidence(
			&baseline,
			queryMatrixJ4BaselineCleanupSurfaces(),
			queryMatrixJ4BaselineCleanupCounts(),
			"sha256:e1e57ce8d510d38b862d8784bd5637a61d0b05bff3e5c51f74e838f5a81b63b7",
		)
		queryMatrixSetActiveSnapshotCleanupEvidence(
			&candidate,
			queryMatrixJ4CurrentCleanupSurfaces(),
			queryMatrixJ4CurrentCleanupCounts(),
			"sha256:573541fae285acc57b15895d1b6fb957ad40e1d218b3a3ded3f203fcd781498c",
		)

		So(j4MatrixCoverageFailure([]perfreport.Report{baseline}, []perfreport.Report{candidate}), ShouldBeBlank)

		baselineCleanup, ok := j4FirstOperationNamed([]perfreport.Report{baseline}, queryOpActiveSnapshotCleanupName)
		So(ok, ShouldBeTrue)
		candidateCleanup, ok := j4FirstOperationNamed([]perfreport.Report{candidate}, queryOpActiveSnapshotCleanupName)
		So(ok, ShouldBeTrue)

		baselineDigest, baselineReason := activeSnapshotCleanupRoleDigestFromInputs(baselineCleanup.Inputs)
		candidateDigest, candidateReason := activeSnapshotCleanupRoleDigestFromInputs(candidateCleanup.Inputs)

		So(baselineReason, ShouldBeBlank)
		So(candidateReason, ShouldBeBlank)
		So(baselineDigest, ShouldEqual, candidateDigest)

		deltas, err := j4MatrixDeltas([]perfreport.Report{baseline}, []perfreport.Report{candidate})
		So(err, ShouldBeNil)
		So(deltas, ShouldHaveLength, len(j4RequiredMatrixOperations()))
	})

	Convey("active snapshot cleanup evidence rejects missing cleanup roles", t, func() {
		valid := queryMatrixCompleteReport()
		queryMatrixSetActiveSnapshotCleanupEvidence(
			&valid,
			queryMatrixJ4BaselineCleanupSurfaces(),
			queryMatrixJ4BaselineCleanupCounts(),
			"sha256:e1e57ce8d510d38b862d8784bd5637a61d0b05bff3e5c51f74e838f5a81b63b7",
		)

		missingBaselineRole := cloneJ4Report(valid)
		mutateJ4ReportOperation(&missingBaselineRole, queryOpActiveSnapshotCleanupName, func(op *perfreport.Operation) {
			counts := queryMatrixJ4BaselineCleanupCounts()
			delete(counts, "wrstat_parent_facts")
			op.Inputs[queryInputAuditCounts] = counts
		})
		So(
			j4MatrixCoverageFailure([]perfreport.Report{valid}, []perfreport.Report{missingBaselineRole}),
			ShouldContainSubstring,
			"wrstat_parent_facts",
		)

		current := queryMatrixCompleteReport()
		queryMatrixSetActiveSnapshotCleanupEvidence(
			&current,
			queryMatrixJ4CurrentCleanupSurfaces(),
			queryMatrixJ4CurrentCleanupCounts(),
			"sha256:573541fae285acc57b15895d1b6fb957ad40e1d218b3a3ded3f203fcd781498c",
		)
		missingCurrentRole := cloneJ4Report(current)
		mutateJ4ReportOperation(&missingCurrentRole, queryOpActiveSnapshotCleanupName, func(op *perfreport.Operation) {
			counts := queryMatrixJ4CurrentCleanupCounts()
			delete(counts, tableActiveVirtualDirs)
			op.Inputs[queryInputAuditCounts] = counts
		})
		So(
			j4MatrixCoverageFailure([]perfreport.Report{current}, []perfreport.Report{missingCurrentRole}),
			ShouldContainSubstring,
			tableActiveVirtualDirs,
		)
	})
}

func queryMatrixTestOptions() QueryOptions {
	return QueryOptions{
		Repeat:        2,
		Splits:        1,
		WalkDepth:     2,
		WalkLimit:     3,
		AncestorLimit: 3,
	}
}

func j4RequiredMatrixOperationSet() map[j4RequiredMatrixOperation]bool {
	rows := make(map[j4RequiredMatrixOperation]bool, len(j4RequiredMatrixOperations()))
	for _, spec := range j4RequiredMatrixOperations() {
		rows[spec] = true
	}

	return rows
}

func queryMatrixRepresentativeOps() []string {
	return []string{
		queryOpTreeDirInfoName,
		queryOpDirInfosBroadName,
		queryOpChildrenName,
		queryOpDirsHaveChildrenBroadName,
		queryOpTreeWhereName,
		queryOpTreeDiskTreeEndName,
		queryOpFilesListDirName,
		queryOpGlobCaseAName,
		queryOpVirtualDirInfoName,
		queryOpBasedirsGroupUsageName,
		queryOpInfoName,
	}
}

func queryMatrixCompleteReport() perfreport.Report {
	report := perfreport.NewReport("clickhouse", "", 1, 0)

	for i, spec := range j4RequiredMatrixOperations() {
		inputs := map[string]any{
			queryInputQueryTypeKey:    spec.QueryType,
			queryInputQueryVariantKey: spec.QueryVariant,
			queryInputDurationSource:  querySourceClickHouseLog,
			queryInputResultDigest:    "sha256:digest",
		}
		queryMatrixAddRequiredEvidence(inputs, spec.Operation)

		report.AddOperationWithCounters(
			spec.Operation,
			inputs,
			[]float64{float64(i + 1)},
			[]uint64{1},
			[]uint64{2},
			[]uint64{3},
			[]uint64{4},
		)
	}

	return report
}

func queryMatrixAddRequiredEvidence(inputs map[string]any, operation string) {
	switch operation {
	case queryOpTreeWhereColdProviderName:
		inputs[queryInputCacheScope] = queryScopeColdProvider
	case queryOpTreeWhereFreshName:
		inputs[queryInputCacheScope] = queryScopeFreshProvider
	case queryOpTreeWhereProviderUpdateName:
		inputs[queryInputCacheScope] = queryScopeProviderUpdateCold
	case queryOpImportReadinessPublishName:
		queryMatrixAddAuditEvidence(inputs, []string{
			tableSchema3SnapshotSets,
			queryAuditSurfaceActiveVirtualReady,
			tableActivePrefixRollupSets,
			queryAuditSurfaceMountEventsPublish,
		})
	case queryOpActiveSnapshotCleanupName:
		queryMatrixAddAuditEvidence(inputs, activeSnapshotCleanupSurfaces())
	case queryOpVirtualActivePrefixRollupName:
		inputs[queryInputActivePrefixRouteProof] = queryActivePrefixRollupRouteProofRead
		inputs[queryInputActivePrefixScalarRootRows] = uint64(1)
	}
}

func queryMatrixAddAuditEvidence(inputs map[string]any, surfaces []string) {
	inputs[queryInputAuditSurfaces] = append([]string(nil), surfaces...)

	counts := make(map[string]uint64, len(surfaces))
	for index, surface := range surfaces {
		counts[surface] = uint64(index + 1)
	}

	inputs[queryInputAuditCounts] = counts
}

func cloneJ4Report(report perfreport.Report) perfreport.Report {
	report.Operations = append([]perfreport.Operation(nil), report.Operations...)
	for i := range report.Operations {
		op := &report.Operations[i]
		op.Inputs = cloneAnyMap(op.Inputs)
		op.DurationsMS = append([]float64(nil), op.DurationsMS...)
		op.ReadRows = append([]uint64(nil), op.ReadRows...)
		op.ReadBytes = append([]uint64(nil), op.ReadBytes...)
		op.ReadMarks = append([]uint64(nil), op.ReadMarks...)
		op.ResultCount = append([]uint64(nil), op.ResultCount...)
	}

	return report
}

func cloneAnyMap(src map[string]any) map[string]any {
	out := make(map[string]any, len(src))
	for key, value := range src {
		out[key] = value
	}

	return out
}

func removeJ4ReportOperation(report *perfreport.Report, name string) {
	for i := range report.Operations {
		if report.Operations[i].Name == name {
			report.Operations = append(report.Operations[:i], report.Operations[i+1:]...)

			return
		}
	}
}

func queryMatrixSetActiveSnapshotCleanupEvidence(
	report *perfreport.Report,
	surfaces []string,
	counts map[string]uint64,
	digest string,
) {
	mutateJ4ReportOperation(report, queryOpActiveSnapshotCleanupName, func(op *perfreport.Operation) {
		op.Inputs[queryInputAuditSurfaces] = surfaces
		op.Inputs[queryInputAuditCounts] = counts
		op.Inputs[queryInputResultDigest] = digest
		op.ResultCount = []uint64{uint64(len(surfaces))}
	})
}

func mutateJ4ReportOperation(
	report *perfreport.Report,
	name string,
	mutate func(*perfreport.Operation),
) {
	for i := range report.Operations {
		if report.Operations[i].Name == name {
			mutate(&report.Operations[i])

			return
		}
	}
}

func queryMatrixJ4BaselineCleanupSurfaces() []string {
	return []string{
		tableMountEvents,
		tableFiles,
		"wrstat_children",
		"wrstat_parent_facts",
		tableDirFacts,
		tableDirSummarySets,
		tableDirFilterAgeAll,
		tableChildFilterAll,
		tableDirFilterAll,
		tableSchema3SnapshotSets,
		tableBasedirsGroupUsage,
		tableBasedirsUserUsage,
		tableBasedirsGroupSubdirs,
		tableBasedirsUserSubdirs,
		tableActiveVirtualSummaries,
		tableActiveVirtualFilterAll,
		tableActiveVirtualChildren,
		tableActiveVirtualSets,
		tableActivePrefixRollups,
		tableActivePrefixFilterAgeAll,
		tableActivePrefixRollupSets,
	}
}

func queryMatrixJ4BaselineCleanupCounts() map[string]uint64 {
	return map[string]uint64{
		tableActivePrefixFilterAgeAll: 51,
		tableActivePrefixRollupSets:   1,
		tableActivePrefixRollups:      3,
		tableActiveVirtualChildren:    4,
		tableActiveVirtualFilterAll:   658,
		tableActiveVirtualSets:        1,
		tableActiveVirtualSummaries:   5,
		tableBasedirsGroupSubdirs:     202,
		tableBasedirsGroupUsage:       60,
		tableBasedirsUserSubdirs:      229,
		tableBasedirsUserUsage:        87,
		tableChildFilterAll:           712009,
		"wrstat_children":             36149,
		tableDirFacts:                 36151,
		tableDirFilterAgeAll:          72434,
		tableDirFilterAll:             712009,
		tableDirSummarySets:           2,
		tableFiles:                    200000,
		tableMountEvents:              2,
		"wrstat_parent_facts":         36151,
		tableSchema3SnapshotSets:      2,
	}
}

func queryMatrixJ4CurrentCleanupSurfaces() []string {
	return []string{
		tableMountEvents,
		tableCatalog,
		tableFiles,
		tableSchema3SnapshotSets,
		tableDirFacts,
		tableDirSummarySets,
		tableDirFilterAgeAll,
		tableChildFilterAll,
		tableDirFilterAll,
		tableBasedirsGroupUsage,
		tableBasedirsUserUsage,
		tableBasedirsGroupSubdirs,
		tableBasedirsUserSubdirs,
		tableActiveVirtualDirs,
		tableActiveVirtualSummaries,
		tableActiveVirtualFilterAll,
		tableActiveVirtualChildren,
		tableActiveVirtualSets,
		tableActivePrefixRollups,
		tableActivePrefixFilterAgeAll,
		tableActivePrefixRollupSets,
	}
}

func queryMatrixJ4CurrentCleanupCounts() map[string]uint64 {
	return map[string]uint64{
		tableActivePrefixFilterAgeAll: 51,
		tableActivePrefixRollupSets:   1,
		tableActivePrefixRollups:      3,
		tableActiveVirtualChildren:    4,
		tableActiveVirtualDirs:        5,
		tableActiveVirtualFilterAll:   658,
		tableActiveVirtualSets:        1,
		tableActiveVirtualSummaries:   5,
		tableBasedirsGroupSubdirs:     202,
		tableBasedirsGroupUsage:       60,
		tableBasedirsUserSubdirs:      229,
		tableBasedirsUserUsage:        87,
		tableChildFilterAll:           711569,
		tableDirFacts:                 36151,
		tableDirFilterAgeAll:          72382,
		tableDirFilterAll:             711569,
		tableDirSummarySets:           2,
		tableCatalog:                  36151,
		tableFiles:                    200000,
		tableMountEvents:              2,
		tableSchema3SnapshotSets:      2,
	}
}
