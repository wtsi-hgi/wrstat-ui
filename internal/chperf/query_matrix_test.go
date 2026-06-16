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

func queryMatrixRepresentativeOps() []string {
	return []string{
		queryOpTreeDirInfoName,
		queryOpDirInfosBroadName,
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
		report.AddOperationWithCounters(
			spec.Operation,
			map[string]any{
				queryInputQueryTypeKey:    spec.QueryType,
				queryInputQueryVariantKey: spec.QueryVariant,
				queryInputDurationSource:  querySourceClickHouseLog,
				queryInputResultDigest:    "sha256:digest",
			},
			[]float64{float64(i + 1)},
			[]uint64{1},
			[]uint64{2},
			[]uint64{3},
			[]uint64{4},
		)
	}

	return report
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
