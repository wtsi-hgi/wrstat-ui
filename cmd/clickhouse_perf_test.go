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

package cmd

import (
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/provider"
)

const chPerfTestT283Dir = "/nfs/t283_imaging/"

func TestClickHousePerfQueryFlags(t *testing.T) {
	Convey("clickhouse-perf query exposes tree benchmark controls", t, func() {
		flags := chPerfQueryCmd.Flags()

		So(flags.Lookup("warmup"), ShouldNotBeNil)
		So(flags.Lookup("splits"), ShouldNotBeNil)
		So(flags.Lookup("walk-depth"), ShouldNotBeNil)
		So(flags.Lookup("walk-limit"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-dir"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-limit"), ShouldNotBeNil)
		So(flags.Lookup("ops"), ShouldNotBeNil)
		So(flags.Lookup("tree-gids"), ShouldNotBeNil)
		So(flags.Lookup("tree-uids"), ShouldNotBeNil)
		So(flags.Lookup("tree-types"), ShouldNotBeNil)
		So(flags.Lookup("tree-ft"), ShouldNotBeNil)
	})

	Convey("clickhouse-perf rest exposes REST and CLI/server harness controls", t, func() {
		flags := chPerfRestCmd.Flags()

		So(flags.Lookup("tree-path"), ShouldNotBeNil)
		So(flags.Lookup("where-dir"), ShouldNotBeNil)
		So(flags.Lookup("groups"), ShouldNotBeNil)
		So(flags.Lookup("users"), ShouldNotBeNil)
		So(flags.Lookup("types"), ShouldNotBeNil)
		So(flags.Lookup("repeat"), ShouldNotBeNil)
		So(flags.Lookup("warmup"), ShouldNotBeNil)
		So(flags.Lookup("splits"), ShouldNotBeNil)
	})

	Convey("bolt-perf query exposes tree benchmark controls", t, func() {
		flags := boltPerfQueryCmd.Flags()

		So(flags.Lookup("warmup"), ShouldNotBeNil)
		So(flags.Lookup("splits"), ShouldNotBeNil)
		So(flags.Lookup("walk-depth"), ShouldNotBeNil)
		So(flags.Lookup("walk-limit"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-dir"), ShouldNotBeNil)
		So(flags.Lookup("ancestor-limit"), ShouldNotBeNil)
		So(flags.Lookup("ops"), ShouldNotBeNil)
		So(flags.Lookup("tree-gids"), ShouldNotBeNil)
		So(flags.Lookup("tree-uids"), ShouldNotBeNil)
		So(flags.Lookup("tree-types"), ShouldNotBeNil)
		So(flags.Lookup("tree-ft"), ShouldNotBeNil)
	})
}

type chPerfRestTestBaseDirs struct{}

func (b *chPerfRestTestBaseDirs) GroupUsage(db.DirGUTAge) ([]*basedirs.Usage, error) {
	return nil, nil
}

func (b *chPerfRestTestBaseDirs) UserUsage(db.DirGUTAge) ([]*basedirs.Usage, error) {
	return nil, nil
}

func (b *chPerfRestTestBaseDirs) GroupSubDirs(
	uint32,
	string,
	db.DirGUTAge,
) ([]*basedirs.SubDir, error) {
	return nil, nil
}

func (b *chPerfRestTestBaseDirs) UserSubDirs(
	uint32,
	string,
	db.DirGUTAge,
) ([]*basedirs.SubDir, error) {
	return nil, nil
}

func (b *chPerfRestTestBaseDirs) History(uint32, string) ([]basedirs.History, error) {
	return nil, nil
}

func (b *chPerfRestTestBaseDirs) SetMountPoints([]string) {}

func (b *chPerfRestTestBaseDirs) SetCachedGroup(uint32, string) {}

func (b *chPerfRestTestBaseDirs) SetCachedUser(uint32, string) {}

func (b *chPerfRestTestBaseDirs) Info() (*basedirs.DBInfo, error) {
	return &basedirs.DBInfo{}, nil
}

func (b *chPerfRestTestBaseDirs) MountTimestamps() (map[string]time.Time, error) {
	return map[string]time.Time{"/": time.Unix(1, 0).UTC()}, nil
}

func (b *chPerfRestTestBaseDirs) Close() error {
	return nil
}

type chPerfRestTestProvider struct {
	tree *db.Tree
	bd   *chPerfRestTestBaseDirs
}

func newCHPerfRestTestProvider() *chPerfRestTestProvider {
	return &chPerfRestTestProvider{
		tree: db.NewTree(&chPerfRestTestDB{}),
		bd:   &chPerfRestTestBaseDirs{},
	}
}

func (p *chPerfRestTestProvider) Tree() *db.Tree {
	return p.tree
}

func (p *chPerfRestTestProvider) BaseDirs() basedirs.Reader {
	return p.bd
}

func (p *chPerfRestTestProvider) OnUpdate(func()) {}

func (p *chPerfRestTestProvider) OnError(func(error)) {}

func (p *chPerfRestTestProvider) Close() error {
	p.tree.Close()

	return p.bd.Close()
}

func TestClickHousePerfRestCounterSources(t *testing.T) {
	Convey("clickhouse-perf rest wires real counter sources into the public report path", t, func() {
		resetClickHousePerfConnectionForTest()

		chPerf.dsn = "clickhouse://example"
		chPerf.database = "wrstat"
		chPerf.repeat = 1
		chPerf.warmup = 0
		chPerf.restTreePath = "/"
		chPerf.restWhereDir = chPerfTestT283Dir
		chPerf.restTypes = db.DGUTAFileTypeOther.String()

		sourceClosed := false
		cacheHitKeyCalls := 0
		priorCacheHitKey := "active_prefix_summary:path=/prior/;filter=ft:1;" +
			"active_set_id=old;query_version=1"
		scopedCacheHitKey := "active_prefix_summary:path=/nfs/t283_imaging/;filter=ft:32768;" +
			"active_set_id=e2-active-set;query_version=1"

		replaceCHPerfRestHooksForTest(
			func(clickhouse.Config) (provider.Provider, error) {
				return newCHPerfRestTestProvider(), nil
			},
			func(clickhouse.Config) (chPerfRestCounterSources, error) {
				var queries, hits, misses uint64

				return chPerfRestCounterSources{
					QueryCount: func() uint64 {
						queries++

						return queries
					},
					CacheStats: func() (uint64, uint64) {
						hits++
						misses += 2

						return hits, misses
					},
					CacheHitKeys: func() []string {
						cacheHitKeyCalls++
						if cacheHitKeyCalls%2 == 1 {
							return []string{priorCacheHitKey}
						}

						return []string{priorCacheHitKey, scopedCacheHitKey}
					},
					QueryCountSource: "test.system.events.Query_delta",
					CacheStatsSource: "test.tree_query_cache_delta",
					Close: func() error {
						sourceClosed = true

						return nil
					},
				}, nil
			},
		)

		report, err := chPerfRestReport(clickhouse.Config{
			DSN:          chPerf.dsn,
			Database:     chPerf.database,
			QueryTimeout: time.Second,
		})

		So(err, ShouldBeNil)
		So(sourceClosed, ShouldBeTrue)

		restTree := chPerfRestReportOperation(report, "rest_tree")
		So(restTree.Inputs["query_count"], ShouldResemble, []uint64{1})
		So(restTree.Inputs["cache_hits"], ShouldResemble, []uint64{1})
		So(restTree.Inputs["cache_misses"], ShouldResemble, []uint64{2})
		So(restTree.Inputs["query_count_source"], ShouldEqual, "test.system.events.Query_delta")
		So(restTree.Inputs["cache_counter_source"], ShouldEqual, "test.tree_query_cache_delta")

		restWhere := chPerfRestReportOperation(report, "rest_where")
		So(restWhere.Inputs["cache_hit_keys"], ShouldResemble, []string{scopedCacheHitKey})
		So(restWhere.Inputs["cache_hit_keys"], ShouldNotContain, priorCacheHitKey)
		So(scopedCacheHitKey, ShouldContainSubstring, "path=/nfs/t283_imaging/")
		So(scopedCacheHitKey, ShouldContainSubstring, "filter=")
		So(scopedCacheHitKey, ShouldContainSubstring, "active_set_id=")
		So(scopedCacheHitKey, ShouldContainSubstring, "query_version=1")
	})
}

func replaceCHPerfRestHooksForTest(
	openProvider func(clickhouse.Config) (provider.Provider, error),
	openSources func(clickhouse.Config) (chPerfRestCounterSources, error),
) {
	origOpenProvider := chPerfOpenProvider
	origOpenSources := chPerfOpenRestCounterSources

	chPerfOpenProvider = openProvider
	chPerfOpenRestCounterSources = openSources

	Reset(func() {
		chPerfOpenProvider = origOpenProvider
		chPerfOpenRestCounterSources = origOpenSources
	})
}

func chPerfRestReportOperation(report perfreport.Report, name string) perfreport.Operation {
	for _, op := range report.Operations {
		if op.Name == name {
			return op
		}
	}

	return perfreport.Operation{}
}

type chPerfRestTestDB struct{}

func (d *chPerfRestTestDB) DirInfo(dir string, _ *db.Filter) (*db.DirSummary, error) {
	return &db.DirSummary{
		Dir:   dir,
		Count: 1,
		GIDs:  []uint32{1},
		Age:   db.DGUTAgeAll,
	}, nil
}

func (d *chPerfRestTestDB) Children(dir string) ([]string, error) {
	if dir != "/" {
		return nil, nil
	}

	return []string{"/child/"}, nil
}

func (d *chPerfRestTestDB) DirsHaveChildren(dirs []string, _ *db.Filter) (map[string]bool, error) {
	result := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		result[dir] = false
	}

	return result, nil
}

func (d *chPerfRestTestDB) Where(dir string, _ *db.Filter, _ func(string) int) (db.DCSs, error) {
	return db.DCSs{{Dir: dir, Count: 1, Age: db.DGUTAgeAll}}, nil
}

func (d *chPerfRestTestDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *chPerfRestTestDB) Close() error {
	return nil
}

func TestE3PerfDocumentedCommandFlags(t *testing.T) {
	Convey("E3 clickhouse-perf import/query/rest expose documented long flags", t, func() {
		assertDocumentedLongFlags(chPerfImportCmd,
			"clickhouse-dsn", "clickhouse-database", "owners", "mounts",
			"maxLines", "batchSize", "parallelism", "json",
		)
		assertDocumentedLongFlags(chPerfQueryCmd,
			"clickhouse-dsn", "clickhouse-database", "owners", "mounts",
			"repeat", "warmup", "uid", "gids", "dir", "tree-gids",
			"tree-uids", "tree-types", "json",
		)
		assertDocumentedLongFlags(chPerfRestCmd,
			"clickhouse-dsn", "clickhouse-database", "owners", "mounts",
			"base-url", "repeat", "warmup", "paths", "where-dir",
			"tree-gids", "tree-uids", "tree-types", "json",
		)
	})

	Convey("E3 bolt-perf import/query expose documented long flags", t, func() {
		assertDocumentedLongFlags(boltPerfImportCmd,
			"out", "quota", "config", "owners", "mounts", "max-lines", "json",
		)
		assertDocumentedLongFlags(boltPerfQueryCmd,
			"owners", "mounts", "repeat", "warmup", "dir", "tree-gids",
			"tree-uids", "tree-types", "json",
		)
	})

	Convey("E3 clickhouse-perf rest maps paths and tree filter flags to harness options", t, func() {
		resetClickHousePerfConnectionForTest()

		chPerf.restBaseURL = "http://127.0.0.1:8080"
		chPerf.restTreePaths = []string{"/", "/lustre/", "/nfs/", chPerfTestT283Dir}
		chPerf.restWhereDir = chPerfTestT283Dir
		chPerf.treeGIDs = "14976"
		chPerf.treeUIDs = "20155"
		chPerf.treeTypes = "other"
		chPerf.repeat = 20
		chPerf.warmup = 0

		opts := chPerfRestOptions()

		So(opts.BaseURL, ShouldEqual, "http://127.0.0.1:8080")
		So(opts.TreePaths, ShouldResemble, []string{"/", "/lustre/", "/nfs/", chPerfTestT283Dir})
		So(opts.WhereDir, ShouldEqual, chPerfTestT283Dir)
		So(opts.WhereGroups, ShouldEqual, "14976")
		So(opts.WhereUsers, ShouldEqual, "20155")
		So(opts.WhereTypes, ShouldEqual, "other")
		So(opts.Repeat, ShouldEqual, 20)
		So(opts.Warmup, ShouldEqual, 0)
	})
}

func assertDocumentedLongFlags(cmd *cobra.Command, names ...string) {
	for _, name := range names {
		So(cmd.Flag(name), ShouldNotBeNil)
	}
}

func TestClickHousePerfRequiresConnectionSettings(t *testing.T) {
	Convey("clickhouse-perf import reports missing DSN and database before input work", t, func() {
		resetClickHousePerfConnectionForTest()

		err := runCHPerfImport("/definitely/not/read")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errClickhouseDSNRequired.Error())
		So(err.Error(), ShouldContainSubstring, errClickhouseDatabaseRequired.Error())
		So(err.Error(), ShouldNotContainSubstring, "definitely/not/read")
	})

	Convey("clickhouse-perf query reports missing DSN and database before running operations", t, func() {
		resetClickHousePerfConnectionForTest()

		err := runCHPerfQuery()

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errClickhouseDSNRequired.Error())
		So(err.Error(), ShouldContainSubstring, errClickhouseDatabaseRequired.Error())
		So(err.Error(), ShouldNotContainSubstring, "active mounts")
	})

	Convey("clickhouse-perf rest reports missing DSN and database before starting a server harness", t, func() {
		resetClickHousePerfConnectionForTest()

		err := runCHPerfRest()

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errClickhouseDSNRequired.Error())
		So(err.Error(), ShouldContainSubstring, errClickhouseDatabaseRequired.Error())
		So(err.Error(), ShouldNotContainSubstring, "provider")
	})
}

func resetClickHousePerfConnectionForTest() {
	origDSN := chPerf.dsn
	origDB := chPerf.database
	origMountpoints := chPerf.mountpoints
	origRepeat := chPerf.repeat
	origWarmup := chPerf.warmup
	origSplits := chPerf.splits
	origTreeGIDs := chPerf.treeGIDs
	origTreeUIDs := chPerf.treeUIDs
	origTreeTypes := chPerf.treeTypes
	origRestBaseURL := chPerf.restBaseURL
	origRestTreePath := chPerf.restTreePath
	origRestTreePaths := append([]string(nil), chPerf.restTreePaths...)
	origRestWhereDir := chPerf.restWhereDir
	origRestGroups := chPerf.restGroups
	origRestUsers := chPerf.restUsers
	origRestTypes := chPerf.restTypes
	origEnvDSN, hadEnvDSN := os.LookupEnv(envClickhouseDSN)
	origEnvDB, hadEnvDB := os.LookupEnv(envClickhouseDatabase)

	chPerf.dsn = ""
	chPerf.database = ""
	chPerf.mountpoints = ""
	_ = os.Unsetenv(envClickhouseDSN)
	_ = os.Unsetenv(envClickhouseDatabase)

	Reset(func() {
		chPerf.dsn = origDSN
		chPerf.database = origDB
		chPerf.mountpoints = origMountpoints
		chPerf.repeat = origRepeat
		chPerf.warmup = origWarmup
		chPerf.splits = origSplits
		chPerf.treeGIDs = origTreeGIDs
		chPerf.treeUIDs = origTreeUIDs
		chPerf.treeTypes = origTreeTypes
		chPerf.restBaseURL = origRestBaseURL
		chPerf.restTreePath = origRestTreePath
		chPerf.restTreePaths = origRestTreePaths
		chPerf.restWhereDir = origRestWhereDir
		chPerf.restGroups = origRestGroups
		chPerf.restUsers = origRestUsers
		chPerf.restTypes = origRestTypes

		restoreEnv(envClickhouseDSN, origEnvDSN, hadEnvDSN)
		restoreEnv(envClickhouseDatabase, origEnvDB, hadEnvDB)
	})
}

func restoreEnv(key, value string, ok bool) {
	if ok {
		_ = os.Setenv(key, value)

		return
	}

	_ = os.Unsetenv(key)
}
