/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/fixtimes"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	"github.com/wtsi-hgi/wrstat-ui/provider"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var (
	errE4GIDLookup     = errors.New("e4 gid lookup failed")
	errE4UIDLookup     = errors.New("e4 uid lookup failed")
	errMountTimestamps = errors.New("mount timestamps error")
)

const (
	d2AllowedGroupName    = "group7"
	d2DisallowedGroupName = "group8"
	d2MountDir            = "/mnt/"
	d2OpenDir             = "/mnt/open/"
	d2ClosedDir           = "/mnt/closed/"
	d2OpenChildDir        = "/mnt/open/child/"
	d2ClosedChildDir      = "/mnt/closed/child/"
	d2GroupsQueryKey      = "groups"
	d2DirQueryKey         = "dir"
	d2TypesQueryKey       = "types"
	d2UsersQueryKey       = "users"
	d2PerfGroupName       = "wrstat_perf_g14976"
	d2PerfUserName        = "wrstat_perf_u20155"
	e2T283Dir             = "/nfs/t283_imaging/"
	e2ScopedCacheHitKey   = "active_prefix_summary:path=/nfs/t283_imaging/;filter=ft:32768;" +
		"active_set_id=e2;query_version=1"
	e4ActiveSetA = "set-a"
	e4ActiveSetB = "set-b"
)

const a5RESTHighFanoutChildCount = 11205

const (
	a5RESTProjectMount = "/m/"
	a5RESTProjectDir   = "/m/project/"
)

func TestIDsToWanted(t *testing.T) {
	Convey("restrictGIDs returns bad query if you don't want any of the given ids", t, func() {
		_, err := restrictGIDs(map[uint32]bool{1: true}, []uint32{2})
		So(err, ShouldNotBeNil)
	})
}

type d2AuthTestDB struct {
	children     map[string][]string
	hasChildren  map[string]bool
	summaries    map[string]*db.DirSummary
	dirInfoCalls int
	whereCalls   int
	whereFilters []*db.Filter
}

func newD2AuthServer(t *testing.T) (*d2AuthTestDB, string, string, string, func()) {
	t.Helper()

	logWriter := gas.NewStringLogger()
	s := New(logWriter)
	database := newD2AuthTestDB()
	s.tree = db.NewTree(database)
	s.gidToNameCache[7] = d2AllowedGroupName
	s.gidToNameCache[8] = d2DisallowedGroupName
	s.Router().Use(gas.IncludeAbortErrorsInBody)

	cert, key, err := gas.CreateTestCert(t)
	So(err, ShouldBeNil)
	So(s.EnableAuth(cert, key, func(username, password string) (bool, string) {
		return true, "1000"
	}), ShouldBeNil)

	s.userToGIDs["user"] = []string{"7"}
	So(s.AddTreePage(), ShouldBeNil)
	s.addBaseDGUTARoutes()

	addr, stop, err := gas.StartTestServer(s, cert, key)
	So(err, ShouldBeNil)

	token, err := gas.Login(gas.NewClientRequest(addr, cert), "user", "pass")
	So(err, ShouldBeNil)

	return database, addr, cert, token, func() {
		So(stop(), ShouldBeNil)
	}
}

func newD2AuthTestDB() *d2AuthTestDB {
	return &d2AuthTestDB{
		children: map[string][]string{
			d2MountDir:  {d2OpenDir, d2ClosedDir},
			d2OpenDir:   {d2OpenChildDir},
			d2ClosedDir: {d2ClosedChildDir},
		},
		hasChildren: map[string]bool{
			d2OpenDir:   true,
			d2ClosedDir: true,
		},
		summaries: map[string]*db.DirSummary{
			d2MountDir: {
				Dir:   d2MountDir,
				Count: 2,
				GIDs:  []uint32{7, 8},
				UIDs:  []uint32{20155},
				FT:    db.DGUTAFileTypeOther,
				Age:   db.DGUTAgeAll,
			},
			d2OpenDir: {
				Dir:   d2OpenDir,
				Count: 1,
				GIDs:  []uint32{7},
				UIDs:  []uint32{20155},
				FT:    db.DGUTAFileTypeOther,
				Age:   db.DGUTAgeAll,
			},
			d2ClosedDir: {
				Dir:   d2ClosedDir,
				Count: 1,
				GIDs:  []uint32{8},
				UIDs:  []uint32{20155},
				FT:    db.DGUTAFileTypeOther,
				Age:   db.DGUTAgeAll,
			},
		},
	}
}

func (d *d2AuthTestDB) DirInfo(dir string, _ *db.Filter) (*db.DirSummary, error) {
	d.dirInfoCalls++

	summary := d.summaries[dir]
	if summary == nil {
		return nil, db.ErrDirNotFound
	}

	cp := *summary

	return &cp, nil
}

func (d *d2AuthTestDB) Children(dir string) ([]string, error) {
	return append([]string(nil), d.children[dir]...), nil
}

func (d *d2AuthTestDB) DirsHaveChildren(dirs []string, _ *db.Filter) (map[string]bool, error) {
	result := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		result[dir] = d.hasChildren[dir]
	}

	return result, nil
}

func (d *d2AuthTestDB) Where(dir string, filter *db.Filter, _ func(string) int) (db.DCSs, error) {
	d.whereCalls++
	d.whereFilters = append(d.whereFilters, cloneD2Filter(filter))

	return db.DCSs{{
		Dir:   dir,
		Count: 1,
		UIDs:  append([]uint32(nil), filter.UIDs...),
		GIDs:  append([]uint32(nil), filter.GIDs...),
		FT:    filter.FT,
		Age:   filter.Age,
	}}, nil
}

func cloneD2Filter(filter *db.Filter) *db.Filter {
	if filter == nil {
		return nil
	}

	return &db.Filter{
		GIDs: append([]uint32(nil), filter.GIDs...),
		UIDs: append([]uint32(nil), filter.UIDs...),
		FT:   filter.FT,
		Age:  filter.Age,
	}
}

func (d *d2AuthTestDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *d2AuthTestDB) Close() error {
	return nil
}

func (d *d2AuthTestDB) summaryRouteReads() int {
	return d.dirInfoCalls + d.whereCalls
}

func TestD2ServerPermissionAuthChecks(t *testing.T) {
	Convey("D2.3 auth tree marks disallowed child summaries as NoAuth without exposing children", t, func() {
		database, addr, cert, token, stop := newD2AuthServer(t)
		defer stop()

		resp, err := gas.NewAuthenticatedClientRequest(addr, cert, token).
			SetResult(&TreeElement{}).
			ForceContentType("application/json").
			SetQueryParam(c3RESTInputPath, d2MountDir).
			Get(EndPointAuthTree)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusOK)

		root, ok := resp.Result().(*TreeElement)
		So(ok, ShouldBeTrue)

		children := d2ChildrenByPath(root.Children)

		open := children[d2OpenDir]
		So(open, ShouldNotBeNil)
		So(open.NoAuth, ShouldBeFalse)
		So(open.HasChildren, ShouldBeTrue)

		closed := children[d2ClosedDir]
		So(closed, ShouldNotBeNil)
		So(closed.NoAuth, ShouldBeTrue)
		So(closed.Children, ShouldBeNil)
		So(database.dirInfoCalls, ShouldBeGreaterThan, 0)
	})

	Convey("D2.4 auth tree and where reject disallowed Unix group before summary reads", t, func() {
		database, addr, cert, token, stop := newD2AuthServer(t)
		defer stop()

		resp, err := gas.NewAuthenticatedClientRequest(addr, cert, token).
			ForceContentType("application/json").
			SetQueryParams(map[string]string{
				c3RESTInputPath:  d2MountDir,
				d2GroupsQueryKey: d2DisallowedGroupName,
			}).
			Get(EndPointAuthTree)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)
		So(resp.String(), ShouldContainSubstring, ErrBadQuery.Error())
		So(database.summaryRouteReads(), ShouldEqual, 0)

		resp, err = gas.NewAuthenticatedClientRequest(addr, cert, token).
			ForceContentType("application/json").
			SetQueryParams(map[string]string{
				d2DirQueryKey:    d2MountDir,
				d2GroupsQueryKey: d2DisallowedGroupName,
			}).
			Get(EndPointAuthWhere)
		So(err, ShouldBeNil)
		So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)
		So(resp.String(), ShouldContainSubstring, ErrBadQuery.Error())
		So(database.summaryRouteReads(), ShouldEqual, 0)
	})

	Convey("D2.5 no-auth where maps cached Unix group and user names before querying", t, func() {
		logWriter := gas.NewStringLogger()
		s := New(logWriter)
		database := newD2AuthTestDB()
		s.tree = db.NewTree(database)
		s.gidToNameCache[14976] = d2PerfGroupName
		s.uidToNameCache[20155] = d2PerfUserName
		s.addBaseDGUTARoutes()

		query := "?" + url.Values{
			d2DirQueryKey:    {d2MountDir},
			d2GroupsQueryKey: {d2PerfGroupName},
			d2UsersQueryKey:  {d2PerfUserName},
			d2TypesQueryKey:  {db.DGUTAFileTypeOther.String()},
		}.Encode()
		response, err := queryWhere(s, query)
		So(err, ShouldBeNil)
		So(response.Code, ShouldEqual, http.StatusOK)

		So(database.whereFilters, ShouldHaveLength, 1)
		filter := database.whereFilters[0]
		So(filter.GIDs, ShouldResemble, []uint32{14976})
		So(filter.UIDs, ShouldResemble, []uint32{20155})
		So(filter.FT, ShouldEqual, db.DGUTAFileTypeOther)

		var got []*DirSummary
		So(json.Unmarshal(response.Body.Bytes(), &got), ShouldBeNil)

		expectedRaw, err := s.tree.Where(d2MountDir, &db.Filter{
			GIDs: []uint32{14976},
			UIDs: []uint32{20155},
			FT:   db.DGUTAFileTypeOther,
			Age:  db.DGUTAgeAll,
		}, split.SplitsToSplitFn(2))
		So(err, ShouldBeNil)

		So(got, ShouldResemble, s.dcssToSummaries(expectedRaw))
	})
}

func d2ChildrenByPath(children []*TreeElement) map[string]*TreeElement {
	byPath := make(map[string]*TreeElement, len(children))
	for _, child := range children {
		byPath[child.Path] = child
	}

	return byPath
}

func TestE1ServerPerfHarnessRecordsRESTAndCLI(t *testing.T) {
	Convey("E1.3/E1.4 REST and CLI/server harness operations record required evidence", t, func() {
		logWriter := gas.NewStringLogger()
		s := New(logWriter)
		database := newD2AuthTestDB()
		s.tree = db.NewTree(database)
		s.gidToNameCache[14976] = d2PerfGroupName
		s.uidToNameCache[20155] = d2PerfUserName

		report, err := s.MeasurePerfHarness(PerfHarnessOptions{
			Repeat:      2,
			TreePath:    d2MountDir,
			WhereDir:    e2T283Dir,
			WhereGroups: d2PerfGroupName,
			WhereUsers:  d2PerfUserName,
			WhereTypes:  db.DGUTAFileTypeOther.String(),
			WhereSplits: defaultSplitsStr,
			QueryCount: func() uint64 {
				return nonNegativeIntToUint64(database.summaryRouteReads())
			},
			QueryCountSource: "test.summaryRouteReads_delta",
			CacheStats: func() (uint64, uint64) {
				reads := nonNegativeIntToUint64(database.summaryRouteReads())

				return reads, reads
			},
			CacheStatsSource: "test.summaryRouteReads_delta",
		})

		So(err, ShouldBeNil)
		So(report.Operations, ShouldHaveLength, 3)

		treeOp := serverPerfOperation(report, "rest_tree")
		assertServerRESTPerfOperation(treeOp)
		So(serverPerfQueryParams(treeOp)["path"], ShouldEqual, d2MountDir)
		So(treeOp.ResultCount, ShouldResemble, []uint64{2, 2})

		whereOp := serverPerfOperation(report, "rest_where")
		assertServerRESTPerfOperation(whereOp)
		So(serverPerfQueryParams(whereOp)["dir"], ShouldEqual, e2T283Dir)
		So(serverPerfQueryParams(whereOp)["groups"], ShouldEqual, d2PerfGroupName)
		So(serverPerfQueryParams(whereOp)["users"], ShouldEqual, d2PerfUserName)
		So(serverPerfQueryParams(whereOp)["types"], ShouldEqual, db.DGUTAFileTypeOther.String())
		So(whereOp.ResultCount, ShouldResemble, []uint64{1, 1})

		cliOp := serverPerfOperation(report, "cli_where")
		assertServerRESTPerfOperation(cliOp)
		So(cliOp.Inputs["command"], ShouldResemble, []string{
			"./wrstat-ui",
			"where",
			"--dir",
			e2T283Dir,
			"--groups",
			d2PerfGroupName,
			"--users",
			d2PerfUserName,
			"--types",
			db.DGUTAFileTypeOther.String(),
			"--json",
		})
		So(cliOp.Inputs["first_run_wall_ms"], ShouldBeGreaterThanOrEqualTo, 0.0)
		So(cliOp.ResultCount, ShouldResemble, []uint64{1, 1})
	})
}

func serverPerfOperation(report perfreport.Report, name string) perfreport.Operation {
	for _, op := range report.Operations {
		if op.Name == name {
			return op
		}
	}

	return perfreport.Operation{}
}

func assertServerRESTPerfOperation(op perfreport.Operation) {
	So(op.Inputs["status_codes"], ShouldResemble, []uint64{200, 200})
	So(serverPerfPositiveUintInputs(op, "json_bytes"), ShouldBeTrue)
	So(serverPerfPositiveUintInputs(op, "gzip_bytes"), ShouldBeTrue)
	So(serverPerfPositiveUintInputs(op, "query_count"), ShouldBeTrue)
	So(serverPerfPositiveUintInputs(op, "cache_hits"), ShouldBeTrue)
	So(serverPerfPositiveUintInputs(op, "cache_misses"), ShouldBeTrue)
	So(op.Inputs["query_count_source"], ShouldEqual, "test.summaryRouteReads_delta")
	So(op.Inputs["cache_counter_source"], ShouldEqual, "test.summaryRouteReads_delta")
	So(op.ResultCount, ShouldHaveLength, 2)
	So(op.P50MS, ShouldBeGreaterThanOrEqualTo, 0.0)
	So(op.P95MS, ShouldBeGreaterThanOrEqualTo, 0.0)
	So(op.P99MS, ShouldBeGreaterThanOrEqualTo, 0.0)
}

func serverPerfPositiveUintInputs(op perfreport.Operation, key string) bool {
	values, ok := op.Inputs[key].([]uint64)
	if !ok || len(values) != 2 {
		return false
	}

	for _, value := range values {
		if value == 0 {
			return false
		}
	}

	return true
}

func serverPerfQueryParams(op perfreport.Operation) map[string]string {
	params, ok := op.Inputs["query_params"].(map[string]string)
	if !ok {
		return nil
	}

	return params
}

func TestA3ServerPerfHarnessRecordsFallbackRoutes(t *testing.T) {
	Convey("A3.3 REST and CLI where perf evidence records schema3 fallback routes", t, func() {
		s := New(gas.NewStringLogger())
		database := newD2AuthTestDB()
		s.tree = db.NewTree(database)

		const route = "parent_facts_fallback"

		fallbackSnapshots := 0
		report, err := s.MeasurePerfHarness(PerfHarnessOptions{
			Repeat:   1,
			WhereDir: e2T283Dir,
			FallbackRoutes: func() map[string]uint64 {
				fallbackSnapshots++
				switch fallbackSnapshots {
				case 4:
					return map[string]uint64{route: 1}
				case 5:
					return map[string]uint64{route: 1}
				case 6:
					return map[string]uint64{route: 2}
				default:
					return map[string]uint64{route: 0}
				}
			},
			FallbackSource: "test.schema3_fallback_routes_delta",
		})
		So(err, ShouldBeNil)

		restWhere := serverPerfOperation(report, "rest_where")
		So(restWhere.Inputs["schema3_fallback_count"], ShouldResemble, []uint64{1})
		So(restWhere.Inputs["schema3_fallback_routes"], ShouldResemble, []string{route})
		So(restWhere.Inputs["schema3_fallback_source"], ShouldEqual, "test.schema3_fallback_routes_delta")

		cliWhere := serverPerfOperation(report, "cli_where")
		So(cliWhere.Inputs["schema3_fallback_count"], ShouldResemble, []uint64{1})
		So(cliWhere.Inputs["schema3_fallback_routes"], ShouldResemble, []string{route})
		So(cliWhere.Inputs["schema3_fallback_source"], ShouldEqual, "test.schema3_fallback_routes_delta")
	})
}

type a5RESTTreeEnv struct {
	cfg       clickhouse.Config
	mountPath string
	provider  provider.Provider
	parentDir string
	server    *Server
}

func newA5RESTTreeEnv(t *testing.T) *a5RESTTreeEnv {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")
	clickhouse.ResetTreeQueryCaches()

	harness := newC3RESTClickHouseHarness(t)
	cfg := harness.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{c3RESTNFST283Imaging + "/"}

	parentDir := c3RESTWideParent + "/"
	seedC3RESTClickHouseHighFanout(t, cfg, parentDir, a5RESTHighFanoutChildCount)
	markA5RESTSchema3Ready(t, cfg, c3RESTNFST283Imaging+"/", parentDir)
	clickhouse.ResetTreeQueryCaches()
	clickhouse.ResetTreeQueryCacheStats(cfg)

	p, err := clickhouse.OpenProvider(cfg)
	So(err, ShouldBeNil)

	s := New(io.Discard)
	So(s.SetProvider(p), ShouldBeNil)

	return &a5RESTTreeEnv{
		cfg:       cfg,
		mountPath: c3RESTNFST283Imaging + "/",
		provider:  p,
		parentDir: parentDir,
		server:    s,
	}
}

func newA5RESTProjectTreeEnv(t *testing.T) *a5RESTTreeEnv {
	t.Helper()

	os.Setenv("WRSTAT_ENV", "test")
	clickhouse.ResetTreeQueryCaches()

	harness := newC3RESTClickHouseHarness(t)
	cfg := harness.newConfig()
	cfg.QueryTimeout = 10 * time.Second
	cfg.PollInterval = 0
	cfg.MountPoints = []string{a5RESTProjectMount}

	seedA5RESTProjectFixture(t, cfg)
	markA5RESTSchema3Ready(t, cfg, a5RESTProjectMount, a5RESTProjectDir)
	clickhouse.ResetTreeQueryCaches()
	clickhouse.ResetTreeQueryCacheStats(cfg)

	p, err := clickhouse.OpenProvider(cfg)
	So(err, ShouldBeNil)

	s := New(io.Discard)
	a5RESTSeedNameCaches(s)
	So(s.SetProvider(p), ShouldBeNil)

	return &a5RESTTreeEnv{
		cfg:       cfg,
		mountPath: a5RESTProjectMount,
		provider:  p,
		parentDir: a5RESTProjectDir,
		server:    s,
	}
}

func (e *a5RESTTreeEnv) close() {
	if e.provider != nil {
		So(e.provider.Close(), ShouldBeNil)
	}

	clickhouse.ResetTreeQueryCaches()
	os.Unsetenv("WRSTAT_ENV")
}

func (e *a5RESTTreeEnv) requestTree(values url.Values) TreeElement {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		EndPointAuthTree+"?"+values.Encode(),
		nil,
	)

	e.server.getTree(c)
	So(w.Code, ShouldEqual, http.StatusOK)

	var got TreeElement
	So(json.Unmarshal(w.Body.Bytes(), &got), ShouldBeNil)

	return got
}

func TestA5RESTTreeEndpointReusesOnePacket(t *testing.T) {
	Convey("A5 REST tree endpoint reuses one broad parent packet for child summaries and flags", t, func() {
		env := newA5RESTTreeEnv(t)
		defer env.close()

		clickhouse.ResetTreeQueryCacheStats(env.cfg)
		clickhouse.ResetSchema3FallbackRoutes()

		got := env.requestTree(url.Values{queryParamPath: {env.parentDir}})
		stats := clickhouse.ReadTreeQueryCacheStats(env.cfg)

		So(got.Children, ShouldHaveLength, a5RESTHighFanoutChildCount)
		So(a5RESTBroadChildHasChildrenMismatches(got), ShouldEqual, 0)
		So(stats.ParentPacketReads, ShouldEqual, uint64(1))
		So(a5RESTPacketKeyCount(stats.ParentPacketReadKeys, env.parentDir), ShouldEqual, 1)
		So(stats.FactVectorReads, ShouldEqual, uint64(0))
		So(stats.ParentPacketMisses, ShouldEqual, uint64(1))
	})

	Convey("A5 REST tree endpoint reuses one full-filter child packet for child summaries and flags", t, func() {
		env := newA5RESTTreeEnv(t)
		defer env.close()

		clickhouse.ResetTreeQueryCacheStats(env.cfg)
		clickhouse.ResetSchema3FallbackRoutes()

		got := env.requestTree(url.Values{
			queryParamPath:   {env.parentDir},
			queryParamGroups: {"7"},
			queryParamUsers:  {"11"},
			queryParamTypes:  {db.DGUTAFileTypeBam.String()},
			queryParamAge:    {strconv.Itoa(int(db.DGUTAgeA6M))},
		})
		stats := clickhouse.ReadTreeQueryCacheStats(env.cfg)

		So(got.Children, ShouldHaveLength, a5RESTFilteredChildCount())
		So(a5RESTAllChildFlags(got), ShouldResemble, map[bool]int{false: a5RESTFilteredChildCount()})
		So(a5RESTPacketKeyCount(stats.ChildFilterReadKeys, env.parentDir), ShouldEqual, 1)
		So(a5RESTPacketKeyCount(stats.ParentPacketReadKeys, env.parentDir), ShouldEqual, 0)
		So(stats.FactVectorReads, ShouldEqual, uint64(0))
		So(clickhouse.ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))
	})

	Convey("A5 REST tree endpoint serves unused and unchanged project fixtures without fallback", t, func() {
		for _, fixture := range a5RESTProjectTreeFixtures() {
			env := newA5RESTProjectTreeEnv(t)
			clickhouse.ResetTreeQueryCacheStats(env.cfg)
			clickhouse.ResetSchema3FallbackRoutes()

			got := env.requestTree(url.Values{
				queryParamPath: {a5RESTProjectDir},
				queryParamAge:  {strconv.Itoa(int(fixture.age))},
			})
			stats := clickhouse.ReadTreeQueryCacheStats(env.cfg)
			env.close()

			So(got.Children, ShouldHaveLength, fixture.childCount)
			So(a5RESTChildDigest(got.Children), ShouldEqual, a5RESTProjectManifestDigest(fixture.manifestKey))
			So(a5RESTAllChildAges(got), ShouldResemble, map[db.DirGUTAge]int{fixture.age: fixture.childCount})
			So(a5RESTPacketKeyCount(stats.ChildFilterReadKeys, a5RESTProjectDir), ShouldEqual, 1)
			So(a5RESTPacketKeyCount(stats.ParentPacketReadKeys, a5RESTProjectDir), ShouldEqual, 0)
			So(stats.FactVectorReads, ShouldEqual, uint64(0))
			So(clickhouse.ReadSchema3FallbackRoutes()["parent_facts_fallback"], ShouldEqual, uint64(0))
		}
	})
}

func a5RESTBroadChildHasChildrenMismatches(got TreeElement) int {
	mismatches := 0

	for _, child := range got.Children {
		expected := child.Path == got.Path+"child000"
		if child.HasChildren != expected {
			mismatches++
		}
	}

	return mismatches
}

func a5RESTPacketKeyCount(keys []string, parentDir string) int {
	count := 0

	needle := ";parent_dir=" + parentDir + ";"
	for _, key := range keys {
		if strings.Contains(key, needle) {
			count++
		}
	}

	return count
}

func a5RESTFilteredChildCount() int {
	return (a5RESTHighFanoutChildCount + 14) / 15
}

func a5RESTAllChildFlags(got TreeElement) map[bool]int {
	flags := make(map[bool]int)
	for _, child := range got.Children {
		flags[child.HasChildren]++
	}

	return flags
}

func a5RESTProjectTreeFixtures() []a5RESTProjectTreeFixture {
	return []a5RESTProjectTreeFixture{
		{
			manifestKey: "project_tree_unused_1y",
			age:         db.DGUTAgeA1Y,
			childCount:  4,
		},
		{
			manifestKey: "project_tree_unchanged_1y",
			age:         db.DGUTAgeM1Y,
			childCount:  2,
		},
	}
}

func a5RESTChildDigest(children []*TreeElement) string {
	elements := make([]a5RESTChildDigestElement, len(children))
	for i, child := range children {
		elements[i] = a5RESTChildDigestElement{
			Path:        child.Path,
			Count:       child.Count,
			Size:        child.Size,
			Age:         child.Age,
			Users:       child.Users,
			Groups:      child.Groups,
			FileTypes:   child.FileTypes,
			HasChildren: child.HasChildren,
		}
	}

	data, err := json.Marshal(elements)
	So(err, ShouldBeNil)

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func a5RESTProjectManifestDigest(key string) string {
	return map[string]string{
		"project_tree_unused_1y":    "sha256:d66a954e159caaf9152f04166f553ca9343298dfe0dcc8ee38cb87bb3d4177b6",
		"project_tree_unchanged_1y": "sha256:35f6e8c39b29de195ee42d8cc1ac30d59a5ff0371c4dc6e99759c520504de449",
	}[key]
}

func a5RESTAllChildAges(got TreeElement) map[db.DirGUTAge]int {
	ages := make(map[db.DirGUTAge]int)
	for _, child := range got.Children {
		ages[child.Age]++
	}

	return ages
}

type a5RESTProjectChild struct {
	dir   string
	age   db.DirGUTAge
	gid   uint32
	uid   uint32
	ft    db.DirGUTAFileType
	count uint64
	size  uint64
}

func a5RESTProjectChildrenForAge(age db.DirGUTAge) []a5RESTProjectChild {
	children := make([]a5RESTProjectChild, 0)

	for _, child := range a5RESTProjectAllChildren() {
		if child.age == age {
			children = append(children, child)
		}
	}

	return children
}

func a5RESTProjectAllChildren() []a5RESTProjectChild {
	return []a5RESTProjectChild{
		{
			dir:   a5RESTProjectDir + "alpha/",
			age:   db.DGUTAgeA1Y,
			gid:   7,
			uid:   11,
			ft:    db.DGUTAFileTypeBam,
			count: 10,
			size:  100,
		},
		{dir: a5RESTProjectDir + "beta/", age: db.DGUTAgeA1Y, gid: 8, uid: 12, ft: db.DGUTAFileTypeCram, count: 8, size: 80},
		{dir: a5RESTProjectDir + "gamma/", age: db.DGUTAgeA1Y, gid: 7, uid: 12, ft: db.DGUTAFileTypeBam, count: 6, size: 60},
		{dir: a5RESTProjectDir + "zeta/", age: db.DGUTAgeA1Y, gid: 9, uid: 13, ft: db.DGUTAFileTypeOther, count: 4, size: 40},
		{dir: a5RESTProjectDir + "delta/", age: db.DGUTAgeM1Y, gid: 8, uid: 11, ft: db.DGUTAFileTypeCram, count: 5, size: 50},
		{
			dir:   a5RESTProjectDir + "omega/",
			age:   db.DGUTAgeM1Y,
			gid:   9,
			uid:   13,
			ft:    db.DGUTAFileTypeOther,
			count: 3,
			size:  30,
		},
	}
}

type a5RESTProjectTreeFixture struct {
	manifestKey string
	age         db.DirGUTAge
	childCount  int
}

type a5RESTChildDigestElement struct {
	Path        string       `json:"path"`
	Count       uint64       `json:"count"`
	Size        uint64       `json:"size"`
	Age         db.DirGUTAge `json:"age"`
	Users       []string     `json:"users"`
	Groups      []string     `json:"groups"`
	FileTypes   []string     `json:"filetypes"`
	HasChildren bool         `json:"has_children"`
}

type e2FilteredRESTDB struct {
	whereCalls []string
}

func (d *e2FilteredRESTDB) DirInfo(_ string, _ *db.Filter) (*db.DirSummary, error) {
	return nil, db.ErrDirNotFound
}

func (d *e2FilteredRESTDB) Children(_ string) ([]string, error) {
	return nil, nil
}

func (d *e2FilteredRESTDB) Where(dir string, filter *db.Filter, _ func(string) int) (db.DCSs, error) {
	d.whereCalls = append(d.whereCalls, dir)

	if filter == nil || filter.FT != db.DGUTAFileTypeOther {
		return nil, ErrBadQuery
	}

	return e2FilteredRESTSummaries(), nil
}

func e2FilteredRESTSummaries() db.DCSs {
	return db.DCSs{
		{Dir: e2T283Dir + "plateA", Count: 50, Size: 500, FT: db.DGUTAFileTypeOther, Age: db.DGUTAgeAll},
		{Dir: e2T283Dir + "plateB", Count: 37, Size: 370, FT: db.DGUTAFileTypeOther, Age: db.DGUTAgeAll},
	}
}

func (d *e2FilteredRESTDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *e2FilteredRESTDB) Close() error {
	return nil
}

func newE2FilteredRESTServer() *Server {
	s := New(gas.NewStringLogger())
	database := &e2FilteredRESTDB{}
	s.tree = db.NewTree(database)
	s.addBaseDGUTARoutes()

	return s
}

type badMountTSReader struct {
	basedirs.Reader
	err error
}

func (b badMountTSReader) MountTimestamps() (map[string]time.Time, error) {
	return nil, b.err
}

func TestServer(t *testing.T) {
	username, uid, gids := GetUserAndGroups(t)
	exampleGIDs := getExampleGIDs(gids)

	refTime := time.Now().Unix()

	Convey("Given a Server", t, func() {
		logWriter := gas.NewStringLogger()
		s := New(logWriter)

		Convey("You can convert dguta.DCSs to DirSummarys", func() {
			uid32, err := strconv.Atoi(uid)
			So(err, ShouldBeNil)
			gid32, err := strconv.Atoi(gids[0])
			So(err, ShouldBeNil)

			dcss := db.DCSs{
				{
					Dir:   "/foo",
					Count: 1,
					Size:  2,
					UIDs:  []uint32{uint32(uid32), 9999999}, //nolint:gosec
					GIDs:  []uint32{uint32(gid32), 9999999}, //nolint:gosec
				},
				{
					Dir:   "/bar",
					Count: 1,
					Size:  2,
					UIDs:  []uint32{uint32(uid32), 9999999}, //nolint:gosec
					GIDs:  []uint32{uint32(gid32), 9999999}, //nolint:gosec
				},
			}

			dss := s.dcssToSummaries(dcss)

			So(len(dss), ShouldEqual, 2)
			So(dss[0].Dir, ShouldEqual, "/foo")
			So(dss[0].Count, ShouldEqual, 1)
			So(dss[0].Size, ShouldEqual, 2)
			So(dss[0].Users, ShouldResemble, []string{username})
			So(dss[0].Groups, ShouldResemble, []string{gidToGroup(t, gids[0])})
		})

		Convey("userGIDs fails with bad UIDs", func() {
			u := &gas.User{
				Username: username,
				UID:      "-1",
			}

			_, err := s.userGIDs(u)
			So(err, ShouldNotBeNil)
		})

		Convey("You can Start the Server", func() {
			certPath, keyPath, err := gas.CreateTestCert(t)
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, certPath, keyPath)
			So(err, ShouldBeNil)

			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

			Convey("The jwt endpoint works after enabling it", func() {
				err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, string) {
					returnUID := uid

					if u == "user" {
						returnUID = "-1"
					}

					return true, returnUID
				})
				So(err, ShouldBeNil)

				r := gas.NewClientRequest(addr, certPath)
				token, errl := gas.Login(r, username, "pass")
				So(errl, ShouldBeNil)

				r = gas.NewAuthenticatedClientRequest(addr, certPath, token)
				tokenBadUID, errl := gas.Login(r, "user", "pass")
				So(errl, ShouldBeNil)
				So(tokenBadUID, ShouldNotBeBlank)

				s.AuthRouter().GET("/test", func(c *gin.Context) {})

				resp, err := r.Get(gas.EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				testRestrictedGroups(t, gids, s, exampleGIDs, addr, certPath, token, tokenBadUID)
			})

			testClientsOnRealServer(t, username, uid, gids, s, addr, certPath, keyPath)
		})

		if len(gids) < 2 {
			SkipConvey("Can't test the where endpoint without you belonging to at least 2 groups", func() {})

			return
		}

		Convey("convertSplitsValue works", func() {
			n := convertSplitsValue("1")
			So(n(""), ShouldEqual, 1)

			n = convertSplitsValue("foo")
			So(n(""), ShouldEqual, 2)
		})

		Convey("You can query the endpoints", func() {
			response, err := queryWhere(s, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusNotFound)
			So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/where")
			So(logWriter.String(), ShouldContainSubstring, "STATUS=404")
			logWriter.Reset()

			response, err = query(s, EndPointBasedirUsageGroup, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusNotFound)
			So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/usage/groups")
			So(logWriter.String(), ShouldContainSubstring, "STATUS=404")
			logWriter.Reset()

			Convey("And given dirguta and basedir databases", func() {
				path, err := CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
				So(err, ShouldBeNil)

				groupA := gidToGroup(t, gids[0])
				groupB := gidToGroup(t, gids[1])

				ownersPath, err := internaldata.CreateOwnersCSV(t, fmt.Sprintf("0,Alan\n%s,Barbara\n%s,Dellilah", gids[0], gids[1]))
				So(err, ShouldBeNil)

				p, err := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
				So(err, ShouldBeNil)
				err = s.SetProvider(p)
				So(err, ShouldBeNil)

				expectedRaw, err := s.tree.Where("/", nil, split.SplitsToSplitFn(2))
				So(err, ShouldBeNil)

				expected := s.dcssToSummaries(expectedRaw)
				fixDirSummaryTimes(expected)
				expectedNonRoot, expectedGroupsRoot := adjustedExpectations(expected, groupA, groupB)

				Convey("You can get dirguta results", func() {
					response, err := queryWhere(s, "")
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/where")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					result, err := decodeWhereResult(response)
					So(err, ShouldBeNil)
					So(result, ShouldResemble, expected)

					Convey("And you can filter results", func() {
						groups := gidsToGroups(t, gids...)

						expectedUsers := expectedNonRoot[0].Users
						sort.Strings(expectedUsers)

						expectedUser := []string{username}
						expectedRoot := []string{"root"}
						expectedGroupsA := []string{groupA}
						expectedGroupsB := []string{groupB}
						expectedGroupsRootA := []string{groupA, "root"}
						sort.Strings(expectedGroupsRootA)

						expectedFTs := expectedNonRoot[0].FileTypes
						expectedBams := []string{"bam", "temp"}
						expectedCrams := []string{"cram"}
						expectedAtime := time.Unix(50, 0)
						matrix := []*matrixElement{
							{"?groups=" + groups[0] + "," + groups[1], expectedNonRoot},
							{"?groups=" + groups[0], []*DirSummary{
								{
									Dir: "/a/b", Count: 13, Size: 120, Atime: expectedAtime,
									Mtime: time.Unix(80, 0), Users: expectedUsers,
									Groups: expectedGroupsA, FileTypes: expectedFTs,
								},
								{
									Dir: "/a/b/d", Count: 11, Size: 110, Atime: expectedAtime,
									Mtime: time.Unix(75, 0), Users: expectedUsers,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d/g", Count: 10, Size: 100, Atime: time.Unix(50, 0),
									Mtime: time.Unix(75, 0), Users: expectedUsers,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d/f", Count: 1, Size: 10, Atime: expectedAtime,
									Mtime: time.Unix(50, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams,
								},
								{
									Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams,
								},
							}},
							{"?users=root," + username, expected},
							{"?users=root", []*DirSummary{
								{
									Dir: "/a", Count: 14, Size: 86, Atime: expectedAtime,
									Mtime: time.Unix(90, 0), Users: expectedRoot,
									Groups: expectedGroupsRoot, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d", Count: 9, Size: 81, Atime: expectedAtime,
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsRootA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d/g", Count: 8, Size: 80, Atime: time.Unix(50, 0),
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/c/d", Count: 5, Size: 5, Atime: time.Unix(90, 0),
									Mtime: time.Unix(90, 0), Users: expectedRoot,
									Groups: expectedGroupsB, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d/i/j", Count: 1, Size: 1, Atime: expectedAtime,
									Mtime: expectedAtime, Users: expectedRoot,
									Groups: expectedRoot, FileTypes: expectedCrams,
								},
							}},
							{"?groups=" + groups[0] + "&users=root", []*DirSummary{
								{
									Dir: "/a/b/d/g", Count: 8, Size: 80, Atime: time.Unix(50, 0),
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
							}},
							{"?types=cram,bam", expected},
							{"?types=bam", []*DirSummary{
								{
									Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: []string{"bam", "temp"},
								},
								{
									Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: []string{"bam", "temp"},
								},
							}},
							{"?groups=" + groups[0] + "&users=root&types=cram,bam", []*DirSummary{
								{
									Dir: "/a/b/d/g", Count: 8, Size: 80, Atime: time.Unix(50, 0),
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
							}},
							{"?groups=" + groups[0] + "&users=root&types=bam", []*DirSummary{}},
							{"?splits=0", []*DirSummary{
								{
									Dir: "/", Count: 24, Size: 141, Atime: expectedAtime,
									Mtime: expectedNonRoot[0].Mtime, Users: expectedUsers,
									Groups: expectedGroupsRoot, FileTypes: expectedFTs,
								},
							}},
							{"?dir=/a&splits=0", []*DirSummary{
								{
									Dir: "/a", Count: 19, Size: 126, Atime: expectedAtime,
									Mtime: time.Unix(90, 0), Users: expectedUsers,
									Groups: expectedGroupsRoot, FileTypes: expectedFTs,
								},
							}},
							{"?dir=/a/b/e/h", []*DirSummary{
								{
									Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams,
								},
								{
									Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams,
								},
							}},
							{"?dir=/k/&age=1", []*DirSummary{
								{
									Dir: "/k/", Count: 4, Size: 10, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-(db.SecondsInAMonth*2), 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeA1M,
								},
							}},
							{"?dir=/k&age=2", []*DirSummary{
								{
									Dir: "/k", Count: 3, Size: 7, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-db.SecondsInAYear, 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeA2M,
								},
							}},
							{"?dir=/k&age=6", []*DirSummary{
								{
									Dir: "/k", Count: 1, Size: 1, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-(db.SecondsInAYear*7), 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeA3Y,
								},
							}},
							{"?dir=/k&age=8", []*DirSummary{}},
							{"?dir=/k&age=11", []*DirSummary{
								{
									Dir: "/k", Count: 3, Size: 7, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-(db.SecondsInAYear), 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeM6M,
								},
							}},
							{"?dir=/k&age=16", []*DirSummary{
								{
									Dir: "/k", Count: 1, Size: 1, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-(db.SecondsInAYear*7), 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeM7Y,
								},
							}},
						}

						runMapMatrixTest(t, matrix, s)
					})

					Convey("Where bad filters fail", func() {
						badFilters := []string{
							"?groups=fo#€o",
							"?users=fo#€o",
							"?types=fo#€o",
						}

						runSliceMatrixTest(t, badFilters, s)
					})

					Convey("Unless you provide an invalid directory", func() {
						response, err = queryWhere(s, "?dir=/foo")
						So(err, ShouldBeNil)
						So(response.Code, ShouldEqual, http.StatusBadRequest)
						So(logWriter.String(), ShouldContainSubstring, "STATUS=400")
						So(logWriter.String(), ShouldContainSubstring, "Error #01: directory not found")
					})
				})

				Convey("You can get basedir results", func() {
					s.basedirs.SetMountPoints([]string{
						"/a/",
						"/k/",
					})

					response, err := query(s, EndPointBasedirUsageGroup, "")
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/usage/groups")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					usageGroup, err := decodeUsageResult(response)
					So(err, ShouldBeNil)
					So(len(usageGroup), ShouldBeGreaterThan, 0)
					So(usageGroup[0].GID, ShouldEqual, 0)
					So(usageGroup[0].UID, ShouldEqual, 0)
					So(usageGroup[0].Name, ShouldNotBeBlank)
					So(usageGroup[0].Owner, ShouldNotBeBlank)
					So(usageGroup[0].BaseDir, ShouldNotBeBlank)

					response, err = query(s, EndPointBasedirUsageUser, "")
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/usage/users")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					usageUser, err := decodeUsageResult(response)
					So(err, ShouldBeNil)
					So(len(usageUser), ShouldBeGreaterThan, 0)
					So(usageUser[0].GID, ShouldEqual, 0)
					So(usageUser[0].UID, ShouldEqual, 0)
					So(usageUser[0].Name, ShouldNotBeBlank)
					So(usageUser[0].Owner, ShouldEqual, "Alan")
					So(usageUser[0].BaseDir, ShouldNotBeBlank)

					response, err = query(s, EndPointBasedirSubdirGroup,
						fmt.Sprintf("?id=%d&basedir=%s", usageGroup[0].GID, usageGroup[0].BaseDir))
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/subdirs/group")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					subdirs, err := decodeSubdirResult(response)
					So(err, ShouldBeNil)
					So(len(subdirs), ShouldEqual, 1)
					So(subdirs[0].SubDir, ShouldEqual, ".")

					response, err = query(s, EndPointBasedirSubdirUser,
						fmt.Sprintf("?id=%d&basedir=%s", usageUser[0].UID, usageUser[0].BaseDir))
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/subdirs/user")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					subdirs, err = decodeSubdirResult(response)
					So(err, ShouldBeNil)
					So(len(subdirs), ShouldEqual, 2)

					response, err = query(s, EndPointBasedirHistory,
						fmt.Sprintf("?id=%d&basedir=%s", usageGroup[0].GID, usageGroup[0].BaseDir))
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/history")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					history, err := decodeHistoryResult(response)
					So(err, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history[0].UsageInodes, ShouldEqual, 1)

					response, err = query(s, EndPointBasedirSubdirUser,
						fmt.Sprintf("?id=%d&basedir=%s&age=%d", usageUser[0].UID, usageUser[0].BaseDir, db.DGUTAgeA3Y))
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/subdirs/user")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					subdirs, err = decodeSubdirResult(response)
					So(err, ShouldBeNil)
					So(len(subdirs), ShouldEqual, 2)
				})
			})
		})

		Convey("SetProvider fails on a nil provider", func() {
			err := s.SetProvider(nil)
			So(err, ShouldNotBeNil)
		})

		Convey("SetProvider fails when provider returns nil basedirs", func() {
			err := s.SetProvider(nilBaseDirsProvider{})
			So(err, ShouldNotBeNil)
		})

		Convey("SetProvider fails when MountTimestamps fails", func() {
			path, err := CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			p, err := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(err, ShouldBeNil)

			tp, ok := p.(*testProvider)
			So(ok, ShouldBeTrue)

			tp.bd = badMountTSReader{Reader: tp.bd, err: errMountTimestamps}
			err = s.SetProvider(tp)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "mount timestamps error")
		})

		Convey("SetProvider logs provider messages", func() {
			path, err := CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			p, err := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(err, ShouldBeNil)

			tp, ok := p.(*testProvider)
			So(ok, ShouldBeTrue)

			tp.pendingMessages = []string{
				"clickhouse: active tree summary refresh scheduled asynchronously active_mounts=1",
				"clickhouse: active tree summary refresh started active_mounts=1",
			}

			err = s.SetProvider(tp)
			So(err, ShouldBeNil)
			So(
				logWriter.String(),
				ShouldContainSubstring,
				"provider message: clickhouse: active tree summary refresh scheduled asynchronously",
			)
			So(
				logWriter.String(),
				ShouldContainSubstring,
				"provider message: clickhouse: active tree summary refresh started",
			)

			tp.triggerMessage(
				"clickhouse: active tree summary refresh completed active_mounts=1",
			)
			So(logWriter.String(), ShouldContainSubstring, "provider message: clickhouse: active tree summary refresh completed")
		})

		Convey("SetProvider sanitises invalid usage times during cache prewarm", func() {
			path, err := CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			p, err := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(err, ShouldBeNil)

			tp, ok := p.(*testProvider)
			So(ok, ShouldBeTrue)

			invalidYear := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
			tpMBD, ok := tp.bd.(*memBaseDirs)
			So(ok, ShouldBeTrue)
			So(tpMBD.groupUsage[db.DGUTAgeAll], ShouldNotBeEmpty)
			So(tpMBD.userUsage[db.DGUTAgeAll], ShouldNotBeEmpty)

			tpMBD.groupUsage[db.DGUTAgeAll][0].Mtime = invalidYear
			tpMBD.groupUsage[db.DGUTAgeAll][0].DateNoSpace = invalidYear
			tpMBD.groupUsage[db.DGUTAgeAll][0].DateNoFiles = invalidYear
			tpMBD.userUsage[db.DGUTAgeAll][0].Mtime = invalidYear

			err = s.SetProvider(tp)
			So(err, ShouldBeNil)

			response, err := query(s, EndPointBasedirUsageGroup, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusOK)

			usageGroup, err := decodeUsageResult(response)
			So(err, ShouldBeNil)
			So(usageGroup, ShouldNotBeEmpty)
			So(usageGroup[0].Mtime.Year(), ShouldBeLessThanOrEqualTo, 9999)
			So(usageGroup[0].DateNoSpace, ShouldEqual, time.Time{})
			So(usageGroup[0].DateNoFiles, ShouldEqual, time.Time{})

			response, err = query(s, EndPointBasedirUsageUser, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusOK)

			usageUser, err := decodeUsageResult(response)
			So(err, ShouldBeNil)
			So(usageUser, ShouldNotBeEmpty)
			So(usageUser[0].Mtime.Year(), ShouldBeLessThanOrEqualTo, 9999)
		})

		Reset(func() { s.Stop() })

		Convey("Server updates when provider updates", func() {
			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			tmp := t.TempDir()

			first := filepath.Join(tmp, "111_keyA")
			err = CreateExampleDBsCustomIDsWithDir(t, first, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			p, err := BuildTestProviderWithMountTimestamps(t, []string{first}, ownersPath, map[string]time.Time{
				"keyA": time.Unix(refTime, 0),
			})
			So(err, ShouldBeNil)

			tp, ok := p.(*testProvider)
			So(ok, ShouldBeTrue)

			err = s.SetProvider(tp)
			So(err, ShouldBeNil)

			dirguta := s.tree
			basedirs := s.basedirs
			lastMod := s.dataTimeStamp["keyA"]

			So(len(s.dataTimeStamp), ShouldEqual, 1)

			second := filepath.Join(tmp, "112_keyB")
			err = CreateExampleDBsCustomIDsWithDir(t, second, uid, gids[0], gids[1], refTime+10)
			So(err, ShouldBeNil)
			s.mu.Lock()
			initialGroupCache := s.groupUsageCache
			initialUserCache := s.userUsageCache
			s.mu.Unlock()

			So(initialGroupCache, ShouldNotBeNil)
			So(initialUserCache, ShouldNotBeNil)

			p2, err := BuildTestProviderWithMountTimestamps(t, []string{first, second}, ownersPath, map[string]time.Time{
				"keyA": time.Unix(refTime, 0),
				"keyB": time.Unix(refTime+10, 0),
			})
			So(err, ShouldBeNil)

			tp2, ok := p2.(*testProvider)
			So(ok, ShouldBeTrue)
			tp.triggerUpdate(tp2.tree, tp2.bd)

			timeout := time.After(time.Second)

		Loop:
			for {
				select {
				case <-timeout:
					break Loop
				case <-time.After(time.Millisecond):
					s.mu.RLock()
					dataTimeStamp := s.dataTimeStamp["keyB"]
					s.mu.RUnlock()

					if dataTimeStamp > lastMod {
						break Loop
					}
				}
			}

			So(s.tree == dirguta, ShouldBeFalse)
			So(s.basedirs == basedirs, ShouldBeFalse)
			So(len(s.dataTimeStamp), ShouldEqual, 2)
			So(s.dataTimeStamp["keyB"], ShouldBeGreaterThan, lastMod)

			s.mu.RLock()
			latestGroupCache := s.groupUsageCache
			latestUserCache := s.userUsageCache
			s.mu.RUnlock()
			So(latestGroupCache, ShouldNotResemble, initialGroupCache)
			So(latestUserCache, ShouldNotResemble, initialUserCache)
		})

		Convey("Server sanitises invalid usage times during provider updates", func() {
			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			tmp := t.TempDir()

			first := filepath.Join(tmp, "111_keyA")
			err = CreateExampleDBsCustomIDsWithDir(t, first, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			p, err := BuildTestProviderWithMountTimestamps(t, []string{first}, ownersPath, map[string]time.Time{
				"keyA": time.Unix(refTime, 0),
			})
			So(err, ShouldBeNil)

			tp, ok := p.(*testProvider)
			So(ok, ShouldBeTrue)

			err = s.SetProvider(tp)
			So(err, ShouldBeNil)

			response, err := query(s, EndPointBasedirUsageGroup, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusOK)

			s.mu.RLock()
			lastMod := s.dataTimeStamp["keyA"]
			s.mu.RUnlock()

			second := filepath.Join(tmp, "112_keyA")
			err = CreateExampleDBsCustomIDsWithDir(t, second, uid, gids[0], gids[1], refTime+10)
			So(err, ShouldBeNil)

			p2, err := BuildTestProviderWithMountTimestamps(t, []string{second}, ownersPath, map[string]time.Time{
				"keyA": time.Unix(refTime+10, 0),
			})
			So(err, ShouldBeNil)

			tp2, ok := p2.(*testProvider)
			So(ok, ShouldBeTrue)

			invalidYear := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
			tp2MBD, ok := tp2.bd.(*memBaseDirs)
			So(ok, ShouldBeTrue)
			So(tp2MBD.groupUsage[db.DGUTAgeAll], ShouldNotBeEmpty)
			So(tp2MBD.userUsage[db.DGUTAgeAll], ShouldNotBeEmpty)

			tp2MBD.groupUsage[db.DGUTAgeAll][0].Mtime = invalidYear
			tp2MBD.groupUsage[db.DGUTAgeAll][0].DateNoSpace = invalidYear
			tp2MBD.groupUsage[db.DGUTAgeAll][0].DateNoFiles = invalidYear
			tp2MBD.userUsage[db.DGUTAgeAll][0].Mtime = invalidYear

			tp.triggerUpdate(tp2.tree, tp2.bd)

			timeout := time.After(2 * time.Second)

			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

		Loop:
			for {
				select {
				case <-timeout:
					t.Fatal("timeout waiting for provider update with sanitised caches")
				case <-ticker.C:
					s.mu.RLock()
					ts, ok := s.dataTimeStamp["keyA"]
					s.mu.RUnlock()

					if ok && ts > lastMod {
						break Loop
					}
				}
			}

			response, err = query(s, EndPointBasedirUsageGroup, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusOK)

			usageGroup, err := decodeUsageResult(response)
			So(err, ShouldBeNil)
			So(usageGroup, ShouldNotBeEmpty)
			So(usageGroup[0].Mtime.Year(), ShouldBeLessThanOrEqualTo, 9999)
			So(usageGroup[0].DateNoSpace, ShouldEqual, time.Time{})
			So(usageGroup[0].DateNoFiles, ShouldEqual, time.Time{})

			response, err = query(s, EndPointBasedirUsageUser, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusOK)

			usageUser, err := decodeUsageResult(response)
			So(err, ShouldBeNil)
			So(usageUser, ShouldNotBeEmpty)
			So(usageUser[0].Mtime.Year(), ShouldBeLessThanOrEqualTo, 9999)
		})

		Convey("prewarmCaches fills caches with JSON and gzip", func() {
			path, err := CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			p, err := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(err, ShouldBeNil)
			err = s.SetProvider(p)
			So(err, ShouldBeNil)

			err = s.prewarmCaches(s.basedirs)
			So(err, ShouldBeNil)

			So(s.groupUsageCache, ShouldNotBeNil)
			So(s.userUsageCache, ShouldNotBeNil)
			So(len(s.groupUsageCache.jsonData), ShouldBeGreaterThan, 0)
			So(len(s.groupUsageCache.gzipData), ShouldBeGreaterThan, 0)
			So(len(s.userUsageCache.jsonData), ShouldBeGreaterThan, 0)
			So(len(s.userUsageCache.gzipData), ShouldBeGreaterThan, 0)
		})

		Convey("prewarmCaches keeps both caches unchanged if user cache build fails", func() {
			path, err := CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			p, err := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(err, ShouldBeNil)
			err = s.SetProvider(p)
			So(err, ShouldBeNil)

			s.mu.RLock()
			initialGroupCache := s.groupUsageCache
			initialUserCache := s.userUsageCache
			s.mu.RUnlock()

			err = s.prewarmCaches(badUserUsageReader{Reader: s.basedirs, err: errMountTimestamps})
			So(err, ShouldEqual, errMountTimestamps)

			s.mu.RLock()
			deferredGroupCache := s.groupUsageCache
			deferredUserCache := s.userUsageCache
			s.mu.RUnlock()

			So(deferredGroupCache, ShouldResemble, initialGroupCache)
			So(deferredUserCache, ShouldResemble, initialUserCache)
		})

		Convey("Provider update refreshes server timestamps", func() {
			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			tmp := t.TempDir()

			first := filepath.Join(tmp, "111_keyA")
			err = CreateExampleDBsCustomIDsWithDir(t, first, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			p, err := BuildTestProviderWithMountTimestamps(t, []string{first}, ownersPath, map[string]time.Time{
				"keyA": time.Unix(refTime, 0),
			})
			So(err, ShouldBeNil)

			tp, ok := p.(*testProvider)
			So(ok, ShouldBeTrue)

			err = s.SetProvider(tp)
			So(err, ShouldBeNil)

			s.mu.RLock()
			oldTree := s.tree
			oldBD := s.basedirs
			oldTS := s.dataTimeStamp["keyA"]
			s.mu.RUnlock()

			second := filepath.Join(tmp, "112_keyA")
			err = CreateExampleDBsCustomIDsWithDir(t, second, uid, gids[0], gids[1], refTime+10)
			So(err, ShouldBeNil)

			p2, err := BuildTestProviderWithMountTimestamps(t, []string{second}, ownersPath, map[string]time.Time{
				"keyA": time.Unix(refTime+10, 0),
			})
			So(err, ShouldBeNil)

			tp2, ok := p2.(*testProvider)
			So(ok, ShouldBeTrue)
			tp.triggerUpdate(tp2.tree, tp2.bd)

			timeout := time.After(2 * time.Second)

		Loop:
			for {
				select {
				case <-timeout:
					t.Fatal("timeout waiting for incremental reload")
				case <-time.After(10 * time.Millisecond):
					s.mu.RLock()
					ts, ok := s.dataTimeStamp["keyA"]
					s.mu.RUnlock()

					if ok && ts > oldTS {
						break Loop
					}
				}
			}

			s.mu.RLock()
			newTree := s.tree
			newBD := s.basedirs
			newTS := s.dataTimeStamp["keyA"]
			s.mu.RUnlock()

			So(s.dataTimeStamp["keyA"], ShouldEqual, newTS)
			So(newTS, ShouldBeGreaterThan, oldTS)

			So(newTree == oldTree, ShouldBeFalse)
			So(newBD == oldBD, ShouldBeFalse)
		})

		Convey("serveGzippedCache serves group and user usage via HTTP", func() {
			path, err := CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, fmt.Sprintf("0,Alan\n%s,Barbara\n%s,Dellilah", gids[0], gids[1]))
			So(err, ShouldBeNil)

			p, err := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(err, ShouldBeNil)
			err = s.SetProvider(p)
			So(err, ShouldBeNil)

			timeout := time.After(time.Second)
			tick := time.Tick(5 * time.Millisecond)

		Loop:
			for {
				select {
				case <-timeout:
					break Loop
				case <-tick:
					s.mu.RLock()
					userReady := len(s.userUsageCache.jsonData) > 0
					s.mu.RUnlock()

					if userReady {
						break Loop
					}
				}
			}

			response, err := query(s, EndPointBasedirUsageGroup, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusOK)

			usageGroup, err := decodeUsageResult(response)
			So(err, ShouldBeNil)
			So(len(usageGroup), ShouldBeGreaterThan, 0)
			So(usageGroup[0].GID, ShouldEqual, 0)
			So(usageGroup[0].UID, ShouldEqual, 0)
			So(usageGroup[0].Name, ShouldNotBeBlank)
			So(usageGroup[0].Owner, ShouldNotBeBlank)
			So(usageGroup[0].BaseDir, ShouldNotBeBlank)

			response, err = query(s, EndPointBasedirUsageUser, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusOK)

			usageUser, err := decodeUsageResult(response)
			So(err, ShouldBeNil)
			So(len(usageUser), ShouldBeGreaterThan, 0)
			So(usageUser[0].GID, ShouldEqual, 0)
			So(usageUser[0].UID, ShouldEqual, 0)
			So(usageUser[0].Name, ShouldNotBeBlank)
			So(usageUser[0].Owner, ShouldNotBeBlank)
			So(usageUser[0].BaseDir, ShouldNotBeBlank)
		})

		Convey("serveGzippedCache serves group and user usage with gzip handling", func() {
			path, err := CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			p, err := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(err, ShouldBeNil)
			err = s.SetProvider(p)
			So(err, ShouldBeNil)

			err = s.prewarmCaches(s.basedirs)
			So(err, ShouldBeNil)

			makeContext := func(acceptEnc string) (*gin.Context, *httptest.ResponseRecorder) {
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)

				req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
				So(err, ShouldBeNil)

				if acceptEnc != "" {
					req.Header.Set("Accept-Encoding", acceptEnc)
				}

				c.Request = req

				return c, w
			}
			c, w := makeContext("")
			s.serveGzippedCache(c, s.userUsageCache)
			So(w.Header().Get("Content-Encoding"), ShouldEqual, "gzip")

			c, w = makeContext("gzip")
			s.serveGzippedCache(c, s.userUsageCache)
			So(w.Header().Get("Content-Encoding"), ShouldEqual, "gzip")

			c, w = makeContext("gzip;q=0")
			s.serveGzippedCache(c, s.userUsageCache)
			So(w.Header().Get("Content-Encoding"), ShouldNotEqual, "gzip")

			c, w = makeContext("*;q=1")
			s.serveGzippedCache(c, s.userUsageCache)
			So(w.Header().Get("Content-Encoding"), ShouldEqual, "gzip")
		})
	})
}

// getExampleGIDs returns some example GIDs to test with, using 2 real ones from
// the given slice if the slice is long enough.
func getExampleGIDs(gids []string) []string {
	exampleGIDs := []string{"3", "4"}
	if len(gids) > 1 {
		exampleGIDs[0] = gids[0]
		exampleGIDs[1] = gids[1]
	}

	return exampleGIDs
}

// gidToGroup converts the given gid to a group name.
func gidToGroup(t *testing.T, gid string) string {
	t.Helper()

	g, err := user.LookupGroupId(gid)
	if err != nil {
		t.Fatalf("LookupGroupId(%s) failed: %s", gid, err)
	}

	return g.Name
}

// testRestrictedGroups does tests for s.getRestrictedGIDs() if user running the
// test has enough groups to make the test viable.
func testRestrictedGroups(t *testing.T, gids []string, s *Server, exampleGIDs []string,
	addr, certPath, token, tokenBadUID string,
) {
	t.Helper()

	if len(gids) < 3 {
		return
	}

	var (
		filterGIDs []uint32
		errg       error
	)

	s.AuthRouter().GET("/groups", func(c *gin.Context) {
		filterGIDs = nil

		groups := c.Query("groups")

		filterGIDs, errg = s.getRestrictedGIDs(c, groups)
	})

	groups := gidsToGroups(t, gids...)
	r := gas.NewAuthenticatedClientRequest(addr, certPath, token)
	_, err := r.Get(gas.EndPointAuth + "/groups?groups=" + groups[0])
	So(err, ShouldBeNil)

	So(errg, ShouldBeNil)

	gid0u, err := strconv.ParseUint(exampleGIDs[0], 10, 32)
	So(err, ShouldBeNil)

	So(filterGIDs, ShouldResemble, []uint32{uint32(gid0u)})

	r = gas.NewAuthenticatedClientRequest(addr, certPath, token)
	_, err = r.Get(gas.EndPointAuth + "/groups?groups=0")
	So(err, ShouldBeNil)

	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)

	s.userToGIDs = make(map[string][]string)

	rBadUID := gas.NewAuthenticatedClientRequest(addr, certPath, tokenBadUID)
	_, err = rBadUID.Get(gas.EndPointAuth + "/groups?groups=" + groups[0])
	So(err, ShouldBeNil)
	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)

	s.WhiteListGroups(func(gid string) bool {
		return gid == gids[0]
	})

	s.userToGIDs = make(map[string][]string)

	r = gas.NewAuthenticatedClientRequest(addr, certPath, token)
	_, err = r.Get(gas.EndPointAuth + "/groups?groups=root")
	So(err, ShouldBeNil)

	So(errg, ShouldBeNil)
	So(filterGIDs, ShouldResemble, []uint32{0})

	s.WhiteListGroups(func(group string) bool {
		return false
	})

	s.userToGIDs = make(map[string][]string)

	r = gas.NewAuthenticatedClientRequest(addr, certPath, token)
	_, err = r.Get(gas.EndPointAuth + "/groups?groups=root")
	So(err, ShouldBeNil)

	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)
}

// gidsToGroups converts the given gids to group names.
func gidsToGroups(t *testing.T, gids ...string) []string {
	t.Helper()

	groups := make([]string, len(gids))

	for i, gid := range gids {
		groups[i] = gidToGroup(t, gid)
	}

	return groups
}

// testClientsOnRealServer tests our client method GetWhereDataIs and the tree
// webpage on a real listening server, if we have at least 2 gids to test with.
func testClientsOnRealServer(t *testing.T, username, uid string, gids []string, s *Server, addr, cert, key string) {
	t.Helper()

	if len(gids) < 2 {
		return
	}

	g, errg := user.LookupGroupId(gids[0])
	So(errg, ShouldBeNil)

	refTime := time.Now().Unix()

	Convey("Given databases", func() {
		jwtBasename := ".wrstat.test.jwt"
		serverTokenBasename := ".wrstat.test.servertoken" //nolint:gosec

		c, err := gas.NewClientCLI(jwtBasename, serverTokenBasename, "localhost:1", cert, true)
		So(err, ShouldBeNil)

		_, _, err = GetWhereDataIs(c, "", "", "", "", db.DGUTAgeAll, "")
		So(err, ShouldNotBeNil)

		path, err := CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
		So(err, ShouldBeNil)

		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		c, err = gas.NewClientCLI(jwtBasename, serverTokenBasename, addr, cert, false)
		So(err, ShouldBeNil)

		Convey("You can't get where data is or add the tree page without auth", func() {
			p, provErr := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(provErr, ShouldBeNil)

			setErr := s.SetProvider(p)
			So(setErr, ShouldBeNil)

			_, _, err = GetWhereDataIs(c, "/", "", "", "", db.DGUTAgeAll, "")
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, gas.ErrNoAuth)

			err = s.AddTreePage()
			So(err, ShouldNotBeNil)
		})

		Convey("Root can see everything", func() {
			err = s.EnableAuthWithServerToken(cert, key, serverTokenBasename, func(username, password string) (bool, string) {
				return true, ""
			})
			So(err, ShouldBeNil)

			p, provErr := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(provErr, ShouldBeNil)

			setErr := s.SetProvider(p)
			So(setErr, ShouldBeNil)

			err = c.Login("user", "pass")
			So(err, ShouldBeNil)

			_, _, err = GetWhereDataIs(c, " ", "", "", "", db.DGUTAgeAll, "")
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, ErrBadQuery)

			json, dcss, errg := GetWhereDataIs(c, "/", "", "", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 24)

			json, dcss, errg = GetWhereDataIs(c, "/", g.Name, "", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 13)

			json, dcss, errg = GetWhereDataIs(c, "/", "", "root", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 14)

			json, dcss, errg = GetWhereDataIs(c, "/", "", "", "", db.DGUTAgeA7Y, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 19)
		})

		Convey("Normal users have access restricted only by group", func() {
			err = s.EnableAuth(cert, key, func(username, password string) (bool, string) {
				return true, uid
			})
			So(err, ShouldBeNil)

			p, provErr := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(provErr, ShouldBeNil)

			setErr := s.SetProvider(p)
			So(setErr, ShouldBeNil)

			err = c.Login("user", "pass")
			So(err, ShouldBeNil)

			json, dcss, errg := GetWhereDataIs(c, "/", "", "", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 23)

			json, dcss, errg = GetWhereDataIs(c, "/", g.Name, "", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 13)

			_, _, errg = GetWhereDataIs(c, "/", "", "root", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 13)
		})

		Convey("Once you add the tree page", func() {
			var logWriter strings.Builder

			s := New(&logWriter)

			err = s.EnableAuth(cert, key, func(username, password string) (bool, string) {
				return true, uid
			})
			So(err, ShouldBeNil)

			p, err := BuildTestProvider(t, []string{path}, ownersPath, time.Unix(refTime, 0))
			So(err, ShouldBeNil)
			err = s.SetProvider(p)
			So(err, ShouldBeNil)

			s.dataTimeStamp = map[string]int64{}

			s.gidToNameCache[1] = "GroupA"
			s.gidToNameCache[2] = "GroupB"
			s.gidToNameCache[3] = "GroupC"
			s.gidToNameCache[77777] = "77777"
			s.uidToNameCache[101] = "UserA"
			s.uidToNameCache[102] = "UserB"
			s.uidToNameCache[103] = "UserC"
			s.uidToNameCache[88888] = "88888"

			s.basedirs.SetCachedGroup(1, "GroupA")
			s.basedirs.SetCachedGroup(2, "GroupB")
			s.basedirs.SetCachedGroup(3, "GroupC")
			s.basedirs.SetCachedUser(77777, "77777")
			s.basedirs.SetCachedUser(101, "UserA")
			s.basedirs.SetCachedUser(102, "UserB")
			s.basedirs.SetCachedUser(103, "UserC")
			s.basedirs.SetCachedUser(88888, "88888")

			err = s.AddTreePage()
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, cert, key)
			So(err, ShouldBeNil)

			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

			token, err := gas.Login(gas.NewClientRequest(addr, cert), "user", "pass")
			So(err, ShouldBeNil)

			Convey("You can get the static tree web page", func() {
				r := gas.NewAuthenticatedClientRequest(addr, cert, token)

				resp, errGet := r.Get("tree/tree.html")
				So(errGet, ShouldBeNil)
				So(strings.ToUpper(string(resp.Body())), ShouldStartWith, "<!DOCTYPE HTML>")

				resp, err = r.Get("")
				So(err, ShouldBeNil)
				So(strings.ToUpper(string(resp.Body())), ShouldStartWith, "<!DOCTYPE HTML>")
			})

			Convey("You can send data to the analytics endpoint", func() {
				So(s.InitAnalyticsDB(filepath.Join(t.TempDir(), "db")), ShouldBeNil)

				getAndClear := func() []analyticsData {
					r, errr := s.analyticsDB.QueryContext(context.Background(), "SELECT user, session, state, time FROM [events];")
					So(errr, ShouldBeNil)

					var rows []analyticsData

					for r.Next() {
						var ad analyticsData

						So(r.Scan(&ad.Name, &ad.Session, &ad.Data, &ad.Time), ShouldBeNil)

						rows = append(rows, ad)
					}

					r.Close()

					_, err = s.analyticsDB.ExecContext(context.Background(), "DELETE FROM [events];")
					So(err, ShouldBeNil)

					return rows
				}

				var start, end int64

				sessionID := "AAA"
				sendBeacon := func(referers ...string) {
					start = time.Now().Unix()

					for _, referer := range referers {
						r := gas.NewClientRequest(addr, cert)
						r.Cookies = append(r.Cookies, &http.Cookie{Name: "jwt", Value: token})
						r.Body = sessionID

						r.Header.Set("Referer", referer)

						_, err = r.Post(EndPointAuthSpyware)
						So(err, ShouldBeNil)
					}

					end = time.Now().Unix() + 1
				}

				checkTimes := func(data []analyticsData) {
					for n := range data {
						So(data[n].Time, ShouldBeBetweenOrEqual, start, end)

						data[n].Time = 0
					}
				}

				sendBeacon("")

				d := getAndClear()

				checkTimes(d)

				So(d, ShouldResemble, []analyticsData{
					{Name: "user", Session: "AAA", Data: "{}\n"},
				})

				sendBeacon(`?useCount=true&owners=["a","bc"]`, `?filterMaxSize=123&users=[1,2,3]&byUser="badString"`)

				d = getAndClear()

				checkTimes(d)

				So(d, ShouldResemble, []analyticsData{
					{Name: "user", Session: "AAA", Data: "{\"owners\":[\"a\",\"bc\"],\"useCount\":true}\n"},
					{Name: "user", Session: "AAA", Data: "{\"filterMaxSize\":123,\"users\":[1,2,3]}\n"},
				})
			})

			Convey("You can access the tree API", func() {
				r := gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, errTree := r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					Get(EndPointAuthTree)

				So(errTree, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				users := []string{"root", username}
				sort.Strings(users)

				unsortedGroups := gidsToGroups(t, gids[0], gids[1], "0")
				groups := make([]string, len(unsortedGroups))
				copy(groups, unsortedGroups)
				sort.Strings(groups)

				expectedFTs := []string{"bam", "cram", "dir", "temp"}
				expectedAtime := "1970-01-01T00:00:50Z"
				expectedMtime := "1970-01-01T00:01:30Z"

				const numRootDirectories = 13

				const numADirectories = 12

				const directorySize = 4096

				tm := *resp.Result().(*TreeElement) //nolint:forcetypeassert,errcheck

				rootExpectedMtime := tm.Mtime
				So(len(tm.Children), ShouldBeGreaterThan, 1)
				kExpectedAtime := tm.Children[1].Atime
				So(tm, ShouldResemble, TreeElement{
					Name:        "/",
					Path:        "/",
					Count:       24 + numRootDirectories + 1,
					Size:        141 + (numRootDirectories+1)*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.Range7Years,
					Mtime:       rootExpectedMtime,
					CommonMTime: summary.Range7Years,
					Users:       users,
					Groups:      groups,
					FileTypes:   expectedFTs,
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "a",
							Path:        "/a",
							Count:       19 + numADirectories,
							Size:        126 + numADirectories*directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.Range7Years,
							Mtime:       expectedMtime,
							CommonMTime: summary.Range7Years,
							Users:       users,
							Groups:      groups,
							FileTypes:   expectedFTs,
							HasChildren: true,
							Children:    nil,
						},
						{
							Name:        "k",
							Path:        "/k",
							Count:       5 + 1,
							Size:        15 + 1*directorySize,
							Atime:       kExpectedAtime,
							CommonATime: summary.RangeLess1Month,
							Mtime:       rootExpectedMtime,
							CommonMTime: summary.RangeLess1Month,
							Users:       []string{username},
							Groups:      []string{unsortedGroups[1]},
							FileTypes:   []string{"cram", "dir"},
							HasChildren: false,
							Children:    nil,
						},
					},
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path":   "/",
						"groups": g.Name,
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				expectedMtime2 := "1970-01-01T00:01:20Z"

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert,errcheck
				So(tm, ShouldResemble, TreeElement{
					Name:        "/",
					Path:        "/",
					Count:       13 + 9,
					Size:        120 + 9*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.Range7Years,
					Mtime:       expectedMtime2,
					CommonMTime: summary.Range7Years,
					Users:       users,
					Groups:      []string{g.Name},
					FileTypes:   expectedFTs,
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "a",
							Path:        "/a",
							Count:       13 + 8,
							Size:        120 + 8*directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.Range7Years,
							Mtime:       expectedMtime2,
							CommonMTime: summary.Range7Years,
							Users:       users,
							Groups:      []string{g.Name},
							FileTypes:   expectedFTs,
							HasChildren: true,
							Children:    nil,
						},
					},
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path": "/a",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				abgroups := gidsToGroups(t, g.Gid, "0")
				sort.Strings(abgroups)

				acgroups := gidsToGroups(t, gids[1])
				cramAndDir := []string{"cram", "dir"}

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert,errcheck
				So(tm, ShouldResemble, TreeElement{
					Name:        "a",
					Path:        "/a",
					Count:       19 + numADirectories,
					Size:        126 + numADirectories*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.Range7Years,
					Mtime:       expectedMtime,
					CommonMTime: summary.Range7Years,
					Users:       users,
					Groups:      groups,
					FileTypes:   expectedFTs,
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "b",
							Path:        "/a/b",
							Count:       19 - 5 + numADirectories - 3,
							Size:        126 - 5 + (numADirectories-3)*directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.Range7Years,
							Mtime:       expectedMtime2,
							CommonMTime: summary.Range7Years,
							Users:       users,
							Groups:      abgroups,
							FileTypes:   expectedFTs,
							HasChildren: true,
							Children:    nil,
						},
						{
							Name:        "c",
							Path:        "/a/c",
							Count:       7,
							Size:        5 + 2*directorySize,
							Atime:       "1970-01-01T00:01:30Z",
							CommonATime: summary.Range7Years,
							Mtime:       expectedMtime,
							CommonMTime: summary.Range7Years,
							Users:       []string{"root"},
							Groups:      acgroups,
							FileTypes:   cramAndDir,
							HasChildren: true,
							Children:    nil,
						},
					},
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path": "/a/b/d",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				dgroups := gidsToGroups(t, gids[0], "0")
				sort.Strings(dgroups)

				root := []string{"root"}

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert,errcheck
				So(tm, ShouldResemble, TreeElement{
					Name:        "d",
					Path:        "/a/b/d",
					Count:       12 + 5,
					Size:        111 + 5*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.Range7Years,
					Mtime:       "1970-01-01T00:01:15Z",
					CommonMTime: summary.Range7Years,
					Users:       users,
					Groups:      dgroups,
					FileTypes:   cramAndDir,
					HasChildren: true,
					NoAuth:      false,
					Children: []*TreeElement{
						{
							Name:        "f",
							Path:        "/a/b/d/f",
							Count:       2,
							Size:        10 + directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.RangeLess1Month,
							Mtime:       "1970-01-01T00:00:50Z",
							CommonMTime: summary.Range7Years,
							Users:       []string{username},
							Groups:      []string{g.Name},
							FileTypes:   cramAndDir,
							HasChildren: false,
							Children:    nil,
							NoAuth:      false,
						},
						{
							Name:        "g",
							Path:        "/a/b/d/g",
							Count:       11,
							Size:        100 + directorySize,
							Atime:       "1970-01-01T00:00:50Z",
							CommonATime: summary.Range7Years,
							Mtime:       "1970-01-01T00:01:15Z",
							CommonMTime: summary.Range7Years,
							Users:       users,
							Groups:      []string{g.Name},
							FileTypes:   cramAndDir,
							HasChildren: false,
							Children:    nil,
							NoAuth:      false,
						},
						{
							Name:        "i",
							Path:        "/a/b/d/i",
							Count:       3,
							Size:        1 + 2*directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.RangeLess1Month,
							Mtime:       "1970-01-01T00:00:50Z",
							CommonMTime: summary.Range7Years,
							Users:       root,
							Groups:      root,
							FileTypes:   cramAndDir,
							HasChildren: true,
							Children:    nil,
							NoAuth:      true,
						},
					},
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path": "/a/b/d/i",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert,errcheck
				So(tm, ShouldResemble, TreeElement{
					Name:        "i",
					Path:        "/a/b/d/i",
					Count:       3,
					Size:        1 + 2*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.RangeLess1Month,
					Mtime:       "1970-01-01T00:00:50Z",
					CommonMTime: summary.Range7Years,
					Users:       root,
					Groups:      root,
					FileTypes:   cramAndDir,
					HasChildren: true,
					Children:    nil,
					NoAuth:      true,
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path":   "/",
						"groups": "adsf@£$",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path": "/foo",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)
			})

			Convey("You can access the group-areas endpoint after AddGroupAreas()", func() {
				c, err = gas.NewClientCLI(jwtBasename, serverTokenBasename, addr, cert, false)
				So(err, ShouldBeNil)

				err = c.Login("user", "pass")
				So(err, ShouldBeNil)

				_, err := GetGroupAreas(c)
				So(err, ShouldNotBeNil)

				expectedAreas := map[string][]string{
					"a": {"1", "2"},
					"b": {"3", "4"},
				}

				s.AddGroupAreas(expectedAreas)

				areas, err := GetGroupAreas(c)
				So(err, ShouldBeNil)
				So(areas, ShouldResemble, expectedAreas)
			})

			Convey("You can access the secure basedirs endpoints after LoadDBs()", func() {
				r := gas.NewAuthenticatedClientRequest(addr, cert, token)

				var usage []*basedirs.Usage

				resp, err := r.SetResult(&usage).
					ForceContentType("application/json").
					Get(EndPointAuthBasedirUsageUser)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(usage), ShouldBeGreaterThan, 0)
				So(usage[0].UID, ShouldEqual, 0)

				userUsageUID := usage[0].UID
				userUsageBasedir := usage[0].BaseDir

				resp, err = r.SetResult(&usage).
					ForceContentType("application/json").
					Get(EndPointAuthBasedirUsageGroup)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(usage), ShouldBeGreaterThan, 0)
				So(usage[0].GID, ShouldEqual, 0)

				var subdirs []*basedirs.SubDir

				resp, err = r.SetResult(&subdirs).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"id":      strconv.FormatUint(uint64(usage[0].GID), 10),
						"basedir": usage[0].BaseDir,
					}).
					Get(EndPointAuthBasedirSubdirGroup)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(subdirs), ShouldEqual, 0)

				resp, err = r.SetResult(&subdirs).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"id":      strconv.FormatUint(uint64(userUsageUID), 10),
						"basedir": userUsageBasedir,
					}).
					Get(EndPointAuthBasedirSubdirUser)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(subdirs), ShouldEqual, 2)

				var history []basedirs.History

				resp, err = r.SetResult(&history).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"id":      strconv.FormatUint(uint64(usage[0].GID), 10),
						"basedir": usage[0].BaseDir,
					}).
					Get(EndPointAuthBasedirHistory)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				Convey("and can read subdirs from a different group if you're on the whitelist", func() {
					s.WhiteListGroups(func(_ string) bool {
						return true
					})

					s.userToGIDs = make(map[string][]string)

					resp, err = r.SetResult(&subdirs).
						ForceContentType("application/json").
						SetQueryParams(map[string]string{
							"id":      strconv.FormatUint(uint64(usage[0].GID), 10),
							"basedir": usage[0].BaseDir,
						}).
						Get(EndPointAuthBasedirSubdirGroup)
					So(err, ShouldBeNil)
					So(resp.Result(), ShouldNotBeNil)
					So(len(subdirs), ShouldEqual, 1)

					resp, err = r.SetResult(&subdirs).
						ForceContentType("application/json").
						SetQueryParams(map[string]string{
							"id":      strconv.FormatUint(uint64(userUsageUID), 10),
							"basedir": userUsageBasedir,
						}).
						Get(EndPointAuthBasedirSubdirUser)
					So(err, ShouldBeNil)
					So(resp.Result(), ShouldNotBeNil)
					So(len(subdirs), ShouldEqual, 2)
				})
			})
		})
	})
}

// queryWhere does a test GET of /rest/v1/where, with extra appended (start it
// with ?).
func queryWhere(s *Server, extra string) (*httptest.ResponseRecorder, error) {
	return query(s, EndPointWhere, extra)
}

func query(s *Server, endpoint, extra string) (*httptest.ResponseRecorder, error) {
	return gas.QueryREST(s.Router(), endpoint, extra)
}

func fixDirSummaryTimes(summaries []*DirSummary) {
	for _, dcss := range summaries {
		dcss.Atime = fixtimes.FixTime(dcss.Atime)
		dcss.Mtime = fixtimes.FixTime(dcss.Mtime)
	}
}

// adjustedExpectations returns expected altered so that /a only has the given
// groups and values appropriate for non-root. It also returns root's unaltered
// set of groups.
func adjustedExpectations(expected []*DirSummary, groupA, groupB string) ([]*DirSummary, []string) {
	var expectedGroupsRoot []string

	expectedNonRoot := make([]*DirSummary, len(expected))
	groups := []string{groupA, groupB}
	sort.Strings(groups)

	for i, ds := range expected {
		expectedNonRoot[i] = ds

		switch ds.Dir {
		case "/a":
			expectedNonRoot[i] = &DirSummary{
				Dir:       ds.Dir,
				Count:     18,
				Size:      125,
				Atime:     time.Unix(50, 0),
				Mtime:     time.Unix(90, 0),
				Users:     ds.Users,
				Groups:    groups,
				FileTypes: ds.FileTypes,
			}

			expectedGroupsRoot = ds.Groups
		case "/a/b", "/a/b/d":
			expectedNonRoot[i] = &DirSummary{
				Dir:       ds.Dir,
				Count:     ds.Count - 1,
				Size:      ds.Size - 1,
				Atime:     ds.Atime,
				Mtime:     ds.Mtime,
				Users:     ds.Users,
				Groups:    []string{groupA},
				FileTypes: ds.FileTypes,
			}
		case "/":
			expectedNonRoot[i] = &DirSummary{
				Dir:       ds.Dir,
				Count:     ds.Count - 1,
				Size:      ds.Size - 1,
				Atime:     ds.Atime,
				Mtime:     ds.Mtime,
				Users:     ds.Users,
				Groups:    groups,
				FileTypes: ds.FileTypes,
			}
		}
	}

	return expectedNonRoot, expectedGroupsRoot
}

// decodeWhereResult decodes the result of a Where query.
func decodeWhereResult(response *httptest.ResponseRecorder) ([]*DirSummary, error) {
	var result []*DirSummary

	err := json.NewDecoder(response.Body).Decode(&result)

	fixDirSummaryTimes(result)

	return result, err
}

// runMapMatrixTest tests queries against expected results on the Server.
func runMapMatrixTest(t *testing.T, matrix []*matrixElement, s *Server) {
	t.Helper()

	for _, m := range matrix {
		fixDirSummaryTimes(m.dss)

		response, err := queryWhere(s, m.filter)
		So(err, ShouldBeNil)
		So(response.Code, ShouldEqual, http.StatusOK)

		result, err := decodeWhereResult(response)
		So(err, ShouldBeNil)
		So(result, ShouldResemble, m.dss)
	}
}

// runSliceMatrixTest tests queries that are expected to fail on the Server.
func runSliceMatrixTest(t *testing.T, matrix []string, s *Server) {
	t.Helper()

	for _, filter := range matrix {
		response, err := queryWhere(s, filter)
		So(err, ShouldBeNil)
		So(response.Code, ShouldEqual, http.StatusBadRequest)
	}
}

// decodeUsageResult decodes the result of a basedirs usage query.
func decodeUsageResult(response *httptest.ResponseRecorder) ([]*basedirs.Usage, error) {
	var result []*basedirs.Usage

	var reader io.Reader = response.Body

	if response.Header().Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(response.Body)
		if err != nil {
			return nil, err
		}

		defer gz.Close()
		reader = gz
	}

	err := json.NewDecoder(reader).Decode(&result)
	return result, err
}

// decodeSubdirResult decodes the result of a basedirs subdir query.
func decodeSubdirResult(response *httptest.ResponseRecorder) ([]*basedirs.SubDir, error) {
	var result []*basedirs.SubDir
	err := json.NewDecoder(response.Body).Decode(&result)

	return result, err
}

func decodeHistoryResult(response *httptest.ResponseRecorder) ([]basedirs.History, error) {
	var result []basedirs.History
	err := json.NewDecoder(response.Body).Decode(&result)

	return result, err
}

type badUserUsageReader struct {
	basedirs.Reader
	err error
}

func (b badUserUsageReader) UserUsage(db.DirGUTAge) ([]*basedirs.Usage, error) {
	return nil, b.err
}

type nilBaseDirsProvider struct{}

func (p nilBaseDirsProvider) Tree() *db.Tree { return nil }

func (p nilBaseDirsProvider) BaseDirs() basedirs.Reader { return nil }

func (p nilBaseDirsProvider) OnUpdate(cb func()) {}

func (p nilBaseDirsProvider) OnError(cb func(error)) {}

func (p nilBaseDirsProvider) Close() error { return nil }

type analyticsData struct {
	Name, Session, Data string
	Time                int64
}

type matrixElement struct {
	filter string
	dss    []*DirSummary
}

type e4ActiveSetProvider struct {
	activeSetID string
}

func (p *e4ActiveSetProvider) Tree() *db.Tree { return nil }

func (p *e4ActiveSetProvider) BaseDirs() basedirs.Reader { return nil }

func (p *e4ActiveSetProvider) OnUpdate(func()) {}

func (p *e4ActiveSetProvider) OnError(func(error)) {}

func (p *e4ActiveSetProvider) Close() error { return nil }

func (p *e4ActiveSetProvider) ActiveSetID() string { return p.activeSetID }

func TestE4ServerTacticalSupport(t *testing.T) {
	Convey("E4.2 REST where response cache misses when active set id or filter changes", t, func() {
		activeSet := &e4ActiveSetProvider{activeSetID: e4ActiveSetA}
		database := &e4ResponseCacheDB{activeSet: activeSet}
		s := New(gas.NewStringLogger())
		s.provider = activeSet
		s.tree = db.NewTree(database)
		s.activeSetID = e4ActiveSetA
		s.addBaseDGUTARoutes()

		first, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other")
		So(err, ShouldBeNil)
		So(first, ShouldResemble, []uint64{101})
		So(database.whereCalls, ShouldEqual, 1)

		repeated, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other")
		So(err, ShouldBeNil)
		So(repeated, ShouldResemble, first)
		So(database.whereCalls, ShouldEqual, 1)

		activeSet.activeSetID = e4ActiveSetB
		s.activeSetID = e4ActiveSetB
		changedActiveSet, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other")
		So(err, ShouldBeNil)
		So(changedActiveSet, ShouldResemble, []uint64{202})
		So(database.whereCalls, ShouldEqual, 2)

		changedFilter, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=bam")
		So(err, ShouldBeNil)
		So(changedFilter, ShouldResemble, []uint64{303})
		So(database.whereCalls, ShouldEqual, 3)
	})

	Convey("E4.2 REST where response cache misses when splits changes response shape", t, func() {
		activeSet := &e4ActiveSetProvider{activeSetID: e4ActiveSetA}
		database := &e4SplitsResponseCacheDB{}
		s := New(gas.NewStringLogger())
		s.provider = activeSet
		s.tree = db.NewTree(database)
		s.activeSetID = e4ActiveSetA
		s.addBaseDGUTARoutes()

		flat, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other&splits=0")
		So(err, ShouldBeNil)
		So(flat, ShouldResemble, []uint64{111})
		So(database.whereCalls, ShouldEqual, 1)

		repeatedFlat, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other&splits=0")
		So(err, ShouldBeNil)
		So(repeatedFlat, ShouldResemble, flat)
		So(database.whereCalls, ShouldEqual, 1)

		split, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other&splits=1")
		So(err, ShouldBeNil)
		So(split, ShouldResemble, []uint64{111, 22})
		So(database.whereCalls, ShouldEqual, 2)
	})

	Convey("E4.2 provider active-set updates cannot poison response cache before tree swap", t, func() {
		activeSet := &e4ActiveSetProvider{activeSetID: e4ActiveSetA}
		oldDatabase := &e4StaticResponseCacheDB{count: 101}
		newDatabase := &e4StaticResponseCacheDB{count: 202}
		s := New(gas.NewStringLogger())
		s.provider = activeSet
		s.tree = db.NewTree(oldDatabase)
		s.activeSetID = e4ActiveSetA
		s.addBaseDGUTARoutes()

		first, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other")
		So(err, ShouldBeNil)
		So(first, ShouldResemble, []uint64{101})
		So(oldDatabase.whereCalls, ShouldEqual, 1)

		activeSet.activeSetID = e4ActiveSetB
		duringUpdate, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other")
		So(err, ShouldBeNil)
		So(duringUpdate, ShouldResemble, first)
		So(oldDatabase.whereCalls, ShouldEqual, 1)

		s.mu.Lock()
		s.tree = db.NewTree(newDatabase)
		s.activeSetID = e4ActiveSetB
		s.mu.Unlock()

		afterSwap, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other")
		So(err, ShouldBeNil)
		So(afterSwap, ShouldResemble, []uint64{202})
		So(newDatabase.whereCalls, ShouldEqual, 1)
	})

	Convey("E4.2 SetProvider clears response cache when replacing provider with same active set", t, func() {
		oldDatabase := &e4StaticResponseCacheDB{count: 101}
		newDatabase := &e4StaticResponseCacheDB{count: 202}
		bd := &memBaseDirs{
			mountTimestamps: map[string]time.Time{e2T283Dir: time.Unix(123, 0)},
		}
		s := New(gas.NewStringLogger())

		oldProvider := &testProvider{
			tree:        db.NewTree(oldDatabase),
			bd:          bd,
			activeSetID: e4ActiveSetA,
		}
		So(s.SetProvider(oldProvider), ShouldBeNil)

		first, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other")
		So(err, ShouldBeNil)
		So(first, ShouldResemble, []uint64{101})
		So(oldDatabase.whereCalls, ShouldEqual, 1)

		newProvider := &testProvider{
			tree:        db.NewTree(newDatabase),
			bd:          bd,
			activeSetID: e4ActiveSetA,
		}
		So(s.SetProvider(newProvider), ShouldBeNil)

		afterSwap, err := e4WhereCounts(s, "?dir=/nfs/t283_imaging/&types=other")
		So(err, ShouldBeNil)
		So(afterSwap, ShouldResemble, []uint64{202})
		So(newDatabase.whereCalls, ShouldEqual, 1)
	})

	Convey("E4.2 cached reverse UID and GID lookups are guarded during response conversion", t, func() {
		s := New(io.Discard)

		for i := range 512 {
			id := uint32(i)
			s.uidToNameCache[id] = fmt.Sprintf("user%d", i)
			s.gidToNameCache[id] = fmt.Sprintf("group%d", i)
		}

		var wg sync.WaitGroup

		errs := make(chan error, 8)
		generatedIDBases := []uint32{100_000, 101_000, 102_000, 103_000}

		for _, base := range generatedIDBases {
			wg.Add(1)
			go func(base uint32) {
				defer wg.Done()

				for offset := range uint32(256) {
					id := base + offset

					s.nameCacheMu.Lock()
					s.uidToNameCache[id] = fmt.Sprintf("generated-user%d", id)
					s.gidToNameCache[id] = fmt.Sprintf("generated-group%d", id)
					s.nameCacheMu.Unlock()

					_ = s.uidsToUsernames([]uint32{id})
					_ = s.gidsToNames([]uint32{id})
				}
			}(base)
		}

		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for range 512 {
					if id, err := s.userNameToUID("user511"); err != nil || id != "511" {
						errs <- errE4UIDLookup

						return
					}

					if id, err := s.groupNameToGID("group511"); err != nil || id != "511" {
						errs <- errE4GIDLookup

						return
					}
				}
			}()
		}

		wg.Wait()
		close(errs)

		var err error
		for got := range errs {
			err = got

			break
		}

		So(err, ShouldBeNil)
	})

	Convey("E4.3 one REST tree request batches child metadata once for the active set", t, func() {
		activeSet := &e4ActiveSetProvider{activeSetID: "tree-set"}
		database := newC3RESTTreeTestDB()
		seedC3RESTHighFanoutParent(database, c3RESTWideParent, 12)

		s := New(io.Discard)
		s.provider = activeSet
		s.tree = db.NewTree(database)
		s.activeSetID = "tree-set"

		got := requestC3RESTTree(t, s, c3RESTWideParent)

		So(got.Children, ShouldHaveLength, 12)
		So(database.dirsHaveChildrenCalls, ShouldHaveLength, 1)
		So(database.dirsHaveChildrenCalls[0], ShouldHaveLength, 12)
	})
}

func e4WhereCounts(s *Server, query string) ([]uint64, error) {
	response, err := queryWhere(s, query)
	if err != nil {
		return nil, err
	}

	result, err := decodeWhereResult(response)
	if err != nil {
		return nil, err
	}

	counts := make([]uint64, len(result))
	for i, summary := range result {
		counts[i] = summary.Count
	}

	return counts, nil
}

func seedA5RESTProjectFixture(t *testing.T, cfg clickhouse.Config) {
	t.Helper()

	paths := internaltest.NewDirectoryPathCreator()

	w, err := clickhouse.NewDGUTAWriter(cfg)
	So(err, ShouldBeNil)
	w.SetMountPath(a5RESTProjectMount)
	w.SetUpdatedAt(time.Date(2026, 6, 7, 18, 30, 0, 0, time.UTC))

	So(w.Add(db.RecordDGUTA{
		Dir:      paths.ToDirectoryPath(a5RESTProjectMount),
		Children: []string{"project/"},
		GUTAs: db.GUTAs{
			c3RESTGUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1),
		},
	}), ShouldBeNil)

	So(w.Add(db.RecordDGUTA{
		Dir:        paths.ToDirectoryPath(a5RESTProjectDir),
		ChildCount: 6,
		Children:   []string{"alpha/", "beta/", "delta/", "gamma/", "omega/", "zeta/"},
		GUTAs: db.GUTAs{
			c3RESTGUTA(7, 11, db.DGUTAFileTypeDir, db.DGUTAgeAll, 1, 1),
		},
	}), ShouldBeNil)

	for _, child := range a5RESTProjectAllChildren() {
		So(w.Add(db.RecordDGUTA{
			Dir: paths.ToDirectoryPath(child.dir),
			GUTAs: db.GUTAs{
				c3RESTGUTA(child.gid, child.uid, child.ft, db.DGUTAgeAll, child.count, child.size),
				c3RESTGUTA(child.gid, child.uid, child.ft, child.age, child.count, child.size),
			},
		}), ShouldBeNil)
	}

	So(w.Close(), ShouldBeNil)
}

func markA5RESTSchema3Ready(t *testing.T, cfg clickhouse.Config, mountPath string, parentDir string) {
	t.Helper()

	conn := openC3RESTClickHouseConn(t, cfg.DSN)
	defer func() { So(conn.Close(), ShouldBeNil) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	So(conn.Exec(
		ctx,
		"CREATE TABLE IF NOT EXISTS wrstat_schema3_snapshot_sets ("+
			"mount_path LowCardinality(String), snapshot_id UUID, schema3_version UInt32, "+
			"dir_facts_rows UInt64, parent_facts_rows UInt64, children_rows UInt64, "+
			"child_filter_all_rows UInt64, dir_filter_all_rows UInt64, manifest_sha256 String, "+
			"refreshed_at DateTime64(3)) ENGINE = MergeTree "+
			"PARTITION BY (mount_path, snapshot_id) ORDER BY (mount_path, snapshot_id, schema3_version)",
	), ShouldBeNil)

	row := conn.QueryRow(
		ctx,
		"SELECT snapshot_id FROM wrstat_mounts_active WHERE mount_path = ?",
		mountPath,
	)

	var snapshotID string
	So(row.Scan(&snapshotID), ShouldBeNil)
	seedA5RESTChildFilterAll(t, conn, snapshotID, mountPath, parentDir)
	seedA5RESTDirFilterAll(t, conn, snapshotID, mountPath)
	So(conn.Exec(
		ctx,
		"INSERT INTO wrstat_schema3_snapshot_sets "+
			"(mount_path, snapshot_id, schema3_version, dir_facts_rows, parent_facts_rows, children_rows, "+
			"child_filter_all_rows, dir_filter_all_rows, manifest_sha256, refreshed_at) "+
			"VALUES (?, toUUID(?), 1, 0, 0, 0, 0, 0, 'a5-rest-test', now())",
		mountPath,
		snapshotID,
	), ShouldBeNil)
}

func seedA5RESTChildFilterAll(t *testing.T, conn ch.Conn, snapshotID string, mountPath string, parentDir string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	So(conn.Exec(
		ctx,
		"CREATE TABLE IF NOT EXISTS wrstat_child_filter_all ("+
			"mount_path LowCardinality(String), snapshot_id UUID, parent_dir String, age UInt8, "+
			"gid UInt32, uid UInt32, ft UInt16, dir String, count UInt64, size UInt64, "+
			"atime_min Int64, mtime_max Int64, atime_buckets Array(UInt64), mtime_buckets Array(UInt64), "+
			"filter_child_count UInt64, child_count UInt64, has_filter_children UInt8, "+
			"has_children UInt8, refreshed_at DateTime64(3)) ENGINE = MergeTree "+
			"PARTITION BY (mount_path, snapshot_id) "+
			"ORDER BY (mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir)",
	), ShouldBeNil)

	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO wrstat_child_filter_all "+
			"(mount_path, snapshot_id, parent_dir, age, gid, uid, ft, dir, count, size, "+
			"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, "+
			"child_count, has_filter_children, has_children, refreshed_at)",
	)
	So(err, ShouldBeNil)

	if parentDir == a5RESTProjectDir {
		seedA5RESTProjectChildFilterAll(t, batch, snapshotID)

		So(batch.Send(), ShouldBeNil)

		return
	}

	for i := range a5RESTHighFanoutChildCount {
		if i%15 != 0 {
			continue
		}

		hasChildren := uint8(0)
		childCount := uint64(0)

		if i == 0 {
			hasChildren = 1
			childCount = 1
		}

		So(batch.Append(
			mountPath,
			snapshotID,
			parentDir,
			uint8(db.DGUTAgeA6M),
			uint32(7),
			uint32(11),
			uint16(db.DGUTAFileTypeBam),
			parentDir+fmt.Sprintf("child%03d", i),
			uint64(1),
			uint64(10+i),
			int64(90),
			int64(250),
			[]uint64{1, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{0, 1, 0, 0, 0, 0, 0, 0, 0},
			uint64(0),
			childCount,
			uint8(0),
			hasChildren,
			time.Now(),
		), ShouldBeNil)
	}

	So(batch.Send(), ShouldBeNil)
}

func seedA5RESTProjectChildFilterAll(t *testing.T, batch interface{ Append(values ...any) error }, snapshotID string) {
	t.Helper()

	for _, child := range a5RESTProjectAllChildren() {
		So(batch.Append(
			a5RESTProjectMount,
			snapshotID,
			a5RESTProjectDir,
			uint8(child.age),
			child.gid,
			child.uid,
			uint16(child.ft),
			child.dir,
			child.count,
			child.size,
			int64(100),
			int64(200),
			[]uint64{child.count, 0, 0, 0, 0, 0, 0, 0, 0},
			[]uint64{0, child.count, 0, 0, 0, 0, 0, 0, 0},
			uint64(0),
			uint64(0),
			uint8(0),
			uint8(0),
			time.Now(),
		), ShouldBeNil)
	}
}

func seedA5RESTDirFilterAll(t *testing.T, conn ch.Conn, snapshotID string, mountPath string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	So(conn.Exec(
		ctx,
		"CREATE TABLE IF NOT EXISTS wrstat_dir_filter_all ("+
			"mount_path LowCardinality(String), snapshot_id UUID, age UInt8, gid UInt32, uid UInt32, "+
			"ft UInt16, dir String, parent_dir String, count UInt64, size UInt64, atime_min Int64, "+
			"mtime_max Int64, atime_buckets Array(UInt64), mtime_buckets Array(UInt64), "+
			"filter_child_count UInt64, child_count UInt64, has_filter_children UInt8, "+
			"has_children UInt8, refreshed_at DateTime64(3)) ENGINE = MergeTree "+
			"PARTITION BY (mount_path, snapshot_id) "+
			"ORDER BY (mount_path, snapshot_id, age, gid, uid, ft, dir)",
	), ShouldBeNil)

	if mountPath != a5RESTProjectMount {
		return
	}

	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO wrstat_dir_filter_all "+
			"(mount_path, snapshot_id, age, gid, uid, ft, dir, parent_dir, count, size, "+
			"atime_min, mtime_max, atime_buckets, mtime_buckets, filter_child_count, "+
			"child_count, has_filter_children, has_children, refreshed_at)",
	)
	So(err, ShouldBeNil)

	for _, fixture := range a5RESTProjectTreeFixtures() {
		for _, child := range a5RESTProjectChildrenForAge(fixture.age) {
			So(batch.Append(
				a5RESTProjectMount,
				snapshotID,
				uint8(child.age),
				child.gid,
				child.uid,
				uint16(child.ft),
				a5RESTProjectDir,
				a5RESTProjectMount,
				child.count,
				child.size,
				int64(100),
				int64(200),
				[]uint64{child.count, 0, 0, 0, 0, 0, 0, 0, 0},
				[]uint64{0, child.count, 0, 0, 0, 0, 0, 0, 0},
				c3RESTNonNegativeIntToUint64(t, fixture.childCount),
				uint64(6),
				uint8(1),
				uint8(1),
				time.Now(),
			), ShouldBeNil)
		}
	}

	So(batch.Send(), ShouldBeNil)
}

type e4ResponseCacheDB struct {
	activeSet  *e4ActiveSetProvider
	whereCalls int
}

func (d *e4ResponseCacheDB) DirInfo(_ string, _ *db.Filter) (*db.DirSummary, error) {
	return nil, db.ErrDirNotFound
}

func (d *e4ResponseCacheDB) Children(_ string) ([]string, error) {
	return nil, nil
}

func (d *e4ResponseCacheDB) Where(dir string, filter *db.Filter, _ func(string) int) (db.DCSs, error) {
	d.whereCalls++

	ft := db.DGUTAFileTypeOther
	if filter != nil && filter.FT != 0 {
		ft = filter.FT
	}

	count := e4ResponseCacheCount(d.activeSet.activeSetID, ft)

	return db.DCSs{{
		Dir:   dir + "facts/",
		Count: count,
		Size:  count * 10,
		FT:    ft,
		Age:   db.DGUTAgeAll,
	}}, nil
}

func e4ResponseCacheCount(activeSetID string, ft db.DirGUTAFileType) uint64 {
	switch {
	case activeSetID == e4ActiveSetA && ft == db.DGUTAFileTypeOther:
		return 101
	case activeSetID == e4ActiveSetB && ft == db.DGUTAFileTypeOther:
		return 202
	case activeSetID == e4ActiveSetB && ft == db.DGUTAFileTypeBam:
		return 303
	default:
		return 404
	}
}

func (d *e4ResponseCacheDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *e4ResponseCacheDB) Close() error {
	return nil
}

type e4SplitsResponseCacheDB struct {
	whereCalls int
}

func (d *e4SplitsResponseCacheDB) DirInfo(_ string, _ *db.Filter) (*db.DirSummary, error) {
	return nil, db.ErrDirNotFound
}

func (d *e4SplitsResponseCacheDB) Children(_ string) ([]string, error) {
	return nil, nil
}

func (d *e4SplitsResponseCacheDB) Where(
	dir string,
	filter *db.Filter,
	recurseCount func(string) int,
) (db.DCSs, error) {
	d.whereCalls++

	ft := db.DGUTAFileTypeOther
	if filter != nil && filter.FT != 0 {
		ft = filter.FT
	}

	dcss := db.DCSs{{
		Dir:   dir + "facts/",
		Count: 111,
		Size:  1110,
		FT:    ft,
		Age:   db.DGUTAgeAll,
	}}
	if recurseCount(dir) > 0 {
		dcss = append(dcss, &db.DirSummary{
			Dir:   dir + "facts/child/",
			Count: 22,
			Size:  220,
			FT:    ft,
			Age:   db.DGUTAgeAll,
		})
	}

	return dcss, nil
}

func (d *e4SplitsResponseCacheDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *e4SplitsResponseCacheDB) Close() error {
	return nil
}

type e4StaticResponseCacheDB struct {
	count      uint64
	whereCalls int
}

func (d *e4StaticResponseCacheDB) DirInfo(_ string, _ *db.Filter) (*db.DirSummary, error) {
	return nil, db.ErrDirNotFound
}

func (d *e4StaticResponseCacheDB) Children(_ string) ([]string, error) {
	return nil, nil
}

func (d *e4StaticResponseCacheDB) Where(dir string, filter *db.Filter, _ func(string) int) (db.DCSs, error) {
	d.whereCalls++

	ft := db.DGUTAFileTypeOther
	if filter != nil && filter.FT != 0 {
		ft = filter.FT
	}

	return db.DCSs{{
		Dir:   dir + "facts/",
		Count: d.count,
		Size:  d.count * 10,
		FT:    ft,
		Age:   db.DGUTAgeAll,
	}}, nil
}

func (d *e4StaticResponseCacheDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (d *e4StaticResponseCacheDB) Close() error {
	return nil
}

func TestE2ServerFilteredRESTAnomaly(t *testing.T) {
	Convey("E2.1 filtered REST where digest and count are independent of root warming order", t, func() {
		directServer := newE2FilteredRESTServer()
		direct, err := queryWhere(directServer, "?dir="+url.QueryEscape(e2T283Dir)+"&types=other")
		So(err, ShouldBeNil)
		So(direct.Code, ShouldEqual, http.StatusOK)

		directResult, err := decodeWhereResult(direct)
		So(err, ShouldBeNil)

		warmedServer := newE2FilteredRESTServer()
		root, err := queryWhere(warmedServer, "?dir=/&types=other")
		So(err, ShouldBeNil)
		So(root.Code, ShouldEqual, http.StatusOK)

		warmed, err := queryWhere(warmedServer, "?dir="+url.QueryEscape(e2T283Dir)+"&types=other")
		So(err, ShouldBeNil)
		So(warmed.Code, ShouldEqual, http.StatusOK)

		warmedResult, err := decodeWhereResult(warmed)
		So(err, ShouldBeNil)

		So(e2ServerSummaryDigest(directResult), ShouldEqual, e2ServerSummaryDigest(warmedResult))
		So(len(directResult), ShouldEqual, len(warmedResult))
		So(len(directResult), ShouldEqual, 2)
	})

	Convey("E2.3 REST perf evidence records result digest and scoped cache hit keys", t, func() {
		s := newE2FilteredRESTServer()
		priorKey := "active_prefix_summary:path=/prior/;filter=ft:1;active_set_id=old;query_version=1"
		keySnapshots := 0
		report, err := s.MeasurePerfHarness(PerfHarnessOptions{
			Repeat:     1,
			WhereDir:   e2T283Dir,
			WhereTypes: db.DGUTAFileTypeOther.String(),
			CacheStats: func() (uint64, uint64) {
				return 1, 0
			},
			CacheHitKeys: func() []string {
				keySnapshots++
				if keySnapshots%2 == 1 {
					return []string{priorKey}
				}

				return []string{priorKey, e2ScopedCacheHitKey}
			},
		})
		So(err, ShouldBeNil)

		whereOp := serverPerfOperation(report, "rest_where")
		So(whereOp.Inputs["result_digest"], ShouldNotBeBlank)
		So(whereOp.Inputs["cache_hit_keys"], ShouldResemble, []string{
			e2ScopedCacheHitKey,
		})
	})
}

func e2ServerSummaryDigest(summaries []*DirSummary) string {
	data, err := json.Marshal(summaries)
	So(err, ShouldBeNil)

	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func a5RESTSeedNameCaches(s *Server) {
	s.gidToNameCache[7] = "group7"
	s.gidToNameCache[8] = "group8"
	s.gidToNameCache[9] = "group9"
	s.uidToNameCache[11] = "user11"
	s.uidToNameCache[12] = "user12"
	s.uidToNameCache[13] = "user13"
}
