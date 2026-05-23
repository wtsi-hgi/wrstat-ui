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
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

var errLegacyPerfTreeOp = errors.New("unexpected legacy perf tree operation")

const (
	legacyPerfOpBasedirsHistory      = "basedirs_history"
	legacyPerfOpMountTimestamps      = "mount_timestamps"
	legacyPerfOpTreeDiskTreeEndpoint = "tree_disktree_endpoint"
)

type legacyPerfQueryBaseDirs struct {
	basedirs.Reader

	calls []string
}

func (r *legacyPerfQueryBaseDirs) MountTimestamps() (map[string]time.Time, error) {
	r.calls = append(r.calls, legacyPerfOpMountTimestamps)

	return map[string]time.Time{"/mnt/": time.Unix(1, 0)}, nil
}

func (r *legacyPerfQueryBaseDirs) History(uint32, string) ([]basedirs.History, error) {
	r.calls = append(r.calls, legacyPerfOpBasedirsHistory)

	return nil, nil
}

func TestRunPerfQuerySuiteOperationSelection(t *testing.T) {
	Convey("runPerfQuerySuite only runs selected operations in default operation order", t, func() {
		restoreBoltPerf := setLegacyPerfBoltFlags([]string{legacyPerfOpBasedirsHistory, legacyPerfOpMountTimestamps})
		defer restoreBoltPerf()

		bd := &legacyPerfQueryBaseDirs{}
		report := newPerfReport(boltPerfBackendInterfaces, "", boltPerf.repeat, boltPerf.warmup)

		err := runPerfQuerySuite(&report, newLegacyPerfQueryContext(bd), func(string, ...any) {})

		So(err, ShouldBeNil)
		So(bd.calls, ShouldResemble, []string{legacyPerfOpMountTimestamps, legacyPerfOpBasedirsHistory})
		So(report.Operations, ShouldHaveLength, 2)
		So(report.Operations[0].Name, ShouldEqual, legacyPerfOpMountTimestamps)
		So(report.Operations[1].Name, ShouldEqual, legacyPerfOpBasedirsHistory)
	})

	Convey("runPerfQuerySuite reports unknown selected operations with available names", t, func() {
		restoreBoltPerf := setLegacyPerfBoltFlags([]string{"not_real", legacyPerfOpMountTimestamps})
		defer restoreBoltPerf()

		bd := &legacyPerfQueryBaseDirs{}
		report := newPerfReport(boltPerfBackendInterfaces, "", boltPerf.repeat, boltPerf.warmup)

		err := runPerfQuerySuite(&report, newLegacyPerfQueryContext(bd), func(string, ...any) {})

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "unknown query ops: not_real")
		So(err.Error(), ShouldContainSubstring, "available ops:")
		So(err.Error(), ShouldContainSubstring, legacyPerfOpMountTimestamps)
		So(err.Error(), ShouldContainSubstring, legacyPerfOpTreeDiskTreeEndpoint)
		So(bd.calls, ShouldBeEmpty)
		So(report.Operations, ShouldHaveLength, 0)
	})
}

func setLegacyPerfBoltFlags(ops []string) func() {
	original := boltPerf
	boltPerf = boltPerfFlags{
		backend:  boltPerfBackendInterfaces,
		repeat:   1,
		warmup:   0,
		splits:   1,
		ancDir:   "/",
		ancLimit: 1,
		ops:      ops,
	}

	return func() {
		boltPerf = original
	}
}

func newLegacyPerfQueryContext(bd basedirs.Reader) perfQueryContext {
	return perfQueryContext{
		datasetDirs: []string{"/tmp/snapshot_mnt"},
		tree:        db.NewTree(legacyPerfTreeDB{}),
		bd:          bd,
		queryDir:    "/mnt/",
		ids: perfQueryIDs{
			gid:     7,
			uid:     8,
			groupBD: "/mnt/group/",
			userBD:  "/mnt/user/",
		},
	}
}

type legacyPerfTreeDB struct{}

func (legacyPerfTreeDB) DirInfo(string, *db.Filter) (*db.DirSummary, error) {
	return nil, errLegacyPerfTreeOp
}

func (legacyPerfTreeDB) Children(string) ([]string, error) {
	return nil, errLegacyPerfTreeOp
}

func (legacyPerfTreeDB) Info() (*db.Info, error) {
	return &db.Info{}, nil
}

func (legacyPerfTreeDB) Close() error {
	return nil
}
