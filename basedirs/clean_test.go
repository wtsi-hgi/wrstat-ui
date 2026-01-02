/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package basedirs_test

import (
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/bolt"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
)

func TestClean(t *testing.T) {
	const (
		defaultSplits  = 4
		defaultMinDirs = 4
	)

	defaultConfig := basedirs.Config{
		{
			Prefix:  split.SplitPath("/lustre/scratch123/hgi/mdt"),
			Splits:  defaultSplits + 1,
			MinDirs: defaultMinDirs + 1,
		},
		{
			Prefix:  split.SplitPath("/lustre/scratch125/"),
			Splits:  defaultSplits,
			MinDirs: defaultMinDirs,
		},
		{
			Prefix:  split.SplitPath("/nfs/scratch123/hgi/mdt"),
			Splits:  defaultSplits + 1,
			MinDirs: defaultMinDirs + 1,
		},
		{
			Splits:  defaultSplits,
			MinDirs: defaultMinDirs,
		},
	}

	mps := []string{
		"/lustre/scratch123/",
		"/lustre/scratch125/",
		"/nfs/",
	}

	modTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	Convey("Given a database with multiple mountpoints of history", t, func() {
		tmp := t.TempDir()
		dbPath := filepath.Join(tmp, "basedirs.db")

		store, err := bolt.NewBaseDirsStore(dbPath, "")
		So(err, ShouldBeNil)
		store.SetMountPath("/lustre/scratch125/")
		store.SetUpdatedAt(time.Now())
		So(store.Reset(), ShouldBeNil)

		creator, err := basedirs.NewCreator(store, &basedirs.Quotas{})
		So(err, ShouldBeNil)
		creator.SetMountPoints(mps)
		creator.SetModTime(modTime)

		f := statsdata.NewRoot("/", 0)
		statsdata.AddFile(f, "lustre/scratch123/hgi/mdt0/teamA/projectB/myFile.txt", 0, 0, 1, 0, 0)
		statsdata.AddFile(f, "lustre/scratch123/hgi/mdt0/teamA/projectC/myFile.txt", 0, 0, 2, 0, 0)
		statsdata.AddFile(f, "lustre/scratch125/abc/teamA/projectD/myFile.txt", 0, 0, 2, 0, 0)
		statsdata.AddFile(f, "nfs/scratch123/hgi/mdt0/teamA/projectB/myFile.txt", 0, 0, 1, 0, 0)

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		s.AddDirectoryOperation(sbasedirs.NewBaseDirs(defaultConfig.PathShouldOutput, creator))
		So(s.Summarise(), ShouldBeNil)
		So(store.Close(), ShouldBeNil)

		Convey("We can find the (gid, mountpath) pairs removable by a prefix", func() {
			m, err := bolt.NewHistoryMaintainer(dbPath)
			So(err, ShouldBeNil)

			toRemove, err := m.FindInvalidHistory("/lustre/scratch123/")
			So(err, ShouldBeNil)
			So(toRemove, ShouldResemble, []basedirs.HistoryIssue{
				{GID: 0, MountPath: "/lustre/scratch125/"},
				{GID: 0, MountPath: "/nfs/"},
			})
		})

		Convey("We can remove all but a single prefix", func() {
			m, err := bolt.NewHistoryMaintainer(dbPath)
			So(err, ShouldBeNil)
			So(m.CleanHistoryForMount("/lustre/scratch123/"), ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			r, err := bolt.OpenBaseDirsReader(dbPath, ownersPath)
			So(err, ShouldBeNil)
			defer r.Close()
			r.SetMountPoints(mps)

			h, err := r.History(0, "/lustre/scratch123/")
			So(err, ShouldBeNil)
			So(h, ShouldResemble, []basedirs.History{{Date: modTime, UsageSize: 3, UsageInodes: 2}})

			_, err = r.History(0, "/lustre/scratch125/")
			So(err, ShouldEqual, basedirs.ErrNoBaseDirHistory)

			_, err = r.History(0, "/nfs/")
			So(err, ShouldEqual, basedirs.ErrNoBaseDirHistory)
		})
	})
}
