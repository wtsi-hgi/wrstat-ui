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
	bolt "github.com/wtsi-hgi/wrstat-ui/bolt"
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

		store, err := bolt.NewBasedirs(dbPath)
		So(err, ShouldBeNil)
		db, err := basedirs.NewCreator(store, &basedirs.Quotas{})
		So(err, ShouldBeNil)

		db.SetMountPoints(mps)
		db.SetModTime(modTime)

		f := statsdata.NewRoot("/", 0)
		statsdata.AddFile(f, "lustre/scratch123/hgi/mdt0/teamA/projectB/myFile.txt", 0, 0, 1, 0, 0)
		statsdata.AddFile(f, "lustre/scratch123/hgi/mdt0/teamA/projectC/myFile.txt", 0, 0, 2, 0, 0)
		statsdata.AddFile(f, "lustre/scratch125/abc/teamA/projectD/myFile.txt", 0, 0, 2, 0, 0)
		statsdata.AddFile(f, "nfs/scratch123/hgi/mdt0/teamA/projectB/myFile.txt", 0, 0, 1, 0, 0)

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		s.AddDirectoryOperation(sbasedirs.NewBaseDirs(defaultConfig.PathShouldOutput, db))

		So(s.Summarise(), ShouldBeNil)
		// Close the writable store before opening any read-only handles to the
		// same DB file to avoid bbolt flock timeouts.
		So(store.Close(), ShouldBeNil)

		Convey("We can find the keys for all by a single prefix", func() {
			ro, err := bolt.OpenReadOnlyBasedirs(dbPath)
			So(err, ShouldBeNil)

			defer ro.Close()

			toRemove, err := basedirs.FindInvalidHistories(ro, "/lustre/scratch123/")
			So(err, ShouldBeNil)
			So(toRemove, ShouldResemble, []basedirs.HistoryRef{{GID: 0, Path: "/lustre/scratch125/"}, {GID: 0, Path: "/nfs/"}})
		})

		Convey("We can remove all but a single prefix", func() {
			// Re-open a writable store now that the earlier one has been closed.
			wstore, err := bolt.NewBasedirs(dbPath)
			So(err, ShouldBeNil)
			So(basedirs.CleanInvalidDBHistory(wstore, "/lustre/scratch123/"), ShouldBeNil)
			So(wstore.Close(), ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			ro, err := bolt.OpenReadOnlyBasedirs(dbPath)
			So(err, ShouldBeNil)
			db, err := basedirs.NewReader(ro, ownersPath)
			So(err, ShouldBeNil)

			db.SetMountPoints(mps)

			h, err := db.History(0, "/lustre/scratch123/")
			So(err, ShouldBeNil)
			So(h, ShouldResemble, []basedirs.History{{Date: modTime, UsageSize: 3, UsageInodes: 2}})

			_, err = db.History(0, "/lustre/scratch125/")
			So(err, ShouldEqual, basedirs.ErrNoBaseDirHistory)

			_, err = db.History(0, "/nfs/")
			So(err, ShouldEqual, basedirs.ErrNoBaseDirHistory)
		})
	})
}
