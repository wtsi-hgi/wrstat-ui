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

package db_test

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/fs"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

func TestTree(t *testing.T) {

	refUnixTime := time.Now().Unix()

	Convey("You can make a Tree from a dguta database", t, func() {
		paths, err := testMakeDBPaths(t)
		So(err, ShouldBeNil)

		tree, errc := db.NewTree(paths[0])
		So(errc, ShouldNotBeNil)
		So(tree, ShouldBeNil)

		errc = testCreateDB(t, paths[0], refUnixTime)
		So(errc, ShouldBeNil)

		tree, errc = db.NewTree(paths[0])
		So(errc, ShouldBeNil)
		So(tree, ShouldNotBeNil)

		dbModTime := fs.ModTime(paths[0])

		expectedUIDs := []uint32{101, 102, 103}
		expectedGIDs := []uint32{1, 2, 3}
		expectedFTs := db.DGUTAFileTypeTemp | db.DGUTAFileTypeBam | db.DGUTAFileTypeCram | db.DGUTAFileTypeDir
		expectedFTsNoDir := db.DGUTAFileTypeTemp | db.DGUTAFileTypeBam | db.DGUTAFileTypeCram
		expectedUIDsOne := []uint32{101}
		expectedGIDsOne := []uint32{1}
		expectedFTsCram := db.DGUTAFileTypeCram
		expectedFTsCramAndDir := db.DGUTAFileTypeCram | db.DGUTAFileTypeDir
		expectedAtime := time.Unix(50, 0)
		expectedAtimeG := time.Unix(60, 0)
		expectedMtime := time.Unix(refUnixTime-(db.SecondsInAYear*3), 0)

		const numDirectories = 10

		const directorySize = 4096

		Convey("You can query the Tree for DirInfo", func() {
			di, err := tree.DirInfo("/", &db.Filter{Age: db.DGUTAgeAll})
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &db.DirInfo{
				Current: &db.DirSummary{
					"/", 21 + numDirectories + 1, 92 + (numDirectories+1)*directorySize,
					expectedAtime, expectedMtime, expectedUIDs, expectedGIDs, expectedFTs, db.DGUTAgeAll, dbModTime,
				},
				Children: []*db.DirSummary{
					{
						"/a", 21 + numDirectories, 92 + numDirectories*directorySize,
						expectedAtime, expectedMtime, expectedUIDs, expectedGIDs, expectedFTs, db.DGUTAgeAll, dbModTime,
					},
				},
			})

			di, err = tree.DirInfo("/a", &db.Filter{Age: db.DGUTAgeAll})
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &db.DirInfo{
				Current: &db.DirSummary{
					"/a", 21 + numDirectories, 92 + numDirectories*directorySize,
					expectedAtime, expectedMtime, expectedUIDs, expectedGIDs, expectedFTs, db.DGUTAgeAll, dbModTime,
				},
				Children: []*db.DirSummary{
					{
						"/a/b", 9 + 7, 80 + 7*directorySize, expectedAtime, time.Unix(80, 0),
						[]uint32{101, 102},
						expectedGIDsOne, expectedFTs, db.DGUTAgeAll, dbModTime,
					},
					{
						"/a/c", 5 + 2 + 7, 5 + 7 + 2*directorySize, time.Unix(90, 0), expectedMtime,
						[]uint32{102, 103},
						[]uint32{2, 3},
						expectedFTsCramAndDir, db.DGUTAgeAll, dbModTime,
					},
				},
			})

			di, err = tree.DirInfo("/a", &db.Filter{FT: db.DGUTAFileTypeBam})
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &db.DirInfo{
				Current: &db.DirSummary{
					"/a", 2, 10, time.Unix(80, 0), time.Unix(80, 0),
					expectedUIDsOne, expectedGIDsOne, db.DGUTAFileTypeBam | db.DGUTAFileTypeTemp, db.DGUTAgeAll, dbModTime,
				},
				Children: []*db.DirSummary{
					{
						"/a/b", 2, 10, time.Unix(80, 0), time.Unix(80, 0),
						expectedUIDsOne, expectedGIDsOne, db.DGUTAFileTypeBam | db.DGUTAFileTypeTemp, db.DGUTAgeAll, dbModTime,
					},
				},
			})

			di, err = tree.DirInfo("/a/b/e/h/tmp", &db.Filter{Age: db.DGUTAgeAll})
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &db.DirInfo{
				Current: &db.DirSummary{
					"/a/b/e/h/tmp", 2, 5 + directorySize, time.Unix(80, 0), time.Unix(80, 0),
					expectedUIDsOne, expectedGIDsOne,

					db.DGUTAFileTypeTemp |
						db.DGUTAFileTypeBam | db.DGUTAFileTypeDir,

					db.DGUTAgeAll, dbModTime,
				},
				Children: nil,
			})

			di, err = tree.DirInfo("/", &db.Filter{FT: db.DGUTAFileTypeCompressed})
			So(err, ShouldBeNil)
			So(di, ShouldBeNil)
		})

		Convey("You can ask the Tree if a dir has children", func() {
			has := tree.DirHasChildren("/", nil)
			So(has, ShouldBeTrue)

			has = tree.DirHasChildren("/a/b/e/h/tmp", nil)
			So(has, ShouldBeFalse)

			has = tree.DirHasChildren("/", &db.Filter{
				GIDs: []uint32{9999},
			})
			So(has, ShouldBeFalse)

			has = tree.DirHasChildren("/foo", nil)
			So(has, ShouldBeFalse)
		})

		Convey("You can find Where() in the Tree files are", func() {
			dcss, err := tree.Where("/", &db.Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FT: expectedFTsCram},
				split.SplitsToSplitFn(0))
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, db.DCSs{
				{
					"/a/b/d", 3, 30, expectedAtime, time.Unix(60, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
			})

			dcss, err = tree.Where("/", &db.Filter{GIDs: []uint32{1}, UIDs: []uint32{101}}, split.SplitsToSplitFn(0))
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, db.DCSs{
				{
					"/a/b", 5, 40, expectedAtime, time.Unix(80, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsNoDir, db.DGUTAgeAll, dbModTime,
				},
			})

			dcss, err = tree.Where("/", &db.Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FT: expectedFTsCram},
				split.SplitsToSplitFn(1))
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, db.DCSs{
				{
					"/a/b/d", 3, 30, expectedAtime, time.Unix(60, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
				{
					"/a/b/d/g", 2, 20, expectedAtimeG, time.Unix(60, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
				{
					"/a/b/d/f", 1, 10, expectedAtime, time.Unix(50, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
			})

			dcss.SortByDirAndAge()
			So(dcss, ShouldResemble, db.DCSs{
				{
					"/a/b/d", 3, 30, expectedAtime, time.Unix(60, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
				{
					"/a/b/d/f", 1, 10, expectedAtime, time.Unix(50, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
				{
					"/a/b/d/g", 2, 20, expectedAtimeG, time.Unix(60, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
			})

			dcss, err = tree.Where("/", &db.Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FT: expectedFTsCram},
				split.SplitsToSplitFn(2))
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, db.DCSs{
				{
					"/a/b/d", 3, 30, expectedAtime, time.Unix(60, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
				{
					"/a/b/d/g", 2, 20, expectedAtimeG, time.Unix(60, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
				{
					"/a/b/d/f", 1, 10, expectedAtime, time.Unix(50, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
			})

			dcss, err = tree.Where("/", nil, split.SplitsToSplitFn(1))
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, db.DCSs{
				{
					"/a", 21, 92, expectedAtime, expectedMtime, expectedUIDs, expectedGIDs,
					expectedFTsNoDir, db.DGUTAgeAll, dbModTime,
				},
				{
					"/a/b", 9, 80, expectedAtime, time.Unix(80, 0),
					[]uint32{101, 102},
					expectedGIDsOne, expectedFTsNoDir, db.DGUTAgeAll, dbModTime,
				},
				{
					"/a/c/d", 12, 12, time.Unix(90, 0), expectedMtime,
					[]uint32{102, 103},
					[]uint32{2, 3},
					expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
			})

			_, err = tree.Where("/foo", nil, split.SplitsToSplitFn(1))
			So(err, ShouldNotBeNil)
		})

		Convey("You can get the FileLocations()", func() {
			dcss, err := tree.FileLocations("/",
				&db.Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FT: expectedFTsCram})
			So(err, ShouldBeNil)

			So(dcss, ShouldResemble, db.DCSs{
				{
					"/a/b/d/f", 1, 10, expectedAtime, time.Unix(50, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
				{
					"/a/b/d/g", 2, 20, expectedAtimeG, time.Unix(60, 0), expectedUIDsOne,
					expectedGIDsOne, expectedFTsCram, db.DGUTAgeAll, dbModTime,
				},
			})

			_, err = tree.FileLocations("/foo", nil)
			So(err, ShouldNotBeNil)
		})

		Convey("Queries fail with bad dirs", func() {
			_, err := tree.DirInfo("/foo", nil)
			// di := &db.DirInfo{Current: &db.DirSummary{
			// 	"/", 14, 85, expectedAtime, expectedMtime,
			// 	expectedUIDs, expectedGIDs, expectedFTs, db.DGUTAgeAll, dbModTime,
			// }}
			// err = tree.addChildInfo(di, []string{"/foo"}, nil)
			// So(err, ShouldNotBeNil)
			// NB: the above is from the end of this block
			So(err, ShouldNotBeNil)
		})

		Convey("Closing works", func() {
			tree.Close()
		})
	})

	Convey("You can make a Tree from multiple dguta databases and query it", t, func() {
		paths1, err := testMakeDBPaths(t)
		So(err, ShouldBeNil)

		adb := db.NewDB(paths1[0])

		adb.SetBatchSize(20)

		files := statsdata.NewRoot("/", 20)
		files.GID = 1
		files.UID = 11
		files.AddDirectory("a").AddDirectory("b").AddDirectory("c").AddDirectory("d").AddFile("file.bam").Size = 1

		err = fillTestDB(adb, files)
		So(err, ShouldBeNil)

		paths2, err := testMakeDBPaths(t)
		So(err, ShouldBeNil)

		adb = db.NewDB(paths2[0])

		adb.SetBatchSize(20)

		files = statsdata.NewRoot("/", 15)
		files.GID = 1
		files.UID = 11
		files.AddDirectory("a").AddDirectory("b").AddDirectory("c").AddDirectory("e").AddFile("file2.bam").Size = 1

		err = fillTestDB(adb, files)
		So(err, ShouldBeNil)

		tree, err := db.NewTree(paths1[0], paths2[0])
		So(err, ShouldBeNil)
		So(tree, ShouldNotBeNil)

		expectedAtime := time.Unix(15, 0)
		expectedMtime := time.Unix(20, 0)

		mtime2 := fs.ModTime(paths2[0])

		dcss, err := tree.Where("/", nil, split.SplitsToSplitFn(0))
		So(err, ShouldBeNil)
		So(dcss, ShouldResemble, db.DCSs{
			{
				"/a/b/c", 2, 2, expectedAtime, expectedMtime,
				[]uint32{11},
				[]uint32{1},
				db.DGUTAFileTypeBam, db.DGUTAgeAll, mtime2,
			},
		})

		dcss, err = tree.Where("/", nil, split.SplitsToSplitFn(1))
		So(err, ShouldBeNil)
		So(dcss, ShouldResemble, db.DCSs{
			{
				"/a/b/c", 2, 2, expectedAtime, expectedMtime,
				[]uint32{11},
				[]uint32{1},
				db.DGUTAFileTypeBam, db.DGUTAgeAll, mtime2,
			},
			{
				"/a/b/c/d", 1, 1, time.Unix(20, 0), expectedMtime,
				[]uint32{11},
				[]uint32{1},
				db.DGUTAFileTypeBam, db.DGUTAgeAll, mtime2,
			},
			{
				"/a/b/c/e", 1, 1, expectedAtime, expectedAtime,
				[]uint32{11},
				[]uint32{1},
				db.DGUTAFileTypeBam, db.DGUTAgeAll, mtime2,
			},
		})
	})
}

func testCreateDB(t *testing.T, path string, refUnixTime int64) error {
	t.Helper()

	return fillTestDB(db.NewDB(path), internaldata.CreateDefaultTestData(1, 2, 1, 101, 102, refUnixTime))
}

func fillTestDB(adb *db.DB, files *statsdata.Directory) error {
	if err := adb.CreateDB(); err != nil {
		return err
	}

	s := summary.NewSummariser(stats.NewStatsParser(files.AsReader()))

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(adb))

	if err := s.Summarise(); err != nil {
		return err
	}

	return adb.Close()
}
