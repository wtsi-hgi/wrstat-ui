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

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

func TestTree(t *testing.T) {
	// expectedFTsBam := []DirGUTAFileType{DGUTAFileTypeBam}

	// refUnixTime := time.Now().Unix()

	Convey("You can make a Tree from a dguta database", t, func() {
		paths, err := testMakeDBPaths(t)
		So(err, ShouldBeNil)

		tree, errc := db.NewTree(paths[0])
		So(errc, ShouldNotBeNil)
		So(tree, ShouldBeNil)

		// 	errc = testCreateDB(t, paths[0], refUnixTime)
		// 	So(errc, ShouldBeNil)

		// 	tree, errc = NewTree(paths[0])
		// 	So(errc, ShouldBeNil)
		// 	So(tree, ShouldNotBeNil)

		// 	dbModTime := fs.ModTime(paths[0])

		// 	expectedUIDs := []uint32{101, 102, 103}
		// 	expectedGIDs := []uint32{1, 2, 3}
		// 	expectedFTs := []DirGUTAFileType{
		// 		DGUTAFileTypeTemp,
		// 		DGUTAFileTypeBam, DGUTAFileTypeCram, DGUTAFileTypeDir,
		// 	}
		// 	expectedUIDsOne := []uint32{101}
		// 	expectedGIDsOne := []uint32{1}
		// 	expectedFTsCram := []DirGUTAFileType{DGUTAFileTypeCram}
		// 	expectedFTsCramAndDir := []DirGUTAFileType{DGUTAFileTypeCram, DGUTAFileTypeDir}
		// 	expectedAtime := time.Unix(50, 0)
		// 	expectedAtimeG := time.Unix(60, 0)
		// 	expectedMtime := time.Unix(refUnixTime-(SecondsInAYear*3), 0)

		// 	const numDirectories = 10

		// 	const directorySize = 1024

		// 	Convey("You can query the Tree for DirInfo", func() {
		// 		di, err := tree.DirInfo("/", &Filter{Age: DGUTAgeAll})
		// 		So(err, ShouldBeNil)
		// 		So(di, ShouldResemble, &DirInfo{
		// 			Current: &DirSummary{
		// 				"/", 21 + numDirectories, 92 + numDirectories*directorySize,
		// 				expectedAtime, expectedMtime, expectedUIDs, expectedGIDs, expectedFTs, DGUTAgeAll, dbModTime,
		// 			},
		// 			Children: []*DirSummary{
		// 				{
		// 					"/a", 21 + numDirectories, 92 + numDirectories*directorySize,
		// 					expectedAtime, expectedMtime, expectedUIDs, expectedGIDs, expectedFTs, DGUTAgeAll, dbModTime,
		// 				},
		// 			},
		// 		})

		// 		di, err = tree.DirInfo("/a", &Filter{Age: DGUTAgeAll})
		// 		So(err, ShouldBeNil)
		// 		So(di, ShouldResemble, &DirInfo{
		// 			Current: &DirSummary{
		// 				"/a", 21 + numDirectories, 92 + numDirectories*directorySize,
		// 				expectedAtime, expectedMtime, expectedUIDs, expectedGIDs, expectedFTs, DGUTAgeAll, dbModTime,
		// 			},
		// 			Children: []*DirSummary{
		// 				{
		// 					"/a/b", 9 + 7, 80 + 7*directorySize, expectedAtime, time.Unix(80, 0),
		// 					[]uint32{101, 102},
		// 					expectedGIDsOne, expectedFTs, DGUTAgeAll, dbModTime,
		// 				},
		// 				{
		// 					"/a/c", 5 + 2 + 7, 5 + 7 + 2*directorySize, time.Unix(90, 0), expectedMtime,
		// 					[]uint32{102, 103},
		// 					[]uint32{2, 3},
		// 					expectedFTsCramAndDir, DGUTAgeAll, dbModTime,
		// 				},
		// 			},
		// 		})

		// 		di, err = tree.DirInfo("/a", &Filter{FTs: expectedFTsBam})
		// 		So(err, ShouldBeNil)
		// 		So(di, ShouldResemble, &DirInfo{
		// 			Current: &DirSummary{
		// 				"/a", 2, 10, time.Unix(80, 0), time.Unix(80, 0),
		// 				expectedUIDsOne, expectedGIDsOne, expectedFTsBam, DGUTAgeAll, dbModTime,
		// 			},
		// 			Children: []*DirSummary{
		// 				{
		// 					"/a/b", 2, 10, time.Unix(80, 0), time.Unix(80, 0),
		// 					expectedUIDsOne, expectedGIDsOne, expectedFTsBam, DGUTAgeAll, dbModTime,
		// 				},
		// 			},
		// 		})

		// 		di, err = tree.DirInfo("/a/b/e/h/tmp", &Filter{Age: DGUTAgeAll})
		// 		So(err, ShouldBeNil)
		// 		So(di, ShouldResemble, &DirInfo{
		// 			Current: &DirSummary{
		// 				"/a/b/e/h/tmp", 2, 5 + directorySize, time.Unix(80, 0), time.Unix(80, 0),
		// 				expectedUIDsOne, expectedGIDsOne,
		// 				[]DirGUTAFileType{
		// 					DGUTAFileTypeTemp,
		// 					DGUTAFileTypeBam, DGUTAFileTypeDir,
		// 				},
		// 				DGUTAgeAll, dbModTime,
		// 			},
		// 			Children: nil,
		// 		})

		// 		di, err = tree.DirInfo("/", &Filter{FTs: []DirGUTAFileType{DGUTAFileTypeCompressed}})
		// 		So(err, ShouldBeNil)
		// 		So(di, ShouldBeNil)
		// 	})

		// 	Convey("You can ask the Tree if a dir has children", func() {
		// 		has := tree.DirHasChildren("/", nil)
		// 		So(has, ShouldBeTrue)

		// 		has = tree.DirHasChildren("/a/b/e/h/tmp", nil)
		// 		So(has, ShouldBeFalse)

		// 		has = tree.DirHasChildren("/", &Filter{
		// 			GIDs: []uint32{9999},
		// 		})
		// 		So(has, ShouldBeFalse)

		// 		has = tree.DirHasChildren("/foo", nil)
		// 		So(has, ShouldBeFalse)
		// 	})

		// 	Convey("You can find Where() in the Tree files are", func() {
		// 		dcss, err := tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: expectedFTsCram},
		// 			split.SplitsToSplitFn(0))
		// 		So(err, ShouldBeNil)
		// 		So(dcss, ShouldResemble, DCSs{
		// 			{
		// 				"/a/b/d", 3, 30, expectedAtime, time.Unix(60, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 		})

		// 		dcss, err = tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}}, split.SplitsToSplitFn(0))
		// 		So(err, ShouldBeNil)
		// 		So(dcss, ShouldResemble, DCSs{
		// 			{
		// 				"/a/b", 5, 40, expectedAtime, time.Unix(80, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTs[:3], DGUTAgeAll, dbModTime,
		// 			},
		// 		})

		// 		dcss, err = tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: expectedFTsCram},
		// 			split.SplitsToSplitFn(1))
		// 		So(err, ShouldBeNil)
		// 		So(dcss, ShouldResemble, DCSs{
		// 			{
		// 				"/a/b/d", 3, 30, expectedAtime, time.Unix(60, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 			{
		// 				"/a/b/d/g", 2, 20, expectedAtimeG, time.Unix(60, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 			{
		// 				"/a/b/d/f", 1, 10, expectedAtime, time.Unix(50, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 		})

		// 		dcss.SortByDirAndAge()
		// 		So(dcss, ShouldResemble, DCSs{
		// 			{
		// 				"/a/b/d", 3, 30, expectedAtime, time.Unix(60, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 			{
		// 				"/a/b/d/f", 1, 10, expectedAtime, time.Unix(50, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 			{
		// 				"/a/b/d/g", 2, 20, expectedAtimeG, time.Unix(60, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 		})

		// 		dcss, err = tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: expectedFTsCram},
		// 			split.SplitsToSplitFn(2))
		// 		So(err, ShouldBeNil)
		// 		So(dcss, ShouldResemble, DCSs{
		// 			{
		// 				"/a/b/d", 3, 30, expectedAtime, time.Unix(60, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 			{
		// 				"/a/b/d/g", 2, 20, expectedAtimeG, time.Unix(60, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 			{
		// 				"/a/b/d/f", 1, 10, expectedAtime, time.Unix(50, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 		})

		// 		dcss, err = tree.Where("/", nil, split.SplitsToSplitFn(1))
		// 		So(err, ShouldBeNil)
		// 		So(dcss, ShouldResemble, DCSs{
		// 			{
		// 				"/a", 21, 92, expectedAtime, expectedMtime, expectedUIDs, expectedGIDs,
		// 				expectedFTs[:3], DGUTAgeAll, dbModTime,
		// 			},
		// 			{
		// 				"/a/b", 9, 80, expectedAtime, time.Unix(80, 0),
		// 				[]uint32{101, 102},
		// 				expectedGIDsOne, expectedFTs[:3], DGUTAgeAll, dbModTime,
		// 			},
		// 			{
		// 				"/a/c/d", 12, 12, time.Unix(90, 0), expectedMtime,
		// 				[]uint32{102, 103},
		// 				[]uint32{2, 3},
		// 				expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 		})

		// 		_, err = tree.Where("/foo", nil, split.SplitsToSplitFn(1))
		// 		So(err, ShouldNotBeNil)
		// 	})

		// 	Convey("You can get the FileLocations()", func() {
		// 		dcss, err := tree.FileLocations("/",
		// 			&Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: expectedFTsCram})
		// 		So(err, ShouldBeNil)

		// 		So(dcss, ShouldResemble, DCSs{
		// 			{
		// 				"/a/b/d/f", 1, 10, expectedAtime, time.Unix(50, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 			{
		// 				"/a/b/d/g", 2, 20, expectedAtimeG, time.Unix(60, 0), expectedUIDsOne,
		// 				expectedGIDsOne, expectedFTsCram, DGUTAgeAll, dbModTime,
		// 			},
		// 		})

		// 		_, err = tree.FileLocations("/foo", nil)
		// 		So(err, ShouldNotBeNil)
		// 	})

		// 	Convey("Queries fail with bad dirs", func() {
		// 		_, err := tree.DirInfo("/foo", nil)
		// 		So(err, ShouldNotBeNil)

		// 		di := &DirInfo{Current: &DirSummary{
		// 			"/", 14, 85, expectedAtime, expectedMtime,
		// 			expectedUIDs, expectedGIDs, expectedFTs, DGUTAgeAll, dbModTime,
		// 		}}
		// 		err = tree.addChildInfo(di, []string{"/foo"}, nil)
		// 		So(err, ShouldNotBeNil)
		// 	})

		// 	Convey("Closing works", func() {
		// 		tree.Close()
		// 	})
		// })

		// Convey("You can make a Tree from multiple dguta databases and query it", t, func() {
		// 	paths1, err := testMakeDBPaths(t)
		// 	So(err, ShouldBeNil)

		// 	db := NewDB(paths1[0])
		// 	data := strings.NewReader(strconv.Quote("/") +
		// 		"\t1\t11\t6\t0\t1\t1\t20\t20\n" +
		// 		strconv.Quote("/a") +
		// 		"\t1\t11\t6\t0\t1\t1\t20\t20\n" +
		// 		strconv.Quote("/a/b") +
		// 		"\t1\t11\t6\t0\t1\t1\t20\t20\n" +
		// 		strconv.Quote("/a/b/c") +
		// 		"\t1\t11\t6\t0\t1\t1\t20\t20\n" +
		// 		strconv.Quote("/a/b/c/d") +
		// 		"\t1\t11\t6\t0\t1\t1\t20\t20\n")
		// 	err = db.Store(data, 20)
		// 	So(err, ShouldBeNil)

		// 	paths2, err := testMakeDBPaths(t)
		// 	So(err, ShouldBeNil)

		// 	db = NewDB(paths2[0])
		// 	data = strings.NewReader(strconv.Quote("/") +
		// 		"\t1\t11\t6\t0\t1\t1\t15\t15\n" +
		// 		strconv.Quote("/a") +
		// 		"\t1\t11\t6\t0\t1\t1\t15\t15\n" +
		// 		strconv.Quote("/a/b") +
		// 		"\t1\t11\t6\t0\t1\t1\t15\t15\n" +
		// 		strconv.Quote("/a/b/c") +
		// 		"\t1\t11\t6\t0\t1\t1\t15\t15\n" +
		// 		strconv.Quote("/a/b/c/e") +
		// 		"\t1\t11\t6\t0\t1\t1\t15\t15\n")
		// 	err = db.Store(data, 20)
		// 	So(err, ShouldBeNil)

		// 	tree, err := NewTree(paths1[0], paths2[0])
		// 	So(err, ShouldBeNil)
		// 	So(tree, ShouldNotBeNil)

		// 	expectedAtime := time.Unix(15, 0)
		// 	expectedMtime := time.Unix(20, 0)

		// 	mtime2 := fs.ModTime(paths2[0])

		// 	dcss, err := tree.Where("/", nil, split.SplitsToSplitFn(0))
		// 	So(err, ShouldBeNil)
		// 	So(dcss, ShouldResemble, DCSs{
		// 		{
		// 			"/a/b/c", 2, 2, expectedAtime, expectedMtime,
		// 			[]uint32{11},
		// 			[]uint32{1},
		// 			expectedFTsBam, DGUTAgeAll, mtime2,
		// 		},
		// 	})

		// 	dcss, err = tree.Where("/", nil, split.SplitsToSplitFn(1))
		// 	So(err, ShouldBeNil)
		// 	So(dcss, ShouldResemble, DCSs{
		// 		{
		// 			"/a/b/c", 2, 2, expectedAtime, expectedMtime,
		// 			[]uint32{11},
		// 			[]uint32{1},
		// 			expectedFTsBam, DGUTAgeAll, mtime2,
		// 		},
		// 		{
		// 			"/a/b/c/d", 1, 1, time.Unix(20, 0), expectedMtime,
		// 			[]uint32{11},
		// 			[]uint32{1},
		// 			expectedFTsBam, DGUTAgeAll, mtime2,
		// 		},
		// 		{
		// 			"/a/b/c/e", 1, 1, expectedAtime, expectedAtime,
		// 			[]uint32{11},
		// 			[]uint32{1},
		// 			expectedFTsBam, DGUTAgeAll, mtime2,
		// 		},
		// 	})
	})
}

// func testCreateDB(t *testing.T, path string, refUnixTime int64) error {
// 	t.Helper()

// 	dgutData := internaldata.TestDGUTAData(t, internaldata.CreateDefaultTestData(1, 2, 1, 101, 102, refUnixTime))
// 	data := strings.NewReader(dgutData)
// 	db := NewDB(path)

// 	return db.Store(data, 20)
// }
