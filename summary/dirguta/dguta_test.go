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

package dirguta

import (
	"math"
	"testing"

	//. "github.com/smartystreets/goconvey/convey"
	bolt "go.etcd.io/bbolt"
)

func TestDGUTA(t *testing.T) {
	// Convey("You can parse a single line of dguta data", t, func() {
	// 	line := strconv.Quote("/") + "\t1\t101\t0\t0\t3\t30\t50\t50\n"
	// 	dir, gut, err := parseDGUTALine(line)
	// 	So(err, ShouldBeNil)
	// 	So(dir, ShouldEqual, "/")
	// 	So(gut, ShouldResemble, &GUTA{
	// 		GID: 1, UID: 101, FT: DGUTAFileTypeOther,
	// 		Age: DGUTAgeAll, Count: 3, Size: 30, Atime: 50, Mtime: 50,
	// 	})

	// 	Convey("But invalid data won't parse", func() {
	// 		_, _, err = parseDGUTALine(strconv.Quote("/") +
	// 			"\t1\t101\t0\t0\t3\t50\t50\n")

	// 		So(err, ShouldEqual, ErrInvalidFormat)

	// 		_, _, err = parseDGUTALine(strconv.Quote("/") +
	// 			"\tfoo\t101\t0\t0\t3\t30\t50\t50\n")
	// 		So(err, ShouldEqual, ErrInvalidFormat)

	// 		_, _, err = parseDGUTALine(strconv.Quote("/") +
	// 			"\t1\tfoo\t0\t0\t3\t30\t50\t50\n")
	// 		So(err, ShouldEqual, ErrInvalidFormat)

	// 		_, _, err = parseDGUTALine(strconv.Quote("/") +
	// 			"\t1\t101\tfoo\t0\t3\t30\t50\t50\n")
	// 		So(err, ShouldEqual, ErrInvalidFormat)

	// 		_, _, err = parseDGUTALine(strconv.Quote("/") +
	// 			"\t1\t101\t0\tfoo\t3\t30\t50\t50\n")
	// 		So(err, ShouldEqual, ErrInvalidFormat)

	// 		_, _, err = parseDGUTALine(strconv.Quote("/") +
	// 			"\t1\t101\t0\t0\tfoo\t30\t50\t50\n")
	// 		So(err, ShouldEqual, ErrInvalidFormat)

	// 		_, _, err = parseDGUTALine(strconv.Quote("/") +
	// 			"\t1\t101\t0\t0\t3\tfoo\t50\t50\n")
	// 		So(err, ShouldEqual, ErrInvalidFormat)

	// 		_, _, err = parseDGUTALine(strconv.Quote("/") +
	// 			"\t1\t101\t0\t0\t3\t30\tfoo\t50\n")
	// 		So(err, ShouldEqual, ErrInvalidFormat)

	// 		_, _, err = parseDGUTALine(strconv.Quote("/") +
	// 			"\t1\t101\t0\t0\t3\t30\t50\tfoo\n")
	// 		So(err, ShouldEqual, ErrInvalidFormat)

	// 		So(err.Error(), ShouldEqual, "the provided data was not in dguta format")

	// 		_, _, err = parseDGUTALine("\t\t\t\t\t\t\t\t\n")
	// 		So(err, ShouldEqual, ErrBlankLine)

	// 		So(err.Error(), ShouldEqual, "the provided line had no information")
	// 	})
	// })

	// refUnixTime := time.Now().Unix()
	// dgutaData, expectedRootGUTAs, expected, expectedKeys := testData(t, refUnixTime)

	// Convey("You can see if a GUTA passes a filter", t, func() {
	// 	numGutas := 17
	// 	emptyGutas := 8
	// 	testIndex := func(index int) int {
	// 		if index > 4 {
	// 			return index*numGutas - emptyGutas*2
	// 		} else if index > 3 {
	// 			return index*numGutas - emptyGutas
	// 		}

	// 		return index * numGutas
	// 	}

	// 	filter := &Filter{}
	// 	a, b := expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeTrue)

	// 	a, b = expectedRootGUTAs[0].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeFalse)

	// 	filter.GIDs = []uint32{3, 4, 5}
	// 	a, b = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
	// 	So(a, ShouldBeFalse)
	// 	So(b, ShouldBeFalse)

	// 	filter.GIDs = []uint32{3, 2, 1}
	// 	a, b = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeTrue)

	// 	filter.UIDs = []uint32{103}
	// 	a, b = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
	// 	So(a, ShouldBeFalse)
	// 	So(b, ShouldBeFalse)

	// 	filter.UIDs = []uint32{103, 102, 101}
	// 	a, b = expectedRootGUTAs[testIndex(1)].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeTrue)

	// 	filter.FTs = []DirGUTAFileType{DGUTAFileTypeTemp}
	// 	a, b = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
	// 	So(a, ShouldBeFalse)
	// 	So(b, ShouldBeFalse)
	// 	a, b = expectedRootGUTAs[0].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeTrue)

	// 	filter.FTs = []DirGUTAFileType{DGUTAFileTypeTemp, DGUTAFileTypeCram}
	// 	a, b = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeTrue)
	// 	a, b = expectedRootGUTAs[0].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeFalse)

	// 	filter.UIDs = nil
	// 	a, b = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeTrue)

	// 	filter.GIDs = nil
	// 	a, b = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeTrue)

	// 	filter.FTs = []DirGUTAFileType{DGUTAFileTypeDir}
	// 	a, b = expectedRootGUTAs[testIndex(3)].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeTrue)

	// 	filter = &Filter{Age: DGUTAgeA1M}
	// 	a, b = expectedRootGUTAs[testIndex(7)+1].PassesFilter(filter)
	// 	So(a, ShouldBeTrue)
	// 	So(b, ShouldBeTrue)

	// 	filter.Age = DGUTAgeA7Y
	// 	a, b = expectedRootGUTAs[testIndex(7)+1].PassesFilter(filter)
	// 	So(a, ShouldBeFalse)
	// 	So(b, ShouldBeFalse)
	// })

	// expectedUIDs := []uint32{101, 102, 103}
	// expectedGIDs := []uint32{1, 2, 3}
	// expectedFTs := []DirGUTAFileType{
	// 	DGUTAFileTypeTemp,
	// 	DGUTAFileTypeBam, DGUTAFileTypeCram, DGUTAFileTypeDir,
	// }

	// const numDirectories = 10

	// const directorySize = 1024

	// expectedMtime := time.Unix(time.Now().Unix()-(SecondsInAYear*3), 0)

	// defaultFilter := &Filter{Age: DGUTAgeAll}

	// Convey("GUTAs can sum the count and size and provide UIDs, GIDs and FTs of their GUTA elements", t, func() {
	// 	ds := expectedRootGUTAs.Summary(defaultFilter)
	// 	So(ds.Count, ShouldEqual, 21+numDirectories)
	// 	So(ds.Size, ShouldEqual, 92+numDirectories*directorySize)
	// 	So(ds.Atime, ShouldEqual, time.Unix(50, 0))
	// 	So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
	// 	So(ds.UIDs, ShouldResemble, expectedUIDs)
	// 	So(ds.GIDs, ShouldResemble, expectedGIDs)
	// 	So(ds.FTs, ShouldResemble, expectedFTs)
	// })

	// Convey("A DGUTA can be encoded and decoded", t, func() {
	// 	ch := new(codec.BincHandle)
	// 	dirb, b := expected[0].encodeToBytes(ch)
	// 	So(len(dirb), ShouldEqual, 1)
	// 	So(len(b), ShouldEqual, 5964)

	// 	d := decodeDGUTAbytes(ch, dirb, b)
	// 	So(d, ShouldResemble, expected[0])
	// })

	// Convey("A DGUTA can sum the count and size and provide UIDs, GIDs and FTs of its GUTs", t, func() {
	// 	ds := expected[0].Summary(defaultFilter)
	// 	So(ds.Count, ShouldEqual, 21+numDirectories)
	// 	So(ds.Size, ShouldEqual, 92+numDirectories*directorySize)
	// 	So(ds.Atime, ShouldEqual, time.Unix(50, 0))
	// 	So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
	// 	So(ds.UIDs, ShouldResemble, expectedUIDs)
	// 	So(ds.GIDs, ShouldResemble, expectedGIDs)
	// 	So(ds.FTs, ShouldResemble, expectedFTs)
	// })

	// Convey("Given multiline dguta data", t, func() {
	// 	data := strings.NewReader(dgutaData)

	// 	Convey("You can parse it", func() {
	// 		i := 0
	// 		cb := func(dguta *DGUTA) {
	// 			So(alterDgutaForTest(dguta), ShouldResemble, expected[i])

	// 			i++
	// 		}

	// 		err := parseDGUTALines(data, cb)
	// 		So(err, ShouldBeNil)
	// 		So(i, ShouldEqual, 11)
	// 	})

	// 	Convey("You can't parse invalid data", func() {
	// 		data = strings.NewReader("foo")
	// 		i := 0
	// 		cb := func(dguta *DGUTA) {
	// 			i++
	// 		}

	// 		err := parseDGUTALines(data, cb)
	// 		So(err, ShouldNotBeNil)
	// 		So(i, ShouldEqual, 0)
	// 	})

	// 	Convey("And database file paths", func() {
	// 		paths, err := testMakeDBPaths(t)
	// 		So(err, ShouldBeNil)

	// 		db := NewDB(paths[0])
	// 		So(db, ShouldNotBeNil)

	// 		Convey("You can store it in a database file", func() {
	// 			_, errs := os.Stat(paths[1])
	// 			So(errs, ShouldNotBeNil)
	// 			_, errs = os.Stat(paths[2])
	// 			So(errs, ShouldNotBeNil)

	// 			err := db.Store(data, 4)
	// 			So(err, ShouldBeNil)

	// 			Convey("The resulting database files have the expected content", func() {
	// 				info, errs := os.Stat(paths[1])
	// 				So(errs, ShouldBeNil)
	// 				So(info.Size(), ShouldBeGreaterThan, 10)
	// 				info, errs = os.Stat(paths[2])
	// 				So(errs, ShouldBeNil)
	// 				So(info.Size(), ShouldBeGreaterThan, 10)

	// 				keys, errt := testGetDBKeys(paths[1], gutaBucket)
	// 				So(errt, ShouldBeNil)
	// 				So(keys, ShouldResemble, expectedKeys)

	// 				keys, errt = testGetDBKeys(paths[2], childBucket)
	// 				So(errt, ShouldBeNil)
	// 				So(keys, ShouldResemble, []string{"/", "/a", "/a/b", "/a/b/d", "/a/b/e", "/a/b/e/h", "/a/c"})

	// 				Convey("You can query a database after Open()ing it", func() {
	// 					db = NewDB(paths[0])

	// 					db.Close()

	// 					err = db.Open()
	// 					So(err, ShouldBeNil)

	// 					ds, errd := db.DirInfo("/", defaultFilter)
	// 					So(errd, ShouldBeNil)
	// 					So(ds.Count, ShouldEqual, 21+numDirectories)
	// 					So(ds.Size, ShouldEqual, 92+numDirectories*directorySize)
	// 					So(ds.Atime, ShouldEqual, time.Unix(50, 0))
	// 					So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
	// 					So(ds.UIDs, ShouldResemble, expectedUIDs)
	// 					So(ds.GIDs, ShouldResemble, expectedGIDs)
	// 					So(ds.FTs, ShouldResemble, expectedFTs)

	// 					ds, errd = db.DirInfo("/", &Filter{Age: DGUTAgeA7Y})
	// 					So(errd, ShouldBeNil)
	// 					So(ds.Count, ShouldEqual, 21-7)
	// 					So(ds.Size, ShouldEqual, 92-7)
	// 					So(ds.Atime, ShouldEqual, time.Unix(50, 0))
	// 					So(ds.Mtime, ShouldEqual, time.Unix(90, 0))
	// 					So(ds.UIDs, ShouldResemble, []uint32{101, 102})
	// 					So(ds.GIDs, ShouldResemble, []uint32{1, 2})
	// 					So(ds.FTs, ShouldResemble, []DirGUTAFileType{
	// 						DGUTAFileTypeTemp,
	// 						DGUTAFileTypeBam, DGUTAFileTypeCram,
	// 					})

	// 					ds, errd = db.DirInfo("/a/c/d", defaultFilter)
	// 					So(errd, ShouldBeNil)
	// 					So(ds.Count, ShouldEqual, 13)
	// 					So(ds.Size, ShouldEqual, 12+directorySize)
	// 					So(ds.Atime, ShouldEqual, time.Unix(90, 0))
	// 					So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
	// 					So(ds.UIDs, ShouldResemble, []uint32{102, 103})
	// 					So(ds.GIDs, ShouldResemble, []uint32{2, 3})
	// 					So(ds.FTs, ShouldResemble, []DirGUTAFileType{DGUTAFileTypeCram, DGUTAFileTypeDir})

	// 					ds, errd = db.DirInfo("/a/b/d/g", defaultFilter)
	// 					So(errd, ShouldBeNil)
	// 					So(ds.Count, ShouldEqual, 7)
	// 					So(ds.Size, ShouldEqual, 60+directorySize)
	// 					So(ds.Atime, ShouldEqual, time.Unix(60, 0))
	// 					So(ds.Mtime, ShouldEqual, time.Unix(75, 0))
	// 					So(ds.UIDs, ShouldResemble, []uint32{101, 102})
	// 					So(ds.GIDs, ShouldResemble, []uint32{1})
	// 					So(ds.FTs, ShouldResemble, []DirGUTAFileType{DGUTAFileTypeCram, DGUTAFileTypeDir})

	// 					_, errd = db.DirInfo("/foo", defaultFilter)
	// 					So(errd, ShouldNotBeNil)
	// 					So(errd, ShouldEqual, ErrDirNotFound)

	// 					ds, errd = db.DirInfo("/", &Filter{GIDs: []uint32{1}})
	// 					So(errd, ShouldBeNil)
	// 					So(ds.Count, ShouldEqual, 17)
	// 					So(ds.Size, ShouldEqual, 8272)
	// 					So(ds.Atime, ShouldEqual, time.Unix(50, 0))
	// 					So(ds.Mtime, ShouldEqual, time.Unix(80, 0))
	// 					So(ds.UIDs, ShouldResemble, []uint32{101, 102})
	// 					So(ds.GIDs, ShouldResemble, []uint32{1})
	// 					So(ds.FTs, ShouldResemble, expectedFTs)

	// 					ds, errd = db.DirInfo("/", &Filter{UIDs: []uint32{102}})
	// 					So(errd, ShouldBeNil)
	// 					So(ds.Count, ShouldEqual, 11)
	// 					So(ds.Size, ShouldEqual, 2093)
	// 					So(ds.Atime, ShouldEqual, time.Unix(75, 0))
	// 					So(ds.Mtime, ShouldEqual, time.Unix(90, 0))
	// 					So(ds.UIDs, ShouldResemble, []uint32{102})
	// 					So(ds.GIDs, ShouldResemble, []uint32{1, 2})
	// 					So(ds.FTs, ShouldResemble, []DirGUTAFileType{DGUTAFileTypeCram, DGUTAFileTypeDir})

	// 					ds, errd = db.DirInfo("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{102}})
	// 					So(errd, ShouldBeNil)
	// 					So(ds.Count, ShouldEqual, 4)
	// 					So(ds.Size, ShouldEqual, 40)
	// 					So(ds.Atime, ShouldEqual, time.Unix(75, 0))
	// 					So(ds.Mtime, ShouldEqual, time.Unix(75, 0))
	// 					So(ds.UIDs, ShouldResemble, []uint32{102})
	// 					So(ds.GIDs, ShouldResemble, []uint32{1})
	// 					So(ds.FTs, ShouldResemble, []DirGUTAFileType{DGUTAFileTypeCram})

	// 					ds, errd = db.DirInfo("/", &Filter{
	// 						GIDs: []uint32{1},
	// 						UIDs: []uint32{102},
	// 						FTs:  []DirGUTAFileType{DGUTAFileTypeTemp},
	// 					})
	// 					So(errd, ShouldBeNil)
	// 					So(ds, ShouldBeNil)

	// 					ds, errd = db.DirInfo("/", &Filter{FTs: []DirGUTAFileType{DGUTAFileTypeTemp}})
	// 					So(errd, ShouldBeNil)
	// 					So(ds.Count, ShouldEqual, 2)
	// 					So(ds.Size, ShouldEqual, 5+directorySize)
	// 					So(ds.Atime, ShouldEqual, time.Unix(80, 0))
	// 					So(ds.Mtime, ShouldEqual, time.Unix(80, 0))
	// 					So(ds.UIDs, ShouldResemble, []uint32{101})
	// 					So(ds.GIDs, ShouldResemble, []uint32{1})
	// 					So(ds.FTs, ShouldResemble, []DirGUTAFileType{DGUTAFileTypeTemp})

	// 					children := db.Children("/a")
	// 					So(children, ShouldResemble, []string{"/a/b", "/a/c"})

	// 					children = db.Children("/a/b/e/h")
	// 					So(children, ShouldResemble, []string{"/a/b/e/h/tmp"})

	// 					children = db.Children("/a/c/d")
	// 					So(children, ShouldBeNil)

	// 					children = db.Children("/foo")
	// 					So(children, ShouldBeNil)

	// 					db.Close()
	// 				})

	// 				Convey("Open()s fail on invalid databases", func() {
	// 					db = NewDB(paths[0])

	// 					db.Close()

	// 					err = os.RemoveAll(paths[2])
	// 					So(err, ShouldBeNil)

	// 					err = os.WriteFile(paths[2], []byte("foo"), 0600)
	// 					So(err, ShouldBeNil)

	// 					err = db.Open()
	// 					So(err, ShouldNotBeNil)

	// 					err = os.RemoveAll(paths[1])
	// 					So(err, ShouldBeNil)

	// 					err = os.WriteFile(paths[1], []byte("foo"), 0600)
	// 					So(err, ShouldBeNil)

	// 					err = db.Open()
	// 					So(err, ShouldNotBeNil)
	// 				})

	// 				Convey("Store()ing multiple times", func() {
	// 					data = strings.NewReader(strconv.Quote("/") +
	// 						"\t3\t103\t7\t0\t2\t2\t25\t25\n" +
	// 						strconv.Quote("/a/i") + "\t3\t103\t7\t0\t1\t1\t25\t25\n" +
	// 						strconv.Quote("/i") + "\t3\t103\t7\t0\t1\t1\t30\t30\n")

	// 					Convey("to the same db file doesn't work", func() {
	// 						err = db.Store(data, 4)
	// 						So(err, ShouldNotBeNil)
	// 						So(err, ShouldEqual, ErrDBExists)
	// 					})

	// 					Convey("to different db directories and loading them all does work", func() {
	// 						path2 := paths[0] + ".2"
	// 						err = os.Mkdir(path2, os.ModePerm)
	// 						So(err, ShouldBeNil)

	// 						db2 := NewDB(path2)
	// 						err = db2.Store(data, 4)
	// 						So(err, ShouldBeNil)

	// 						db = NewDB(paths[0], path2)
	// 						err = db.Open()
	// 						So(err, ShouldBeNil)

	// 						ds, errd := db.DirInfo("/", &Filter{})
	// 						So(errd, ShouldBeNil)
	// 						So(ds.Count, ShouldEqual, 33)
	// 						So(ds.Size, ShouldEqual, 10334)
	// 						So(ds.Atime, ShouldEqual, time.Unix(25, 0))
	// 						So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
	// 						So(ds.UIDs, ShouldResemble, []uint32{101, 102, 103})
	// 						So(ds.GIDs, ShouldResemble, []uint32{1, 2, 3})
	// 						So(ds.FTs, ShouldResemble, expectedFTs)

	// 						children := db.Children("/")
	// 						So(children, ShouldResemble, []string{"/a", "/i"})

	// 						children = db.Children("/a")
	// 						So(children, ShouldResemble, []string{"/a/b", "/a/c", "/a/i"})
	// 					})
	// 				})
	// 			})

	// 			Convey("You can get info on the database files", func() {
	// 				info, err := db.Info()
	// 				So(err, ShouldBeNil)
	// 				So(info, ShouldResemble, &DBInfo{
	// 					NumDirs:     11,
	// 					NumDGUTAs:   620,
	// 					NumParents:  7,
	// 					NumChildren: 10,
	// 				})
	// 			})
	// 		})

	// 		Convey("Storing with a batch size == directories works", func() {
	// 			err := db.Store(data, len(expectedKeys))
	// 			So(err, ShouldBeNil)

	// 			keys, errt := testGetDBKeys(paths[1], gutaBucket)
	// 			So(errt, ShouldBeNil)
	// 			So(keys, ShouldResemble, expectedKeys)
	// 		})

	// 		Convey("Storing with a batch size > directories works", func() {
	// 			err := db.Store(data, len(expectedKeys)+2)
	// 			So(err, ShouldBeNil)

	// 			keys, errt := testGetDBKeys(paths[1], gutaBucket)
	// 			So(errt, ShouldBeNil)
	// 			So(keys, ShouldResemble, expectedKeys)
	// 		})

	// 		Convey("You can't store to db if data is invalid", func() {
	// 			err := db.Store(strings.NewReader("foo"), 4)
	// 			So(err, ShouldNotBeNil)
	// 			So(db.writeErr, ShouldBeNil)
	// 		})

	// 		Convey("You can't store to db if", func() {
	// 			db.batchSize = 4
	// 			err := db.createDB()
	// 			So(err, ShouldBeNil)

	// 			Convey("the first db gets closed", func() {
	// 				err = db.writeSet.dgutas.Close()
	// 				So(err, ShouldBeNil)

	// 				db.writeErr = nil
	// 				err = db.storeData(data)
	// 				So(err, ShouldBeNil)
	// 				So(db.writeErr, ShouldNotBeNil)
	// 			})

	// 			Convey("the second db gets closed", func() {
	// 				err = db.writeSet.children.Close()
	// 				So(err, ShouldBeNil)

	// 				db.writeErr = nil
	// 				err = db.storeData(data)
	// 				So(err, ShouldBeNil)
	// 				So(db.writeErr, ShouldNotBeNil)
	// 			})

	// 			Convey("the put fails", func() {
	// 				db.writeBatch = expected

	// 				err = db.writeSet.children.View(db.storeChildren)
	// 				So(err, ShouldNotBeNil)

	// 				err = db.writeSet.dgutas.View(db.storeDGUTAs)
	// 				So(err, ShouldNotBeNil)
	// 			})
	// 		})
	// 	})

	// 	Convey("You can't Store to or Open an unwritable location", func() {
	// 		db := NewDB("/dguta.db")
	// 		So(db, ShouldNotBeNil)

	// 		err := db.Store(data, 4)
	// 		So(err, ShouldNotBeNil)

	// 		err = db.Open()
	// 		So(err, ShouldNotBeNil)

	// 		paths, err := testMakeDBPaths(t)
	// 		So(err, ShouldBeNil)

	// 		db = NewDB(paths[0])

	// 		err = os.WriteFile(paths[2], []byte("foo"), 0600)
	// 		So(err, ShouldBeNil)

	// 		err = db.Store(data, 4)
	// 		So(err, ShouldNotBeNil)
	// 	})
	// })
}

type gutaInfo struct {
	GID         uint32
	UID         uint32
	FT          DirGUTAFileType
	aCount      uint64
	mCount      uint64
	aSize       uint64
	mSize       uint64
	aTime       int64
	mTime       int64
	orderOfAges []DirGUTAge
}

// testData provides some test data and expected results.
// func testData(t *testing.T, refUnixTime int64) (dgutaData string, expectedRootGUTAs GUTAs,
// 	expected []*DGUTA, expectedKeys []string) {
// 	t.Helper()

// 	dgutaData = internaldata.TestDGUTAData(t, internaldata.CreateDefaultTestData(1, 2, 1, 101, 102, refUnixTime))

// 	orderOfOldAges := DirGUTAges[:]

// 	orderOfDiffAMtimesAges := []DirGUTAge{
// 		DGUTAgeAll, DGUTAgeA1M, DGUTAgeA2M, DGUTAgeA6M,
// 		DGUTAgeA1Y, DGUTAgeM1M, DGUTAgeM2M, DGUTAgeM6M,
// 		DGUTAgeM1Y, DGUTAgeM2Y, DGUTAgeM3Y,
// 	}

// 	expectedRootGUTAs = addGUTAs(t, []gutaInfo{
// 		{1, 101, DGUTAFileTypeTemp, 1, 2, 5, 1029, 80, 80, orderOfOldAges},
// 		{1, 101, DGUTAFileTypeBam, 2, 2, 10, 10, 80, 80, orderOfOldAges},
// 		{1, 101, DGUTAFileTypeCram, 3, 3, 30, 30, 50, 60, orderOfOldAges},
// 		{1, 101, DGUTAFileTypeDir, 0, 8, 0, 8192, math.MaxInt, 1, orderOfOldAges},
// 		{1, 102, DGUTAFileTypeCram, 4, 4, 40, 40, 75, 75, orderOfOldAges},
// 		{2, 102, DGUTAFileTypeCram, 5, 5, 5, 5, 90, 90, orderOfOldAges},
// 		{2, 102, DGUTAFileTypeDir, 0, 2, 0, 2048, math.MaxInt, 1, orderOfOldAges},
// 		{
// 			3, 103, DGUTAFileTypeCram, 7, 7, 7, 7, time.Now().Unix() - SecondsInAYear,
// 			time.Now().Unix() - (SecondsInAYear * 3), orderOfDiffAMtimesAges,
// 		},
// 	})

// 	expected = []*DGUTA{
// 		{
// 			Dir: "/", GUTAs: expectedRootGUTAs,
// 		},
// 		{
// 			Dir: "/a", GUTAs: expectedRootGUTAs,
// 		},
// 		{
// 			Dir: "/a/b", GUTAs: addGUTAs(t, []gutaInfo{
// 				{1, 101, DGUTAFileTypeTemp, 1, 2, 5, 1029, 80, 80, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeBam, 2, 2, 10, 10, 80, 80, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeCram, 3, 3, 30, 30, 50, 60, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeDir, 0, 7, 0, 7168, math.MaxInt, 1, orderOfOldAges},
// 				{1, 102, DGUTAFileTypeCram, 4, 4, 40, 40, 75, 75, orderOfOldAges},
// 			}),
// 		},
// 		{
// 			Dir: "/a/b/d", GUTAs: addGUTAs(t, []gutaInfo{
// 				{1, 101, DGUTAFileTypeCram, 3, 3, 30, 30, 50, 60, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeDir, 0, 3, 0, 3072, math.MaxInt, 1, orderOfOldAges},
// 				{1, 102, DGUTAFileTypeCram, 4, 4, 40, 40, 75, 75, orderOfOldAges},
// 			}),
// 		},
// 		{
// 			Dir: "/a/b/d/f", GUTAs: addGUTAs(t, []gutaInfo{
// 				{1, 101, DGUTAFileTypeCram, 1, 1, 10, 10, 50, 50, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeDir, 0, 1, 0, 1024, math.MaxInt, 1, orderOfOldAges},
// 			}),
// 		},
// 		{
// 			Dir: "/a/b/d/g", GUTAs: addGUTAs(t, []gutaInfo{
// 				{1, 101, DGUTAFileTypeCram, 2, 2, 20, 20, 60, 60, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeDir, 0, 1, 0, 1024, math.MaxInt, 1, orderOfOldAges},
// 				{1, 102, DGUTAFileTypeCram, 4, 4, 40, 40, 75, 75, orderOfOldAges},
// 			}),
// 		},
// 		{
// 			Dir: "/a/b/e", GUTAs: addGUTAs(t, []gutaInfo{
// 				{1, 101, DGUTAFileTypeTemp, 1, 2, 5, 1029, 80, 80, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeBam, 2, 2, 10, 10, 80, 80, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeDir, 0, 3, 0, 3072, math.MaxInt, 1, orderOfOldAges},
// 			}),
// 		},
// 		{
// 			Dir: "/a/b/e/h", GUTAs: addGUTAs(t, []gutaInfo{
// 				{1, 101, DGUTAFileTypeTemp, 1, 2, 5, 1029, 80, 80, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeBam, 2, 2, 10, 10, 80, 80, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeDir, 0, 2, 0, 2048, math.MaxInt, 1, orderOfOldAges},
// 			}),
// 		},
// 		{
// 			Dir: "/a/b/e/h/tmp", GUTAs: addGUTAs(t, []gutaInfo{
// 				{1, 101, DGUTAFileTypeTemp, 1, 2, 5, 1029, 80, 80, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeBam, 1, 1, 5, 5, 80, 80, orderOfOldAges},
// 				{1, 101, DGUTAFileTypeDir, 0, 1, 0, 1024, math.MaxInt, 1, orderOfOldAges},
// 			}),
// 		},
// 		{
// 			Dir: "/a/c", GUTAs: addGUTAs(t, []gutaInfo{
// 				{2, 102, DGUTAFileTypeCram, 5, 5, 5, 5, 90, 90, orderOfOldAges},
// 				{2, 102, DGUTAFileTypeDir, 0, 2, 0, 2048, math.MaxInt, 1, orderOfOldAges},
// 				{
// 					3, 103, DGUTAFileTypeCram, 7, 7, 7, 7, time.Now().Unix() - SecondsInAYear,
// 					time.Now().Unix() - (SecondsInAYear * 3), orderOfDiffAMtimesAges,
// 				},
// 			}),
// 		},
// 		{
// 			Dir: "/a/c/d", GUTAs: addGUTAs(t, []gutaInfo{
// 				{2, 102, DGUTAFileTypeCram, 5, 5, 5, 5, 90, 90, orderOfOldAges},
// 				{2, 102, DGUTAFileTypeDir, 0, 1, 0, 1024, math.MaxInt, 1, orderOfOldAges},
// 				{
// 					3, 103, DGUTAFileTypeCram, 7, 7, 7, 7, time.Now().Unix() - SecondsInAYear,
// 					time.Now().Unix() - (SecondsInAYear * 3), orderOfDiffAMtimesAges,
// 				},
// 			}),
// 		},
// 	}

// 	for _, dir := range []string{
// 		"/", "/a", "/a/b", "/a/b/d", "/a/b/d/f",
// 		"/a/b/d/g", "/a/b/e", "/a/b/e/h", "/a/b/e/h/tmp", "/a/c", "/a/c/d",
// 	} {
// 		for age := 0; age < len(DirGUTAges); age++ {
// 			expectedKeys = append(expectedKeys, dir+string(byte(age)))
// 		}
// 	}

// 	return dgutaData, expectedRootGUTAs, expected, expectedKeys
// }

func addGUTAs(t *testing.T, gutaInfo []gutaInfo) []*GUTA {
	t.Helper()

	GUTAs := []*GUTA{}

	for _, info := range gutaInfo {
		for _, age := range info.orderOfAges {
			count, size, exists := determineCountSize(age, info.aCount, info.mCount, info.aSize, info.mSize)
			if !exists {
				continue
			}

			GUTAs = append(GUTAs, &GUTA{
				GID: info.GID, UID: info.UID, FT: info.FT,
				Age: age, Count: count, Size: size, Atime: info.aTime, Mtime: info.mTime,
			})
		}
	}

	return GUTAs
}

func determineCountSize(age DirGUTAge, aCount, mCount, aSize, mSize uint64) (count, size uint64, exists bool) {
	if ageIsForAtime(age) {
		if aCount == 0 {
			return 0, 0, false
		}

		return aCount, aSize, true
	}

	return mCount, mSize, true
}

func ageIsForAtime(age DirGUTAge) bool {
	return age < 9 && age != 0
}

// testMakeDBPaths creates a temp dir that will be cleaned up automatically, and
// returns the paths to the directory and dguta and children database files
// inside that would be created. The files aren't actually created.
func testMakeDBPaths(t *testing.T) ([]string, error) {
	t.Helper()

	dir := t.TempDir()

	set, err := newDBSet(dir)
	if err != nil {
		return nil, err
	}

	paths := set.paths()

	return append([]string{dir}, paths...), nil
}

// testGetDBKeys returns all the keys in the db at the given path.
func testGetDBKeys(path, bucket string) ([]string, error) {
	rdb, err := bolt.Open(path, dbOpenMode, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = rdb.Close()
	}()

	var keys []string

	err = rdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))

		return b.ForEach(func(k, v []byte) error {
			keys = append(keys, string(k))

			return nil
		})
	})

	return keys, err
}

func alterDgutaForTest(dguta *DGUTA) *DGUTA {
	for _, guta := range dguta.GUTAs {
		if guta.FT == DGUTAFileTypeDir && guta.Count > 0 {
			guta.Atime = math.MaxInt
		}
	}

	return dguta
}
