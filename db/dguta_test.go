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
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	wrbolt "github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
	bolt "go.etcd.io/bbolt"
)

const (
	gutaBucket     = "gut"
	childBucket    = "children"
	boltOpenMode   = 0o640
	dgutaDBName    = "dguta.db"
	childrenDBName = "dguta.db.children"
)

func TestDGUTA(t *testing.T) {
	Convey("", t, func() {
		refUnixTime := time.Now().Unix()
		data, expectedRootGUTAs, expected, expectedKeys := testData(t, refUnixTime)

		Convey("You can see if a GUTA passes a filter", func() {
			numGutas := 17
			emptyGutas := 8
			testIndex := func(index int) int {
				switch {
				case index > 5:
					return index*numGutas - emptyGutas*3
				case index > 4:
					return index*numGutas - emptyGutas*2
				case index > 3:
					return index*numGutas - emptyGutas
				default:
					return index * numGutas
				}
			}

			filter := &db.Filter{}
			a := expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
			So(a, ShouldBeTrue)

			a = expectedRootGUTAs[0].PassesFilter(filter)
			So(a, ShouldBeTrue)

			filter.GIDs = []uint32{3, 4, 5}
			a = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
			So(a, ShouldBeFalse)

			filter.GIDs = []uint32{3, 2, 1}
			a = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
			So(a, ShouldBeTrue)

			filter.UIDs = []uint32{103}
			a = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
			So(a, ShouldBeFalse)

			filter.UIDs = []uint32{103, 102, 101}
			a = expectedRootGUTAs[testIndex(1)].PassesFilter(filter)
			So(a, ShouldBeTrue)

			filter.FT = db.DGUTAFileTypeTemp
			a = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
			So(a, ShouldBeFalse)
			a = expectedRootGUTAs[0].PassesFilter(filter)
			So(a, ShouldBeTrue)

			filter.FT = db.DGUTAFileTypeTemp | db.DGUTAFileTypeCram
			a = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
			So(a, ShouldBeTrue)
			a = expectedRootGUTAs[0].PassesFilter(filter)
			So(a, ShouldBeTrue)

			filter.UIDs = nil
			a = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
			So(a, ShouldBeTrue)

			filter.GIDs = nil
			a = expectedRootGUTAs[testIndex(2)].PassesFilter(filter)
			So(a, ShouldBeTrue)

			filter.FT = db.DGUTAFileTypeDir
			a = expectedRootGUTAs[testIndex(3)].PassesFilter(filter)
			So(a, ShouldBeTrue)

			filter = &db.Filter{Age: db.DGUTAgeA1M}
			a = expectedRootGUTAs[testIndex(8)+1].PassesFilter(filter)
			So(a, ShouldBeTrue)

			filter.Age = db.DGUTAgeA7Y
			a = expectedRootGUTAs[testIndex(8)+1].PassesFilter(filter)
			So(a, ShouldBeFalse)
		})

		expectedUIDs := []uint32{101, 102, 103}
		expectedGIDs := []uint32{1, 2, 3}
		expectedFTs := db.DGUTAFileTypeTemp |
			db.DGUTAFileTypeBam |
			db.DGUTAFileTypeCram |
			db.DGUTAFileTypeDir

		const numDirectories = 11

		const directorySize = 4096

		expectedMtime := time.Unix(time.Now().Unix()-(db.SecondsInAYear*3), 0)

		defaultFilter := &db.Filter{Age: db.DGUTAgeAll}

		Convey("GUTAs can sum the count and size and provide UIDs, GIDs and FTs of their GUTA elements", func() {
			ds := expectedRootGUTAs.Summary(defaultFilter)
			So(ds.Count, ShouldEqual, 21+numDirectories)
			So(ds.Size, ShouldEqual, 92+numDirectories*directorySize)
			So(ds.Atime, ShouldEqual, time.Unix(50, 0))
			So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
			So(ds.UIDs, ShouldResemble, expectedUIDs)
			So(ds.GIDs, ShouldResemble, expectedGIDs)
			So(ds.FT, ShouldEqual, expectedFTs)
		})

		Convey("A DGUTA can be encoded and decoded", func() {
			d := internaldata.NewDirectoryPathCreator()

			r := db.RecordDGUTA{
				Dir:   d.ToDirectoryPath(expected[0].Dir),
				GUTAs: expected[0].GUTAs,
			}

			dirb, b := r.EncodeToBytes()
			So(len(dirb), ShouldEqual, 2) // 98, 255
			So(len(b), ShouldEqual, 4275)

			dd := db.DecodeDGUTAbytes(dirb, b)
			So(dd, ShouldResemble, expected[0])
		})

		Convey("A DGUTA can sum the count and size and provide UIDs, GIDs and FTs of its GUTs", func() {
			ds := expected[0].Summary(defaultFilter)
			So(ds.Count, ShouldEqual, 21+numDirectories)
			So(ds.Size, ShouldEqual, 92+numDirectories*directorySize)
			So(ds.Atime, ShouldEqual, time.Unix(50, 0))
			So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
			So(ds.UIDs, ShouldResemble, expectedUIDs)
			So(ds.GIDs, ShouldResemble, expectedGIDs)
			So(ds.FT, ShouldResemble, expectedFTs)
		})

		Convey("Given multiline dguta data", func() {
			Convey("And database file paths", func() {
				paths, err := testMakeDBPaths(t)
				So(err, ShouldBeNil)

				Convey("You can store it in a database file", func() {
					_, errs := os.Stat(paths[1])
					So(errs, ShouldNotBeNil)
					_, errs = os.Stat(paths[2])
					So(errs, ShouldNotBeNil)

					err := store(paths[0], data, 4)
					So(err, ShouldBeNil)

					Convey("The resulting database files have the expected content", func() {
						info, errs := os.Stat(paths[1])
						So(errs, ShouldBeNil)
						So(info.Size(), ShouldBeGreaterThan, 10)
						info, errs = os.Stat(paths[2])
						So(errs, ShouldBeNil)
						So(info.Size(), ShouldBeGreaterThan, 10)

						keys, errt := testGetDBKeys(paths[1], gutaBucket)
						So(errt, ShouldBeNil)
						So(keys, ShouldResemble, expectedKeys)

						keys, errt = testGetDBKeys(paths[2], childBucket)
						So(errt, ShouldBeNil)
						So(keys, ShouldResemble, []string{"/", "/a/", "/a/b/", "/a/b/d/", "/a/b/e/", "/a/b/e/h/", "/a/c/"})
						Convey("You can query a database after opening it", func() {
							d, errOpen := wrbolt.OpenDatabase(paths[0])
							So(errOpen, ShouldBeNil)

							defer d.Close()

							ds, errd := d.DirInfo("/", defaultFilter)
							So(errd, ShouldBeNil)
							So(ds.Count, ShouldEqual, 21+numDirectories)
							So(ds.Size, ShouldEqual, 92+numDirectories*directorySize)
							So(ds.Atime, ShouldEqual, time.Unix(50, 0))
							So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
							So(ds.UIDs, ShouldResemble, expectedUIDs)
							So(ds.GIDs, ShouldResemble, expectedGIDs)
							So(ds.FT, ShouldResemble, expectedFTs)

							ds, errd = d.DirInfo("/", &db.Filter{Age: db.DGUTAgeA7Y})
							So(errd, ShouldBeNil)
							So(ds.Count, ShouldEqual, 21-7)
							So(ds.Size, ShouldEqual, 92-7)
							So(ds.Atime, ShouldEqual, time.Unix(50, 0))
							So(ds.Mtime, ShouldEqual, time.Unix(90, 0))
							So(ds.UIDs, ShouldResemble, []uint32{101, 102})
							So(ds.GIDs, ShouldResemble, []uint32{1, 2})
							So(ds.FT, ShouldResemble,
								db.DGUTAFileTypeTemp|db.DGUTAFileTypeBam|db.DGUTAFileTypeCram,
							)

							ds, errd = d.DirInfo("/a/c/d", defaultFilter)
							So(errd, ShouldBeNil)
							So(ds.Count, ShouldEqual, 13)
							So(ds.Size, ShouldEqual, 12+directorySize)
							So(ds.Atime, ShouldEqual, time.Unix(90, 0))
							So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
							So(ds.UIDs, ShouldResemble, []uint32{102, 103})
							So(ds.GIDs, ShouldResemble, []uint32{2, 3})
							So(ds.FT, ShouldResemble, db.DGUTAFileTypeCram|db.DGUTAFileTypeDir)

							ds, errd = d.DirInfo("/a/b/d/g", defaultFilter)
							So(errd, ShouldBeNil)
							So(ds.Count, ShouldEqual, 7)
							So(ds.Size, ShouldEqual, 60+directorySize)
							So(ds.Atime, ShouldEqual, time.Unix(60, 0))
							So(ds.Mtime, ShouldEqual, time.Unix(75, 0))
							So(ds.UIDs, ShouldResemble, []uint32{101, 102})
							So(ds.GIDs, ShouldResemble, []uint32{1})
							So(ds.FT, ShouldResemble, db.DGUTAFileTypeCram|db.DGUTAFileTypeDir)

							_, errd = d.DirInfo("/foo", defaultFilter)
							So(errd, ShouldNotBeNil)
							So(errd, ShouldEqual, db.ErrDirNotFound)

							ds, errd = d.DirInfo("/", &db.Filter{GIDs: []uint32{1}})
							So(errd, ShouldBeNil)
							So(ds.Count, ShouldEqual, 18)
							So(ds.Size, ShouldEqual, 80+9*directorySize)
							So(ds.Atime, ShouldEqual, time.Unix(50, 0))
							So(ds.Mtime, ShouldEqual, time.Unix(80, 0))
							So(ds.UIDs, ShouldResemble, []uint32{101, 102})
							So(ds.GIDs, ShouldResemble, []uint32{1})
							So(ds.FT, ShouldResemble, expectedFTs)

							ds, errd = d.DirInfo("/", &db.Filter{UIDs: []uint32{102}})
							So(errd, ShouldBeNil)
							So(ds.Count, ShouldEqual, 11)
							So(ds.Size, ShouldEqual, 45+2*directorySize)
							So(ds.Atime, ShouldEqual, time.Unix(75, 0))
							So(ds.Mtime, ShouldEqual, time.Unix(90, 0))
							So(ds.UIDs, ShouldResemble, []uint32{102})
							So(ds.GIDs, ShouldResemble, []uint32{1, 2})
							So(ds.FT, ShouldResemble, db.DGUTAFileTypeCram|db.DGUTAFileTypeDir)

							ds, errd = d.DirInfo("/", &db.Filter{GIDs: []uint32{1}, UIDs: []uint32{102}})
							So(errd, ShouldBeNil)
							So(ds.Count, ShouldEqual, 4)
							So(ds.Size, ShouldEqual, 40)
							So(ds.Atime, ShouldEqual, time.Unix(75, 0))
							So(ds.Mtime, ShouldEqual, time.Unix(75, 0))
							So(ds.UIDs, ShouldResemble, []uint32{102})
							So(ds.GIDs, ShouldResemble, []uint32{1})
							So(ds.FT, ShouldResemble, db.DGUTAFileTypeCram)

							ds, errd = d.DirInfo("/", &db.Filter{
								GIDs: []uint32{1},
								UIDs: []uint32{102},
								FT:   db.DGUTAFileTypeTemp,
							})
							So(errd, ShouldBeNil)
							So(ds, ShouldBeNil)

							ds, errd = d.DirInfo("/", &db.Filter{FT: db.DGUTAFileTypeTemp})
							So(errd, ShouldBeNil)
							So(ds.Count, ShouldEqual, 2)
							So(ds.Size, ShouldEqual, 5+directorySize)
							So(ds.Atime, ShouldEqual, time.Unix(80, 0))
							So(ds.Mtime, ShouldEqual, time.Unix(80, 0))
							So(ds.UIDs, ShouldResemble, []uint32{101})
							So(ds.GIDs, ShouldResemble, []uint32{1})
							So(ds.FT, ShouldResemble, db.DGUTAFileTypeTemp|db.DGUTAFileTypeBam|db.DGUTAFileTypeDir)

							children, errCh := d.Children("/a")
							So(errCh, ShouldBeNil)
							So(children, ShouldResemble, []string{"/a/b", "/a/c"})

							children, errCh = d.Children("/a/b/e/h")
							So(errCh, ShouldBeNil)
							So(children, ShouldResemble, []string{"/a/b/e/h/tmp"})

							children, errCh = d.Children("/a/c/d")
							So(errCh, ShouldBeNil)
							So(children, ShouldBeNil)

							children, errCh = d.Children("/foo")
							So(errCh, ShouldBeNil)
							So(children, ShouldBeNil)
						})

						Convey("Opens fail on invalid databases", func() {
							err = os.RemoveAll(paths[2])
							So(err, ShouldBeNil)

							err = os.WriteFile(paths[2], []byte("foo"), 0o600)
							So(err, ShouldBeNil)

							_, errOpen := wrbolt.OpenDatabase(paths[0])
							So(errOpen, ShouldNotBeNil)

							err = os.RemoveAll(paths[1])
							So(err, ShouldBeNil)

							err = os.WriteFile(paths[1], []byte("foo"), 0o600)
							So(err, ShouldBeNil)

							_, errOpen = wrbolt.OpenDatabase(paths[0])
							So(errOpen, ShouldNotBeNil)
						})

						Convey("Store()ing multiple times", func() {
							pd := statsdata.NewRoot("/", 25)
							pd.GID = 3
							pd.UID = 103
							pd.AddDirectory("a").AddDirectory("i").AddFile("something.cram").Size = 1
							f := pd.AddDirectory("i").AddFile("something.cram")
							f.ATime = 30
							f.MTime = 30
							f.Size = 1

							data = pd.AsReader()

							Convey("to the same db file doesn't work", func() {
								err = store(paths[0], data, 4)
								So(err, ShouldNotBeNil)
								So(err, ShouldEqual, db.ErrDBExists)
							})

							Convey("to different db directories and loading them all does work", func() {
								path2 := paths[0] + ".2"
								err = os.Mkdir(path2, os.ModePerm)
								So(err, ShouldBeNil)

								err = store(path2, data, 4)
								So(err, ShouldBeNil)

								d, errOpen := wrbolt.OpenDatabase(paths[0], path2)
								So(errOpen, ShouldBeNil)

								defer d.Close()

								ds, errd := d.DirInfo("/", &db.Filter{})
								So(errd, ShouldBeNil)
								So(ds.Count, ShouldEqual, 21+2+(numDirectories+4))
								So(ds.Size, ShouldEqual, 92+2+(numDirectories+4)*directorySize)
								So(ds.Atime, ShouldEqual, time.Unix(25, 0))
								So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
								So(ds.UIDs, ShouldResemble, []uint32{101, 102, 103})
								So(ds.GIDs, ShouldResemble, []uint32{1, 2, 3})
								So(ds.FT, ShouldResemble, expectedFTs)

								children, errCh := d.Children("/")
								So(errCh, ShouldBeNil)
								So(children, ShouldResemble, []string{"/a", "/i"})

								children, errCh = d.Children("/a")
								So(errCh, ShouldBeNil)
								So(children, ShouldResemble, []string{"/a/b", "/a/c", "/a/i"})
							})
						})
					})

					Convey("You can get info on the database files", func() {
						d, errOpen := wrbolt.OpenDatabase(paths[0])
						So(errOpen, ShouldBeNil)

						defer d.Close()

						info, err := d.Info()
						So(err, ShouldBeNil)
						So(info, ShouldResemble, &db.DBInfo{
							NumDirs:     numDirectories,
							NumDGUTAs:   648,
							NumParents:  7,
							NumChildren: 10,
						})
					})
				})

				Convey("Storing with a batch size == directories works", func() {
					err := store(paths[0], data, len(expectedKeys))
					So(err, ShouldBeNil)

					keys, errt := testGetDBKeys(paths[1], gutaBucket)
					So(errt, ShouldBeNil)
					So(keys, ShouldResemble, expectedKeys)
				})

				Convey("Storing with a batch size > directories works", func() {
					err := store(paths[0], data, len(expectedKeys)+2)
					So(err, ShouldBeNil)

					keys, errt := testGetDBKeys(paths[1], gutaBucket)
					So(errt, ShouldBeNil)
					So(keys, ShouldResemble, expectedKeys)
				})

				Convey("You can't store to db if data is invalid", func() {
					err := store(paths[0], strings.NewReader("foo"), 4)
					So(err, ShouldNotBeNil)
				})

				Convey("You can't store to db if db already exists", func() {
					err := store(paths[0], data, 4)
					So(err, ShouldBeNil)

					// Try to store again - should fail
					err = store(paths[0], data, 4)
					So(err, ShouldNotBeNil)
					So(err, ShouldEqual, db.ErrDBExists)
				})

				Convey("Writer add fails on too-long keys", func() {
					writer, err := wrbolt.NewDGUTAWriter(paths[0])
					So(err, ShouldBeNil)

					writer.SetMountPath("/")
					writer.SetUpdatedAt(time.Now())
					writer.SetBatchSize(1)

					err = writer.Add(db.RecordDGUTA{
						Dir: &summary.DirectoryPath{
							Name: strings.Repeat("a", bolt.MaxKeySize),
						},
						GUTAs: expected[0].GUTAs,
					})
					So(err, ShouldNotBeNil)

					writer.Close()
				})
			})

			Convey("You can't Store to or Open an unwritable location", func() {
				err := store("/dguta.db", data, 4)
				So(err, ShouldNotBeNil)

				_, errOpen := wrbolt.OpenDatabase("/nonexistent")
				So(errOpen, ShouldNotBeNil)

				paths, err := testMakeDBPaths(t)
				So(err, ShouldBeNil)

				err = os.WriteFile(paths[2], []byte("foo"), 0o600)
				So(err, ShouldBeNil)

				err = store(paths[0], data, 4)
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func store(dir string, r io.Reader, batchSize int) error {
	writer, err := wrbolt.NewDGUTAWriter(dir)
	if err != nil {
		return err
	}

	writer.SetMountPath("/")
	writer.SetUpdatedAt(time.Now())
	writer.SetBatchSize(batchSize)

	s := summary.NewSummariser(stats.NewStatsParser(r))
	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(writer))

	if err := s.Summarise(); err != nil {
		_ = writer.Close()

		return err
	}

	return writer.Close()
}

type gutaInfo struct {
	GID         uint32
	UID         uint32
	FT          db.DirGUTAFileType
	aCount      uint64
	mCount      uint64
	aSize       uint64
	mSize       uint64
	aTime       int64
	mTime       int64
	orderOfAges []db.DirGUTAge
}

// testData provides some test data and expected results.
func testData(t *testing.T, refUnixTime int64) (dgutaData io.Reader, expectedRootGUTAs db.GUTAs,
	expected []*db.DGUTA, expectedKeys []string,
) {
	t.Helper()

	dgutaData = internaldata.CreateDefaultTestData(1, 2, 1, 101, 102, refUnixTime).AsReader()

	orderOfOldAges := db.DirGUTAges[:]

	orderOfDiffAMtimesAges := []db.DirGUTAge{
		db.DGUTAgeAll, db.DGUTAgeA1M, db.DGUTAgeA2M, db.DGUTAgeA6M,
		db.DGUTAgeA1Y, db.DGUTAgeM1M, db.DGUTAgeM2M, db.DGUTAgeM6M,
		db.DGUTAgeM1Y, db.DGUTAgeM2Y, db.DGUTAgeM3Y,
	}

	expectedRootGUTAs = addGUTAs(t, []gutaInfo{
		{1, 101, db.DGUTAFileTypeTemp | db.DGUTAFileTypeBam, 1, 1, 5, 5, 80, 80, orderOfOldAges},
		{1, 101, db.DGUTAFileTypeBam, 1, 1, 5, 5, 80, 80, orderOfOldAges},
		{1, 101, db.DGUTAFileTypeCram, 3, 3, 30, 30, 50, 60, orderOfOldAges},
		{1, 101, db.DGUTAFileTypeDir | db.DGUTAFileTypeTemp, 0, 1, 0, 4096, math.MaxInt, 1, orderOfOldAges},
		{1, 101, db.DGUTAFileTypeDir, 0, 7, 0, 28672, math.MaxInt, 1, orderOfOldAges},
		{1, 102, db.DGUTAFileTypeCram, 4, 4, 40, 40, 75, 75, orderOfOldAges},
		{2, 102, db.DGUTAFileTypeCram, 5, 5, 5, 5, 90, 90, orderOfOldAges},
		{2, 102, db.DGUTAFileTypeDir, 0, 2, 0, 8192, math.MaxInt, 1, orderOfOldAges},
		{
			3, 103, db.DGUTAFileTypeCram, 7, 7, 7, 7, time.Now().Unix() - db.SecondsInAYear,
			time.Now().Unix() - (db.SecondsInAYear * 3), orderOfDiffAMtimesAges,
		},
		{1, 101, db.DGUTAFileTypeDir, 1, 1, 4096, 4096, 0, 0, orderOfOldAges},
	})

	expected = []*db.DGUTA{
		{
			Dir: "/", GUTAs: expectedRootGUTAs,
		},
		{
			Dir: "/a/", GUTAs: expectedRootGUTAs,
		},
		{
			Dir: "/a/b/", GUTAs: addGUTAs(t, []gutaInfo{
				{1, 101, db.DGUTAFileTypeBam | db.DGUTAFileTypeTemp, 1, 1, 5, 5, 80, 80, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeBam, 1, 1, 5, 5, 80, 80, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeCram, 3, 3, 30, 30, 50, 60, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir, 0, 1, 0, 24576, math.MaxInt, 1, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir | db.DGUTAFileTypeTemp, 0, 1, 0, 4096, math.MaxInt, 1, orderOfOldAges},

				{1, 102, db.DGUTAFileTypeCram, 4, 4, 40, 40, 75, 75, orderOfOldAges},
			}),
		},
		{
			Dir: "/a/b/d/", GUTAs: addGUTAs(t, []gutaInfo{
				{1, 101, db.DGUTAFileTypeCram, 3, 3, 30, 30, 50, 60, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir, 0, 3, 0, 12288, math.MaxInt, 1, orderOfOldAges},
				{1, 102, db.DGUTAFileTypeCram, 4, 4, 40, 40, 75, 75, orderOfOldAges},
			}),
		},
		{
			Dir: "/a/b/d/f/", GUTAs: addGUTAs(t, []gutaInfo{
				{1, 101, db.DGUTAFileTypeCram, 1, 1, 10, 10, 50, 50, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir, 0, 1, 0, 4096, math.MaxInt, 1, orderOfOldAges},
			}),
		},
		{
			Dir: "/a/b/d/g/", GUTAs: addGUTAs(t, []gutaInfo{
				{1, 101, db.DGUTAFileTypeCram, 2, 2, 20, 20, 60, 60, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir, 0, 1, 0, 4096, math.MaxInt, 1, orderOfOldAges},
				{1, 102, db.DGUTAFileTypeCram, 4, 4, 40, 40, 75, 75, orderOfOldAges},
			}),
		},
		{
			Dir: "/a/b/e/", GUTAs: addGUTAs(t, []gutaInfo{
				{1, 101, db.DGUTAFileTypeBam | db.DGUTAFileTypeTemp, 1, 1, 5, 5, 80, 80, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeBam, 1, 1, 5, 5, 80, 80, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir, 0, 2, 0, 8192, math.MaxInt, 1, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir | db.DGUTAFileTypeTemp, 0, 1, 0, 4096, math.MaxInt, 1, orderOfOldAges},
			}),
		},
		{
			Dir: "/a/b/e/h/", GUTAs: addGUTAs(t, []gutaInfo{
				{1, 101, db.DGUTAFileTypeBam | db.DGUTAFileTypeTemp, 1, 1, 5, 5, 80, 80, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeBam, 1, 1, 5, 5, 80, 80, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir, 0, 1, 0, 4096, math.MaxInt, 1, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir | db.DGUTAFileTypeTemp, 0, 1, 0, 4096, math.MaxInt, 1, orderOfOldAges},
			}),
		},
		{
			Dir: "/a/b/e/h/tmp/", GUTAs: addGUTAs(t, []gutaInfo{
				{1, 101, db.DGUTAFileTypeBam | db.DGUTAFileTypeTemp, 1, 1, 5, 5, 80, 80, orderOfOldAges},
				{1, 101, db.DGUTAFileTypeDir | db.DGUTAFileTypeTemp, 0, 1, 0, 4096, math.MaxInt, 1, orderOfOldAges},
			}),
		},
		{
			Dir: "/a/c/", GUTAs: addGUTAs(t, []gutaInfo{
				{2, 102, db.DGUTAFileTypeCram, 5, 5, 5, 5, 90, 90, orderOfOldAges},
				{2, 102, db.DGUTAFileTypeDir, 0, 2, 0, 8192, math.MaxInt, 1, orderOfOldAges},
				{
					3, 103, db.DGUTAFileTypeCram, 7, 7, 7, 7, time.Now().Unix() - db.SecondsInAYear,
					time.Now().Unix() - (db.SecondsInAYear * 3), orderOfDiffAMtimesAges,
				},
			}),
		},
		{
			Dir: "/a/c/d/", GUTAs: addGUTAs(t, []gutaInfo{
				{2, 102, db.DGUTAFileTypeCram, 5, 5, 5, 5, 90, 90, orderOfOldAges},
				{2, 102, db.DGUTAFileTypeDir, 0, 1, 0, 4096, math.MaxInt, 1, orderOfOldAges},
				{
					3, 103, db.DGUTAFileTypeCram, 7, 7, 7, 7, time.Now().Unix() - db.SecondsInAYear,
					time.Now().Unix() - (db.SecondsInAYear * 3), orderOfDiffAMtimesAges,
				},
			}),
		},
	}

	for _, dir := range []string{
		"/a/b/d/f/",
		"/a/b/d/g/",
		"/a/b/d/",
		"/a/b/e/h/tmp/",
		"/a/b/e/h/",
		"/a/b/e/",
		"/a/b/",
		"/a/c/d/",
		"/a/c/",
		"/a/",
		"/",
	} {
		expectedKeys = append(expectedKeys, dir+"\xff")
	}

	return dgutaData, expectedRootGUTAs, expected, expectedKeys
}

func addGUTAs(t *testing.T, gutaInfo []gutaInfo) []*db.GUTA {
	t.Helper()

	GUTAs := []*db.GUTA{}

	for _, info := range gutaInfo {
		for _, age := range info.orderOfAges {
			count, size, exists := determineCountSize(age, info.aCount, info.mCount, info.aSize, info.mSize)
			if !exists {
				continue
			}

			GUTAs = append(GUTAs, &db.GUTA{
				GID: info.GID, UID: info.UID, FT: info.FT,
				Age: age, Count: count, Size: size, Atime: info.aTime, Mtime: info.mTime,
			})
		}
	}

	return GUTAs
}

func determineCountSize(age db.DirGUTAge, aCount, mCount, aSize, mSize uint64) (count, size uint64, exists bool) {
	if ageIsForAtime(age) {
		if aCount == 0 {
			return 0, 0, false
		}

		return aCount, aSize, true
	}

	return mCount, mSize, true
}

func ageIsForAtime(age db.DirGUTAge) bool {
	return age < 9 && age != 0
}

// testMakeDBPaths creates a temp dir that will be cleaned up automatically, and
// returns the paths to the directory and dguta and children database files
// inside that would be created. The files aren't actually created.
func testMakeDBPaths(t *testing.T) ([]string, error) {
	t.Helper()

	dir := t.TempDir()

	return []string{
		dir,
		filepath.Join(dir, dgutaDBName),
		filepath.Join(dir, childrenDBName),
	}, nil
}

// testGetDBKeys returns all the keys in the db at the given path.
func testGetDBKeys(path, bucket string) ([]string, error) {
	rdb, err := bolt.Open(path, boltOpenMode, nil)
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
