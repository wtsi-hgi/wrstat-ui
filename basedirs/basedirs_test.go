/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/fixtimes"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	bolt "go.etcd.io/bbolt"
)

func TestBaseDirs(t *testing.T) {
	const (
		defaultSplits  = 4
		defaultMinDirs = 4
	)

	csvPath := internaldata.CreateQuotasCSV(t, `1,/lustre/scratch125,4000000000,20
2,/lustre/scratch125,300,30
2,/lustre/scratch123,400,40
77777,/lustre/scratch125,500,50
1,/nfs/scratch125,4000000000,20
2,/nfs/scratch125,300,30
2,/nfs/scratch123,400,40
77777,/nfs/scratch125,500,50
3,/lustre/scratch125,300,30
`)

	mps := []string{
		"/lustre/scratch123/",
		"/lustre/scratch125/",
	}

	defaultConfig := basedirs.Config{
		{
			Prefix:  split.SplitPath("/lustre/scratch123/hgi/mdt"),
			Splits:  defaultSplits + 1,
			MinDirs: defaultMinDirs + 1,
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

	ageGroupName := "3"

	ageGroup, err := user.LookupGroupId("3")
	if err == nil {
		ageGroupName = ageGroup.Name
	}

	ageUserName := "103"

	ageUser, err := user.LookupId("103")
	if err == nil {
		ageUserName = ageUser.Username
	}

	refTime := time.Now().Unix()
	expectedAgeAtime2 := time.Unix(refTime-db.SecondsInAYear*3, 0)
	expectedAgeMtime := time.Unix(refTime-db.SecondsInAYear*3, 0)
	expectedAgeMtime2 := time.Unix(refTime-db.SecondsInAYear*5, 0)
	expectedFixedAgeMtime := fixtimes.FixTime(expectedAgeMtime)
	expectedFixedAgeMtime2 := fixtimes.FixTime(expectedAgeMtime2)

	Convey("Given a Tree and Quotas you can make a BaseDirs", t, func() {
		gid, uid, groupName, username, err := internaluser.RealGIDAndUID()
		So(err, ShouldBeNil)

		const (
			halfGig = 1 << 29
			twoGig  = 1 << 31
		)

		locDirs, root := internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid,
			"lustre", 1, halfGig, twoGig, true, refTime)

		now := fixtimes.FixTime(time.Now())
		yesterday := fixtimes.FixTime(now.Add(-24 * time.Hour))

		projectA := locDirs[0]
		projectB125 := locDirs[1]
		projectB123 := locDirs[2]
		projectC1 := locDirs[3]
		user2 := locDirs[5]
		projectD := locDirs[6]

		quotas, err := basedirs.ParseQuotas(csvPath)
		So(err, ShouldBeNil)

		dir := t.TempDir()
		dbPath := filepath.Join(dir, "basedir.db")

		basedirsCreator := func(modtime time.Time) {
			t.Helper()

			bd, errr := basedirs.NewCreator(dbPath, quotas)
			So(errr, ShouldBeNil)
			So(bd, ShouldNotBeNil)

			bd.SetMountPoints(mps)
			bd.SetModTime(modtime)

			s := summary.NewSummariser(stats.NewStatsParser(root.AsReader()))
			s.AddDirectoryOperation(sbasedirs.NewBaseDirs(defaultConfig.PathShouldOutput, bd))

			errr = s.Summarise()
			So(errr, ShouldBeNil)
		}

		basedirsCreator(yesterday)

		_ = expectedAgeAtime2

		Convey("With which you can store group and user summary info in a database", func() {
			_, err = os.Stat(dbPath)
			So(err, ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			baseDirsReader := func() *basedirs.BaseDirReader {
				t.Helper()

				bdr, errr := basedirs.NewReader(dbPath, ownersPath)
				So(errr, ShouldBeNil)

				bdr.SetMountPoints(mps)

				return bdr
			}

			bdr := baseDirsReader()

			Convey("and then read the database", func() {
				bdr.SetCachedGroup(1, "group1")
				bdr.SetCachedGroup(2, "group2")
				bdr.SetCachedUser(101, "user101")
				bdr.SetCachedUser(102, "user102")

				expectedMtime := fixtimes.FixTime(time.Unix(50, 0))
				expectedMtimeA := fixtimes.FixTime(time.Unix(100, 0))

				Convey("getting group and user usage info", func() {
					mainTable, errr := bdr.GroupUsage(db.DGUTAgeAll)
					fixUsageTimes(mainTable)

					expectedUsageTable := []*basedirs.Usage{
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectA,
							UsageSize: halfGig + twoGig, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
						},
						{
							Name: ageGroupName, GID: 3, UIDs: []uint32{103}, Owner: "", BaseDir: projectA,
							UsageSize: 100, QuotaSize: 300, UsageInodes: 2, QuotaInodes: 30, Mtime: expectedFixedAgeMtime,
						},
						{
							Name: groupName, GID: gid, UIDs: []uint32{uid}, BaseDir: projectD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							DateNoSpace: yesterday, DateNoFiles: yesterday,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: user2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
						},
					}

					sortByDatabaseKeyOrder(expectedUsageTable)

					So(errr, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 7)
					So(mainTable, ShouldResemble, expectedUsageTable)

					mainTable, errr = bdr.GroupUsage(db.DGUTAgeA3Y)
					fixUsageTimes(mainTable)

					expectedUsageTable = []*basedirs.Usage{
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectA,
							UsageSize: halfGig + twoGig, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA, Age: db.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: ageGroupName, GID: 3, UIDs: []uint32{103}, Owner: "", BaseDir: projectA,
							UsageSize: 40, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedFixedAgeMtime2,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: groupName, GID: gid, UIDs: []uint32{uid}, BaseDir: projectD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: user2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
					}
					sortByDatabaseKeyOrder(expectedUsageTable)

					So(errr, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 7)
					So(mainTable, ShouldResemble, expectedUsageTable)

					mainTable, errr = bdr.GroupUsage(db.DGUTAgeA7Y)
					fixUsageTimes(mainTable)

					expectedUsageTable = []*basedirs.Usage{
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectA,
							UsageSize: halfGig + twoGig, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA, Age: db.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: groupName, GID: uint32(gid), UIDs: []uint32{uint32(uid)}, BaseDir: projectD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: user2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
					}
					sortByDatabaseKeyOrder(expectedUsageTable)

					So(errr, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 6)
					So(mainTable, ShouldResemble, expectedUsageTable)

					mainTable, errr = bdr.UserUsage(db.DGUTAgeAll)
					fixUsageTimes(mainTable)

					expectedMainTable := []*basedirs.Usage{
						{
							Name: "88888", UID: 88888, GIDs: []uint32{2}, BaseDir: projectC1, UsageSize: 40,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user101", UID: 101, GIDs: []uint32{1}, BaseDir: projectA,
							UsageSize: halfGig + twoGig, UsageInodes: 2, Mtime: expectedMtimeA,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{2}, BaseDir: projectB123, UsageSize: 30,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{2}, BaseDir: projectB125, UsageSize: 20,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{77777}, BaseDir: user2, UsageSize: 60,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: username, UID: uid, GIDs: []uint32{gid}, BaseDir: projectD,
							UsageSize: 15, UsageInodes: 5, Mtime: expectedMtime,
						},
						{
							Name: ageUserName, UID: 103, GIDs: []uint32{3}, BaseDir: projectA, UsageSize: 100,
							UsageInodes: 2, Mtime: expectedFixedAgeMtime,
						},
					}

					sortByDatabaseKeyOrder(expectedMainTable)

					So(errr, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 7)
					So(mainTable, ShouldResemble, expectedMainTable)
				})

				Convey("getting group historical quota", func() {
					expectedAHistory := basedirs.History{
						Date:        yesterday,
						UsageSize:   halfGig + twoGig,
						QuotaSize:   4000000000,
						UsageInodes: 2,
						QuotaInodes: 20,
					}

					history, errr := bdr.History(1, projectA)
					fixHistoryTimes(history)

					So(errr, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []basedirs.History{expectedAHistory})

					history, errr = bdr.History(1, filepath.Join(projectA, "newsub"))
					fixHistoryTimes(history)

					So(errr, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []basedirs.History{expectedAHistory})

					history, errr = bdr.History(2, projectB125)
					fixHistoryTimes(history)

					So(errr, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []basedirs.History{
						{
							Date:        yesterday,
							UsageSize:   20,
							QuotaSize:   300,
							UsageInodes: 1,
							QuotaInodes: 30,
						},
					})

					dtrSize, dtrInode := basedirs.DateQuotaFull(history)
					So(dtrSize, ShouldEqual, time.Time{})
					So(dtrInode, ShouldEqual, time.Time{})

					err = bdr.Close()
					So(err, ShouldBeNil)

					Convey("then adding the same database twice doesn't duplicate history.", func() {
						// Add existing…
						basedirsCreator(yesterday)

						bdr = baseDirsReader()

						history, err = bdr.History(1, projectA)
						fixHistoryTimes(history)
						So(err, ShouldBeNil)

						So(len(history), ShouldEqual, 1)

						err = bdr.Close()
						So(err, ShouldBeNil)

						// Add existing again…
						basedirsCreator(yesterday)

						bdr = baseDirsReader()

						history, err = bdr.History(1, projectA)
						fixHistoryTimes(history)
						So(err, ShouldBeNil)

						So(len(history), ShouldEqual, 1)

						err = bdr.Close()
						So(err, ShouldBeNil)

						// Add new…
						basedirsCreator(now)

						bdr = baseDirsReader()

						history, err = bdr.History(1, projectA)
						fixHistoryTimes(history)
						So(err, ShouldBeNil)

						So(len(history), ShouldEqual, 2)

						err = bdr.Close()
						So(err, ShouldBeNil)
					})

					Convey("Then you can add and retrieve a new day's usage and quota", func() {
						_, root = internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid,
							"lustre", 2, halfGig, twoGig, false, refTime)

						const fiveGig = 5 * (1 << 30)

						csvPath := internaldata.CreateQuotasCSV(t, fmt.Sprintf(`1,/lustre/scratch125,%d,%d
2,/lustre/scratch125,300,30
2,/lustre/scratch123,400,40
77777,/lustre/scratch125,500,50
1,/nfs/scratch125,4000000000,20
2,/nfs/scratch125,300,30
2,/nfs/scratch123,400,40
77777,/nfs/scratch125,500,50
3,/lustre/scratch125,300,30
`, fiveGig, 21))

						quotas, err = basedirs.ParseQuotas(csvPath)
						So(err, ShouldBeNil)

						basedirsCreator(now)

						bdr = baseDirsReader()

						bdr.SetCachedGroup(1, "group1")
						bdr.SetCachedGroup(2, "group2")
						bdr.SetCachedUser(101, "user101")
						bdr.SetCachedUser(102, "user102")

						mainTable, errr := bdr.GroupUsage(db.DGUTAgeAll)
						So(errr, ShouldBeNil)
						fixUsageTimes(mainTable)

						leeway := 5 * time.Minute

						dateNoSpace := now.Add(4 * 24 * time.Hour)
						So(mainTable[0].DateNoSpace, ShouldHappenOnOrBetween,
							dateNoSpace.Add(-leeway), dateNoSpace.Add(leeway))

						dateNoTime := now.Add(18 * 24 * time.Hour)
						So(mainTable[0].DateNoFiles, ShouldHappenOnOrBetween,
							dateNoTime.Add(-leeway), dateNoTime.Add(leeway))

						mainTable[0].DateNoSpace = time.Time{}
						mainTable[0].DateNoFiles = time.Time{}

						mainTableExpectation := []*basedirs.Usage{
							{
								Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectA,
								UsageSize: twoGig + halfGig*2, QuotaSize: fiveGig,
								UsageInodes: 3, QuotaInodes: 21, Mtime: expectedMtimeA,
							},
							{
								Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectC1,
								UsageSize: 40, QuotaSize: 400, UsageInodes: 1,
								QuotaInodes: 40, Mtime: expectedMtime,
							},
							{
								Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB123,
								UsageSize: 30, QuotaSize: 400, UsageInodes: 1,
								QuotaInodes: 40, Mtime: expectedMtime,
							},
							{
								Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB125,
								UsageSize: 20, QuotaSize: 300, UsageInodes: 1,
								QuotaInodes: 30, Mtime: expectedMtime,
							},
							{
								Name: ageGroupName, GID: 3, UIDs: []uint32{103}, Owner: "", BaseDir: projectA,
								UsageSize: 100, QuotaSize: 300, UsageInodes: 2,
								QuotaInodes: 30, Mtime: expectedFixedAgeMtime,
							},
							{
								Name: groupName, GID: gid, UIDs: []uint32{uid}, BaseDir: projectD,
								UsageSize: 10, QuotaSize: 0, UsageInodes: 4, QuotaInodes: 0, Mtime: expectedMtime,
								DateNoSpace: now, DateNoFiles: now,
							},
							{
								Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: user2,
								UsageSize: 60, QuotaSize: 500, UsageInodes: 1,
								QuotaInodes: 50, Mtime: expectedMtime,
							},
						}

						sort.Slice(mainTable, func(i, j int) bool {
							return bytes.Compare(
								idToByteSlice(mainTable[i].GID),
								idToByteSlice(mainTable[j].GID),
							) != -1
						})

						sort.Slice(mainTableExpectation, func(i, j int) bool {
							return bytes.Compare(
								idToByteSlice(mainTableExpectation[i].GID),
								idToByteSlice(mainTableExpectation[j].GID),
							) != -1
						})

						So(len(mainTable), ShouldEqual, 7)
						So(mainTable, ShouldResemble, mainTableExpectation)

						history, errr := bdr.History(1, projectA)
						fixHistoryTimes(history)

						So(errr, ShouldBeNil)
						So(len(history), ShouldEqual, 2)
						So(history, ShouldResemble, []basedirs.History{
							expectedAHistory,
							{
								Date:        now,
								UsageSize:   twoGig + halfGig*2,
								QuotaSize:   fiveGig,
								UsageInodes: 3,
								QuotaInodes: 21,
							},
						})

						expectedUntilSize := now.Add(time.Hour * 24 * 4).Unix()
						expectedUntilInode := now.Add(time.Hour * 24 * 18).Unix()

						var leewaySeconds int64 = 500

						dtrSize, dtrInode := basedirs.DateQuotaFull(history)
						So(dtrSize.Unix(), ShouldBeBetween, expectedUntilSize-leewaySeconds, expectedUntilSize+leewaySeconds)
						So(dtrInode.Unix(), ShouldBeBetween, expectedUntilInode-leewaySeconds, expectedUntilInode+leewaySeconds)
					})
				})

				expectedProjectASubDirs := []*basedirs.SubDir{
					{
						SubDir:    ".",
						NumFiles:  1,
						SizeFiles: halfGig,
						// actually expectedMtime, but we don't  have a way
						// of getting correct answer for "."
						LastModified: expectedMtimeA,
						FileUsage: map[db.DirGUTAFileType]uint64{
							db.DGUTAFileTypeBam: halfGig,
						},
					},
					{
						SubDir:       "sub",
						NumFiles:     1,
						SizeFiles:    twoGig,
						LastModified: expectedMtimeA,
						FileUsage: map[db.DirGUTAFileType]uint64{
							db.DGUTAFileTypeBam: twoGig,
						},
					},
				}

				Convey("getting subdir information for a group-basedir", func() {
					unknownProject, errr := bdr.GroupSubDirs(1, "unknown", db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(unknownProject, ShouldBeNil)

					unknownGroup, errr := bdr.GroupSubDirs(10, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(unknownGroup, ShouldBeNil)

					subdirsA1, errr := bdr.GroupSubDirs(1, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsA1)
					So(subdirsA1, ShouldResemble, expectedProjectASubDirs)

					subdirsA3, errr := bdr.GroupSubDirs(3, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsA3)
					So(subdirsA3, ShouldResemble, []*basedirs.SubDir{
						{
							SubDir:       ".",
							NumFiles:     2,
							SizeFiles:    100,
							LastModified: expectedFixedAgeMtime,
							FileUsage: map[db.DirGUTAFileType]uint64{
								db.DGUTAFileTypeBam: 100,
							},
						},
					})

					subdirsA3, errr = bdr.GroupSubDirs(3, projectA, db.DGUTAgeA3Y)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsA3)
					So(subdirsA3, ShouldResemble, []*basedirs.SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    40,
							LastModified: expectedFixedAgeMtime2,
							FileUsage: map[db.DirGUTAFileType]uint64{
								db.DGUTAFileTypeBam: 40,
							},
						},
					})
				})

				Convey("getting subdir information for a user-basedir", func() {
					unknownProject, errr := bdr.UserSubDirs(101, "unknown", db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(unknownProject, ShouldBeNil)

					unknownGroup, errr := bdr.UserSubDirs(999, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(unknownGroup, ShouldBeNil)

					subdirsA1, errr := bdr.UserSubDirs(101, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsA1)
					So(subdirsA1, ShouldResemble, expectedProjectASubDirs)

					subdirsB125, errr := bdr.UserSubDirs(102, projectB125, db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsB125)
					So(subdirsB125, ShouldResemble, []*basedirs.SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    20,
							LastModified: expectedMtime,
							FileUsage: basedirs.UsageBreakdownByType{
								db.DGUTAFileTypeBam: 20,
							},
						},
					})

					subdirsB123, errr := bdr.UserSubDirs(102, projectB123, db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsB123)
					So(subdirsB123, ShouldResemble, []*basedirs.SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    30,
							LastModified: expectedMtime,
							FileUsage: basedirs.UsageBreakdownByType{
								db.DGUTAFileTypeBam: 30,
							},
						},
					})

					subdirsD, errr := bdr.UserSubDirs(uid, projectD, db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsD)
					So(subdirsD, ShouldResemble, []*basedirs.SubDir{
						{
							SubDir:       "sub1",
							NumFiles:     3,
							SizeFiles:    6,
							LastModified: expectedMtime,
							FileUsage: basedirs.UsageBreakdownByType{
								db.DGUTAFileTypeTemp: 2,
								db.DGUTAFileTypeBam:  1,
								db.DGUTAFileTypeSam:  2,
								db.DGUTAFileTypeCram: 3,
							},
						},
						{
							SubDir:       "sub2",
							NumFiles:     2,
							SizeFiles:    9,
							LastModified: expectedMtime,
							FileUsage: basedirs.UsageBreakdownByType{
								db.DGUTAFileTypePedBed: 9,
							},
						},
					})

					subdirsA3, errr := bdr.UserSubDirs(103, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsA3)
					So(subdirsA3, ShouldResemble, []*basedirs.SubDir{
						{
							SubDir:       ".",
							NumFiles:     2,
							SizeFiles:    100,
							LastModified: expectedFixedAgeMtime,
							FileUsage: map[db.DirGUTAFileType]uint64{
								db.DGUTAFileTypeBam: 100,
							},
						},
					})

					subdirsA3, errr = bdr.UserSubDirs(103, projectA, db.DGUTAgeA3Y)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsA3)
					So(subdirsA3, ShouldResemble, []*basedirs.SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    40,
							LastModified: expectedFixedAgeMtime2,
							FileUsage: map[db.DirGUTAFileType]uint64{
								db.DGUTAFileTypeBam: 40,
							},
						},
					})
				})

				joinWithNewLines := func(rows ...string) string {
					return strings.Join(rows, "\n") + "\n"
				}

				joinWithTabs := func(cols ...string) string {
					return strings.Join(cols, "\t")
				}

				daysSince := func(mtime time.Time) uint64 {
					return uint64(time.Since(mtime) / (time.Hour * 24)) //nolint:gosec
				}

				daysSinceString := func(mtime time.Time) string {
					return strconv.FormatUint(daysSince(mtime), 10)
				}

				expectedDaysSince := daysSinceString(expectedMtime)
				expectedAgeDaysSince := daysSinceString(expectedFixedAgeMtime)

				Convey("getting weaver-like output for group base-dirs", func() {
					wbo, errr := bdr.GroupUsageTable(db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					groupsToID := make(map[string]uint32)

					for gid, name := range bdr.IterCachedGroups() {
						groupsToID[name] = gid
					}

					rowsData := [][]string{
						{
							"group1",
							"Alan",
							projectA,
							expectedDaysSince,
							"2684354560",
							"4000000000",
							"2",
							"20",
							basedirs.QuotaStatusOK,
						},
						{
							groupName,
							"",
							projectD,
							expectedDaysSince,
							"15",
							"0",
							"5",
							"0",
							basedirs.QuotaStatusNotOK,
						},
						{
							"group2",
							"Barbara",
							projectC1,
							expectedDaysSince,
							"40",
							"400",
							"1",
							"40",
							basedirs.QuotaStatusOK,
						},
						{
							"group2",
							"Barbara",
							projectB123,
							expectedDaysSince,
							"30",
							"400",
							"1",
							"40",
							basedirs.QuotaStatusOK,
						},
						{
							"group2",
							"Barbara",
							projectB125,
							expectedDaysSince,
							"20",
							"300",
							"1",
							"30",
							basedirs.QuotaStatusOK,
						},
						{
							ageGroupName,
							"",
							projectA,
							expectedAgeDaysSince,
							"100",
							"300",
							"2",
							"30",
							basedirs.QuotaStatusOK,
						},
						{
							"77777",
							"",
							user2,
							expectedDaysSince,
							"60",
							"500",
							"1",
							"50",
							basedirs.QuotaStatusOK,
						},
					}

					sort.Slice(rowsData, func(i, j int) bool {
						iIDbs := idToByteSlice(groupsToID[rowsData[i][0]])
						jIDbs := idToByteSlice(groupsToID[rowsData[j][0]])
						comparison := bytes.Compare(iIDbs, jIDbs)

						return comparison == -1
					})

					rows := make([]string, len(rowsData))
					for n, r := range rowsData {
						rows[n] = joinWithTabs(r...)
					}

					So(wbo, ShouldEqual, joinWithNewLines(rows...))
				})

				Convey("getting weaver-like output for user base-dirs", func() {
					wbo, errr := bdr.UserUsageTable(db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					groupsToID := make(map[string]uint32)

					for uid, name := range bdr.IterCachedUsers() {
						groupsToID[name] = uid
					}

					rowsData := [][]string{
						{
							ageUserName,
							"",
							projectA,
							expectedAgeDaysSince,
							"100",
							"0",
							"2",
							"0",
							basedirs.QuotaStatusOK,
						},
						{
							"user101",
							"",
							projectA,
							expectedDaysSince,
							"2684354560",
							"0",
							"2",
							"0",
							basedirs.QuotaStatusOK,
						},
						{
							"user102",
							"",
							projectB123,
							expectedDaysSince,
							"30",
							"0",
							"1",
							"0",
							basedirs.QuotaStatusOK,
						},
						{
							"user102",
							"",
							projectB125,
							expectedDaysSince,
							"20",
							"0",
							"1",
							"0",
							basedirs.QuotaStatusOK,
						},
						{
							"user102",
							"",
							user2,
							expectedDaysSince,
							"60",
							"0",
							"1",
							"0",
							basedirs.QuotaStatusOK,
						},
						{
							"88888",
							"",
							projectC1,
							expectedDaysSince,
							"40",
							"0",
							"1",
							"0",
							basedirs.QuotaStatusOK,
						},
						{
							username,
							"",
							projectD,
							expectedDaysSince,
							"15",
							"0",
							"5",
							"0",
							basedirs.QuotaStatusOK,
						},
					}

					sort.Slice(rowsData, func(i, j int) bool {
						iIDbs := idToByteSlice(groupsToID[rowsData[i][0]])
						jIDbs := idToByteSlice(groupsToID[rowsData[j][0]])
						comparison := bytes.Compare(iIDbs, jIDbs)

						return comparison == -1
					})

					rows := make([]string, len(rowsData))
					for n, r := range rowsData {
						rows[n] = joinWithTabs(r...)
					}

					So(wbo, ShouldEqual, joinWithNewLines(rows...))
				})

				expectedProjectASubDirUsage := joinWithNewLines(
					joinWithTabs(
						projectA,
						".",
						"1",
						"536870912",
						expectedDaysSince,
						"bam: 0.50",
					),
					joinWithTabs(
						projectA,
						"sub",
						"1",
						"2147483648",
						expectedDaysSince,
						"bam: 2.00",
					),
				)

				Convey("getting weaver-like output for group sub-dirs", func() {
					unknown, errr := bdr.GroupSubDirUsageTable(1, "unknown", db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(unknown, ShouldBeEmpty)

					badgroup, errr := bdr.GroupSubDirUsageTable(999, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(badgroup, ShouldBeEmpty)

					wso, errr := bdr.GroupSubDirUsageTable(1, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(wso, ShouldEqual, expectedProjectASubDirUsage)
				})

				Convey("getting weaver-like output for user sub-dirs", func() {
					unknown, errr := bdr.UserSubDirUsageTable(1, "unknown", db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(unknown, ShouldBeEmpty)

					badgroup, errr := bdr.UserSubDirUsageTable(999, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(badgroup, ShouldBeEmpty)

					wso, errr := bdr.UserSubDirUsageTable(101, projectA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(wso, ShouldEqual, expectedProjectASubDirUsage)
				})
			})

			Convey("and merge with another database", func() {
				_, root = internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid, "nfs", 1, 10, 11, true, refTime)
				oldPath := dbPath
				newPath := filepath.Join(dir, "newdir.db")
				oldMPs := mps
				mps = []string{
					"/nfs/scratch123/",
					"/nfs/scratch125/",
				}

				dbPath = newPath

				basedirsCreator(yesterday)

				bdr = baseDirsReader()
				dbPath = oldPath
				mps = oldMPs

				outputDBPath := filepath.Join(dir, "merged.db")

				err = basedirs.MergeDBs(oldPath, newPath, outputDBPath)
				So(err, ShouldBeNil)

				db, err := basedirs.OpenDBRO(outputDBPath)

				So(err, ShouldBeNil)

				defer db.Close()

				countKeys := func(bucket string) (int, int) {
					lustreKeys, nfsKeys := 0, 0

					db.View(func(tx *bolt.Tx) error { //nolint:errcheck
						bucket := tx.Bucket([]byte(bucket))

						return bucket.ForEach(func(k, _ []byte) error {
							if !basedirs.CheckAgeOfKeyIsAll(k) {
								return nil
							}

							if strings.Contains(string(k), "/lustre/") {
								lustreKeys++
							}

							if strings.Contains(string(k), "/nfs/") {
								nfsKeys++
							}

							return nil
						})
					})

					return lustreKeys, nfsKeys
				}

				expectedKeys := 7

				lustreKeys, nfsKeys := countKeys(basedirs.GroupUsageBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)

				lustreKeys, nfsKeys = countKeys(basedirs.GroupHistoricalBucket)
				So(lustreKeys, ShouldEqual, 6)
				So(nfsKeys, ShouldEqual, 6)

				lustreKeys, nfsKeys = countKeys(basedirs.GroupSubDirsBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)

				lustreKeys, nfsKeys = countKeys(basedirs.UserUsageBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)

				lustreKeys, nfsKeys = countKeys(basedirs.UserSubDirsBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)
			})

			Convey("and get basic info about it", func() {
				info, err := basedirs.Info(dbPath)
				So(err, ShouldBeNil)
				So(info, ShouldResemble, &basedirs.DBInfo{
					GroupDirCombos:    7,
					GroupMountCombos:  6,
					GroupHistories:    6,
					GroupSubDirCombos: 7,
					GroupSubDirs:      9,
					UserDirCombos:     7,
					UserSubDirCombos:  7,
					UserSubDirs:       9,
				})
			})
		})
	})
}

func TestCaches(t *testing.T) {
	Convey("Given a GroupCache, accessing it in multiple threads should be safe.", t, func() {
		var wg sync.WaitGroup

		g := basedirs.NewGroupCache()

		wg.Add(2)

		go func() {
			g.GroupName(0)
			wg.Done()
		}()

		go func() {
			g.GroupName(0)
			wg.Done()
		}()

		wg.Wait()
	})

	Convey("Given a UserCache, accessing it in multiple threads should be safe.", t, func() {
		var wg sync.WaitGroup

		u := basedirs.NewUserCache()

		wg.Add(2)

		go func() {
			u.UserName(0)
			wg.Done()
		}()

		go func() {
			u.UserName(0)
			wg.Done()
		}()

		wg.Wait()
	})
}

func fixUsageTimes(mt []*basedirs.Usage) {
	for _, u := range mt {
		u.Mtime = fixtimes.FixTime(u.Mtime)

		if !u.DateNoSpace.IsZero() {
			u.DateNoSpace = fixtimes.FixTime(u.DateNoSpace)
			u.DateNoFiles = fixtimes.FixTime(u.DateNoFiles)
		}
	}
}

func fixHistoryTimes(history []basedirs.History) {
	for n := range history {
		history[n].Date = fixtimes.FixTime(history[n].Date)
	}
}

func fixSubDirTimes(sds []*basedirs.SubDir) {
	for n := range sds {
		sds[n].LastModified = fixtimes.FixTime(sds[n].LastModified)
	}
}

func sortByDatabaseKeyOrder(usageTable []*basedirs.Usage) {
	if usageTable[0].UID != 0 {
		sortByUID(usageTable)

		return
	}

	sortByGID(usageTable)
}

func idToByteSlice(id uint32) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, id)

	return bs
}

func sortByGID(usageTable []*basedirs.Usage) {
	sort.Slice(usageTable, func(i, j int) bool {
		iID := idToByteSlice(usageTable[i].GID)
		jID := idToByteSlice(usageTable[j].GID)
		comparison := bytes.Compare(iID, jID)

		return comparison == -1
	})
}

func sortByUID(usageTable []*basedirs.Usage) {
	sort.Slice(usageTable, func(i, j int) bool {
		iID := idToByteSlice(usageTable[i].UID)
		jID := idToByteSlice(usageTable[j].UID)
		comparison := bytes.Compare(iID, jID)

		return comparison == -1
	})
}
