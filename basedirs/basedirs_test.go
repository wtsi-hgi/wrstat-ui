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

package basedirs

import (
	"bytes"
	"encoding/binary"
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
	"github.com/wtsi-hgi/wrstat-ui/dguta"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	internaldb "github.com/wtsi-hgi/wrstat-ui/internal/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/fixtimes"
	"github.com/wtsi-hgi/wrstat-ui/internal/fs"
	"github.com/wtsi-hgi/wrstat-ui/summary"
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

	defaultConfig := Config{
		{
			Prefix:  "/lustre/scratch123/hgi/mdt",
			Score:   4,
			Splits:  defaultSplits + 1,
			MinDirs: defaultMinDirs + 1,
		},
		{
			Prefix:  "/nfs/scratch123/hgi/mdt",
			Score:   4,
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
	expectedAgeAtime2 := time.Unix(refTime-summary.SecondsInAYear*3, 0)
	expectedAgeMtime := time.Unix(refTime-summary.SecondsInAYear*3, 0)
	expectedAgeMtime2 := time.Unix(refTime-summary.SecondsInAYear*5, 0)
	expectedFixedAgeMtime := fixtimes.FixTime(expectedAgeMtime)
	expectedFixedAgeMtime2 := fixtimes.FixTime(expectedAgeMtime2)

	Convey("Given a Tree and Quotas you can make a BaseDirs", t, func() {
		gid, uid, groupName, username, err := internaldata.RealGIDAndUID()
		So(err, ShouldBeNil)

		locDirs, files := internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid, refTime)

		const (
			halfGig = 1 << 29
			twoGig  = 1 << 31
		)

		files[0].SizeOfEachFile = halfGig
		files[1].SizeOfEachFile = twoGig

		yesterday := fixtimes.FixTime(time.Now().Add(-24 * time.Hour))
		tree, treePath, err := internaldb.CreateDGUTADBFromFakeFiles(t, files, yesterday)
		So(err, ShouldBeNil)

		projectA := locDirs[0]
		projectB125 := locDirs[1]
		projectB123 := locDirs[2]
		projectC1 := locDirs[3]
		user2 := locDirs[5]
		projectD := locDirs[6]

		quotas, err := ParseQuotas(csvPath)
		So(err, ShouldBeNil)

		dir := t.TempDir()
		dbPath := filepath.Join(dir, "basedir.db")

		dbModTime := fs.ModTime(treePath)

		bd, err := NewCreator(dbPath, defaultConfig, tree, quotas)
		So(err, ShouldBeNil)
		So(bd, ShouldNotBeNil)

		bd.mountPoints = mountPoints{
			"/lustre/scratch123/",
			"/lustre/scratch125/",
		}

		Convey("With which you can calculate base directories", func() {
			expectedAtime := time.Unix(50, 0)
			expectedMtime := time.Unix(50, 0)
			expectedMtimeA := time.Unix(100, 0)
			expectedFTsBam := []summary.DirGUTAFileType{summary.DGUTAFileTypeBam}

			expectedDCSsWithAges := func(dirSummaries []dguta.DirSummary) dguta.DCSs {
				var dcss dguta.DCSs

				for _, dirSummary := range dirSummaries {
					for _, age := range summary.DirGUTAges {
						ageDirSummary := dirSummary
						ageDirSummary.Age = age

						dcss = append(dcss, &ageDirSummary)
					}
				}

				return dcss
			}

			Convey("of each group", func() {
				Convey("with old files", func() { //nolint:dupl
					dcss, err := bd.calculateForGroup(1)
					So(err, ShouldBeNil)
					So(dcss, ShouldResemble, expectedDCSsWithAges(
						[]dguta.DirSummary{
							{
								Dir:     projectA,
								Count:   2,
								Size:    halfGig + twoGig,
								Atime:   expectedAtime,
								Mtime:   expectedMtimeA,
								GIDs:    []uint32{1},
								UIDs:    []uint32{101},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
							},
						}))

					dcss, err = bd.calculateForGroup(2)
					So(err, ShouldBeNil)
					So(dcss, ShouldResemble, expectedDCSsWithAges(
						[]dguta.DirSummary{
							{
								Dir:     projectC1,
								Count:   1,
								Size:    40,
								Atime:   expectedAtime,
								Mtime:   expectedMtime,
								GIDs:    []uint32{2},
								UIDs:    []uint32{88888},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
							},
							{
								Dir:     projectB123,
								Count:   1,
								Size:    30,
								Atime:   expectedAtime,
								Mtime:   expectedMtime,
								GIDs:    []uint32{2},
								UIDs:    []uint32{102},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
							},
							{
								Dir:     projectB125,
								Count:   1,
								Size:    20,
								Atime:   expectedAtime,
								Mtime:   expectedMtime,
								GIDs:    []uint32{2},
								UIDs:    []uint32{102},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
							},
						}),
					)
				})
				Convey("with newer files", func() {
					dcss, err := bd.calculateForGroup(3)
					So(err, ShouldBeNil)
					So(dcss, ShouldResemble,
						dguta.DCSs{
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeAll,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeA1M,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeA2M,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeA6M,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeA1Y,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeA2Y,
							},
							{
								Dir:     projectA,
								Count:   1,
								Size:    40,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime2,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeA3Y,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeM1M,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeM2M,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeM6M,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeM1Y,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeM2Y,
							},
							{
								Dir:     projectA,
								Count:   2,
								Size:    100,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeM3Y,
							},
							{
								Dir:     projectA,
								Count:   1,
								Size:    40,
								Atime:   expectedAgeAtime2,
								Mtime:   expectedAgeMtime2,
								GIDs:    []uint32{3},
								UIDs:    []uint32{103},
								FTs:     expectedFTsBam,
								Modtime: dbModTime,
								Age:     summary.DGUTAgeM5Y,
							},
						},
					)
				},
				)
			})

			Convey("of each user", func() { //nolint:dupl
				dcss, err := bd.calculateForUser(101)
				So(err, ShouldBeNil)
				So(dcss, ShouldResemble, expectedDCSsWithAges(
					[]dguta.DirSummary{
						{
							Dir:     projectA,
							Count:   2,
							Size:    halfGig + twoGig,
							Atime:   expectedAtime,
							Mtime:   expectedMtimeA,
							GIDs:    []uint32{1},
							UIDs:    []uint32{101},
							FTs:     expectedFTsBam,
							Modtime: dbModTime,
						},
					}),
				)

				dcss, err = bd.calculateForUser(102)
				So(err, ShouldBeNil)
				So(dcss, ShouldResemble, expectedDCSsWithAges(
					[]dguta.DirSummary{
						{
							Dir:     projectB123,
							Count:   1,
							Size:    30,
							Atime:   expectedAtime,
							Mtime:   expectedMtime,
							GIDs:    []uint32{2},
							UIDs:    []uint32{102},
							FTs:     expectedFTsBam,
							Modtime: dbModTime,
						},
						{
							Dir:     projectB125,
							Count:   1,
							Size:    20,
							Atime:   expectedAtime,
							Mtime:   expectedMtime,
							GIDs:    []uint32{2},
							UIDs:    []uint32{102},
							FTs:     expectedFTsBam,
							Modtime: dbModTime,
						},
						{
							Dir:     user2,
							Count:   1,
							Size:    60,
							Atime:   expectedAtime,
							Mtime:   expectedMtime,
							GIDs:    []uint32{77777},
							UIDs:    []uint32{102},
							FTs:     expectedFTsBam,
							Modtime: dbModTime,
						},
					}),
				)
			})
		})

		Convey("With which you can store group and user summary info in a database", func() {
			err := bd.CreateDatabase()
			So(err, ShouldBeNil)

			_, err = os.Stat(dbPath)
			So(err, ShouldBeNil)

			Convey("and then read the database", func() {
				ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
				So(err, ShouldBeNil)

				bdr, err := NewReader(dbPath, ownersPath)
				So(err, ShouldBeNil)

				bdr.mountPoints = bd.mountPoints

				groupCache := &GroupCache{
					data: map[uint32]string{
						1: "group1",
						2: "group2",
					},
				}
				bdr.groupCache = groupCache

				bdr.userCache = &UserCache{
					data: map[uint32]string{
						101: "user101",
						102: "user102",
					},
				}

				expectedMtime := fixtimes.FixTime(time.Unix(50, 0))
				expectedMtimeA := fixtimes.FixTime(time.Unix(100, 0))

				Convey("getting group and user usage info", func() {
					mainTable, err := bdr.GroupUsage(summary.DGUTAgeAll)
					fixUsageTimes(mainTable)

					expectedUsageTable := []*Usage{
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
							Name: groupName, GID: uint32(gid), UIDs: []uint32{uint32(uid)}, BaseDir: projectD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							DateNoSpace: yesterday, DateNoFiles: yesterday,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: user2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
						},
					}

					sortByDatabaseKeyOrder(expectedUsageTable)

					So(err, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 7)
					So(mainTable, ShouldResemble, expectedUsageTable)

					mainTable, err = bdr.GroupUsage(summary.DGUTAgeA3Y)
					fixUsageTimes(mainTable)

					expectedUsageTable = []*Usage{
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectA,
							UsageSize: halfGig + twoGig, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA, Age: summary.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: summary.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: summary.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
							Age: summary.DGUTAgeA3Y,
						},
						{
							Name: ageGroupName, GID: 3, UIDs: []uint32{103}, Owner: "", BaseDir: projectA,
							UsageSize: 40, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedFixedAgeMtime2,
							Age: summary.DGUTAgeA3Y,
						},
						{
							Name: groupName, GID: uint32(gid), UIDs: []uint32{uint32(uid)}, BaseDir: projectD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							Age: summary.DGUTAgeA3Y,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: user2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
							Age: summary.DGUTAgeA3Y,
						},
					}
					sortByDatabaseKeyOrder(expectedUsageTable)

					So(err, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 7)
					So(mainTable, ShouldResemble, expectedUsageTable)

					mainTable, err = bdr.GroupUsage(summary.DGUTAgeA7Y)
					fixUsageTimes(mainTable)

					expectedUsageTable = []*Usage{
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectA,
							UsageSize: halfGig + twoGig, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA, Age: summary.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: summary.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: summary.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
							Age: summary.DGUTAgeA7Y,
						},
						{
							Name: groupName, GID: uint32(gid), UIDs: []uint32{uint32(uid)}, BaseDir: projectD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							Age: summary.DGUTAgeA7Y,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: user2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
							Age: summary.DGUTAgeA7Y,
						},
					}
					sortByDatabaseKeyOrder(expectedUsageTable)

					So(err, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 6)
					So(mainTable, ShouldResemble, expectedUsageTable)

					mainTable, err = bdr.UserUsage(summary.DGUTAgeAll)
					fixUsageTimes(mainTable)

					expectedMainTable := []*Usage{
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
							Name: username, UID: uint32(uid), GIDs: []uint32{uint32(gid)}, BaseDir: projectD,
							UsageSize: 15, UsageInodes: 5, Mtime: expectedMtime,
						},
						{
							Name: ageUserName, UID: 103, GIDs: []uint32{3}, BaseDir: projectA, UsageSize: 100,
							UsageInodes: 2, Mtime: expectedFixedAgeMtime,
						},
					}

					sortByDatabaseKeyOrder(expectedMainTable)

					So(err, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 7)
					So(mainTable, ShouldResemble, expectedMainTable)
				})

				Convey("getting group historical quota", func() {
					expectedAHistory := History{
						Date:        yesterday,
						UsageSize:   halfGig + twoGig,
						QuotaSize:   4000000000,
						UsageInodes: 2,
						QuotaInodes: 20,
					}

					history, err := bdr.History(1, projectA)
					fixHistoryTimes(history)

					So(err, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []History{expectedAHistory})

					history, err = bdr.History(1, filepath.Join(projectA, "newsub"))
					fixHistoryTimes(history)

					So(err, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []History{expectedAHistory})

					history, err = bdr.History(2, projectB125)
					fixHistoryTimes(history)

					So(err, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []History{
						{
							Date:        yesterday,
							UsageSize:   20,
							QuotaSize:   300,
							UsageInodes: 1,
							QuotaInodes: 30,
						},
					})

					dtrSize, dtrInode := DateQuotaFull(history)
					So(dtrSize, ShouldEqual, time.Time{})
					So(dtrInode, ShouldEqual, time.Time{})

					err = bdr.Close()
					So(err, ShouldBeNil)

					Convey("then adding the same database twice doesn't duplicate history.", func() {
						// Add existing…
						bd, err = NewCreator(dbPath, defaultConfig, tree, quotas)
						So(err, ShouldBeNil)
						So(bd, ShouldNotBeNil)

						err = bd.CreateDatabase()
						So(err, ShouldBeNil)

						bdr, err = NewReader(dbPath, ownersPath)
						So(err, ShouldBeNil)

						history, err = bdr.History(1, projectA)
						fixHistoryTimes(history)
						So(err, ShouldBeNil)

						So(len(history), ShouldEqual, 1)

						err = bdr.Close()
						So(err, ShouldBeNil)

						// Add existing again…
						bd, err = NewCreator(dbPath, defaultConfig, tree, quotas)
						So(err, ShouldBeNil)
						So(bd, ShouldNotBeNil)

						err = bd.CreateDatabase()
						So(err, ShouldBeNil)

						bdr, err = NewReader(dbPath, ownersPath)
						So(err, ShouldBeNil)

						history, err = bdr.History(1, projectA)
						fixHistoryTimes(history)
						So(err, ShouldBeNil)

						So(len(history), ShouldEqual, 1)

						err = bdr.Close()
						So(err, ShouldBeNil)

						// Add new…
						err = fs.Touch(treePath, time.Now())
						So(err, ShouldBeNil)

						tree, err = dguta.NewTree(treePath)
						So(err, ShouldBeNil)

						bd, err = NewCreator(dbPath, defaultConfig, tree, quotas)
						So(err, ShouldBeNil)
						So(bd, ShouldNotBeNil)

						err = bd.CreateDatabase()
						So(err, ShouldBeNil)

						bdr, err = NewReader(dbPath, ownersPath)
						So(err, ShouldBeNil)

						history, err = bdr.History(1, projectA)
						fixHistoryTimes(history)
						So(err, ShouldBeNil)

						So(len(history), ShouldEqual, 2)

						err = bdr.Close()
						So(err, ShouldBeNil)
					})

					Convey("Then you can add and retrieve a new day's usage and quota", func() {
						_, files := internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid, refTime)
						files[0].NumFiles = 2
						files[0].SizeOfEachFile = halfGig
						files[1].SizeOfEachFile = twoGig

						files = files[:len(files)-1]
						today := fixtimes.FixTime(time.Now())
						tree, _, err = internaldb.CreateDGUTADBFromFakeFiles(t, files, today)
						So(err, ShouldBeNil)

						const fiveGig = 5 * (1 << 30)

						quotas.gids[1][0].quotaSize = fiveGig
						quotas.gids[1][0].quotaInode = 21

						mp := bd.mountPoints

						bd, err = NewCreator(dbPath, defaultConfig, tree, quotas)
						So(err, ShouldBeNil)
						So(bd, ShouldNotBeNil)

						bd.mountPoints = mp

						err := bd.CreateDatabase()
						So(err, ShouldBeNil)

						bdr, err = NewReader(dbPath, ownersPath)
						So(err, ShouldBeNil)

						bdr.mountPoints = bd.mountPoints
						bdr.groupCache = groupCache

						mainTable, err := bdr.GroupUsage(summary.DGUTAgeAll)
						So(err, ShouldBeNil)
						fixUsageTimes(mainTable)

						leeway := 5 * time.Minute

						dateNoSpace := today.Add(4 * 24 * time.Hour)
						So(mainTable[0].DateNoSpace, ShouldHappenOnOrBetween,
							dateNoSpace.Add(-leeway), dateNoSpace.Add(leeway))

						dateNoTime := today.Add(18 * 24 * time.Hour)
						So(mainTable[0].DateNoFiles, ShouldHappenOnOrBetween,
							dateNoTime.Add(-leeway), dateNoTime.Add(leeway))

						mainTable[0].DateNoSpace = time.Time{}
						mainTable[0].DateNoFiles = time.Time{}

						mainTableExpectation := []*Usage{
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
								Name: groupName, GID: uint32(gid), UIDs: []uint32{uint32(uid)}, BaseDir: projectD,
								UsageSize: 10, QuotaSize: 0, UsageInodes: 4, QuotaInodes: 0, Mtime: expectedMtime,
								DateNoSpace: today, DateNoFiles: today,
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

						history, err := bdr.History(1, projectA)
						fixHistoryTimes(history)

						So(err, ShouldBeNil)
						So(len(history), ShouldEqual, 2)
						So(history, ShouldResemble, []History{
							expectedAHistory,
							{
								Date:        today,
								UsageSize:   twoGig + halfGig*2,
								QuotaSize:   fiveGig,
								UsageInodes: 3,
								QuotaInodes: 21,
							},
						})

						expectedUntilSize := today.Add(secondsInDay * 4).Unix()
						expectedUntilInode := today.Add(secondsInDay * 18).Unix()

						var leewaySeconds int64 = 500

						dtrSize, dtrInode := DateQuotaFull(history)
						So(dtrSize.Unix(), ShouldBeBetween, expectedUntilSize-leewaySeconds, expectedUntilSize+leewaySeconds)
						So(dtrInode.Unix(), ShouldBeBetween, expectedUntilInode-leewaySeconds, expectedUntilInode+leewaySeconds)
					})
				})

				expectedProjectASubDirs := []*SubDir{
					{
						SubDir:    ".",
						NumFiles:  1,
						SizeFiles: halfGig,
						// actually expectedMtime, but we don't  have a way
						// of getting correct answer for "."
						LastModified: expectedMtimeA,
						FileUsage: map[summary.DirGUTAFileType]uint64{
							summary.DGUTAFileTypeBam: halfGig,
						},
					},
					{
						SubDir:       "sub",
						NumFiles:     1,
						SizeFiles:    twoGig,
						LastModified: expectedMtimeA,
						FileUsage: map[summary.DirGUTAFileType]uint64{
							summary.DGUTAFileTypeBam: twoGig,
						},
					},
				}

				Convey("getting subdir information for a group-basedir", func() {
					unknownProject, err := bdr.GroupSubDirs(1, "unknown", summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(unknownProject, ShouldBeNil)

					unknownGroup, err := bdr.GroupSubDirs(10, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(unknownGroup, ShouldBeNil)

					subdirsA1, err := bdr.GroupSubDirs(1, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsA1)
					So(subdirsA1, ShouldResemble, expectedProjectASubDirs)

					subdirsA3, err := bdr.GroupSubDirs(3, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsA3)
					So(subdirsA3, ShouldResemble, []*SubDir{
						{
							SubDir:       ".",
							NumFiles:     2,
							SizeFiles:    100,
							LastModified: expectedFixedAgeMtime,
							FileUsage: map[summary.DirGUTAFileType]uint64{
								summary.DGUTAFileTypeBam: 100,
							},
						},
					})

					subdirsA3, err = bdr.GroupSubDirs(3, projectA, summary.DGUTAgeA3Y)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsA3)
					So(subdirsA3, ShouldResemble, []*SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    40,
							LastModified: expectedFixedAgeMtime2,
							FileUsage: map[summary.DirGUTAFileType]uint64{
								summary.DGUTAFileTypeBam: 40,
							},
						},
					})
				})

				Convey("getting subdir information for a user-basedir", func() {
					unknownProject, err := bdr.UserSubDirs(101, "unknown", summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(unknownProject, ShouldBeNil)

					unknownGroup, err := bdr.UserSubDirs(999, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(unknownGroup, ShouldBeNil)

					subdirsA1, err := bdr.UserSubDirs(101, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsA1)
					So(subdirsA1, ShouldResemble, expectedProjectASubDirs)

					subdirsB125, err := bdr.UserSubDirs(102, projectB125, summary.DGUTAgeAll)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsB125)
					So(subdirsB125, ShouldResemble, []*SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    20,
							LastModified: expectedMtime,
							FileUsage: UsageBreakdownByType{
								summary.DGUTAFileTypeBam: 20,
							},
						},
					})

					subdirsB123, err := bdr.UserSubDirs(102, projectB123, summary.DGUTAgeAll)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsB123)
					So(subdirsB123, ShouldResemble, []*SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    30,
							LastModified: expectedMtime,
							FileUsage: UsageBreakdownByType{
								summary.DGUTAFileTypeBam: 30,
							},
						},
					})

					subdirsD, err := bdr.UserSubDirs(uint32(uid), projectD, summary.DGUTAgeAll)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsD)
					So(subdirsD, ShouldResemble, []*SubDir{
						{
							SubDir:       "sub1",
							NumFiles:     3,
							SizeFiles:    6,
							LastModified: expectedMtime,
							FileUsage: UsageBreakdownByType{
								summary.DGUTAFileTypeTemp: 1026,
								summary.DGUTAFileTypeBam:  1,
								summary.DGUTAFileTypeSam:  2,
								summary.DGUTAFileTypeCram: 3,
							},
						},
						{
							SubDir:       "sub2",
							NumFiles:     2,
							SizeFiles:    9,
							LastModified: expectedMtime,
							FileUsage: UsageBreakdownByType{
								summary.DGUTAFileTypePedBed: 9,
							},
						},
					})

					subdirsA3, err := bdr.UserSubDirs(103, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsA3)
					So(subdirsA3, ShouldResemble, []*SubDir{
						{
							SubDir:       ".",
							NumFiles:     2,
							SizeFiles:    100,
							LastModified: expectedFixedAgeMtime,
							FileUsage: map[summary.DirGUTAFileType]uint64{
								summary.DGUTAFileTypeBam: 100,
							},
						},
					})

					subdirsA3, err = bdr.UserSubDirs(103, projectA, summary.DGUTAgeA3Y)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsA3)
					So(subdirsA3, ShouldResemble, []*SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    40,
							LastModified: expectedFixedAgeMtime2,
							FileUsage: map[summary.DirGUTAFileType]uint64{
								summary.DGUTAFileTypeBam: 40,
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

				daysSinceString := func(mtime time.Time) string {
					return strconv.FormatUint(daysSince(mtime), 10)
				}

				expectedDaysSince := daysSinceString(expectedMtime)
				expectedAgeDaysSince := daysSinceString(expectedFixedAgeMtime)

				Convey("getting weaver-like output for group base-dirs", func() {
					wbo, err := bdr.GroupUsageTable(summary.DGUTAgeAll)
					So(err, ShouldBeNil)

					groupsToID := make(map[string]uint32, len(bdr.groupCache.data))

					for gid, name := range bdr.groupCache.data {
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
							quotaStatusOK,
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
							quotaStatusNotOK,
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
							quotaStatusOK,
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
							quotaStatusOK,
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
							quotaStatusOK,
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
							quotaStatusOK,
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
							quotaStatusOK,
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
					wbo, err := bdr.UserUsageTable(summary.DGUTAgeAll)
					So(err, ShouldBeNil)

					groupsToID := make(map[string]uint32, len(bdr.userCache.data))

					for uid, name := range bdr.userCache.data {
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
							quotaStatusOK,
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
							quotaStatusOK,
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
							quotaStatusOK,
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
							quotaStatusOK,
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
							quotaStatusOK,
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
							quotaStatusOK,
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
							quotaStatusOK,
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
					unknown, err := bdr.GroupSubDirUsageTable(1, "unknown", summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(unknown, ShouldBeEmpty)

					badgroup, err := bdr.GroupSubDirUsageTable(999, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(badgroup, ShouldBeEmpty)

					wso, err := bdr.GroupSubDirUsageTable(1, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(wso, ShouldEqual, expectedProjectASubDirUsage)
				})

				Convey("getting weaver-like output for user sub-dirs", func() {
					unknown, err := bdr.UserSubDirUsageTable(1, "unknown", summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(unknown, ShouldBeEmpty)

					badgroup, err := bdr.UserSubDirUsageTable(999, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(badgroup, ShouldBeEmpty)

					wso, err := bdr.UserSubDirUsageTable(101, projectA, summary.DGUTAgeAll)
					So(err, ShouldBeNil)
					So(wso, ShouldEqual, expectedProjectASubDirUsage)
				})
			})

			Convey("and merge with another database", func() {
				_, newFiles := internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid, refTime)
				for i := range newFiles {
					newFiles[i].Path = "/nfs" + newFiles[i].Path[7:]
				}

				newTree, _, err := internaldb.CreateDGUTADBFromFakeFiles(t, newFiles, yesterday)
				So(err, ShouldBeNil)

				newDBPath := filepath.Join(dir, "newdir.db")

				newBd, err := NewCreator(newDBPath, defaultConfig, newTree, quotas)
				So(err, ShouldBeNil)
				So(bd, ShouldNotBeNil)

				newBd.mountPoints = mountPoints{
					"/nfs/scratch123/",
					"/nfs/scratch125/",
				}

				err = newBd.CreateDatabase()
				So(err, ShouldBeNil)

				outputDBPath := filepath.Join(dir, "merged.db")

				err = MergeDBs(dbPath, newDBPath, outputDBPath)
				So(err, ShouldBeNil)

				db, err := openDBRO(outputDBPath)

				So(err, ShouldBeNil)
				defer db.Close()

				countKeys := func(bucket string) (int, int) {
					lustreKeys, nfsKeys := 0, 0

					db.View(func(tx *bolt.Tx) error { //nolint:errcheck
						bucket := tx.Bucket([]byte(bucket))

						return bucket.ForEach(func(k, _ []byte) error {
							if !checkAgeOfKeyIsAll(k) {
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

				lustreKeys, nfsKeys := countKeys(groupUsageBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)

				lustreKeys, nfsKeys = countKeys(groupHistoricalBucket)
				So(lustreKeys, ShouldEqual, 6)
				So(nfsKeys, ShouldEqual, 6)

				lustreKeys, nfsKeys = countKeys(groupSubDirsBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)

				lustreKeys, nfsKeys = countKeys(userUsageBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)

				lustreKeys, nfsKeys = countKeys(userSubDirsBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)
			})

			Convey("and get basic info about it", func() {
				info, err := Info(dbPath)
				So(err, ShouldBeNil)
				So(info, ShouldResemble, &DBInfo{
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

func TestOwners(t *testing.T) {
	Convey("Given an owners tsv you can parse it", t, func() {
		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		owners, err := parseOwners(ownersPath)
		So(err, ShouldBeNil)
		So(owners, ShouldResemble, map[uint32]string{
			1: "Alan",
			2: "Barbara",
			4: "Dellilah",
		})
	})
}

func TestCaches(t *testing.T) {
	Convey("Given a GroupCache, accessing it in multiple threads should be safe.", t, func() {
		var wg sync.WaitGroup

		g := NewGroupCache()

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

		u := NewUserCache()

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

func fixUsageTimes(mt []*Usage) {
	for _, u := range mt {
		u.Mtime = fixtimes.FixTime(u.Mtime)

		if !u.DateNoSpace.IsZero() {
			u.DateNoSpace = fixtimes.FixTime(u.DateNoSpace)
			u.DateNoFiles = fixtimes.FixTime(u.DateNoFiles)
		}
	}
}

func fixHistoryTimes(history []History) {
	for n := range history {
		history[n].Date = fixtimes.FixTime(history[n].Date)
	}
}

func fixSubDirTimes(sds []*SubDir) {
	for n := range sds {
		sds[n].LastModified = fixtimes.FixTime(sds[n].LastModified)
	}
}

func sortByDatabaseKeyOrder(usageTable []*Usage) {
	if usageTable[0].UID != 0 {
		sortByUID(usageTable)

		return
	}

	sortByGID(usageTable)
}

func idToByteSlice(id uint32) []byte {
	bs := make([]byte, sizeOfUint32)
	binary.LittleEndian.PutUint32(bs, id)

	return bs
}

func sortByGID(usageTable []*Usage) {
	sort.Slice(usageTable, func(i, j int) bool {
		iID := idToByteSlice(usageTable[i].GID)
		jID := idToByteSlice(usageTable[j].GID)
		comparison := bytes.Compare(iID, jID)

		return comparison == -1
	})
}

func sortByUID(usageTable []*Usage) {
	sort.Slice(usageTable, func(i, j int) bool {
		iID := idToByteSlice(usageTable[i].UID)
		jID := idToByteSlice(usageTable[j].UID)
		comparison := bytes.Compare(iID, jID)

		return comparison == -1
	})
}
