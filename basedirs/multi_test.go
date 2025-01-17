/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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
	"os/user"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/fixtimes"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
)

func TestMulti(t *testing.T) {
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
		"/nfs/scratch123/",
		"/nfs/scratch125/",
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

		locDirsA, rootA := internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid,
			"lustre", 1, halfGig, twoGig, true, refTime)

		locDirsB, rootB := internaldata.FakeFilesForDGUTADBForBasedirsTesting(gid, uid,
			"nfs", 1, 10, 11, false, refTime)

		now := fixtimes.FixTime(time.Now())
		yesterday := fixtimes.FixTime(now.Add(-24 * time.Hour))

		projectAA := locDirsA[0]
		projectAB125 := locDirsA[1]
		projectAB123 := locDirsA[2]
		projectAC1 := locDirsA[3]
		userA2 := locDirsA[5]
		projectAD := locDirsA[6]

		projectBA := locDirsB[0]
		projectBB125 := locDirsB[1]
		projectBB123 := locDirsB[2]
		projectBC1 := locDirsB[3]
		userB2 := locDirsB[5]
		projectBD := locDirsB[6]

		quotas, err := basedirs.ParseQuotas(csvPath)
		So(err, ShouldBeNil)

		dir := t.TempDir()
		dbPathA := filepath.Join(dir, "basedirA.db")
		dbPathB := filepath.Join(dir, "basedirB.db")

		baseDirCreator := func(modtime time.Time, root *statsdata.Directory, dbPath string) {
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

		basedirsCreator := func(modtime time.Time) {
			baseDirCreator(modtime, rootA, dbPathA)
			baseDirCreator(modtime, rootB, dbPathB)
		}

		basedirsCreator(yesterday)

		_ = expectedAgeAtime2

		Convey("With which you can store group and user summary info in a database", func() {
			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			baseDirsReader := func() basedirs.MultiReader {
				t.Helper()

				bdr, errr := basedirs.OpenMulti(ownersPath, dbPathA, dbPathB)
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

					expectedUsageTable := sortByDatabaseKeyOrder([]*basedirs.Usage{ //nolint:dupl
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectAA,
							UsageSize: halfGig + twoGig, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectAC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectAB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectAB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
						},
						{
							Name: ageGroupName, GID: 3, UIDs: []uint32{103}, Owner: "", BaseDir: projectAA,
							UsageSize: 100, QuotaSize: 300, UsageInodes: 2, QuotaInodes: 30, Mtime: expectedFixedAgeMtime,
						},
						{
							Name: groupName, GID: gid, UIDs: []uint32{uid}, BaseDir: projectAD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							DateNoSpace: yesterday, DateNoFiles: yesterday,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: userA2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
						},
					})

					expectedUsageTable = append(expectedUsageTable, sortByDatabaseKeyOrder([]*basedirs.Usage{
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectBA,
							UsageSize: 21, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectBC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectBB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectBB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
						},
						{
							Name: ageGroupName, GID: 3, UIDs: []uint32{103}, Owner: "", BaseDir: projectBA,
							UsageSize: 100, QuotaSize: 0, UsageInodes: 2, QuotaInodes: 0, Mtime: expectedFixedAgeMtime,
							DateNoSpace: yesterday, DateNoFiles: yesterday,
						},
						{
							Name: groupName, GID: gid, UIDs: []uint32{uid}, BaseDir: projectBD,
							UsageSize: 10, QuotaSize: 0, UsageInodes: 4, QuotaInodes: 0, Mtime: expectedMtime,
							DateNoSpace: yesterday, DateNoFiles: yesterday,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: userB2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
						},
					})...)

					So(errr, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 14)
					So(mainTable, ShouldResemble, expectedUsageTable)

					mainTable, errr = bdr.GroupUsage(db.DGUTAgeA3Y)
					fixUsageTimes(mainTable)

					expectedUsageTable = sortByDatabaseKeyOrder([]*basedirs.Usage{ //nolint:dupl
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectAA,
							UsageSize: halfGig + twoGig, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA, Age: db.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectAC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectAB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectAB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: ageGroupName, GID: 3, UIDs: []uint32{103}, Owner: "", BaseDir: projectAA,
							UsageSize: 40, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedFixedAgeMtime2,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: groupName, GID: gid, UIDs: []uint32{uid}, BaseDir: projectAD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: userA2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
					})

					expectedUsageTable = append(expectedUsageTable, sortByDatabaseKeyOrder([]*basedirs.Usage{
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectBA,
							UsageSize: 21, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA, Age: db.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectBC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectBB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectBB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: ageGroupName, GID: 3, UIDs: []uint32{103}, Owner: "", BaseDir: projectBA,
							UsageSize: 40, QuotaSize: 0, UsageInodes: 1, QuotaInodes: 0, Mtime: expectedFixedAgeMtime2,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: groupName, GID: gid, UIDs: []uint32{uid}, BaseDir: projectBD,
							UsageSize: 10, QuotaSize: 0, UsageInodes: 4, QuotaInodes: 0, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: userB2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
							Age: db.DGUTAgeA3Y,
						},
					})...)

					So(errr, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 14)
					So(mainTable, ShouldResemble, expectedUsageTable)

					mainTable, errr = bdr.GroupUsage(db.DGUTAgeA7Y)
					fixUsageTimes(mainTable)

					expectedUsageTable = sortByDatabaseKeyOrder([]*basedirs.Usage{ //nolint:dupl
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectAA,
							UsageSize: halfGig + twoGig, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA, Age: db.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectAC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectAB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectAB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: groupName, GID: gid, UIDs: []uint32{uid}, BaseDir: projectAD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: userA2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
					})

					expectedUsageTable = append(expectedUsageTable, sortByDatabaseKeyOrder([]*basedirs.Usage{
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectBA,
							UsageSize: 21, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA, Age: db.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectBC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectBB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectBB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: groupName, GID: gid, UIDs: []uint32{uid}, BaseDir: projectBD,
							UsageSize: 10, QuotaSize: 0, UsageInodes: 4, QuotaInodes: 0, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: userB2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
							Age: db.DGUTAgeA7Y,
						},
					})...)

					So(errr, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 12)
					So(mainTable, ShouldResemble, expectedUsageTable)

					mainTable, errr = bdr.UserUsage(db.DGUTAgeAll)
					fixUsageTimes(mainTable)

					expectedMainTable := sortByDatabaseKeyOrder([]*basedirs.Usage{ //nolint:dupl
						{
							Name: "88888", UID: 88888, GIDs: []uint32{2}, BaseDir: projectAC1, UsageSize: 40,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user101", UID: 101, GIDs: []uint32{1}, BaseDir: projectAA,
							UsageSize: halfGig + twoGig, UsageInodes: 2, Mtime: expectedMtimeA,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{2}, BaseDir: projectAB123, UsageSize: 30,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{2}, BaseDir: projectAB125, UsageSize: 20,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{77777}, BaseDir: userA2, UsageSize: 60,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: username, UID: uid, GIDs: []uint32{gid}, BaseDir: projectAD,
							UsageSize: 15, UsageInodes: 5, Mtime: expectedMtime,
						},
						{
							Name: ageUserName, UID: 103, GIDs: []uint32{3}, BaseDir: projectAA, UsageSize: 100,
							UsageInodes: 2, Mtime: expectedFixedAgeMtime,
						},
					})

					expectedMainTable = append(expectedMainTable, sortByDatabaseKeyOrder([]*basedirs.Usage{
						{
							Name: "88888", UID: 88888, GIDs: []uint32{2}, BaseDir: projectBC1, UsageSize: 40,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user101", UID: 101, GIDs: []uint32{1}, BaseDir: projectBA,
							UsageSize: 21, UsageInodes: 2, Mtime: expectedMtimeA,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{2}, BaseDir: projectBB123, UsageSize: 30,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{2}, BaseDir: projectBB125, UsageSize: 20,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{77777}, BaseDir: userB2, UsageSize: 60,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: username, UID: uid, GIDs: []uint32{gid}, BaseDir: projectBD,
							UsageSize: 10, UsageInodes: 4, Mtime: expectedMtime,
						},
						{
							Name: ageUserName, UID: 103, GIDs: []uint32{3}, BaseDir: projectBA, UsageSize: 100,
							UsageInodes: 2, Mtime: expectedFixedAgeMtime,
						},
					})...)

					So(errr, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 14)
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

					history, errr := bdr.History(1, projectAA)
					fixHistoryTimes(history)

					So(errr, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []basedirs.History{expectedAHistory})

					history, errr = bdr.History(1, filepath.Join(projectAA, "newsub"))
					fixHistoryTimes(history)

					So(errr, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []basedirs.History{expectedAHistory})

					history, errr = bdr.History(2, projectAB125)
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
				})

				expectedProjectBASubDirs := []*basedirs.SubDir{
					{
						SubDir:    ".",
						NumFiles:  1,
						SizeFiles: 10,
						// actually expectedMtime, but we don't  have a way
						// of getting correct answer for "."
						LastModified: expectedMtimeA,
						FileUsage: map[db.DirGUTAFileType]uint64{
							db.DGUTAFileTypeBam: 10,
						},
					},
					{
						SubDir:       "sub",
						NumFiles:     1,
						SizeFiles:    11,
						LastModified: expectedMtimeA,
						FileUsage: map[db.DirGUTAFileType]uint64{
							db.DGUTAFileTypeBam: 11,
						},
					},
				}

				Convey("getting subdir information for a group-basedir", func() { //nolint:dupl
					unknownProject, errr := bdr.GroupSubDirs(1, "unknown", db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(unknownProject, ShouldBeNil)

					unknownGroup, errr := bdr.GroupSubDirs(10, projectAA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(unknownGroup, ShouldBeNil)

					subdirsA1, errr := bdr.GroupSubDirs(1, projectBA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsA1)
					So(subdirsA1, ShouldResemble, expectedProjectBASubDirs)

					subdirsA3, errr := bdr.GroupSubDirs(3, projectAA, db.DGUTAgeAll)
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

					subdirsA3, errr = bdr.GroupSubDirs(3, projectAA, db.DGUTAgeA3Y)
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

					unknownGroup, errr := bdr.UserSubDirs(999, projectAA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)
					So(unknownGroup, ShouldBeNil)

					subdirsA1, errr := bdr.UserSubDirs(101, projectBA, db.DGUTAgeAll)
					So(errr, ShouldBeNil)

					fixSubDirTimes(subdirsA1)
					So(subdirsA1, ShouldResemble, expectedProjectBASubDirs)

					subdirsB125, errr := bdr.UserSubDirs(102, projectAB125, db.DGUTAgeAll)
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

					subdirsB123, errr := bdr.UserSubDirs(102, projectAB123, db.DGUTAgeAll)
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

					subdirsD, errr := bdr.UserSubDirs(uid, projectAD, db.DGUTAgeAll)
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

					subdirsA3, errr := bdr.UserSubDirs(103, projectAA, db.DGUTAgeAll)
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

					subdirsA3, errr = bdr.UserSubDirs(103, projectAA, db.DGUTAgeA3Y)
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
			})
		})
	})
}
