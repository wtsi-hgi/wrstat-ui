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

package server

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-ssg/wrstat/v4/basedirs"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	internaldata "github.com/wtsi-ssg/wrstat/v4/internal/data"
	internaldb "github.com/wtsi-ssg/wrstat/v4/internal/db"
	"github.com/wtsi-ssg/wrstat/v4/internal/fixtimes"
	ifs "github.com/wtsi-ssg/wrstat/v4/internal/fs"
)

const dirPerms = 0755
const exampleDgutDirParentSuffix = "dgut.dbs"

func TestIDsToWanted(t *testing.T) {
	Convey("restrictIDsToWanted returns bad query if you don't want any of the given ids", t, func() {
		_, err := restrictIDsToWanted([]string{"a"}, map[string]bool{"b": true})
		So(err, ShouldNotBeNil)
	})
}

func TestServer(t *testing.T) {
	username, uid, gids := internaldb.GetUserAndGroups(t)
	exampleGIDs := getExampleGIDs(gids)
	sentinelPollFrequency := 10 * time.Millisecond

	Convey("Given a Server", t, func() {
		logWriter := gas.NewStringLogger()
		s := New(logWriter)

		Convey("You can convert dgut.DCSs to DirSummarys", func() {
			uid32, err := strconv.Atoi(uid)
			So(err, ShouldBeNil)
			gid32, err := strconv.Atoi(gids[0])
			So(err, ShouldBeNil)

			dcss := dgut.DCSs{
				{
					Dir:   "/foo",
					Count: 1,
					Size:  2,
					UIDs:  []uint32{uint32(uid32), 9999999},
					GIDs:  []uint32{uint32(gid32), 9999999},
				},
				{
					Dir:   "/bar",
					Count: 1,
					Size:  2,
					UIDs:  []uint32{uint32(uid32), 9999999},
					GIDs:  []uint32{uint32(gid32), 9999999},
				},
			}

			dss := s.dcssToSummaries(dcss)

			So(len(dss), ShouldEqual, 2)
			So(dss[0].Dir, ShouldEqual, "/foo")
			So(dss[0].Count, ShouldEqual, 1)
			So(dss[0].Size, ShouldEqual, 2)
			So(dss[0].Users, ShouldResemble, []string{username})
			So(dss[0].Groups, ShouldResemble, []string{gidToGroup(t, gids[0])})
		})

		Convey("userGIDs fails with bad UIDs", func() {
			u := &gas.User{
				Username: username,
				UID:      "-1",
			}

			_, err := s.userGIDs(u)
			So(err, ShouldNotBeNil)
		})

		Convey("You can Start the Server", func() {
			certPath, keyPath, err := gas.CreateTestCert(t)
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, certPath, keyPath)
			So(err, ShouldBeNil)
			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

			client := resty.New()
			client.SetRootCertificate(certPath)

			Convey("The jwt endpoint works after enabling it", func() {
				err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, string) {
					returnUID := uid

					if u == "user" {
						returnUID = "-1"
					}

					return true, returnUID
				})
				So(err, ShouldBeNil)

				token, errl := gas.Login(addr, certPath, username, "pass")
				So(errl, ShouldBeNil)
				r := gas.NewAuthenticatedClientRequest(addr, certPath, token)

				tokenBadUID, errl := gas.Login(addr, certPath, "user", "pass")
				So(errl, ShouldBeNil)
				So(tokenBadUID, ShouldNotBeBlank)

				s.AuthRouter().GET("/test", func(c *gin.Context) {})

				rBadUID := gas.NewAuthenticatedClientRequest(addr, certPath, tokenBadUID)
				resp, err := r.Get(gas.EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				testRestrictedGroups(t, gids, s, r, rBadUID, exampleGIDs)
			})

			testClientsOnRealServer(t, username, uid, gids, s, addr, certPath, keyPath)
		})

		if len(gids) < 2 {
			SkipConvey("Can't test the where endpoint without you belonging to at least 2 groups", func() {})

			return
		}

		Convey("convertSplitsValue works", func() {
			n := convertSplitsValue("1")
			So(n, ShouldEqual, 1)

			n = convertSplitsValue("foo")
			So(n, ShouldEqual, 2)
		})

		Convey("You can query the where endpoint", func() {
			response, err := queryWhere(s, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusNotFound)
			So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/where")
			So(logWriter.String(), ShouldContainSubstring, "STATUS=404")
			logWriter.Reset()

			Convey("And given a dgut database", func() {
				path, err := internaldb.CreateExampleDGUTDBCustomIDs(t, uid, gids[0], gids[1])
				So(err, ShouldBeNil)
				groupA := gidToGroup(t, gids[0])
				groupB := gidToGroup(t, gids[1])

				tree, err := dgut.NewTree(path)
				So(err, ShouldBeNil)

				expectedRaw, err := tree.Where("/", nil, 2)
				So(err, ShouldBeNil)

				expected := s.dcssToSummaries(expectedRaw)

				fixDirSummaryTimes(expected)

				expectedNonRoot, expectedGroupsRoot := adjustedExpectations(expected, groupA, groupB)

				expectedNoTemp := removeTempFromDSs(expected)

				tree.Close()

				Convey("You can get results after calling LoadDGUTDB", func() {
					err = s.LoadDGUTDBs(path)
					So(err, ShouldBeNil)

					response, err := queryWhere(s, "")
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/where")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					result, err := decodeWhereResult(response)
					So(err, ShouldBeNil)
					So(result, ShouldResemble, expected)

					Convey("And you can filter results", func() {
						groups := gidsToGroups(t, gids...)

						expectedUsers := expectedNonRoot[0].Users
						sort.Strings(expectedUsers)
						expectedUser := []string{username}
						expectedRoot := []string{"root"}
						expectedGroupsA := []string{groupA}
						expectedGroupsB := []string{groupB}
						expectedFTs := expectedNonRoot[0].FileTypes
						expectedBams := []string{"bam", "temp"}
						expectedCrams := []string{"cram"}
						expectedAtime := time.Unix(50, 0)

						matrix := []*matrixElement{
							{"?groups=" + groups[0] + "," + groups[1], expectedNonRoot},
							{"?groups=" + groups[0], []*DirSummary{
								{Dir: "/a/b", Count: 13, Size: 120, Atime: expectedAtime,
									Mtime: time.Unix(80, 0), Users: expectedUsers,
									Groups: expectedGroupsA, FileTypes: expectedFTs},
								{Dir: "/a/b/d", Count: 11, Size: 110, Atime: expectedAtime,
									Mtime: time.Unix(75, 0), Users: expectedUsers,
									Groups: expectedGroupsA, FileTypes: expectedCrams},
								{Dir: "/a/b/d/g", Count: 10, Size: 100, Atime: time.Unix(60, 0),
									Mtime: time.Unix(75, 0), Users: expectedUsers,
									Groups: expectedGroupsA, FileTypes: expectedCrams},
								{Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams},
								{Dir: "/a/b/d/f", Count: 1, Size: 10, Atime: expectedAtime,
									Mtime: time.Unix(50, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedCrams},
								{Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams},
							}},
							{"?users=root," + username, expected},
							{"?users=root", []*DirSummary{
								{Dir: "/a", Count: 14, Size: 86, Atime: expectedAtime,
									Mtime: time.Unix(90, 0), Users: expectedRoot,
									Groups: expectedGroupsRoot, FileTypes: expectedCrams},
								{Dir: "/a/b/d/g", Count: 8, Size: 80, Atime: time.Unix(75, 0),
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsA, FileTypes: expectedCrams},
								{Dir: "/a/c/d", Count: 5, Size: 5, Atime: time.Unix(90, 0),
									Mtime: time.Unix(90, 0), Users: expectedRoot,
									Groups: expectedGroupsB, FileTypes: expectedCrams},
							}},
							{"?groups=" + groups[0] + "&users=root", []*DirSummary{
								{Dir: "/a/b/d/g", Count: 8, Size: 80, Atime: time.Unix(75, 0),
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsA, FileTypes: expectedCrams},
							}},
							{"?types=cram,bam", expectedNoTemp},
							{"?types=bam", []*DirSummary{
								{Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: []string{"bam"}},
								{Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: []string{"bam"}},
							}},
							{"?groups=" + groups[0] + "&users=root&types=cram,bam", []*DirSummary{
								{Dir: "/a/b/d/g", Count: 8, Size: 80, Atime: time.Unix(75, 0),
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsA, FileTypes: expectedCrams},
							}},
							{"?groups=" + groups[0] + "&users=root&types=bam", []*DirSummary{
								{Dir: "/", Count: 0, Size: 0, Atime: time.Unix(0, 0),
									Mtime: time.Unix(0, 0), Users: []string{}, Groups: []string{},
									FileTypes: []string{}},
							}},
							{"?splits=0", []*DirSummary{
								{Dir: "/a", Count: 19, Size: 126, Atime: expectedAtime,
									Mtime: time.Unix(90, 0), Users: expectedUsers,
									Groups: expectedGroupsRoot, FileTypes: expectedFTs},
							}},
							{"?dir=/a/b/e/h", []*DirSummary{
								{Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams},
								{Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams},
							}},
						}

						runMapMatrixTest(t, matrix, s)
					})

					Convey("Where bad filters fail", func() {
						badFilters := []string{
							"?groups=fo#€o",
							"?users=fo#€o",
							"?types=fo#€o",
						}

						runSliceMatrixTest(t, badFilters, s)
					})

					Convey("Unless you provide an invalid directory", func() {
						response, err = queryWhere(s, "?dir=/foo")
						So(err, ShouldBeNil)
						So(response.Code, ShouldEqual, http.StatusBadRequest)
						So(logWriter.String(), ShouldContainSubstring, "STATUS=400")
						So(logWriter.String(), ShouldContainSubstring, "Error #01: directory not found")
					})

					Convey("And you can auto-reload a new database", func() {
						pathNew, errc := internaldb.CreateExampleDGUTDBCustomIDs(t, uid, gids[1], gids[0])
						So(errc, ShouldBeNil)

						grandparentDir := filepath.Dir(filepath.Dir(path))
						newerPath := filepath.Join(grandparentDir, "newer."+internaldb.ExampleDgutDirParentSuffix, "0")
						err = os.MkdirAll(filepath.Dir(newerPath), internaldb.DirPerms)
						So(err, ShouldBeNil)
						err = os.Rename(pathNew, newerPath)
						So(err, ShouldBeNil)

						later := time.Now().Local().Add(1 * time.Second)
						err = os.Chtimes(filepath.Dir(newerPath), later, later)
						So(err, ShouldBeNil)

						response, err = queryWhere(s, "")
						So(err, ShouldBeNil)
						result, err = decodeWhereResult(response)
						So(err, ShouldBeNil)
						So(result, ShouldResemble, expected)

						sentinel := path + ".sentinel"

						err = s.EnableDGUTDBReloading(sentinel, grandparentDir,
							internaldb.ExampleDgutDirParentSuffix, sentinelPollFrequency)
						So(err, ShouldNotBeNil)

						file, err := os.Create(sentinel)
						So(err, ShouldBeNil)
						err = file.Close()
						So(err, ShouldBeNil)

						s.treeMutex.RLock()
						So(s.dataTimeStamp.IsZero(), ShouldBeTrue)
						s.treeMutex.RUnlock()

						err = s.EnableDGUTDBReloading(sentinel, grandparentDir,
							internaldb.ExampleDgutDirParentSuffix, sentinelPollFrequency)
						So(err, ShouldBeNil)

						s.treeMutex.RLock()
						So(s.dataTimeStamp.IsZero(), ShouldBeFalse)
						previous := s.dataTimeStamp
						s.treeMutex.RUnlock()

						response, err = queryWhere(s, "")
						So(err, ShouldBeNil)
						result, err = decodeWhereResult(response)

						So(err, ShouldBeNil)
						So(result, ShouldResemble, expected)

						_, err = os.Stat(path)
						So(err, ShouldBeNil)

						now := time.Now().Local()
						err = os.Chtimes(sentinel, now, now)
						So(err, ShouldBeNil)

						waitForFileToBeDeleted(t, path)

						s.treeMutex.RLock()
						So(s.dataTimeStamp.After(previous), ShouldBeTrue)
						s.treeMutex.RUnlock()

						_, err = os.Stat(path)
						So(err, ShouldNotBeNil)

						parent := filepath.Dir(path)
						_, err = os.Stat(parent)
						So(err, ShouldBeNil)

						response, err = queryWhere(s, "")
						So(err, ShouldBeNil)
						So(response.Code, ShouldEqual, http.StatusOK)
						result, err = decodeWhereResult(response)
						So(err, ShouldBeNil)
						So(result, ShouldNotResemble, expected)

						s.dgutWatcher.RLock()
						So(s.dgutWatcher, ShouldNotBeNil)
						s.dgutWatcher.RUnlock()
						So(s.tree, ShouldNotBeNil)

						certPath, keyPath, err := gas.CreateTestCert(t)
						So(err, ShouldBeNil)
						_, stop, err := gas.StartTestServer(s, certPath, keyPath)
						So(err, ShouldBeNil)

						errs := stop()
						So(errs, ShouldBeNil)
						So(s.dgutWatcher, ShouldBeNil)
						So(s.tree, ShouldBeNil)

						s.Stop()
					})

					Convey("EnableDGUTDBReloading logs errors", func() {
						sentinel := path + ".sentinel"
						testSuffix := "test"

						file, err := os.Create(sentinel)
						So(err, ShouldBeNil)
						err = file.Close()
						So(err, ShouldBeNil)

						testReloadFail := func(dir, message string) {
							err = s.EnableDGUTDBReloading(sentinel, dir, testSuffix, sentinelPollFrequency)
							So(err, ShouldBeNil)

							now := time.Now().Local()
							err = os.Chtimes(sentinel, now, now)
							So(err, ShouldBeNil)

							<-time.After(50 * time.Millisecond)

							s.treeMutex.RLock()
							defer s.treeMutex.RUnlock()
							So(logWriter.String(), ShouldContainSubstring, message)
						}

						grandparentDir := filepath.Dir(filepath.Dir(path))

						makeTestPath := func() string {
							tpath := filepath.Join(grandparentDir, "new."+testSuffix)
							err = os.MkdirAll(tpath, internaldb.DirPerms)
							So(err, ShouldBeNil)

							return tpath
						}

						Convey("when the directory doesn't contain the suffix", func() {
							testReloadFail(".", "file not found in directory")
						})

						Convey("when the directory doesn't exist", func() {
							testReloadFail("/sdf@£$", "no such file or directory")
						})

						Convey("when the suffix subdir can't be opened", func() {
							tpath := makeTestPath()

							err = os.Chmod(tpath, 0000)
							So(err, ShouldBeNil)

							testReloadFail(grandparentDir, "permission denied")
						})

						Convey("when the directory contains no subdirs", func() {
							makeTestPath()

							testReloadFail(grandparentDir, "file not found in directory")
						})

						Convey("when the new database path is invalid", func() {
							tpath := makeTestPath()

							dbPath := filepath.Join(tpath, "0")
							err = os.Mkdir(dbPath, internaldb.DirPerms)
							So(err, ShouldBeNil)

							testReloadFail(grandparentDir, "database doesn't exist")
						})

						Convey("when the old path can't be deleted", func() {
							s.dgutPaths = []string{"."}
							tpath := makeTestPath()

							cmd := exec.Command("cp", "--recursive", path, filepath.Join(tpath, "0"))
							err = cmd.Run()
							So(err, ShouldBeNil)

							testReloadFail(grandparentDir, "invalid argument")
						})

						Convey("when there's an issue with getting dir mtime, it is ignored", func() {
							t := ifs.DirEntryModTime(&mockDirEntry{})
							So(t.IsZero(), ShouldBeTrue)
						})
					})
				})
			})

			Convey("LoadDGUTDBs fails on an invalid path", func() {
				err := s.LoadDGUTDBs("/foo")
				So(err, ShouldNotBeNil)
			})
		})

		Convey("You can query the basedirs endpoints", func() {
			response, err := query(s, EndPointBasedirUsageGroup, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusNotFound)
			So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/usage/groups")
			So(logWriter.String(), ShouldContainSubstring, "STATUS=404")
			logWriter.Reset()

			Convey("And given a basedirs database", func() {
				tree, _, err := internaldb.CreateExampleDGUTDBForBasedirs(t)
				So(err, ShouldBeNil)

				dbPath, ownersPath, err := createExampleBasedirsDB(t, tree)
				So(err, ShouldBeNil)

				Convey("You can get results after calling LoadBasedirsDB", func() {
					err = s.LoadBasedirsDB(dbPath, ownersPath)
					So(err, ShouldBeNil)

					s.basedirs.SetMountPoints([]string{
						"/lustre/scratch123/",
						"/lustre/scratch125/",
					})

					response, err := query(s, EndPointBasedirUsageGroup, "")
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/usage/groups")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					usageGroup, err := decodeUsageResult(response)
					So(err, ShouldBeNil)
					So(len(usageGroup), ShouldEqual, 6)
					So(usageGroup[0].GID, ShouldNotEqual, 0)
					So(usageGroup[0].UID, ShouldEqual, 0)
					So(usageGroup[0].Name, ShouldNotBeBlank)
					So(usageGroup[0].Owner, ShouldNotBeBlank)
					So(usageGroup[0].BaseDir, ShouldNotBeBlank)

					response, err = query(s, EndPointBasedirUsageUser, "")
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/usage/users")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					usageUser, err := decodeUsageResult(response)
					So(err, ShouldBeNil)
					So(len(usageUser), ShouldEqual, 6)
					So(usageUser[0].GID, ShouldEqual, 0)
					So(usageUser[0].UID, ShouldNotEqual, 0)
					So(usageUser[0].Name, ShouldNotBeBlank)
					So(usageUser[0].Owner, ShouldBeBlank)
					So(usageUser[0].BaseDir, ShouldNotBeBlank)

					response, err = query(s, EndPointBasedirSubdirGroup,
						fmt.Sprintf("?id=%d&basedir=%s", usageGroup[0].GID, usageGroup[0].BaseDir))
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/subdirs/group")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					subdirs, err := decodeSubdirResult(response)
					So(err, ShouldBeNil)
					So(len(subdirs), ShouldEqual, 2)
					So(subdirs[0].SubDir, ShouldEqual, ".")
					So(subdirs[1].SubDir, ShouldEqual, "sub")

					response, err = query(s, EndPointBasedirSubdirUser,
						fmt.Sprintf("?id=%d&basedir=%s", usageUser[0].UID, usageUser[0].BaseDir))
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/subdirs/user")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					subdirs, err = decodeSubdirResult(response)
					So(err, ShouldBeNil)
					So(len(subdirs), ShouldEqual, 2)

					response, err = query(s, EndPointBasedirHistory,
						fmt.Sprintf("?id=%d&basedir=%s", usageGroup[0].GID, usageGroup[0].BaseDir))
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/history")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					history, err := decodeHistoryResult(response)
					So(err, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history[0].UsageInodes, ShouldEqual, 2)

					Convey("Which get updated by an auto-reload when the sentinal file changes", func() {
						parentDir := filepath.Dir(filepath.Dir(dbPath))
						sentinel := filepath.Join(parentDir, ".sentinel")
						file, err := os.Create(sentinel)
						So(err, ShouldBeNil)
						err = file.Close()
						So(err, ShouldBeNil)

						err = s.EnableBasedirDBReloading(sentinel, parentDir,
							filepath.Base(dbPath), sentinelPollFrequency)
						So(err, ShouldBeNil)

						gid, uid, _, _, err := internaldata.RealGIDAndUID()
						So(err, ShouldBeNil)

						_, files := internaldata.FakeFilesForDGUTDBForBasedirsTesting(gid, uid)
						tree, err = internaldb.CreateDGUTDBFromFakeFiles(t, files[:1])
						So(err, ShouldBeNil)

						pathNew, _, err := createExampleBasedirsDB(t, tree)
						So(err, ShouldBeNil)

						newerPath := filepath.Join(parentDir, "newer.basedir.db")
						err = os.Rename(pathNew, newerPath)
						So(err, ShouldBeNil)

						later := time.Now().Local().Add(1 * time.Second)
						err = os.Chtimes(newerPath, later, later)
						So(err, ShouldBeNil)

						response, err := query(s, EndPointBasedirUsageGroup, "")
						So(err, ShouldBeNil)
						So(response.Code, ShouldEqual, http.StatusOK)

						usageGroup, err := decodeUsageResult(response)
						So(err, ShouldBeNil)
						So(len(usageGroup), ShouldEqual, 6)

						err = os.Chtimes(sentinel, later, later)
						So(err, ShouldBeNil)

						waitForFileToBeDeleted(t, dbPath)

						_, err = os.Stat(dbPath)
						So(err, ShouldNotBeNil)

						response, err = query(s, EndPointBasedirUsageGroup, "")
						So(err, ShouldBeNil)
						So(response.Code, ShouldEqual, http.StatusOK)

						usageGroup, err = decodeUsageResult(response)
						So(err, ShouldBeNil)
						So(len(usageGroup), ShouldEqual, 1)
					})
				})
			})
		})
	})
}

// getExampleGIDs returns some example GIDs to test with, using 2 real ones from
// the given slice if the slice is long enough.
func getExampleGIDs(gids []string) []string {
	exampleGIDs := []string{"3", "4"}
	if len(gids) > 1 {
		exampleGIDs[0] = gids[0]
		exampleGIDs[1] = gids[1]
	}

	return exampleGIDs
}

func fixDirSummaryTimes(summaries []*DirSummary) {
	for _, dcss := range summaries {
		dcss.Atime = fixtimes.FixTime(dcss.Atime)
		dcss.Mtime = fixtimes.FixTime(dcss.Mtime)
	}
}

// testClientsOnRealServer tests our client method GetWhereDataIs and the tree
// webpage on a real listening server, if we have at least 2 gids to test with.
func testClientsOnRealServer(t *testing.T, username, uid string, gids []string, s *Server, addr, cert, key string) {
	t.Helper()

	if len(gids) < 2 {
		return
	}

	g, errg := user.LookupGroupId(gids[0])
	So(errg, ShouldBeNil)

	Convey("Given databases", func() {
		_, _, err := GetWhereDataIs("localhost:1", cert, "", "", "", "", "", "")
		So(err, ShouldNotBeNil)

		path, err := internaldb.CreateExampleDGUTDBCustomIDs(t, uid, gids[0], gids[1])
		So(err, ShouldBeNil)

		tree, _, err := internaldb.CreateExampleDGUTDBForBasedirs(t)
		So(err, ShouldBeNil)

		basedirsDBPath, ownersPath, err := createExampleBasedirsDB(t, tree)
		So(err, ShouldBeNil)

		Convey("You can't get where data is or add the tree page without auth", func() {
			err = s.LoadDGUTDBs(path)
			So(err, ShouldBeNil)

			_, _, err = GetWhereDataIs(addr, cert, "", "/", "", "", "", "")
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, gas.ErrNoAuth)

			err = s.AddTreePage()
			So(err, ShouldNotBeNil)
		})

		Convey("Root can see everything", func() {
			err = s.EnableAuth(cert, key, func(username, password string) (bool, string) {
				return true, ""
			})
			So(err, ShouldBeNil)

			err = s.LoadDGUTDBs(path)
			So(err, ShouldBeNil)

			token, errl := gas.Login(addr, cert, "user", "pass")
			So(errl, ShouldBeNil)

			_, _, err = GetWhereDataIs(addr, cert, token, "", "", "", "", "")
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, ErrBadQuery)

			json, dcss, errg := GetWhereDataIs(addr, cert, token, "/", "", "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 19)

			json, dcss, errg = GetWhereDataIs(addr, cert, token, "/", g.Name, "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 13)

			json, dcss, errg = GetWhereDataIs(addr, cert, token, "/", "", "root", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 14)
		})

		Convey("Normal users have access restricted only by group", func() {
			err = s.EnableAuth(cert, key, func(username, password string) (bool, string) {
				return true, uid
			})
			So(err, ShouldBeNil)

			err = s.LoadDGUTDBs(path)
			So(err, ShouldBeNil)

			token, errl := gas.Login(addr, cert, "user", "pass")
			So(errl, ShouldBeNil)

			json, dcss, errg := GetWhereDataIs(addr, cert, token, "/", "", "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 18)

			json, dcss, errg = GetWhereDataIs(addr, cert, token, "/", g.Name, "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 13)

			_, _, errg = GetWhereDataIs(addr, cert, token, "/", "", "root", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 13)
		})

		Convey("Once you add the tree page", func() {
			var logWriter strings.Builder
			s := New(&logWriter)

			err = s.EnableAuth(cert, key, func(username, password string) (bool, string) {
				return true, ""
			})
			So(err, ShouldBeNil)

			err = s.LoadDGUTDBs(path)
			So(err, ShouldBeNil)

			err = s.LoadBasedirsDB(basedirsDBPath, ownersPath)
			So(err, ShouldBeNil)

			err = s.AddTreePage()
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, cert, key)
			So(err, ShouldBeNil)
			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

			token, err := gas.Login(addr, cert, "user", "pass")
			So(err, ShouldBeNil)

			Convey("You can get the static tree web page", func() {
				r := gas.NewAuthenticatedClientRequest(addr, cert, token)

				resp, err := r.Get("tree/tree.html")
				So(err, ShouldBeNil)
				So(string(resp.Body()), ShouldStartWith, "<!DOCTYPE html>")

				resp, err = r.Get("")
				So(err, ShouldBeNil)
				So(string(resp.Body()), ShouldStartWith, "<!DOCTYPE html>")
			})

			Convey("You can access the tree API", func() {
				r := gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err := r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				users := []string{"root", username}
				sort.Strings(users)
				groups := gidsToGroups(t, gids[0], gids[1], "0")
				sort.Strings(groups)

				expectedFTs := []string{"bam", "cram", "dir", "temp"}
				expectedAtime := "1970-01-01T00:00:50Z"
				expectedMtime := "1970-01-01T00:01:30Z"

				const numDirectories = 10

				const directorySize = 1024

				tm := *resp.Result().(*TreeElement) //nolint:forcetypeassert
				So(tm, ShouldResemble, TreeElement{
					Name:        "/",
					Path:        "/",
					Count:       19 + numDirectories,
					Size:        126 + numDirectories*directorySize,
					Atime:       expectedAtime,
					Mtime:       expectedMtime,
					Users:       users,
					Groups:      groups,
					FileTypes:   expectedFTs,
					TimeStamp:   "0001-01-01T00:00:00Z",
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "a",
							Path:        "/a",
							Count:       19 + numDirectories,
							Size:        126 + numDirectories*directorySize,
							Atime:       expectedAtime,
							Mtime:       expectedMtime,
							Users:       users,
							Groups:      groups,
							FileTypes:   expectedFTs,
							TimeStamp:   "0001-01-01T00:00:00Z",
							HasChildren: true,
							Children:    nil,
						},
					},
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path":   "/",
						"groups": g.Name,
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				expectedMtime = "1970-01-01T00:01:20Z"

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert
				So(tm, ShouldResemble, TreeElement{
					Name:        "/",
					Path:        "/",
					Count:       13 + 8,
					Size:        120 + 8*directorySize,
					Atime:       expectedAtime,
					Mtime:       expectedMtime,
					Users:       users,
					Groups:      []string{g.Name},
					FileTypes:   expectedFTs,
					TimeStamp:   "0001-01-01T00:00:00Z",
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "a",
							Path:        "/a",
							Count:       13 + 8,
							Size:        120 + 8*directorySize,
							Atime:       expectedAtime,
							Mtime:       expectedMtime,
							Users:       users,
							Groups:      []string{g.Name},
							FileTypes:   expectedFTs,
							TimeStamp:   "0001-01-01T00:00:00Z",
							HasChildren: true,
							Children:    nil,
						},
					},
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path":   "/",
						"groups": "adsf@£$",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path": "/foo",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)
			})

			Convey("You can access the group-areas endpoint after AddGroupAreas()", func() {
				_, err := GetGroupAreas(addr, cert, token)
				So(err, ShouldNotBeNil)

				expectedAreas := map[string][]string{
					"a": {"1", "2"},
					"b": {"3", "4"},
				}

				s.AddGroupAreas(expectedAreas)

				areas, err := GetGroupAreas(addr, cert, token)
				So(err, ShouldBeNil)
				So(areas, ShouldResemble, expectedAreas)

				_, err = GetGroupAreas(addr, cert, "foo")
				So(err, ShouldNotBeNil)
			})

			Convey("You can access the secure basedirs endpoints after LoadBasedirsDB()", func() {
				r := gas.NewAuthenticatedClientRequest(addr, cert, token)

				var usage []*basedirs.Usage

				resp, err := r.SetResult(&usage).
					ForceContentType("application/json").
					Get(EndPointAuthBasedirUsageUser)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(usage), ShouldEqual, 6)
				So(usage[0].UID, ShouldNotEqual, 0)

				userUsageUID := usage[0].UID
				userUsageBasedir := usage[0].BaseDir

				resp, err = r.SetResult(&usage).
					ForceContentType("application/json").
					Get(EndPointAuthBasedirUsageGroup)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(usage), ShouldEqual, 6)
				So(usage[0].GID, ShouldNotEqual, 0)

				var subdirs []*basedirs.SubDir

				resp, err = r.SetResult(&subdirs).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"id":      fmt.Sprintf("%d", usage[0].GID),
						"basedir": usage[0].BaseDir,
					}).
					Get(EndPointAuthBasedirSubdirGroup)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(subdirs), ShouldEqual, 2)

				resp, err = r.SetResult(&subdirs).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"id":      fmt.Sprintf("%d", userUsageUID),
						"basedir": userUsageBasedir,
					}).
					Get(EndPointAuthBasedirSubdirUser)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(subdirs), ShouldEqual, 2)

				var history []basedirs.History

				resp, err = r.SetResult(&history).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"id":      fmt.Sprintf("%d", usage[0].GID),
						"basedir": usage[0].BaseDir,
					}).
					Get(EndPointAuthBasedirHistory)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
			})
		})
	})
}

// getUserAndGroups returns the current users username, uid and gids.
func getUserAndGroups(t *testing.T) (string, string, []string) {
	t.Helper()

	uu, err := user.Current()
	if err != nil {
		t.Logf("getting current user failed: %s", err.Error())

		return "", "", nil
	}

	gids, err := uu.GroupIds()
	if err != nil {
		t.Logf("getting group ids failed: %s", err.Error())

		return "", "", nil
	}

	return uu.Username, uu.Uid, gids
}

// queryWhere does a test GET of /rest/v1/where, with extra appended (start it
// with ?).
func queryWhere(s *Server, extra string) (*httptest.ResponseRecorder, error) {
	return query(s, EndPointWhere, extra)
}

func query(s *Server, endpoint, extra string) (*httptest.ResponseRecorder, error) {
	return gas.QueryREST(s.Router(), endpoint, extra)
}

// decodeWhereResult decodes the result of a Where query.
func decodeWhereResult(response *httptest.ResponseRecorder) ([]*DirSummary, error) {
	var result []*DirSummary
	err := json.NewDecoder(response.Body).Decode(&result)

	fixDirSummaryTimes(result)

	return result, err
}

// testRestrictedGroups does tests for s.restrictedGroups() if user running the
// test has enough groups to make the test viable.
func testRestrictedGroups(t *testing.T, gids []string, s *Server, r, rBadUID *resty.Request, exampleGIDs []string) {
	t.Helper()

	if len(gids) < 3 {
		return
	}

	var filterGIDs []string

	var errg error

	s.AuthRouter().GET("/groups", func(c *gin.Context) {
		groups := c.Query("groups")
		filterGIDs, errg = s.restrictedGroups(c, groups)
	})

	groups := gidsToGroups(t, gids...)
	_, err := r.Get(gas.EndPointAuth + "/groups?groups=" + groups[0])
	So(err, ShouldBeNil)

	So(errg, ShouldBeNil)
	So(filterGIDs, ShouldResemble, []string{exampleGIDs[0]})

	_, err = r.Get(gas.EndPointAuth + "/groups?groups=0")
	So(err, ShouldBeNil)

	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)

	s.userToGIDs = make(map[string][]string)

	_, err = rBadUID.Get(gas.EndPointAuth + "/groups?groups=" + groups[0])
	So(err, ShouldBeNil)
	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)

	s.WhiteListGroups(func(gid string) bool {
		return gid == gids[0]
	})

	s.userToGIDs = make(map[string][]string)

	_, err = r.Get(gas.EndPointAuth + "/groups?groups=root")
	So(err, ShouldBeNil)

	So(errg, ShouldBeNil)
	So(filterGIDs, ShouldResemble, []string{"0"})

	s.WhiteListGroups(func(group string) bool {
		return false
	})

	s.userToGIDs = make(map[string][]string)

	_, err = r.Get(gas.EndPointAuth + "/groups?groups=root")
	So(err, ShouldBeNil)

	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)
}

// gidsToGroups converts the given gids to group names.
func gidsToGroups(t *testing.T, gids ...string) []string {
	t.Helper()

	groups := make([]string, len(gids))

	for i, gid := range gids {
		groups[i] = gidToGroup(t, gid)
	}

	return groups
}

// gidToGroup converts the given gid to a group name.
func gidToGroup(t *testing.T, gid string) string {
	t.Helper()

	g, err := user.LookupGroupId(gid)
	if err != nil {
		t.Fatalf("LookupGroupId(%s) failed: %s", gid, err)
	}

	return g.Name
}

// adjustedExpectations returns expected altered so that /a only has the given
// groups and values appropriate for non-root. It also returns root's unaltered
// set of groups.
func adjustedExpectations(expected []*DirSummary, groupA, groupB string) ([]*DirSummary, []string) {
	var expectedGroupsRoot []string

	expectedNonRoot := make([]*DirSummary, len(expected))
	for i, ds := range expected {
		expectedNonRoot[i] = ds

		if ds.Dir == "/a" {
			groups := []string{groupA, groupB}
			sort.Strings(groups)

			expectedNonRoot[i] = &DirSummary{
				Dir:       "/a",
				Count:     18,
				Size:      125,
				Atime:     time.Unix(50, 0),
				Mtime:     time.Unix(90, 0),
				Users:     ds.Users,
				Groups:    groups,
				FileTypes: ds.FileTypes,
			}

			expectedGroupsRoot = ds.Groups
		}
	}

	return expectedNonRoot, expectedGroupsRoot
}

// removeTempFromDSs clones the given DirSummary slice and returns the clone,
// but altering it so that it has no temporary file types in each DirSummary.
func removeTempFromDSs(expected []*DirSummary) []*DirSummary {
	expectedNoTemp := make([]*DirSummary, len(expected))

	for i, ds := range expected {
		nt := &DirSummary{
			Dir:    ds.Dir,
			Count:  ds.Count,
			Size:   ds.Size,
			Atime:  ds.Atime,
			Mtime:  ds.Mtime,
			Users:  ds.Users,
			Groups: ds.Groups,
		}

		if len(ds.FileTypes) == 1 {
			nt.FileTypes = ds.FileTypes
		} else {
			fts := make([]string, len(ds.FileTypes)-1)
			for j := range fts {
				fts[j] = ds.FileTypes[j]
			}
			nt.FileTypes = fts
		}

		expectedNoTemp[i] = nt
	}

	return expectedNoTemp
}

type matrixElement struct {
	filter string
	dss    []*DirSummary
}

// runMapMatrixTest tests queries against expected results on the Server.
func runMapMatrixTest(t *testing.T, matrix []*matrixElement, s *Server) {
	t.Helper()

	for _, m := range matrix {
		fixDirSummaryTimes(m.dss)

		response, err := queryWhere(s, m.filter)
		So(err, ShouldBeNil)
		So(response.Code, ShouldEqual, http.StatusOK)

		result, err := decodeWhereResult(response)
		So(err, ShouldBeNil)
		So(result, ShouldResemble, m.dss)
	}
}

// runSliceMatrixTest tests queries that are expected to fail on the Server.
func runSliceMatrixTest(t *testing.T, matrix []string, s *Server) {
	t.Helper()

	for _, filter := range matrix {
		response, err := queryWhere(s, filter)
		So(err, ShouldBeNil)
		So(response.Code, ShouldEqual, http.StatusBadRequest)
	}
}

// waitForFileToBeDeleted waits for the given file to not exist. Times out after
// 10 seconds.
func waitForFileToBeDeleted(t *testing.T, path string) {
	t.Helper()

	wait := make(chan bool, 1)

	go func() {
		defer func() {
			wait <- true
		}()

		limit := time.After(10 * time.Second)
		ticker := time.NewTicker(50 * time.Millisecond)

		for {
			select {
			case <-ticker.C:
				_, err := os.Stat(path)
				if err != nil {
					ticker.Stop()

					return
				}
			case <-limit:
				ticker.Stop()
				t.Logf("timed out waiting for deletion; %s still exists\n", path)

				return
			}
		}
	}()

	<-wait
}

type mockDirEntry struct{}

func (m *mockDirEntry) Name() string {
	return ""
}

func (m *mockDirEntry) IsDir() bool {
	return false
}

func (m *mockDirEntry) Type() fs.FileMode {
	return fs.ModeDir
}

func (m *mockDirEntry) Info() (fs.FileInfo, error) {
	return nil, fs.ErrNotExist
}

// createExampleBasedirsDB creates a temporary basedirs.db and returns the path
// to the database file.
func createExampleBasedirsDB(t *testing.T, tree *dgut.Tree) (string, string, error) {
	t.Helper()

	csvPath := internaldata.CreateQuotasCSV(t, internaldata.ExampleQuotaCSV)

	quotas, err := basedirs.ParseQuotas(csvPath)
	if err != nil {
		return "", "", err
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "basedir.db")

	bd, err := basedirs.NewCreator(dbPath, tree, quotas)
	if err != nil {
		return "", "", err
	}

	bd.SetMountPoints([]string{
		"/lustre/scratch123/",
		"/lustre/scratch125/",
	})

	err = bd.CreateDatabase(time.Now())
	if err != nil {
		return "", "", err
	}

	ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)

	return dbPath, ownersPath, err
}

// decodeUsageResult decodes the result of a basedirs usage query.
func decodeUsageResult(response *httptest.ResponseRecorder) ([]*basedirs.Usage, error) {
	var result []*basedirs.Usage
	err := json.NewDecoder(response.Body).Decode(&result)

	return result, err
}

// decodeSubdirResult decodes the result of a basedirs subdir query.
func decodeSubdirResult(response *httptest.ResponseRecorder) ([]*basedirs.SubDir, error) {
	var result []*basedirs.SubDir
	err := json.NewDecoder(response.Body).Decode(&result)

	return result, err
}

func decodeHistoryResult(response *httptest.ResponseRecorder) ([]basedirs.History, error) {
	var result []basedirs.History
	err := json.NewDecoder(response.Body).Decode(&result)

	return result, err
}
