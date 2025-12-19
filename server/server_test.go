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
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	internaldb "github.com/wtsi-hgi/wrstat-ui/internal/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/fixtimes"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

func TestIDsToWanted(t *testing.T) {
	Convey("restrictGIDs returns bad query if you don't want any of the given ids", t, func() {
		_, err := restrictGIDs(map[uint32]bool{1: true}, []uint32{2})
		So(err, ShouldNotBeNil)
	})
}

func TestServer(t *testing.T) {
	username, uid, gids := internaldb.GetUserAndGroups(t)
	exampleGIDs := getExampleGIDs(gids)
	sentinelPollFrequency := 10 * time.Millisecond

	refTime := time.Now().Unix()

	Convey("Given a Server", t, func() {
		logWriter := gas.NewStringLogger()
		s := New(logWriter)

		Convey("You can convert dguta.DCSs to DirSummarys", func() {
			uid32, err := strconv.Atoi(uid)
			So(err, ShouldBeNil)
			gid32, err := strconv.Atoi(gids[0])
			So(err, ShouldBeNil)

			dcss := db.DCSs{
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

			Convey("The jwt endpoint works after enabling it", func() {
				err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, string) {
					returnUID := uid

					if u == "user" {
						returnUID = "-1"
					}

					return true, returnUID
				})
				So(err, ShouldBeNil)

				r := gas.NewClientRequest(addr, certPath)
				token, errl := gas.Login(r, username, "pass")
				So(errl, ShouldBeNil)

				r = gas.NewAuthenticatedClientRequest(addr, certPath, token)
				tokenBadUID, errl := gas.Login(r, "user", "pass")
				So(errl, ShouldBeNil)
				So(tokenBadUID, ShouldNotBeBlank)

				s.AuthRouter().GET("/test", func(c *gin.Context) {})

				resp, err := r.Get(gas.EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				testRestrictedGroups(t, gids, s, exampleGIDs, addr, certPath, token, tokenBadUID)
			})

			testClientsOnRealServer(t, username, uid, gids, s, addr, certPath, keyPath)
		})

		if len(gids) < 2 {
			SkipConvey("Can't test the where endpoint without you belonging to at least 2 groups", func() {})

			return
		}

		Convey("convertSplitsValue works", func() {
			n := convertSplitsValue("1")
			So(n(""), ShouldEqual, 1)

			n = convertSplitsValue("foo")
			So(n(""), ShouldEqual, 2)
		})

		Convey("You can query the endpoints", func() {
			response, err := queryWhere(s, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusNotFound)
			So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/where")
			So(logWriter.String(), ShouldContainSubstring, "STATUS=404")
			logWriter.Reset()

			response, err = query(s, EndPointBasedirUsageGroup, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusNotFound)
			So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/usage/groups")
			So(logWriter.String(), ShouldContainSubstring, "STATUS=404")
			logWriter.Reset()

			Convey("And given dirguta and basedir databases", func() {
				path, err := internaldb.CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
				So(err, ShouldBeNil)
				groupA := gidToGroup(t, gids[0])
				groupB := gidToGroup(t, gids[1])

				tree, err := db.NewTree(filepath.Join(path, "dirguta"))
				So(err, ShouldBeNil)

				expectedRaw, err := tree.Where("/", nil, split.SplitsToSplitFn(2))
				So(err, ShouldBeNil)

				expected := s.dcssToSummaries(expectedRaw)

				fixDirSummaryTimes(expected)

				expectedNonRoot, expectedGroupsRoot := adjustedExpectations(expected, groupA, groupB)

				tree.Close()

				ownersPath, err := internaldata.CreateOwnersCSV(t, fmt.Sprintf("0,Alan\n%s,Barbara\n%s,Dellilah", gids[0], gids[1]))
				So(err, ShouldBeNil)

				err = s.LoadDBs([]string{path}, "dirguta", "basedir.db", ownersPath)
				So(err, ShouldBeNil)

				Convey("You can get dirguta results", func() {
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
						expectedGroupsRootA := []string{groupA, "root"}
						sort.Strings(expectedGroupsRootA)
						expectedFTs := expectedNonRoot[0].FileTypes
						expectedBams := []string{"bam", "temp"}
						expectedCrams := []string{"cram"}
						expectedAtime := time.Unix(50, 0)
						matrix := []*matrixElement{
							{"?groups=" + groups[0] + "," + groups[1], expectedNonRoot},
							{"?groups=" + groups[0], []*DirSummary{
								{
									Dir: "/a/b", Count: 13, Size: 120, Atime: expectedAtime,
									Mtime: time.Unix(80, 0), Users: expectedUsers,
									Groups: expectedGroupsA, FileTypes: expectedFTs,
								},
								{
									Dir: "/a/b/d", Count: 11, Size: 110, Atime: expectedAtime,
									Mtime: time.Unix(75, 0), Users: expectedUsers,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d/g", Count: 10, Size: 100, Atime: time.Unix(50, 0),
									Mtime: time.Unix(75, 0), Users: expectedUsers,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d/f", Count: 1, Size: 10, Atime: expectedAtime,
									Mtime: time.Unix(50, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams,
								},
								{
									Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams,
								},
							}},
							{"?users=root," + username, expected},
							{"?users=root", []*DirSummary{
								{
									Dir: "/a", Count: 14, Size: 86, Atime: expectedAtime,
									Mtime: time.Unix(90, 0), Users: expectedRoot,
									Groups: expectedGroupsRoot, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d", Count: 9, Size: 81, Atime: expectedAtime,
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsRootA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d/g", Count: 8, Size: 80, Atime: time.Unix(50, 0),
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/c/d", Count: 5, Size: 5, Atime: time.Unix(90, 0),
									Mtime: time.Unix(90, 0), Users: expectedRoot,
									Groups: expectedGroupsB, FileTypes: expectedCrams,
								},
								{
									Dir: "/a/b/d/i/j", Count: 1, Size: 1, Atime: expectedAtime,
									Mtime: expectedAtime, Users: expectedRoot,
									Groups: expectedRoot, FileTypes: expectedCrams,
								},
							}},
							{"?groups=" + groups[0] + "&users=root", []*DirSummary{
								{
									Dir: "/a/b/d/g", Count: 8, Size: 80, Atime: time.Unix(50, 0),
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
							}},
							{"?types=cram,bam", expected},
							{"?types=bam", []*DirSummary{
								{
									Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: []string{"bam", "temp"},
								},
								{
									Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: []string{"bam", "temp"},
								},
							}},
							{"?groups=" + groups[0] + "&users=root&types=cram,bam", []*DirSummary{
								{
									Dir: "/a/b/d/g", Count: 8, Size: 80, Atime: time.Unix(50, 0),
									Mtime: time.Unix(75, 0), Users: expectedRoot,
									Groups: expectedGroupsA, FileTypes: expectedCrams,
								},
							}},
							{"?groups=" + groups[0] + "&users=root&types=bam", []*DirSummary{}},
							{"?splits=0", []*DirSummary{
								{
									Dir: "/", Count: 24, Size: 141, Atime: expectedAtime,
									Mtime: expectedNonRoot[0].Mtime, Users: expectedUsers,
									Groups: expectedGroupsRoot, FileTypes: expectedFTs,
								},
							}},
							{"?dir=/a&splits=0", []*DirSummary{
								{
									Dir: "/a", Count: 19, Size: 126, Atime: expectedAtime,
									Mtime: time.Unix(90, 0), Users: expectedUsers,
									Groups: expectedGroupsRoot, FileTypes: expectedFTs,
								},
							}},
							{"?dir=/a/b/e/h", []*DirSummary{
								{
									Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams,
								},
								{
									Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: time.Unix(80, 0),
									Mtime: time.Unix(80, 0), Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: expectedBams,
								},
							}},
							{"?dir=/k/&age=1", []*DirSummary{
								{
									Dir: "/k/", Count: 4, Size: 10, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-(db.SecondsInAMonth*2), 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeA1M,
								},
							}},
							{"?dir=/k&age=2", []*DirSummary{
								{
									Dir: "/k", Count: 3, Size: 7, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-db.SecondsInAYear, 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeA2M,
								},
							}},
							{"?dir=/k&age=6", []*DirSummary{
								{
									Dir: "/k", Count: 1, Size: 1, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-(db.SecondsInAYear*7), 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeA3Y,
								},
							}},
							{"?dir=/k&age=8", []*DirSummary{}},
							{"?dir=/k&age=11", []*DirSummary{
								{
									Dir: "/k", Count: 3, Size: 7, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-(db.SecondsInAYear), 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeM6M,
								},
							}},
							{"?dir=/k&age=16", []*DirSummary{
								{
									Dir: "/k", Count: 1, Size: 1, Atime: expectedNonRoot[3].Atime,
									Mtime: time.Unix(refTime-(db.SecondsInAYear*7), 0), Users: expectedUser,
									Groups: expectedGroupsB, FileTypes: expectedCrams, Age: db.DGUTAgeM7Y,
								},
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
				})

				Convey("You can get basedir results", func() {
					s.basedirs.SetMountPoints([]string{
						"/a/",
						"/k/",
					})

					response, err := query(s, EndPointBasedirUsageGroup, "")
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/usage/groups")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					usageGroup, err := decodeUsageResult(response)
					So(err, ShouldBeNil)
					So(len(usageGroup), ShouldEqual, 51)
					So(usageGroup[0].GID, ShouldEqual, 0)
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
					So(len(usageUser), ShouldEqual, 34)
					So(usageUser[0].GID, ShouldEqual, 0)
					So(usageUser[0].UID, ShouldEqual, 0)
					So(usageUser[0].Name, ShouldNotBeBlank)
					So(usageUser[0].Owner, ShouldEqual, "Alan")
					So(usageUser[0].BaseDir, ShouldNotBeBlank)

					response, err = query(s, EndPointBasedirSubdirGroup,
						fmt.Sprintf("?id=%d&basedir=%s", usageGroup[0].GID, usageGroup[0].BaseDir))
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/subdirs/group")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					subdirs, err := decodeSubdirResult(response)
					So(err, ShouldBeNil)
					So(len(subdirs), ShouldEqual, 1)
					So(subdirs[0].SubDir, ShouldEqual, ".")

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
					So(history[0].UsageInodes, ShouldEqual, 1)

					response, err = query(s, EndPointBasedirSubdirUser,
						fmt.Sprintf("?id=%d&basedir=%s&age=%d", usageUser[0].UID, usageUser[0].BaseDir, db.DGUTAgeA3Y))
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/basedirs/subdirs/user")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					subdirs, err = decodeSubdirResult(response)
					So(err, ShouldBeNil)
					So(len(subdirs), ShouldEqual, 2)
				})
			})
		})

		Convey("LoadDBs fails on an invalid path", func() {
			err := s.LoadDBs([]string{"/foo"}, "something", "anything", "")
			So(err, ShouldNotBeNil)
		})

		Reset(func() { s.Stop() })

		Convey("You can enable automatic reloading", func() {
			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			tmp := t.TempDir()

			first := filepath.Join(tmp, "111_keyA")

			err = internaldb.CreateExampleDBsCustomIDsWithDir(t, first, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			So(os.Chtimes(first, time.Unix(refTime, 0), time.Unix(refTime, 0)), ShouldBeNil)

			err = s.EnableDBReloading(tmp, "dirguta", "basedir.db", ownersPath, sentinelPollFrequency, true)
			So(err, ShouldBeNil)

			dirguta := s.tree
			basedirs := s.basedirs
			lastMod := s.dataTimeStamp["keyA"]

			So(len(s.dataTimeStamp), ShouldEqual, 1)

			secondDot := filepath.Join(tmp, ".112_keyB")
			second := filepath.Join(tmp, "112_keyB")

			err = internaldb.CreateExampleDBsCustomIDsWithDir(t, secondDot, uid, gids[0], gids[1], refTime+10)
			So(err, ShouldBeNil)

			So(os.Chtimes(secondDot, time.Unix(refTime+10, 0), time.Unix(refTime+10, 0)), ShouldBeNil)
			s.mu.Lock()
			initialGroupCache := s.groupUsageCache
			initialUserCache := s.userUsageCache
			s.mu.Unlock()

			So(initialGroupCache, ShouldNotBeNil)
			So(initialUserCache, ShouldNotBeNil)

			err = os.Rename(secondDot, second)
			So(err, ShouldBeNil)

			timeout := time.After(time.Second)

		Loop:
			for {
				select {
				case <-timeout:
					break Loop
				case <-time.After(time.Millisecond):
					s.mu.RLock()
					dataTimeStamp := s.dataTimeStamp["keyB"]
					s.mu.RUnlock()

					if dataTimeStamp > lastMod {
						break Loop
					}
				}
			}

			So(s.tree, ShouldNotEqual, dirguta)
			So(s.basedirs, ShouldNotEqual, basedirs)
			So(len(s.dataTimeStamp), ShouldEqual, 2)
			So(s.dataTimeStamp["keyB"], ShouldBeGreaterThan, lastMod)

			s.mu.RLock()
			latestGroupCache := s.groupUsageCache
			latestUserCache := s.userUsageCache
			s.mu.RUnlock()
			So(latestGroupCache, ShouldNotResemble, initialGroupCache)
			So(latestUserCache, ShouldNotResemble, initialUserCache)

			thirdDot := filepath.Join(tmp, ".113_keyA")
			third := filepath.Join(tmp, "113_keyA")

			err = internaldb.CreateExampleDBsCustomIDsWithDir(t, thirdDot, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			err = os.Rename(thirdDot, third)
			So(err, ShouldBeNil)

			waitForFileToBeDeleted(t, first)
			_, err = os.Stat(first)
			So(os.IsNotExist(err), ShouldBeTrue)
		})

		Convey("prewarmCaches fills caches with JSON and gzip", func() {
			err := s.prewarmCaches(s.basedirs)
			So(err, ShouldBeNil)

			So(s.groupUsageCache, ShouldNotBeNil)
			So(s.userUsageCache, ShouldNotBeNil)
			So(len(s.groupUsageCache.jsonData), ShouldBeGreaterThan, 0)
			So(len(s.groupUsageCache.gzipData), ShouldBeGreaterThan, 0)
			So(len(s.userUsageCache.jsonData), ShouldBeGreaterThan, 0)
			So(len(s.userUsageCache.gzipData), ShouldBeGreaterThan, 0)
		})

		Convey("Incremental reload updates only new or modified databases", func() {
			ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
			So(err, ShouldBeNil)

			tmp := t.TempDir()

			first := filepath.Join(tmp, "111_keyA")
			err = internaldb.CreateExampleDBsCustomIDsWithDir(t, first, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)
			So(os.Chtimes(first, time.Unix(refTime, 0), time.Unix(refTime, 0)), ShouldBeNil)

			err = s.EnableDBReloading(tmp, "dirguta", "basedir.db", ownersPath, sentinelPollFrequency, false)
			So(err, ShouldBeNil)

			s.mu.RLock()
			oldTree := s.tree
			oldBD := s.basedirs
			oldTS := s.dataTimeStamp["keyA"]
			s.mu.RUnlock()

			So(**(**[]string)(unsafe.Pointer(oldTree)), ShouldResemble, []string{filepath.Join(first, "dirguta")})

			secondDot := filepath.Join(tmp, ".112_keyA")
			second := filepath.Join(tmp, "112_keyA")
			err = internaldb.CreateExampleDBsCustomIDsWithDir(t, secondDot, uid, gids[0], gids[1], refTime+10)
			So(err, ShouldBeNil)
			So(os.Chtimes(secondDot, time.Unix(refTime+10, 0), time.Unix(refTime+10, 0)), ShouldBeNil)
			err = os.Rename(secondDot, second)
			So(err, ShouldBeNil)

			timeout := time.After(2 * time.Second)
		Loop:
			for {
				select {
				case <-timeout:
					t.Fatal("timeout waiting for incremental reload")
				case <-time.After(10 * time.Millisecond):
					s.mu.RLock()
					ts, ok := s.dataTimeStamp["keyA"]
					s.mu.RUnlock()
					if ok && ts > oldTS {
						break Loop
					}
				}
			}

			s.mu.RLock()
			newTree := s.tree
			newBD := s.basedirs
			newTS := s.dataTimeStamp["keyA"]
			s.mu.RUnlock()

			So(**(**[]string)(unsafe.Pointer(newTree)), ShouldResemble, []string{filepath.Join(second, "dirguta")})

			So(s.dataTimeStamp["keyA"], ShouldEqual, newTS)

			So(newTS, ShouldBeGreaterThan, oldTS)

			So(newTree, ShouldNotEqual, oldTree)
			So(newBD, ShouldNotEqual, oldBD)
		})

		Convey("serveGzippedCache serves group and user usage via HTTP", func() {
			path, err := internaldb.CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
			So(err, ShouldBeNil)

			ownersPath, err := internaldata.CreateOwnersCSV(t, fmt.Sprintf("0,Alan\n%s,Barbara\n%s,Dellilah", gids[0], gids[1]))
			So(err, ShouldBeNil)

			err = s.LoadDBs([]string{path}, "dirguta", "basedir.db", ownersPath)
			So(err, ShouldBeNil)

			timeout := time.After(time.Second)
			tick := time.Tick(5 * time.Millisecond)

		Loop:
			for {
				select {
				case <-timeout:
					break Loop
				case <-tick:
					s.mu.RLock()
					userReady := len(s.userUsageCache.jsonData) > 0
					s.mu.RUnlock()
					if userReady {
						break Loop
					}
				}
			}

			response, err := query(s, EndPointBasedirUsageGroup, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusOK)

			usageGroup, err := decodeUsageResult(response)
			So(err, ShouldBeNil)
			So(len(usageGroup), ShouldBeGreaterThan, 0)
			So(usageGroup[0].GID, ShouldEqual, 0)
			So(usageGroup[0].UID, ShouldEqual, 0)
			So(usageGroup[0].Name, ShouldNotBeBlank)
			So(usageGroup[0].Owner, ShouldNotBeBlank)
			So(usageGroup[0].BaseDir, ShouldNotBeBlank)

			response, err = query(s, EndPointBasedirUsageUser, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusOK)

			usageUser, err := decodeUsageResult(response)
			So(err, ShouldBeNil)
			So(len(usageUser), ShouldBeGreaterThan, 0)
			So(usageUser[0].GID, ShouldEqual, 0)
			So(usageUser[0].UID, ShouldEqual, 0)
			So(usageUser[0].Name, ShouldNotBeBlank)
			So(usageUser[0].Owner, ShouldNotBeBlank)
			So(usageUser[0].BaseDir, ShouldNotBeBlank)
		})

		Convey("serveGzippedCache serves group and user usage with gzip handling", func() {
			err := s.prewarmCaches(s.basedirs)
			So(err, ShouldBeNil)

			makeContext := func(acceptEnc string) (*gin.Context, *httptest.ResponseRecorder) {
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)

				req, err := http.NewRequest(http.MethodGet, "/", nil)
				So(err, ShouldBeNil)

				if acceptEnc != "" {
					req.Header.Set("Accept-Encoding", acceptEnc)
				}

				c.Request = req

				return c, w
			}
			c, w := makeContext("")
			s.serveGzippedCache(c, s.userUsageCache)
			So(w.Header().Get("Content-Encoding"), ShouldEqual, "gzip")

			c, w = makeContext("gzip")
			s.serveGzippedCache(c, s.userUsageCache)
			So(w.Header().Get("Content-Encoding"), ShouldEqual, "gzip")

			c, w = makeContext("gzip;q=0")
			s.serveGzippedCache(c, s.userUsageCache)
			So(w.Header().Get("Content-Encoding"), ShouldNotEqual, "gzip")

			c, w = makeContext("*;q=1")
			s.serveGzippedCache(c, s.userUsageCache)
			So(w.Header().Get("Content-Encoding"), ShouldEqual, "gzip")
		})
	})
}

type analyticsData struct {
	Name, Session, Data string
	Time                int64
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

	refTime := time.Now().Unix()

	Convey("Given databases", func() {
		jwtBasename := ".wrstat.test.jwt"
		serverTokenBasename := ".wrstat.test.servertoken" //nolint:gosec

		c, err := gas.NewClientCLI(jwtBasename, serverTokenBasename, "localhost:1", cert, true)
		So(err, ShouldBeNil)

		_, _, err = GetWhereDataIs(c, "", "", "", "", db.DGUTAgeAll, "")
		So(err, ShouldNotBeNil)

		path, err := internaldb.CreateExampleDBsCustomIDs(t, uid, gids[0], gids[1], refTime)
		So(err, ShouldBeNil)

		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		c, err = gas.NewClientCLI(jwtBasename, serverTokenBasename, addr, cert, false)
		So(err, ShouldBeNil)

		Convey("You can't get where data is or add the tree page without auth", func() {
			err = s.LoadDBs([]string{path}, "dirguta", "basedir.db", ownersPath)
			So(err, ShouldBeNil)

			_, _, err = GetWhereDataIs(c, "/", "", "", "", db.DGUTAgeAll, "")
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, gas.ErrNoAuth)

			err = s.AddTreePage()
			So(err, ShouldNotBeNil)
		})

		Convey("Root can see everything", func() {
			err = s.EnableAuthWithServerToken(cert, key, serverTokenBasename, func(username, password string) (bool, string) {
				return true, ""
			})
			So(err, ShouldBeNil)

			err = s.LoadDBs([]string{path}, "dirguta", "basedir.db", ownersPath)
			So(err, ShouldBeNil)

			err = c.Login("user", "pass")
			So(err, ShouldBeNil)

			_, _, err = GetWhereDataIs(c, " ", "", "", "", db.DGUTAgeAll, "")
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, ErrBadQuery)

			json, dcss, errg := GetWhereDataIs(c, "/", "", "", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 24)

			json, dcss, errg = GetWhereDataIs(c, "/", g.Name, "", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 13)

			json, dcss, errg = GetWhereDataIs(c, "/", "", "root", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 14)

			json, dcss, errg = GetWhereDataIs(c, "/", "", "", "", db.DGUTAgeA7Y, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 19)
		})

		Convey("Normal users have access restricted only by group", func() {
			err = s.EnableAuth(cert, key, func(username, password string) (bool, string) {
				return true, uid
			})
			So(err, ShouldBeNil)

			err = s.LoadDBs([]string{path}, "dirguta", "basedir.db", ownersPath)
			So(err, ShouldBeNil)

			err = c.Login("user", "pass")
			So(err, ShouldBeNil)

			json, dcss, errg := GetWhereDataIs(c, "/", "", "", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 23)

			json, dcss, errg = GetWhereDataIs(c, "/", g.Name, "", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 13)

			_, _, errg = GetWhereDataIs(c, "/", "", "root", "", db.DGUTAgeAll, "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 13)
		})

		Convey("Once you add the tree page", func() {
			var logWriter strings.Builder
			s := New(&logWriter)

			err = s.EnableAuth(cert, key, func(username, password string) (bool, string) {
				return true, uid
			})
			So(err, ShouldBeNil)

			err = s.LoadDBs([]string{path}, "dirguta", "basedir.db", ownersPath)
			So(err, ShouldBeNil)

			s.dataTimeStamp = map[string]int64{}

			s.gidToNameCache[1] = "GroupA"
			s.gidToNameCache[2] = "GroupB"
			s.gidToNameCache[3] = "GroupC"
			s.gidToNameCache[77777] = "77777"
			s.uidToNameCache[101] = "UserA"
			s.uidToNameCache[102] = "UserB"
			s.uidToNameCache[103] = "UserC"
			s.uidToNameCache[88888] = "88888"

			s.basedirs.SetCachedGroup(1, "GroupA")
			s.basedirs.SetCachedGroup(2, "GroupB")
			s.basedirs.SetCachedGroup(3, "GroupC")
			s.basedirs.SetCachedUser(77777, "77777")
			s.basedirs.SetCachedUser(101, "UserA")
			s.basedirs.SetCachedUser(102, "UserB")
			s.basedirs.SetCachedUser(103, "UserC")
			s.basedirs.SetCachedUser(88888, "88888")

			err = s.AddTreePage()
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, cert, key)
			So(err, ShouldBeNil)
			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

			token, err := gas.Login(gas.NewClientRequest(addr, cert), "user", "pass")
			So(err, ShouldBeNil)

			Convey("You can get the static tree web page", func() {
				r := gas.NewAuthenticatedClientRequest(addr, cert, token)

				resp, err := r.Get("tree/tree.html")
				So(err, ShouldBeNil)
				So(strings.ToUpper(string(resp.Body())), ShouldStartWith, "<!DOCTYPE HTML>")

				resp, err = r.Get("")
				So(err, ShouldBeNil)
				So(strings.ToUpper(string(resp.Body())), ShouldStartWith, "<!DOCTYPE HTML>")
			})

			Convey("You can send data to the analytics endpoint", func() {
				So(s.InitAnalyticsDB(filepath.Join(t.TempDir(), "db")), ShouldBeNil)

				getAndClear := func() []analyticsData {
					r, errr := s.analyticsDB.Query("SELECT user, session, state, time FROM [events];")
					So(errr, ShouldBeNil)

					var rows []analyticsData

					for r.Next() {
						var ad analyticsData

						So(r.Scan(&ad.Name, &ad.Session, &ad.Data, &ad.Time), ShouldBeNil)

						rows = append(rows, ad)
					}

					r.Close()

					_, err = s.analyticsDB.Exec("DELETE FROM [events];")
					So(err, ShouldBeNil)

					return rows
				}

				var start, end int64

				sessionID := "AAA"
				sendBeacon := func(referers ...string) {
					start = time.Now().Unix()

					for _, referer := range referers {
						r := gas.NewClientRequest(addr, cert)
						r.Cookies = append(r.Cookies, &http.Cookie{Name: "jwt", Value: token})
						r.Body = sessionID

						r.Header.Set("Referer", referer)

						_, err = r.Post(EndPointAuthSpyware)
						So(err, ShouldBeNil)
					}

					end = time.Now().Unix() + 1
				}

				checkTimes := func(data []analyticsData) {
					for n := range data {
						So(data[n].Time, ShouldBeBetweenOrEqual, start, end)

						data[n].Time = 0
					}
				}

				sendBeacon("")

				d := getAndClear()

				checkTimes(d)

				So(d, ShouldResemble, []analyticsData{
					{Name: "user", Session: "AAA", Data: "{}\n"},
				})

				sendBeacon(`?useCount=true&owners=["a","bc"]`, `?filterMaxSize=123&users=[1,2,3]&byUser="badString"`)

				d = getAndClear()

				checkTimes(d)

				So(d, ShouldResemble, []analyticsData{
					{Name: "user", Session: "AAA", Data: "{\"owners\":[\"a\",\"bc\"],\"useCount\":true}\n"},
					{Name: "user", Session: "AAA", Data: "{\"filterMaxSize\":123,\"users\":[1,2,3]}\n"},
				})
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

				unsortedGroups := gidsToGroups(t, gids[0], gids[1], "0")
				groups := make([]string, len(unsortedGroups))
				copy(groups, unsortedGroups)
				sort.Strings(groups)

				expectedFTs := []string{"bam", "cram", "dir", "temp"}
				expectedAtime := "1970-01-01T00:00:50Z"
				expectedMtime := "1970-01-01T00:01:30Z"

				const numRootDirectories = 13

				const numADirectories = 12

				const directorySize = 4096

				tm := *resp.Result().(*TreeElement) //nolint:forcetypeassert

				rootExpectedMtime := tm.Mtime
				So(len(tm.Children), ShouldBeGreaterThan, 1)
				kExpectedAtime := tm.Children[1].Atime
				So(tm, ShouldResemble, TreeElement{
					Name:        "/",
					Path:        "/",
					Count:       24 + numRootDirectories + 1,
					Size:        141 + (numRootDirectories+1)*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.Range7Years,
					Mtime:       rootExpectedMtime,
					CommonMTime: summary.Range7Years,
					Users:       users,
					Groups:      groups,
					FileTypes:   expectedFTs,
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "a",
							Path:        "/a",
							Count:       19 + numADirectories,
							Size:        126 + numADirectories*directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.Range7Years,
							Mtime:       expectedMtime,
							CommonMTime: summary.Range7Years,
							Users:       users,
							Groups:      groups,
							FileTypes:   expectedFTs,
							HasChildren: true,
							Children:    nil,
						},
						{
							Name:        "k",
							Path:        "/k",
							Count:       5 + 1,
							Size:        15 + 1*directorySize,
							Atime:       kExpectedAtime,
							CommonATime: summary.RangeLess1Month,
							Mtime:       rootExpectedMtime,
							CommonMTime: summary.RangeLess1Month,
							Users:       []string{username},
							Groups:      []string{unsortedGroups[1]},
							FileTypes:   []string{"cram", "dir"},
							HasChildren: false,
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

				expectedMtime2 := "1970-01-01T00:01:20Z"

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert
				So(tm, ShouldResemble, TreeElement{
					Name:        "/",
					Path:        "/",
					Count:       13 + 9,
					Size:        120 + 9*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.Range7Years,
					Mtime:       expectedMtime2,
					CommonMTime: summary.Range7Years,
					Users:       users,
					Groups:      []string{g.Name},
					FileTypes:   expectedFTs,
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "a",
							Path:        "/a",
							Count:       13 + 8,
							Size:        120 + 8*directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.Range7Years,
							Mtime:       expectedMtime2,
							CommonMTime: summary.Range7Years,
							Users:       users,
							Groups:      []string{g.Name},
							FileTypes:   expectedFTs,
							HasChildren: true,
							Children:    nil,
						},
					},
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path": "/a",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				abgroups := gidsToGroups(t, g.Gid, "0")
				sort.Strings(abgroups)

				acgroups := gidsToGroups(t, gids[1])
				cramAndDir := []string{"cram", "dir"}

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert
				So(tm, ShouldResemble, TreeElement{
					Name:        "a",
					Path:        "/a",
					Count:       19 + numADirectories,
					Size:        126 + numADirectories*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.Range7Years,
					Mtime:       expectedMtime,
					CommonMTime: summary.Range7Years,
					Users:       users,
					Groups:      groups,
					FileTypes:   expectedFTs,
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "b",
							Path:        "/a/b",
							Count:       19 - 5 + numADirectories - 3,
							Size:        126 - 5 + (numADirectories-3)*directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.Range7Years,
							Mtime:       expectedMtime2,
							CommonMTime: summary.Range7Years,
							Users:       users,
							Groups:      abgroups,
							FileTypes:   expectedFTs,
							HasChildren: true,
							Children:    nil,
						},
						{
							Name:        "c",
							Path:        "/a/c",
							Count:       7,
							Size:        5 + 2*directorySize,
							Atime:       "1970-01-01T00:01:30Z",
							CommonATime: summary.Range7Years,
							Mtime:       expectedMtime,
							CommonMTime: summary.Range7Years,
							Users:       []string{"root"},
							Groups:      acgroups,
							FileTypes:   cramAndDir,
							HasChildren: true,
							Children:    nil,
						},
					},
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path": "/a/b/d",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				dgroups := gidsToGroups(t, gids[0], "0")
				sort.Strings(dgroups)

				root := []string{"root"}

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert
				So(tm, ShouldResemble, TreeElement{
					Name:        "d",
					Path:        "/a/b/d",
					Count:       12 + 5,
					Size:        111 + 5*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.Range7Years,
					Mtime:       "1970-01-01T00:01:15Z",
					CommonMTime: summary.Range7Years,
					Users:       users,
					Groups:      dgroups,
					FileTypes:   cramAndDir,
					HasChildren: true,
					NoAuth:      false,
					Children: []*TreeElement{
						{
							Name:        "f",
							Path:        "/a/b/d/f",
							Count:       2,
							Size:        10 + directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.RangeLess1Month,
							Mtime:       "1970-01-01T00:00:50Z",
							CommonMTime: summary.Range7Years,
							Users:       []string{username},
							Groups:      []string{g.Name},
							FileTypes:   cramAndDir,
							HasChildren: false,
							Children:    nil,
							NoAuth:      false,
						},
						{
							Name:        "g",
							Path:        "/a/b/d/g",
							Count:       11,
							Size:        100 + directorySize,
							Atime:       "1970-01-01T00:00:50Z",
							CommonATime: summary.Range7Years,
							Mtime:       "1970-01-01T00:01:15Z",
							CommonMTime: summary.Range7Years,
							Users:       users,
							Groups:      []string{g.Name},
							FileTypes:   cramAndDir,
							HasChildren: false,
							Children:    nil,
							NoAuth:      false,
						},
						{
							Name:        "i",
							Path:        "/a/b/d/i",
							Count:       3,
							Size:        1 + 2*directorySize,
							Atime:       expectedAtime,
							CommonATime: summary.RangeLess1Month,
							Mtime:       "1970-01-01T00:00:50Z",
							CommonMTime: summary.Range7Years,
							Users:       root,
							Groups:      root,
							FileTypes:   cramAndDir,
							HasChildren: true,
							Children:    nil,
							NoAuth:      true,
						},
					},
				})

				r = gas.NewAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path": "/a/b/d/i",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert
				So(tm, ShouldResemble, TreeElement{
					Name:        "i",
					Path:        "/a/b/d/i",
					Count:       3,
					Size:        1 + 2*directorySize,
					Atime:       expectedAtime,
					CommonATime: summary.RangeLess1Month,
					Mtime:       "1970-01-01T00:00:50Z",
					CommonMTime: summary.Range7Years,
					Users:       root,
					Groups:      root,
					FileTypes:   cramAndDir,
					HasChildren: true,
					Children:    nil,
					NoAuth:      true,
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
				c, err = gas.NewClientCLI(jwtBasename, serverTokenBasename, addr, cert, false)
				So(err, ShouldBeNil)

				err = c.Login("user", "pass")
				So(err, ShouldBeNil)

				_, err := GetGroupAreas(c)
				So(err, ShouldNotBeNil)

				expectedAreas := map[string][]string{
					"a": {"1", "2"},
					"b": {"3", "4"},
				}

				s.AddGroupAreas(expectedAreas)

				areas, err := GetGroupAreas(c)
				So(err, ShouldBeNil)
				So(areas, ShouldResemble, expectedAreas)
			})

			Convey("You can access the secure basedirs endpoints after LoadDBs()", func() {
				r := gas.NewAuthenticatedClientRequest(addr, cert, token)

				var usage []*basedirs.Usage

				resp, err := r.SetResult(&usage).
					ForceContentType("application/json").
					Get(EndPointAuthBasedirUsageUser)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(usage), ShouldEqual, 34)
				So(usage[0].UID, ShouldEqual, 0)

				userUsageUID := usage[0].UID
				userUsageBasedir := usage[0].BaseDir

				resp, err = r.SetResult(&usage).
					ForceContentType("application/json").
					Get(EndPointAuthBasedirUsageGroup)
				So(err, ShouldBeNil)
				So(resp.Result(), ShouldNotBeNil)
				So(len(usage), ShouldEqual, 51)
				So(usage[0].GID, ShouldEqual, 0)

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
				So(len(subdirs), ShouldEqual, 0)

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

				Convey("and can read subdirs from a different group if you're on the whitelist", func() {
					s.WhiteListGroups(func(_ string) bool {
						return true
					})

					s.userToGIDs = make(map[string][]string)

					resp, err = r.SetResult(&subdirs).
						ForceContentType("application/json").
						SetQueryParams(map[string]string{
							"id":      fmt.Sprintf("%d", usage[0].GID),
							"basedir": usage[0].BaseDir,
						}).
						Get(EndPointAuthBasedirSubdirGroup)
					So(err, ShouldBeNil)
					So(resp.Result(), ShouldNotBeNil)
					So(len(subdirs), ShouldEqual, 1)

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
				})
			})
		})
	})
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

// testRestrictedGroups does tests for s.getRestrictedGIDs() if user running the
// test has enough groups to make the test viable.
func testRestrictedGroups(t *testing.T, gids []string, s *Server, exampleGIDs []string,
	addr, certPath, token, tokenBadUID string,
) {
	t.Helper()

	if len(gids) < 3 {
		return
	}

	var (
		filterGIDs []uint32
		errg       error
	)

	s.AuthRouter().GET("/groups", func(c *gin.Context) {
		filterGIDs = nil

		groups := c.Query("groups")

		filterGIDs, errg = s.getRestrictedGIDs(c, groups)
	})

	groups := gidsToGroups(t, gids...)
	r := gas.NewAuthenticatedClientRequest(addr, certPath, token)
	_, err := r.Get(gas.EndPointAuth + "/groups?groups=" + groups[0])
	So(err, ShouldBeNil)

	So(errg, ShouldBeNil)

	gid0, err := strconv.Atoi(exampleGIDs[0])
	So(err, ShouldBeNil)

	So(filterGIDs, ShouldResemble, []uint32{uint32(gid0)})

	r = gas.NewAuthenticatedClientRequest(addr, certPath, token)
	_, err = r.Get(gas.EndPointAuth + "/groups?groups=0")
	So(err, ShouldBeNil)

	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)

	s.userToGIDs = make(map[string][]string)

	rBadUID := gas.NewAuthenticatedClientRequest(addr, certPath, tokenBadUID)
	_, err = rBadUID.Get(gas.EndPointAuth + "/groups?groups=" + groups[0])
	So(err, ShouldBeNil)
	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)

	s.WhiteListGroups(func(gid string) bool {
		return gid == gids[0]
	})

	s.userToGIDs = make(map[string][]string)

	r = gas.NewAuthenticatedClientRequest(addr, certPath, token)
	_, err = r.Get(gas.EndPointAuth + "/groups?groups=root")
	So(err, ShouldBeNil)

	So(errg, ShouldBeNil)
	So(filterGIDs, ShouldResemble, []uint32{0})

	s.WhiteListGroups(func(group string) bool {
		return false
	})

	s.userToGIDs = make(map[string][]string)

	r = gas.NewAuthenticatedClientRequest(addr, certPath, token)
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
	groups := []string{groupA, groupB}
	sort.Strings(groups)

	for i, ds := range expected {
		expectedNonRoot[i] = ds

		switch ds.Dir {
		case "/a":
			expectedNonRoot[i] = &DirSummary{
				Dir:       ds.Dir,
				Count:     18,
				Size:      125,
				Atime:     time.Unix(50, 0),
				Mtime:     time.Unix(90, 0),
				Users:     ds.Users,
				Groups:    groups,
				FileTypes: ds.FileTypes,
			}

			expectedGroupsRoot = ds.Groups
		case "/a/b", "/a/b/d":
			expectedNonRoot[i] = &DirSummary{
				Dir:       ds.Dir,
				Count:     ds.Count - 1,
				Size:      ds.Size - 1,
				Atime:     ds.Atime,
				Mtime:     ds.Mtime,
				Users:     ds.Users,
				Groups:    []string{groupA},
				FileTypes: ds.FileTypes,
			}
		case "/":
			expectedNonRoot[i] = &DirSummary{
				Dir:       ds.Dir,
				Count:     ds.Count - 1,
				Size:      ds.Size - 1,
				Atime:     ds.Atime,
				Mtime:     ds.Mtime,
				Users:     ds.Users,
				Groups:    groups,
				FileTypes: ds.FileTypes,
			}
		}
	}

	return expectedNonRoot, expectedGroupsRoot
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

// decodeUsageResult decodes the result of a basedirs usage query.
func decodeUsageResult(response *httptest.ResponseRecorder) ([]*basedirs.Usage, error) {
	var result []*basedirs.Usage

	var reader io.Reader = response.Body

	if response.Header().Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(response.Body)
		if err != nil {
			return nil, err
		}

		defer gz.Close()
		reader = gz
	}

	err := json.NewDecoder(reader).Decode(&result)
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
