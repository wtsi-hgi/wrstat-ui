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
	"github.com/wtsi-ssg/wrstat/v3/dgut"
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
	username, uid, gids := getUserAndGroups(t)
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
			defer dfunc()

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
				path, err := createExampleDB(t, uid, gids[0], gids[1])
				So(err, ShouldBeNil)
				groupA := gidToGroup(t, gids[0])
				groupB := gidToGroup(t, gids[1])

				tree, err := dgut.NewTree(path)
				So(err, ShouldBeNil)

				expectedRaw, err := tree.Where("/", nil, 2)
				So(err, ShouldBeNil)

				expected := s.dcssToSummaries(expectedRaw)

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
								{Dir: "/a/b", Count: 9, Size: 80, Atime: expectedAtime,
									Users: expectedUsers, Groups: expectedGroupsA, FileTypes: expectedFTs},
								{Dir: "/a/b/d", Count: 7, Size: 70, Atime: expectedAtime,
									Users: expectedUsers, Groups: expectedGroupsA, FileTypes: expectedCrams},
								{Dir: "/a/b/d/g", Count: 6, Size: 60, Atime: expectedAtime,
									Users: expectedUsers, Groups: expectedGroupsA, FileTypes: expectedCrams},
								{Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: expectedAtime,
									Users: expectedUser, Groups: expectedGroupsA, FileTypes: expectedBams},
								{Dir: "/a/b/d/f", Count: 1, Size: 10, Atime: time.Unix(75, 0),
									Users: expectedUser, Groups: expectedGroupsA, FileTypes: expectedCrams},
								{Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: expectedAtime,
									Users: expectedUser, Groups: expectedGroupsA, FileTypes: expectedBams},
							}},
							{"?users=root," + username, expected},
							{"?users=root", []*DirSummary{
								{Dir: "/a", Count: 10, Size: 46, Atime: expectedAtime,
									Users: expectedRoot, Groups: expectedGroupsRoot, FileTypes: expectedCrams},
								{Dir: "/a/b/d/g", Count: 4, Size: 40, Atime: expectedAtime,
									Users: expectedRoot, Groups: expectedGroupsA, FileTypes: expectedCrams},
								{Dir: "/a/c/d", Count: 5, Size: 5, Atime: expectedAtime,
									Users: expectedRoot, Groups: expectedGroupsB, FileTypes: expectedCrams},
							}},
							{"?groups=" + groups[0] + "&users=root", []*DirSummary{
								{Dir: "/a/b/d/g", Count: 4, Size: 40, Atime: expectedAtime,
									Users: expectedRoot, Groups: expectedGroupsA, FileTypes: expectedCrams},
							}},
							{"?types=cram,bam", expectedNoTemp},
							{"?types=bam", []*DirSummary{
								{Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: expectedAtime,
									Users: expectedUser, Groups: expectedGroupsA, FileTypes: []string{"bam"}},
								{Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: expectedAtime, Users: expectedUser,
									Groups: expectedGroupsA, FileTypes: []string{"bam"}},
							}},
							{"?groups=" + groups[0] + "&users=root&types=cram,bam", []*DirSummary{
								{Dir: "/a/b/d/g", Count: 4, Size: 40, Atime: expectedAtime,
									Users: expectedRoot, Groups: expectedGroupsA, FileTypes: expectedCrams},
							}},
							{"?groups=" + groups[0] + "&users=root&types=bam", []*DirSummary{
								{Dir: "/", Count: 0, Size: 0, Atime: time.Unix(0, 0),
									Users: []string{}, Groups: []string{}, FileTypes: []string{}},
							}},
							{"?splits=0", []*DirSummary{
								{Dir: "/a", Count: 15, Size: 86, Atime: expectedAtime,
									Users: expectedUsers, Groups: expectedGroupsRoot, FileTypes: expectedFTs},
							}},
							{"?dir=/a/b/e/h", []*DirSummary{
								{Dir: "/a/b/e/h", Count: 2, Size: 10, Atime: expectedAtime,
									Users: expectedUser, Groups: expectedGroupsA, FileTypes: expectedBams},
								{Dir: "/a/b/e/h/tmp", Count: 1, Size: 5, Atime: expectedAtime,
									Users: expectedUser, Groups: expectedGroupsA, FileTypes: expectedBams},
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
						pathNew, errc := createExampleDB(t, uid, gids[1], gids[0])
						So(errc, ShouldBeNil)

						grandparentDir := filepath.Dir(filepath.Dir(path))
						newerPath := filepath.Join(grandparentDir, "newer."+exampleDgutDirParentSuffix, "0")
						err = os.MkdirAll(filepath.Dir(newerPath), dirPerms)
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

						err = s.EnableDGUTDBReloading(sentinel, grandparentDir, exampleDgutDirParentSuffix, sentinelPollFrequency)
						So(err, ShouldNotBeNil)

						file, err := os.Create(sentinel)
						So(err, ShouldBeNil)
						err = file.Close()
						So(err, ShouldBeNil)

						s.treeMutex.RLock()
						So(s.dataTimeStamp.IsZero(), ShouldBeTrue)
						s.treeMutex.RUnlock()

						err = s.EnableDGUTDBReloading(sentinel, grandparentDir, exampleDgutDirParentSuffix, sentinelPollFrequency)
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

						stop()
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
							err = os.MkdirAll(tpath, dirPerms)
							So(err, ShouldBeNil)

							return tpath
						}

						Convey("when the directory doesn't contain the suffix", func() {
							testReloadFail(".", "dgut database directory not found")
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

							testReloadFail(grandparentDir, "dgut database directory not found")
						})

						Convey("when the new database path is invalid", func() {
							tpath := makeTestPath()

							dbPath := filepath.Join(tpath, "0")
							err = os.Mkdir(dbPath, dirPerms)
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
							t := dirEntryModTime(&mockDirEntry{})
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

// testClientsOnRealServer tests our client method GetWhereDataIs and the tree
// webpage on a real listening server, if we have at least 2 gids to test with.
func testClientsOnRealServer(t *testing.T, username, uid string, gids []string, s *Server, addr, cert, key string) {
	t.Helper()

	if len(gids) < 2 {
		return
	}

	g, errg := user.LookupGroupId(gids[0])
	So(errg, ShouldBeNil)

	Convey("Given a database", func() {
		_, _, err := GetWhereDataIs("localhost:1", cert, "", "", "", "", "", "")
		So(err, ShouldNotBeNil)

		path, err := createExampleDB(t, uid, gids[0], gids[1])
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
			So(dcss[0].Count, ShouldEqual, 15)

			json, dcss, errg = GetWhereDataIs(addr, cert, token, "/", g.Name, "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 9)

			json, dcss, errg = GetWhereDataIs(addr, cert, token, "/", "", "root", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 10)
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
			So(dcss[0].Count, ShouldEqual, 14)

			json, dcss, errg = GetWhereDataIs(addr, cert, token, "/", g.Name, "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 9)

			_, _, errg = GetWhereDataIs(addr, cert, token, "/", "", "root", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 9)
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

			err = s.AddTreePage()
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, cert, key)
			So(err, ShouldBeNil)
			defer dfunc()

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

				expectedFTs := []string{"bam", "cram", "temp"}
				expectedAtime := "1970-01-01T00:00:50Z"

				tm := *resp.Result().(*TreeElement) //nolint:forcetypeassert
				So(tm, ShouldResemble, TreeElement{
					Name:        "/",
					Path:        "/",
					Count:       15,
					Size:        86,
					Atime:       expectedAtime,
					Users:       users,
					Groups:      groups,
					FileTypes:   expectedFTs,
					TimeStamp:   "0001-01-01T00:00:00Z",
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "a",
							Path:        "/a",
							Count:       15,
							Size:        86,
							Atime:       expectedAtime,
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

				tm = *resp.Result().(*TreeElement) //nolint:forcetypeassert
				So(tm, ShouldResemble, TreeElement{
					Name:        "/",
					Path:        "/",
					Count:       9,
					Size:        80,
					Atime:       expectedAtime,
					Users:       users,
					Groups:      []string{g.Name},
					FileTypes:   expectedFTs,
					TimeStamp:   "0001-01-01T00:00:00Z",
					HasChildren: true,
					Children: []*TreeElement{
						{
							Name:        "a",
							Path:        "/a",
							Count:       9,
							Size:        80,
							Atime:       expectedAtime,
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
	return gas.QueryREST(s.Router(), EndPointWhere, extra)
}

// decodeWhereResult decodes the result of a Where query.
func decodeWhereResult(response *httptest.ResponseRecorder) ([]*DirSummary, error) {
	var result []*DirSummary
	err := json.NewDecoder(response.Body).Decode(&result)

	for _, ds := range result {
		ds.Atime = ds.Atime.Local()
	}

	return result, err
}

// createExampleDB creates a temporary dgut.db from some example data that uses
// the given uid and gids, and returns the path to the database directory.
func createExampleDB(t *testing.T, uid, gidA, gidB string) (string, error) {
	t.Helper()

	dir, err := createExampleDgutDir(t)
	if err != nil {
		return dir, err
	}

	dgutData := exampleDGUTData(uid, gidA, gidB)
	data := strings.NewReader(dgutData)
	db := dgut.NewDB(dir)

	err = db.Store(data, 20)

	return dir, err
}

// createExampleDgutDir creates a temp directory structure to hold dgut db files
// in the same way that 'wrstat tidy' organises them.
func createExampleDgutDir(t *testing.T) (string, error) {
	t.Helper()

	tdir := t.TempDir()
	dir := filepath.Join(tdir, "orig."+exampleDgutDirParentSuffix, "0")
	err := os.MkdirAll(dir, dirPerms)

	return dir, err
}

// exampleDGUTData is some example DGUT data that uses the given uid and gids,
// along with root's uid.
func exampleDGUTData(uid, gidA, gidB string) string {
	data := `/	x	z	7	3	30	50
/	x	z	6	2	10	50
/	x	z	1	1	5	50
/	x	0	7	4	40	50
/	y	0	7	5	5	50
/	0	0	7	1	1	50
/a	x	z	7	3	30	50
/a	x	z	6	2	10	50
/a	x	z	1	1	5	50
/a	x	0	7	4	40	50
/a	y	0	7	5	5	50
/a	0	0	7	1	1	50
/a/b	x	z	7	3	30	50
/a/b	x	z	6	2	10	50
/a/b	x	z	1	1	5	50
/a/b	x	0	7	4	40	50
/a/b/d	x	z	7	3	30	50
/a/b/d	x	0	7	4	40	50
/a/b/d/f	x	z	7	1	10	75
/a/b/d/g	x	z	7	2	20	50
/a/b/d/g	x	0	7	4	40	50
/a/b/e	x	z	6	2	10	50
/a/b/e	x	z	1	1	5	50
/a/b/e/h	x	z	6	2	10	50
/a/b/e/h	x	z	1	1	5	50
/a/b/e/h/tmp	x	z	6	1	5	50
/a/b/e/h/tmp	x	z	1	1	5	50
/a/c	y	0	7	5	5	50
/a/c/d	y	0	7	5	5	50
`

	data = strings.ReplaceAll(data, "x", gidA)
	data = strings.ReplaceAll(data, "y", gidB)
	data = strings.ReplaceAll(data, "z", uid)

	return data
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
				Count:     14,
				Size:      85,
				Atime:     time.Unix(50, 0),
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

	for i, m := range matrix {
		t.Logf("matrix test %d", i)

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
