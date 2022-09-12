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
	"context"
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
	"sync"
	"testing"
	"time"

	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	gjwt "github.com/golang-jwt/jwt/v4"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wr/network/port"
	"github.com/wtsi-ssg/wrstat/dgut"
	"golang.org/x/sync/errgroup"
)

const dirPerms = 0755
const exampleDgutDirParentSuffix = "dgut.dbs"

// stringLogger is a thread-safe logger that logs to a string.
type stringLogger struct {
	builder *strings.Builder
	sync.RWMutex
}

// newStringLogger returns a new stringLogger.
func newStringLogger() *stringLogger {
	var builder strings.Builder

	return &stringLogger{
		builder: &builder,
	}
}

// Write passes through to our strings.Builder while being thread-safe.
func (s *stringLogger) Write(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()

	return s.builder.Write(p)
}

// String passes through to our strings.Builder while being thread-safe.
func (s *stringLogger) String() string {
	s.RLock()
	defer s.RUnlock()

	return s.builder.String()
}

// Reset passes through to our strings.Builder while being thread-safe.
func (s *stringLogger) Reset() {
	s.Lock()
	defer s.Unlock()

	s.builder.Reset()
}

func TestIDsToWanted(t *testing.T) {
	Convey("restrictIDsToWanted returns bad query if you don't want any of the given ids", t, func() {
		_, err := restrictIDsToWanted([]string{"a"}, map[string]bool{"b": true})
		So(err, ShouldNotBeNil)
	})
}

func TestServer(t *testing.T) {
	username, uid, gids := getUserAndGroups(t)
	exampleGIDs := getExampleGIDs(gids)
	exampleUser := &User{Username: "user", UID: uid}
	sentinelPollFrequency := 10 * time.Millisecond

	Convey("hasError tells you about errors", t, func() {
		So(hasError(nil, nil), ShouldBeFalse)
		So(hasError(nil, ErrBadQuery, nil), ShouldBeTrue)
	})

	Convey("You can get access to static website files", t, func() {
		envVals := []string{devEnvVal, "0"}

		for _, envVal := range envVals {
			os.Setenv(devEnvKey, envVal)
			fsys := getStaticFS()

			f, err := fsys.Open("tree.html")
			So(err, ShouldBeNil)

			clen := 15
			content := make([]byte, clen)
			n, err := f.Read(content)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, clen)
			So(string(content), ShouldEqual, "<!DOCTYPE html>")
		}
	})

	Convey("Given a Server", t, func() {
		logWriter := newStringLogger()
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
			u := &User{
				Username: username,
				UID:      "-1",
			}

			_, err := s.userGIDs(u)
			So(err, ShouldNotBeNil)
		})

		Convey("You can Start the Server", func() {
			certPath, keyPath, err := createTestCert(t)
			So(err, ShouldBeNil)

			addr, dfunc := startTestServer(s, certPath, keyPath)
			defer dfunc()

			client := resty.New()
			client.SetRootCertificate(certPath)

			resp, err := client.R().Get("http://" + addr + "/foo")
			So(err, ShouldBeNil)
			So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)

			resp, err = client.R().Get("https://" + addr + "/foo")
			So(err, ShouldBeNil)
			So(resp.StatusCode(), ShouldEqual, http.StatusNotFound)

			Convey("The jwt endpoint works after enabling it", func() {
				err = s.EnableAuth("/foo", "/bar", func(u, p string) (bool, string) {
					return false, ""
				})
				So(err, ShouldNotBeNil)

				err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, string) {
					ok := p == "pass"

					return ok, uid
				})
				So(err, ShouldBeNil)

				r := newClientRequest(addr, certPath)
				resp, err = r.Post(EndPointJWT)
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"missing Username or Password"}`)

				_, err = Login("foo", certPath, "user", "foo")
				So(err, ShouldNotBeNil)

				var token string
				token, err = Login(addr, certPath, "user", "foo")
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, ErrNoAuth)
				So(token, ShouldBeBlank)

				token, err = Login(addr, certPath, "user", "pass")
				So(err, ShouldBeNil)
				So(token, ShouldNotBeBlank)

				var called int
				var claims jwt.MapClaims
				var userI interface{}
				var gu *User

				s.authGroup.GET("/test", func(c *gin.Context) {
					called++
					userI, _ = c.Get(userKey)
					gu = s.getUser(c)
					claims = jwt.ExtractClaims(c)
				})

				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"auth header is empty"}`)

				r = newAuthenticatedClientRequest(addr, certPath, "{sdf.sdf.sdf}")
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"illegal base64 data at input byte 0"}`)

				start := time.Now()
				end := start.Add(1 * time.Minute)

				var noClaimToken string
				noClaimToken, err = makeTestToken(keyPath, start, end, false)
				So(err, ShouldBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, noClaimToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":403,"message":"you don't have permission to access this resource"}`)

				var keyPath2 string
				_, keyPath2, err = createTestCert(t)
				So(err, ShouldBeNil)

				var manualWronglySignedToken string
				manualWronglySignedToken, err = makeTestToken(keyPath2, start, end, true)
				So(err, ShouldBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, manualWronglySignedToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"crypto/rsa: verification error"}`)

				var manualCorrectlySignedToken string
				manualCorrectlySignedToken, err = makeTestToken(keyPath, start, end, true)
				So(err, ShouldBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, manualCorrectlySignedToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				var manualExpiredToken string
				manualExpiredToken, err = makeTestToken(keyPath, start, start.Add(time.Nanosecond), true)
				So(err, ShouldBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, manualExpiredToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"Token is expired"}`)

				_, err = RefreshJWT("foo", certPath, manualExpiredToken)
				So(err, ShouldNotBeNil)

				_, err = RefreshJWT(addr, certPath, manualWronglySignedToken)
				So(err, ShouldNotBeNil)

				var refreshedToken string
				refreshedToken, err = RefreshJWT(addr, certPath, manualExpiredToken)
				So(err, ShouldBeNil)
				So(refreshedToken, ShouldNotBeBlank)

				r = newAuthenticatedClientRequest(addr, certPath, refreshedToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				past := start.Add(-(2 * tokenDuration) - (2 * time.Nanosecond))
				manualExpiredToken, err = makeTestToken(keyPath, past, past.Add(time.Nanosecond), true)
				So(err, ShouldBeNil)

				_, err = RefreshJWT(addr, certPath, manualExpiredToken)
				So(err, ShouldNotBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, token)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				So(called, ShouldEqual, 3)
				So(claims[userKey], ShouldBeNil)
				So(claims[claimKeyUsername], ShouldEqual, "user")
				user, ok := userI.(*User)
				So(ok, ShouldBeTrue)
				So(user, ShouldResemble, exampleUser)
				So(gu, ShouldResemble, exampleUser)

				s.authCB = func(u, p string) (bool, string) {
					return true, "-1"
				}

				tokenBadUID, errl := Login(addr, certPath, "user", "pass")
				So(errl, ShouldBeNil)
				So(token, ShouldNotBeBlank)

				rBadUID := newAuthenticatedClientRequest(addr, certPath, tokenBadUID)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				testRestrictedGroups(t, gids, s, r, rBadUID, exampleGIDs)
			})

			Convey("authPayLoad correctly maps a User to claims, or returns none", func() {
				data := "foo"
				claims := authPayLoad(data)
				So(len(claims), ShouldEqual, 0)

				claims = authPayLoad(exampleUser)
				So(len(claims), ShouldEqual, 2)
				So(claims, ShouldResemble, jwt.MapClaims{
					claimKeyUsername: "user",
					claimKeyUID:      uid,
				})
			})

			Convey("retrieveClaimString fails with bad claims", func() {
				claims := jwt.MapClaims{"foo": []string{"bar"}}

				_, errc := retrieveClaimString(claims, "abc")
				So(errc, ShouldNotBeNil)

				str, errc := retrieveClaimString(claims, "foo")
				So(errc, ShouldNotBeNil)
				So(errc, ShouldEqual, ErrBadJWTClaim)
				So(str, ShouldBeBlank)
			})

			Convey("getUser fails without the user key having a valid value", func() {
				called := 0

				var user1, user2 *User

				s.router.GET("/test", func(c *gin.Context) {
					user1 = s.getUser(c)
					c.Keys = map[string]interface{}{userKey: "foo"}
					user2 = s.getUser(c)

					called++
				})

				r := newClientRequest(addr, certPath)
				resp, err = r.Get("https://" + addr + "/test")
				So(err, ShouldBeNil)

				So(called, ShouldEqual, 1)
				So(user1, ShouldBeNil)
				So(user2, ShouldBeNil)
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

						certPath, keyPath, err := createTestCert(t)
						So(err, ShouldBeNil)
						_, stop := startTestServer(s, certPath, keyPath)

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

		Convey("Endpoints that panic are logged", func() {
			s.router.GET("/foo", func(c *gin.Context) {
				panic("bar")
			})

			response, err := queryREST(s, "/foo", "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusInternalServerError)
			So(logWriter.String(), ShouldContainSubstring, "STATUS=500")
			So(logWriter.String(), ShouldContainSubstring, "panic")
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
			So(err, ShouldEqual, ErrNoAuth)

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

			token, errl := Login(addr, cert, "user", "pass")
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

			token, errl := Login(addr, cert, "user", "pass")
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

			addr, dfunc := startTestServer(s, cert, key)
			defer dfunc()

			token, err := Login(addr, cert, "user", "pass")
			So(err, ShouldBeNil)

			Convey("You can get the static tree web page", func() {
				r := newAuthenticatedClientRequest(addr, cert, token)

				resp, err := r.Get("tree/tree.html")
				So(err, ShouldBeNil)
				So(string(resp.Body()), ShouldStartWith, "<!DOCTYPE html>")

				resp, err = r.Get("")
				So(err, ShouldBeNil)
				So(string(resp.Body()), ShouldStartWith, "<!DOCTYPE html>")
			})

			Convey("You can access the tree API", func() {
				r := newAuthenticatedClientRequest(addr, cert, token)
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

				r = newAuthenticatedClientRequest(addr, cert, token)
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

				r = newAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path":   "/",
						"groups": "adsf@£$",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)

				r = newAuthenticatedClientRequest(addr, cert, token)
				resp, err = r.SetResult(&TreeElement{}).
					ForceContentType("application/json").
					SetQueryParams(map[string]string{
						"path": "/foo",
					}).
					Get(EndPointAuthTree)

				So(err, ShouldBeNil)
				So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)
			})
		})
	})
}

func TestServerOktaLogin(t *testing.T) {
	issuer := os.Getenv("OKTA_OAUTH2_ISSUER")
	clientID := os.Getenv("OKTA_OAUTH2_CLIENT_ID")
	secret := os.Getenv("OKTA_OAUTH2_CLIENT_SECRET")
	addr := os.Getenv("OKTA_WRSTAT_ADDR")
	certPath := os.Getenv("OKTA_WRSTAT_CERT")
	keyPath := os.Getenv("OKTA_WRSTAT_KEY")
	username := os.Getenv("OKTA_USERNAME")
	password := os.Getenv("OKTA_PASSWORD")

	if hasBlankValue(issuer, clientID, secret, addr, certPath, keyPath, username, password) {
		SkipConvey("Can't do Okta tests without the OKTA_* env vars set", t, func() {})

		return
	}

	oktaDomain := strings.TrimSuffix(issuer, "oauth2/default")

	Convey("Given a started Server with auth enabled", t, func() {
		logWriter := newStringLogger()
		s := New(logWriter)

		jwt, err := LoginWithOKTA(addr, certPath, "foo")
		So(err, ShouldNotBeNil)
		So(jwt, ShouldBeBlank)

		dfunc := startTestServerUsingAddress(addr, s, certPath, keyPath)
		defer dfunc()

		err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, string) {
			return false, ""
		})
		So(err, ShouldBeNil)

		Convey("You can't LoginWithOkta without first getting a code", func() {
			_, err = LoginWithOKTA(addr, certPath, "foo")
			So(err, ShouldNotBeNil)
		})

		Convey("After AddOIDCRoutes you can access the login-cli endpoint and LoginWithOKTA to get a JWT", func() {
			s.AddOIDCRoutes(addr, issuer, clientID, secret)
			r := newClientRequest(addr, "")

			resp, errp := r.Get(EndpointOIDCCLILogin)
			So(errp, ShouldBeNil)
			content := resp.String()
			So(content, ShouldContainSubstring, `ok12static.oktacdn.com`)
			So(content, ShouldContainSubstring, `redirect_uri&#x3d;https&#x25;3A&#x25;2F&#x25;2F`)
			So(content, ShouldContainSubstring, `callback-cli`)

			resp = authnLogin(s, r, oktaDomain, username, password, addr, clientID)

			redirectURL := resp.RawResponse.Request.URL
			So(redirectURL.String(), ShouldContainSubstring, `callback-cli?code=`)

			resp, errp = r.Get(redirectURL.String())
			So(errp, ShouldBeNil)

			code := resp.String()
			So(code, ShouldNotBeBlank)

			jwt, errp := LoginWithOKTA(addr, "", code)
			So(errp, ShouldBeNil)
			So(jwt, ShouldNotBeBlank)

			So(checkJWTWorks(t, s, addr, certPath, jwt), ShouldBeTrue)
		})

		Convey("After AddOIDCRoutes you can access the login endpoint", func() {
			s.AddOIDCRoutes(addr, issuer, clientID, secret)
			r := newClientRequest(addr, "")

			resp, errp := r.Get(EndpointOIDCLogin)
			So(errp, ShouldBeNil)
			content := resp.String()
			So(content, ShouldContainSubstring, `ok12static.oktacdn.com`)
			So(content, ShouldContainSubstring, `redirect_uri&#x3d;https&#x25;3A&#x25;2F&#x25;2F`)
			So(content, ShouldNotContainSubstring, `callback-cli`)
			So(content, ShouldContainSubstring, `callback`)
		})
	})
}

// ManualOktaAuthn is for testing purposes only.
type ManualOktaAuthn struct {
	SessionToken string `json:"sessionToken" binding:"required"`
}

// hasBlankValue returns true if any of the given values is "".
func hasBlankValue(vals ...string) bool {
	for _, val := range vals {
		if val == "" {
			return true
		}
	}

	return false
}

// createTestCert creates a self-signed cert and key, returning their paths.
func createTestCert(t *testing.T) (string, string, error) {
	t.Helper()

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert")
	keyPath := filepath.Join(dir, "key")

	cmd := exec.Command("openssl", "req", "-new", "-newkey", "rsa:4096",
		"-days", "1", "-nodes", "-x509", "-subj", "/CN=localhost",
		"-addext", "subjectAltName = DNS:localhost",
		"-keyout", keyPath, "-out", certPath,
	)

	err := cmd.Run()

	return certPath, keyPath, err
}

// makeTestToken creates a JWT signed with the key at the given path, that
// has orig_iat of start and exp of end, and includes a claimKeyUsername claim
// if withUserClaims is true.
func makeTestToken(keyPath string, start, end time.Time, withUserClaims bool) (string, error) {
	token := gjwt.New(gjwt.GetSigningMethod("RS512"))

	claims, ok := token.Claims.(gjwt.MapClaims)
	if !ok {
		return "", ErrNoAuth
	}

	if withUserClaims {
		claims[claimKeyUsername] = "root"
		claims[claimKeyUID] = ""
	}

	claims["orig_iat"] = start.Unix()
	claims["exp"] = end.Unix()

	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		return "", err
	}

	key, err := gjwt.ParseRSAPrivateKeyFromPEM(keyData)
	if err != nil {
		return "", err
	}

	return token.SignedString(key)
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
	return queryREST(s, EndPointWhere, extra)
}

// queryREST does a test GET of the given REST endpoint (start it with /), with
// extra appended (start it with ?).
func queryREST(s *Server, endpoint, extra string) (*httptest.ResponseRecorder, error) {
	response := httptest.NewRecorder()

	req, err := http.NewRequestWithContext(context.Background(), "GET", endpoint+extra, nil)
	if err != nil {
		return nil, err
	}

	s.router.ServeHTTP(response, req)

	return response, nil
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

	s.authGroup.GET("/groups", func(c *gin.Context) {
		groups := c.Query("groups")
		filterGIDs, errg = s.restrictedGroups(c, groups)
	})

	groups := gidsToGroups(t, gids...)
	_, err := r.Get(EndPointAuth + "/groups?groups=" + groups[0])
	So(err, ShouldBeNil)

	So(errg, ShouldBeNil)
	So(filterGIDs, ShouldResemble, []string{exampleGIDs[0]})

	_, err = r.Get(EndPointAuth + "/groups?groups=0")
	So(err, ShouldBeNil)

	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)

	s.userToGIDs = make(map[string][]string)

	_, err = rBadUID.Get(EndPointAuth + "/groups?groups=" + groups[0])
	So(err, ShouldBeNil)
	So(errg, ShouldNotBeNil)
	So(filterGIDs, ShouldBeNil)

	s.WhiteListGroups(func(gid string) bool {
		return gid == gids[0]
	})

	s.userToGIDs = make(map[string][]string)

	_, err = r.Get(EndPointAuth + "/groups?groups=root")
	So(err, ShouldBeNil)

	So(errg, ShouldBeNil)
	So(filterGIDs, ShouldResemble, []string{"0"})

	s.WhiteListGroups(func(group string) bool {
		return false
	})

	s.userToGIDs = make(map[string][]string)

	_, err = r.Get(EndPointAuth + "/groups?groups=root")
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

// startTestServer starts the given server using the given cert and key paths
// and returns the address and a func you should defer to stop the server.
func startTestServer(s *Server, certPath, keyPath string) (string, func()) {
	addr := getTestServerAddress()
	dfunc := startTestServerUsingAddress(addr, s, certPath, keyPath)

	return addr, dfunc
}

// getTestServerAddress determines a free port and returns localhost:port.
func getTestServerAddress() string {
	checker, err := port.NewChecker("localhost")
	So(err, ShouldBeNil)
	port, _, err := checker.AvailableRange(2)
	So(err, ShouldBeNil)

	return fmt.Sprintf("localhost:%d", port)
}

// startTestServerUsingAddress does what startTestServer, but using the given
// address.
func startTestServerUsingAddress(addr string, s *Server, certPath, keyPath string) func() {
	var g errgroup.Group

	g.Go(func() error {
		return s.Start(addr, certPath, keyPath)
	})

	<-time.After(100 * time.Millisecond)

	return func() {
		s.Stop()

		err := g.Wait()
		So(err, ShouldBeNil)
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

// authnLogin is used to login with username and password via the authn endpoint
// on the given oktaDomain, since we can't use the browser form during this
// test. Also provide a resty.Request from newClientRequest(server_address).
// Returns a resty.Response from trying to authenticate in a similar way as the
// browser would do after submitting the username/password form.
func authnLogin(s *Server, r *resty.Request, oktaDomain, username, password, addr, clientID string) *resty.Response {
	var oauthState, codeChallenge string

	handleOIDCTest := func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache")

		o := s.cliOAuth

		session := o.getSession(c)
		if session == nil {
			return
		}

		oauthState = session.Values[oauth2StateKeyCookie].(string) //nolint:errcheck,forcetypeassert
		codeChallenge = o.ccs256
	}

	s.router.GET("/login-test", handleOIDCTest)

	moa := &ManualOktaAuthn{}

	rOkta := resty.New()
	resp, err := rOkta.R().
		SetHeader("Content-Type", "application/json").
		SetBody(map[string]interface{}{"username": username, "password": password}).
		SetResult(moa).
		Post(oktaDomain + "api/v1/authn")
	So(err, ShouldBeNil)
	So(resp.String(), ShouldContainSubstring, `"status":"SUCCESS"`)

	sessionToken := moa.SessionToken
	So(sessionToken, ShouldNotBeBlank)

	_, err = r.Get("/login-test")
	So(err, ShouldBeNil)
	So(oauthState, ShouldNotBeBlank)
	So(codeChallenge, ShouldNotBeBlank)

	resp, err = rOkta.R().
		SetQueryParams(map[string]string{
			"client_id":                  clientID,
			"response_type":              oauth2AuthCodeKey,
			"response_mode":              "query",
			"scope":                      "openid email",
			"redirect_uri":               ClientProtocol + addr + EndpointAuthCLICallback,
			"state":                      oauthState,
			"sessionToken":               sessionToken,
			oauth2AuthChallengeKey:       codeChallenge,
			oauth2AuthChallengeMethodKey: oauth2AuthChallengeMethod,
		}).
		SetHeader("Accept", "application/json").
		Get(s.cliOAuth.Endpoint.AuthURL)
	So(err, ShouldBeNil)

	return resp
}

// checkJWTWorks calls LoadDGUTDBs on the given server, then sees if we can
// GetWhereDataIs using the given jwt. Returns true if it all worked, or if we
// can't do this test due to not belonging to at least 2 unix groups.
func checkJWTWorks(t *testing.T, s *Server, addr, certPath, jwt string) bool {
	t.Helper()

	_, uid, gids := getUserAndGroups(t)
	if len(gids) < 2 {
		return true
	}

	path, err := createExampleDB(t, uid, gids[0], gids[1])
	So(err, ShouldBeNil)
	err = s.LoadDGUTDBs(path)
	So(err, ShouldBeNil)

	_, _, err = GetWhereDataIs(addr, certPath, jwt, "/", "", "", "", "0")

	return err == nil
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
