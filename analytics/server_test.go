//go:build cgo

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

package analytics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/server"
)

func TestServer(t *testing.T) {
	Convey("Given a wrstat-ui server and an analytics server", t, func() {
		tmp := t.TempDir()
		dbPath := filepath.Join(tmp, "db")

		srv := server.New(io.Discard)

		certPath, keyPath, err := gas.CreateTestCert(t)
		So(err, ShouldBeNil)

		So(srv.EnableAuth(certPath, keyPath, func(username, password string) (bool, string) {
			return true, "user"
		}), ShouldBeNil)

		addr, dfunc, err := gas.StartTestServer(srv, certPath, keyPath)
		So(err, ShouldBeNil)

		defer func() {
			So(dfunc(), ShouldBeNil)
		}()

		So(srv.InitAnalyticsDB(dbPath), ShouldBeNil)

		l, err := net.Listen("tcp", "localhost:0")
		So(err, ShouldBeNil)

		port := l.Addr().(*net.TCPAddr).Port //nolint:errcheck,forcetypeassert
		url := fmt.Sprintf("http://127.0.0.1:%d/", port)

		So(l.Close(), ShouldBeNil)

		go StartServer(":"+strconv.Itoa(port), dbPath, addr) //nolint:errcheck

		time.Sleep(time.Second)

		request := func(endpoint string, request, dbResponse any) error {
			var buf bytes.Buffer

			So(json.NewEncoder(&buf).Encode(request), ShouldBeNil)
			resp, err := http.Post(url+endpoint, "application/json", &buf) //nolint:noctx
			So(err, ShouldBeNil)

			return json.NewDecoder(resp.Body).Decode(dbResponse)
		}

		Convey("You can retrieve the hostname of the wrstat-ui server", func() {
			var hostnameR string

			So(request("host", 0, &hostnameR), ShouldBeNil)
			So(hostnameR, ShouldEqual, addr)
		})

		Convey("You can retrieve analytics data", func() {
			sessionID := "AAA"

			r := gas.NewClientRequest(addr, certPath)
			token, errl := gas.Login(r, "user", "pass")
			So(errl, ShouldBeNil)

			addData := func(referer string) {
				r := gas.NewClientRequest(addr, certPath)
				r.Cookies = append(r.Cookies, &http.Cookie{Name: "jwt", Value: token})
				r.Body = sessionID

				r.Header.Set("Referer", referer)

				_, err := r.Post(server.EndPointAuthSpyware)
				So(err, ShouldBeNil)
			}

			now := time.Now().Unix()

			addData("")

			var dbResponse response

			So(request("analytics", summaryInput{}, &dbResponse), ShouldBeNil)
			So(dbResponse, ShouldResemble, response{})

			So(request("analytics", summaryInput{StartTime: now - 100, EndTime: now + 100}, &dbResponse), ShouldBeNil)

			fixTimes(dbResponse)

			So(dbResponse, ShouldResemble, response{
				"user": {
					"AAA": {
						{State: json.RawMessage("{}")},
					},
				},
			})

			addData("")

			So(request("analytics", summaryInput{StartTime: now - 100, EndTime: now + 100}, &dbResponse), ShouldBeNil)

			fixTimes(dbResponse)

			So(dbResponse, ShouldResemble, response{
				"user": {
					"AAA": {
						{State: json.RawMessage("{}")},
						{State: json.RawMessage("{}")},
					},
				},
			})

			sessionID = "BBB"

			addData("?byUser=true")

			So(request("analytics", summaryInput{StartTime: now - 100, EndTime: now + 100}, &dbResponse), ShouldBeNil)

			fixTimes(dbResponse)

			So(dbResponse, ShouldResemble, response{
				"user": {
					"AAA": {
						{State: json.RawMessage("{}")},
						{State: json.RawMessage("{}")},
					},
					"BBB": {
						{State: json.RawMessage(`{"byUser":true}`)},
					},
				},
			})

			dbResponse = nil

			So(request("analytics", summaryInput{StartTime: now - 200, EndTime: now - 100}, &dbResponse), ShouldBeNil)

			So(dbResponse, ShouldResemble, response{})
		})
	})
}

func fixTimes(response response) {
	for n := range response {
		us := response[n]

		for s := range us {
			sess := us[s]

			for n := range sess {
				sess[n].Timestamp = 0
			}
		}
	}
}
