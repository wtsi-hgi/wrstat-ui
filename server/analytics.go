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

package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3" //
	gas "github.com/wtsi-hgi/go-authserver"
)

var (
	sessionPool     = sync.Pool{New: func() any { return &[14]byte{} }}   //nolint:gochecknoglobals
	boolPool        = sync.Pool{New: func() any { return new(bool) }}     //nolint:gochecknoglobals
	intPool         = sync.Pool{New: func() any { return new(int) }}      //nolint:gochecknoglobals
	intSlicePool    = sync.Pool{New: func() any { return new([]int) }}    //nolint:gochecknoglobals
	stringPool      = sync.Pool{New: func() any { return new(string) }}   //nolint:gochecknoglobals
	stringSlicePool = sync.Pool{New: func() any { return new([]string) }} //nolint:gochecknoglobals
)

const (
	spywarePath         = "/spyware"
	EndPointAuthSpyware = gas.EndPointAuth + spywarePath
)

// InitAnalyticsDB adds user activity recording to the server, saving users
// sessions to an SQLite database stored at the given path.
func (s *Server) InitAnalyticsDB(dbPath string) error {
	authGroup := s.AuthRouter()
	if authGroup == nil {
		return gas.ErrNeedsAuth
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(1)

	if err = initDB(db); err != nil {
		return err
	}

	s.analyticsStmt, err = db.Prepare(
		"INSERT INTO [events] (user, session, state, time) VALUES (?, ?, ?, ?);")
	if err != nil {
		return err
	}

	s.analyticsDB = db

	authGroup.POST(spywarePath, s.recordAnalytics)

	return nil
}

func initDB(db *sql.DB) error {
	var err error

	for _, cmd := range [...]string{
		`PRAGMA JOURNAL_MODE = DELETE;`,
		`PRAGMA page_size = 1024;`,
		`CREATE TABLE IF NOT EXISTS [events] (user TEXT, session TEXT, state TEXT, time INTEGER)`,
		`CREATE INDEX IF NOT EXISTS eventTime ON [events] (time)`,
		`VACUUM;`,
	} {
		if _, err = db.Exec(cmd); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) recordAnalytics(c *gin.Context) {
	code, err := s.handleAnalytics(c)
	if err != nil {
		c.AbortWithError(code, err) //nolint:errcheck
	} else {
		c.AbortWithStatus(code)
	}
}

func (s *Server) handleAnalytics(c *gin.Context) (int, error) {
	if s.analyticsDB == nil {
		return http.StatusServiceUnavailable, nil
	}

	username, state, code, err := s.dataFromHeaders(c)
	if username == "" {
		return code, err
	}

	sessionID, l, code, err := dataFromBody(c)

	defer sessionPool.Put(sessionID)

	if err != nil {
		return code, err
	}

	if _, err := s.analyticsStmt.Exec(
		username,
		unsafe.String(&sessionID[0], l),
		createStateData(state),
		time.Now().Unix(),
	); err != nil {
		return http.StatusInternalServerError, nil
	}

	return http.StatusNoContent, nil
}

func (s *Server) dataFromHeaders(c *gin.Context) (string, url.Values, int, error) {
	u, err := url.Parse(c.Request.Referer())
	if err != nil {
		return "", nil, http.StatusBadRequest, err
	}

	username := s.GetUser(c)
	if username == nil {
		return "", nil, http.StatusUnauthorized, nil
	}

	return username.Username, u.Query(), 0, nil
}

func dataFromBody(c *gin.Context) (*[14]byte, int, int, error) {
	data := sessionPool.Get().(*[14]byte) //nolint:forcetypeassert,errcheck

	n, err := io.ReadFull(c.Request.Body, data[:])
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) || n == 0 {
		return data, 0, http.StatusBadRequest, err
	}

	return data, n, 0, nil
}

func createStateData(state url.Values) string {
	stateMap := make(map[string]json.RawMessage, len(state))

	for key := range state {
		v := state.Get(key)
		pool := getPoolFromKey(key)

		j := pool.Get()
		err := json.Unmarshal([]byte(v), j)

		pool.Put(j)

		if err != nil {
			continue
		}

		stateMap[key] = json.RawMessage(unsafe.Slice(unsafe.StringData(v), len(v)))
	}

	var stateData strings.Builder

	json.NewEncoder(&stateData).Encode(stateMap) //nolint:errcheck,errchkjson

	return stateData.String()
}

func getPoolFromKey(key string) *sync.Pool {
	switch key {
	case "filterMinSize", "filterMaxSize", "filterMinDaysAgo", "filterMaxDaysAgo", "sinceLastAccess", "selectedID":
		return &intPool
	case "owners", "treeTypes":
		return &stringSlicePool
	case "groups", "users":
		return &intSlicePool
	case "useCount", "useMTime", "byUser", "viewDiskList":
		return &boolPool
	}

	return &stringPool
}
