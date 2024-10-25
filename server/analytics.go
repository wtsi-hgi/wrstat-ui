package server

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
)

var (
	sessionPool     = sync.Pool{New: func() any { return &[14]byte{} }}
	boolPool        = sync.Pool{New: func() any { return new(bool) }}
	intPool         = sync.Pool{New: func() any { return new(int) }}
	intSlicePool    = sync.Pool{New: func() any { return new([]int) }}
	stringPool      = sync.Pool{New: func() any { return new(string) }}
	stringSlicePool = sync.Pool{New: func() any { return new([]string) }}
)

func (s *Server) InitAnalyticsDB(dbPath string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(1)

	for _, table := range [...]string{
		`CREATE TABLE IF NOT EXISTS [events] (user TEXT, session TEXT, state TEXT, time INTEGER)`,
		`CREATE INDEX IF NOT EXISTS username ON [events] (user)`,
		`CREATE INDEX IF NOT EXISTS sessionID ON [events] (session)`,
	} {
		if _, err := db.Exec(table); err != nil {
			return err
		}
	}

	s.analyticsStmt, err = db.Prepare(
		"INSERT INTO [events] (user, session, state, time) VALUES (?, ?, ?, ?);")

	if err != nil {
		return err
	}

	s.analyticsDB = db

	return nil
}

func (s *Server) recordAnalytics(c *gin.Context) {
	code, err := s.handleAnalytics(c)
	if err != nil {
		c.AbortWithError(code, err)
	} else {
		c.AbortWithStatus(code)
	}
}

func (s *Server) handleAnalytics(c *gin.Context) (int, error) {
	if s.analyticsDB == nil {
		return http.StatusServiceUnavailable, nil
	}

	username, state, code, err := s.dataFromHeaders(c)
	if code != 0 || err != nil {
		return code, err
	}

	data := sessionPool.Get().(*[14]byte)
	defer sessionPool.Put(data)

	n, err := io.ReadFull(c.Request.Body, data[:])
	if err != nil && err != io.ErrUnexpectedEOF || n == 0 {
		return http.StatusBadRequest, err

	}

	if _, err := s.analyticsStmt.Exec(
		username,
		unsafe.String(&data[0], n),
		createStateData(state),
		time.Now().Unix(),
	); err != nil {
		return http.StatusInternalServerError, nil
	}

	return http.StatusNoContent, nil
}

func (s *Server) dataFromHeaders(c *gin.Context) (string, url.Values, int, error) {
	ar := s.AuthRouter()
	if ar == nil {
		return "", nil, http.StatusInternalServerError, nil
	}

	u, err := url.Parse(c.Request.Referer())
	if err != nil {
		return "", nil, http.StatusBadRequest, err
	}

	jwt, _ := c.Cookie("jwt")

	c.Request.Header.Set("Authorization", "Bearer "+jwt)

	for _, h := range ar.Handlers {
		h(c)
	}

	username := s.GetUser(c)
	if username == nil {
		return "", nil, http.StatusUnauthorized, nil
	}

	return username.Username, u.Query(), 0, nil
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

	json.NewEncoder(&stateData).Encode(stateMap)

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
