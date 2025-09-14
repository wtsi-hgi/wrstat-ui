/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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

// package server provides a web server for a REST API and website.

package server

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"embed"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

//go:embed static
var staticFS embed.FS

const (
	wherePath = "/where"

	// EndPointWhere is the endpoint for making where queries if authorization
	// isn't implemented.
	EndPointWhere = gas.EndPointREST + wherePath

	// EndPointAuthWhere is the endpoint for making where queries if
	// authorization is implemented.
	EndPointAuthWhere = gas.EndPointAuth + wherePath

	groupAreasPaths = "/group-areas"

	// EndPointAuthGroupAreas is the endpoint for making queries on what the
	// group areas are, which is available if authorization is implemented.
	EndPointAuthGroupAreas = gas.EndPointAuth + groupAreasPaths

	basedirsPath            = "/basedirs"
	basedirsUsagePath       = basedirsPath + "/usage"
	basedirsGroupUsagePath  = basedirsUsagePath + "/groups"
	basedirsUserUsagePath   = basedirsUsagePath + "/users"
	basedirsSubdirPath      = basedirsPath + "/subdirs"
	basedirsGroupSubdirPath = basedirsSubdirPath + "/group"
	basedirsUserSubdirPath  = basedirsSubdirPath + "/user"
	basedirsHistoryPath     = basedirsPath + "/history"

	// EndPointBasedir* are the endpoints for making base directory related
	// queries if authorization isn't implemented.
	EndPointBasedirUsageGroup  = gas.EndPointREST + basedirsGroupUsagePath
	EndPointBasedirUsageUser   = gas.EndPointREST + basedirsUserUsagePath
	EndPointBasedirSubdirGroup = gas.EndPointREST + basedirsGroupSubdirPath
	EndPointBasedirSubdirUser  = gas.EndPointREST + basedirsUserSubdirPath
	EndPointBasedirHistory     = gas.EndPointREST + basedirsHistoryPath

	// EndPointAuthBasedir* are the endpoints for making base directory related
	// queries if authorization is implemented.
	EndPointAuthBasedirUsageGroup  = gas.EndPointAuth + basedirsGroupUsagePath
	EndPointAuthBasedirUsageUser   = gas.EndPointAuth + basedirsUserUsagePath
	EndPointAuthBasedirSubdirGroup = gas.EndPointAuth + basedirsGroupSubdirPath
	EndPointAuthBasedirSubdirUser  = gas.EndPointAuth + basedirsUserSubdirPath
	EndPointAuthBasedirHistory     = gas.EndPointAuth + basedirsHistoryPath

	// TreePath is the path to the static tree website.
	TreePath = "/tree"

	DBsUpdated = "/dbsUpdated"

	// EndPointAuthTree is the endpoint for making treemap queries when
	// authorization is implemented.
	EndPointAuthTree = gas.EndPointAuth + TreePath

	EndPointAuthDBsUpdated = gas.EndPointAuth + DBsUpdated

	defaultDir = "/"
	unknown    = "#unknown"
)

// Server is used to start a web server that provides a REST API to the dgut
// package's database, and a website that displays the information nicely.
type Server struct {
	gas.Server

	mu             sync.RWMutex
	basedirs       basedirs.MultiReader
	tree           *db.Tree
	whiteCB        WhiteListCallback
	uidToNameCache map[uint32]string
	gidToNameCache map[uint32]string
	userToGIDs     map[string][]string
	dataTimeStamp  map[string]int64
	areas          map[string][]string

	stopCh chan struct{}

	analyticsDB     *sql.DB
	analyticsStmt   *sql.Stmt
	groupUsageCache usageCache
	userUsageCache  usageCache

	// ClickHouse client (optional; used when WRSTAT_USE_CLICKHOUSE=1)
	ch *clickhouse.Clickhouse
}

// usageCache holds precomputed JSON data for a response.
// jsonData: the uncompressed JSON payload.
// gzipData: the gzip-compressed JSON payload for clients that support it.
type usageCache struct {
	jsonData []byte
	gzipData []byte
}

// New creates a Server which can serve a REST API and website.
//
// It logs to the given io.Writer, which could for example be syslog using the
// log/syslog pkg with syslog.new(syslog.LOG_INFO, "tag").
func New(logWriter io.Writer) *Server {
	s := &Server{
		Server:         *gas.New(logWriter),
		uidToNameCache: make(map[uint32]string),
		gidToNameCache: make(map[uint32]string),
		userToGIDs:     make(map[string][]string),
		stopCh:         make(chan struct{}),
	}

	s.SetStopCallBack(s.stop)

	return s
}

// stop is called when the server is Stop()ped, cleaning up our additional
// properties.
func (s *Server) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.stopCh)

	if s.tree != nil {
		s.tree.Close()
		s.tree = nil
	}

	if s.ch != nil {
		_ = s.ch.Close()
		s.ch = nil
	}

	if s.analyticsDB != nil {
		s.analyticsDB.Close()
	}
}

// useClickHouseFeatureFlag returns true when the server should use the
// ClickHouse-backed endpoints instead of Bolt-backed ones. Phase 0: controlled
// by env WRSTAT_USE_CLICKHOUSE == "1".
func useClickHouseFeatureFlag() bool {
	return getenv("WRSTAT_USE_CLICKHOUSE") == "1"
}

// getenv is a tiny indirection for testing/mocking.
func getenv(k string) string {
	return os.Getenv(k)
}

// getClickHouse lazily creates and memoizes a ClickHouse client using env vars.
// Defaults are applied if variables are unset.
//
//	WRSTAT_CH_HOST (default 127.0.0.1)
//	WRSTAT_CH_PORT (default 9000)
//	WRSTAT_CH_DATABASE (default default)
//	WRSTAT_CH_USERNAME (default default)
//	WRSTAT_CH_PASSWORD (default empty)
func (s *Server) getClickHouse() (*clickhouse.Clickhouse, error) { //nolint:funlen
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ch != nil {
		return s.ch, nil
	}

	params := clickhouse.ConnectionParams{
		Host:     getenv("WRSTAT_CH_HOST"),
		Port:     getenv("WRSTAT_CH_PORT"),
		Database: getenv("WRSTAT_CH_DATABASE"),
		Username: getenv("WRSTAT_CH_USERNAME"),
		Password: getenv("WRSTAT_CH_PASSWORD"),
	}

	if params.Host == "" {
		params.Host = "127.0.0.1"
	}

	if params.Port == "" {
		params.Port = "9000"
	}

	if params.Database == "" {
		params.Database = "default"
	}

	if params.Username == "" {
		params.Username = "default"
	}

	ch, err := clickhouse.New(params)
	if err != nil {
		return nil, err
	}

	s.ch = ch

	return ch, nil
}

// notImplementedHandler returns 501 for endpoints not yet supported by the
// ClickHouse backend during the migration phases.
func notImplementedHandler(c *gin.Context) {
	c.AbortWithStatus(http.StatusNotImplemented)
}

// prewarmCaches precomputes the group and user usage caches. It serialises
// usage data into JSON and gzip. so serveGzippedCache can serve quickly.
// Returns an error if any cache build fails.
func (s *Server) prewarmCaches(bd basedirs.MultiReader) error {
	// In ClickHouse mode (phase 1), we don't use basedirs
	if useClickHouseFeatureFlag() {
		emptyJSON := []byte("[]")
		emptyGzip, err := compressGzip(emptyJSON)
		if err != nil {
			// Fallback to uncompressed bytes if gzip fails
			emptyGzip = emptyJSON
		}

		s.groupUsageCache = usageCache{
			jsonData: emptyJSON,
			gzipData: emptyGzip,
		}

		s.userUsageCache = usageCache{
			jsonData: emptyJSON,
			gzipData: emptyGzip,
		}

		return nil
	}

	if err := s.buildCache(bd.GroupUsage, &s.groupUsageCache); err != nil {
		return err
	}

	return s.buildCache(bd.UserUsage, &s.userUsageCache)
}

// buildCache computes usage data, serialises it, compresses it, and stores it
// in the cache.
func (s *Server) buildCache(
	usageFunc func(db.DirGUTAge) ([]*basedirs.Usage, error),
	cache *usageCache,
) error {
	results, err := s.collectUsage(usageFunc)
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(results)
	if err != nil {
		return err
	}

	gzipData, err := compressGzip(jsonData)
	if err != nil {
		return err
	}

	*cache = usageCache{
		jsonData: jsonData,
		gzipData: gzipData,
	}

	return nil
}

// collectUsage runs the usage function across all DirGUTAge values and combines
// results.
func (s *Server) collectUsage(
	usageFunc func(db.DirGUTAge) ([]*basedirs.Usage, error),
) ([]*basedirs.Usage, error) {
	var results []*basedirs.Usage

	for _, age := range db.DirGUTAges {
		result, err := usageFunc(age)
		if err != nil {
			return nil, err
		}

		results = append(results, result...)
	}

	return results, nil
}

// compressGzip compresses JSON into gzip format.
func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if _, err := gz.Write(data); err != nil {
		return nil, err
	}

	if err := gz.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
