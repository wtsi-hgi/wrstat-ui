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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/perfreport"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
)

const (
	defaultSplits           = 2
	defaultSplitsStr        = "2"
	perfHarnessNonTreeSpecs = 2
	responseCacheMaxEntries = 256
	responseContentType     = "application/json; charset=utf-8"

	queryParamDir    = "dir"
	queryParamGroups = "groups"
	queryParamPath   = "path"
	queryParamSplits = "splits"
	queryParamTypes  = "types"
	queryParamUsers  = "users"
	queryParamAge    = "age"

	authTreeResponseEndpointVersion = "auth_tree:v1"
	whereResponseEndpointVersion    = "where:v1"
)

type responseCacheKey struct {
	endpointVersion string
	activeSetID     string
	path            string
	responseShape   string
	filter          string
	permissions     string
}

func (s *Server) responseCacheKeyLocked(
	endpointVersion string,
	path string,
	responseShape string,
	filter *db.Filter,
	permissions map[uint32]bool,
) (responseCacheKey, bool) {
	activeSetID := s.activeSetID
	if activeSetID == "" {
		return responseCacheKey{}, false
	}

	return responseCacheKey{
		endpointVersion: endpointVersion,
		activeSetID:     activeSetID,
		path:            path,
		responseShape:   responseShape,
		filter:          responseFilterKey(filter),
		permissions:     responsePermissionKey(permissions),
	}, true
}

func responseFilterKey(filter *db.Filter) string {
	if filter == nil {
		return "nil"
	}

	return "gids=" + uint32ValuesKey(filter.GIDs) +
		";uids=" + uint32ValuesKey(filter.UIDs) +
		";ft=" + strconv.FormatUint(uint64(filter.FT), 10) +
		";age=" + strconv.FormatUint(uint64(filter.Age), 10)
}

func uint32ValuesKey(values []uint32) string {
	if values == nil {
		return "nil"
	}

	sorted := slices.Clone(values)
	slices.Sort(sorted)

	var b strings.Builder
	b.WriteString(strconv.Itoa(len(sorted)))
	b.WriteByte(':')

	for _, value := range sorted {
		b.WriteString(strconv.FormatUint(uint64(value), 10))
		b.WriteByte(',')
	}

	return b.String()
}

func responsePermissionKey(permissions map[uint32]bool) string {
	if permissions == nil {
		return "unrestricted"
	}

	values := make([]uint32, 0, len(permissions))
	for value := range permissions {
		values = append(values, value)
	}

	return uint32ValuesKey(values)
}

func (s *Server) serveCachedJSON(c *gin.Context, key responseCacheKey) bool {
	body, ok := s.responseCache.get(key)
	if !ok {
		return false
	}

	c.Data(http.StatusOK, responseContentType, body)

	return true
}

func (s *Server) cacheAndServeJSON(c *gin.Context, key responseCacheKey, value any) error {
	body, err := marshalResponseBody(value)
	if err != nil {
		return err
	}

	s.responseCache.put(key, body)
	c.Data(http.StatusOK, responseContentType, body)

	return nil
}

func marshalResponseBody(value any) ([]byte, error) {
	return json.Marshal(value)
}

type responseCache struct {
	mu      sync.Mutex
	entries map[responseCacheKey][]byte
	order   []responseCacheKey
}

func (c *responseCache) get(key responseCacheKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	body, ok := c.entries[key]
	if !ok {
		return nil, false
	}

	return append([]byte(nil), body...), true
}

func (c *responseCache) put(key responseCacheKey, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries == nil {
		c.entries = make(map[responseCacheKey][]byte)
	}

	if _, ok := c.entries[key]; !ok {
		c.order = append(c.order, key)
		c.evictOldest()
	}

	c.entries[key] = append([]byte(nil), body...)
}

func (c *responseCache) evictOldest() {
	for len(c.order) > responseCacheMaxEntries {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, oldest)
	}
}

// PerfHarnessOptions configures REST/CLI-server perf evidence collection.
type PerfHarnessOptions struct {
	Repeat int
	Warmup int

	BaseURL   string
	TreePath  string
	TreePaths []string

	WhereDir    string
	WhereGroups string
	WhereUsers  string
	WhereTypes  string
	WhereSplits string

	QueryCount       func() uint64
	QueryCountSource string
	CacheStats       func() (hits uint64, misses uint64)
	CacheStatsSource string
	CacheHitKeys     func() []string
}

func normalisePerfHarnessOptions(opts PerfHarnessOptions) PerfHarnessOptions {
	if opts.Repeat <= 0 {
		opts.Repeat = 1
	}

	if opts.TreePath == "" {
		opts.TreePath = defaultDir
	}

	opts.TreePaths = normalisePerfHarnessTreePaths(opts.TreePath, opts.TreePaths)

	if opts.WhereDir == "" {
		opts.WhereDir = defaultDir
	}

	if opts.WhereSplits == "" {
		opts.WhereSplits = defaultSplitsStr
	}

	return opts
}

// MeasurePerfHarness records REST tree, REST where, and CLI-shaped where
// operations using the server handlers and the shared perf report format.
func (s *Server) MeasurePerfHarness(opts PerfHarnessOptions) (perfreport.Report, error) {
	opts = normalisePerfHarnessOptions(opts)
	report := perfreport.NewReport("clickhouse_rest", opts.BaseURL, opts.Repeat, opts.Warmup)

	for _, spec := range s.perfHarnessSpecs(opts) {
		if err := s.addPerfHarnessOperation(&report, spec, opts); err != nil {
			return perfreport.Report{}, err
		}
	}

	return report, nil
}

func (s *Server) perfHarnessSpecs(opts PerfHarnessOptions) []perfHarnessSpec {
	whereValues := perfHarnessWhereValues(opts)

	specs := make([]perfHarnessSpec, 0, len(opts.TreePaths)+perfHarnessNonTreeSpecs)
	for _, path := range opts.TreePaths {
		specs = append(specs, s.perfHarnessTreeSpec(path))
	}

	return append(specs,
		s.perfHarnessWhereSpec(whereValues),
		s.perfHarnessCLIWhereSpec(opts, whereValues),
	)
}

func perfHarnessWhereValues(opts PerfHarnessOptions) url.Values {
	return url.Values{
		queryParamDir:    {opts.WhereDir},
		queryParamGroups: {opts.WhereGroups},
		queryParamUsers:  {opts.WhereUsers},
		queryParamTypes:  {opts.WhereTypes},
		queryParamSplits: {opts.WhereSplits},
	}
}

func (s *Server) perfHarnessCLIWhereSpec(opts PerfHarnessOptions, values url.Values) perfHarnessSpec {
	return perfHarnessSpec{
		name:        "cli_where",
		endpoint:    EndPointWhere,
		values:      values,
		command:     cliWhereCommandShape(opts),
		handler:     s.getWhere,
		resultCount: restWhereResultCount,
	}
}

func cliWhereCommandShape(opts PerfHarnessOptions) []string {
	command := []string{"./wrstat-ui", "where", "--dir", opts.WhereDir}

	if opts.WhereGroups != "" {
		command = append(command, "--groups", opts.WhereGroups)
	}

	if opts.WhereUsers != "" {
		command = append(command, "--users", opts.WhereUsers)
	}

	if opts.WhereTypes != "" {
		command = append(command, "--types", opts.WhereTypes)
	}

	return append(command, "--json")
}

func (s *Server) addPerfHarnessOperation(
	report *perfreport.Report,
	spec perfHarnessSpec,
	opts PerfHarnessOptions,
) error {
	if err := s.runPerfHarnessWarmups(spec, opts.Warmup); err != nil {
		return err
	}

	samples, err := s.measurePerfHarnessSamples(spec, opts)
	if err != nil {
		return err
	}

	addPerfHarnessSamples(report, spec, samples, opts)

	return nil
}

func (s *Server) measurePerfHarnessSamples(
	spec perfHarnessSpec,
	opts PerfHarnessOptions,
) ([]perfHarnessSample, error) {
	samples := make([]perfHarnessSample, 0, opts.Repeat)
	for range opts.Repeat {
		sample, err := s.measurePerfHarnessRequest(spec, opts)
		if err != nil {
			return nil, err
		}

		samples = append(samples, sample)
	}

	return samples, nil
}

func (s *Server) measurePerfHarnessRequest(
	spec perfHarnessSpec,
	opts PerfHarnessOptions,
) (perfHarnessSample, error) {
	beforeQueries := perfHarnessCounter(opts.QueryCount)
	beforeHits, beforeMisses := perfHarnessCacheStats(opts.CacheStats)
	beforeKeys := perfHarnessCacheHitKeySnapshot(opts.CacheHitKeys)
	start := time.Now()

	result, err := s.runPerfHarnessRequest(spec)
	if err != nil {
		return perfHarnessSample{}, err
	}

	afterHits, afterMisses := perfHarnessCacheStats(opts.CacheStats)
	afterKeys := perfHarnessCacheHitKeySnapshot(opts.CacheHitKeys)

	return perfHarnessSample{
		durationMS:   durationMS(time.Since(start)),
		statusCode:   nonNegativeIntToUint64(result.statusCode),
		jsonBytes:    uint64(len(result.body)),
		gzipBytes:    uint64(len(result.gzipBody)),
		queryCount:   perfHarnessDelta(beforeQueries, perfHarnessCounter(opts.QueryCount)),
		cacheHits:    perfHarnessDelta(beforeHits, afterHits),
		cacheMisses:  perfHarnessDelta(beforeMisses, afterMisses),
		cacheHitKeys: perfHarnessCacheHitKeyDelta(beforeKeys, afterKeys),
		resultCount:  spec.resultCount(result.body),
		resultDigest: perfHarnessDigest(
			result.body,
		),
	}, nil
}

func perfHarnessCounter(counter func() uint64) uint64 {
	if counter == nil {
		return 0
	}

	return counter()
}

func perfHarnessCacheStats(stats func() (uint64, uint64)) (uint64, uint64) {
	if stats == nil {
		return 0, 0
	}

	return stats()
}

func perfHarnessCacheHitKeySnapshot(keys func() []string) []string {
	if keys == nil {
		return nil
	}

	return append([]string(nil), keys()...)
}

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func nonNegativeIntToUint64(value int) uint64 {
	if value <= 0 {
		return 0
	}

	return uint64(value)
}

func perfHarnessDelta(before, after uint64) uint64 {
	if after <= before {
		return 0
	}

	return after - before
}

func perfHarnessCacheHitKeyDelta(before, after []string) []string {
	if len(after) == 0 {
		return nil
	}

	if len(before) == 0 {
		return append([]string(nil), after...)
	}

	for shared := min(len(before), len(after)); shared > 0; shared-- {
		if slices.Equal(before[len(before)-shared:], after[:shared]) {
			return append([]string(nil), after[shared:]...)
		}
	}

	return append([]string(nil), after...)
}

func perfHarnessDigest(data []byte) string {
	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func addPerfHarnessSamples(
	report *perfreport.Report,
	spec perfHarnessSpec,
	samples []perfHarnessSample,
	opts PerfHarnessOptions,
) {
	inputs := perfHarnessInputs(spec, samples, opts)
	report.AddOperationWithCounters(
		spec.name,
		inputs,
		perfHarnessDurations(samples),
		nil,
		nil,
		nil,
		perfHarnessResultCounts(samples),
	)
}

func perfHarnessDurations(samples []perfHarnessSample) []float64 {
	values := make([]float64, len(samples))
	for i, sample := range samples {
		values[i] = sample.durationMS
	}

	return values
}

func perfHarnessResultCounts(samples []perfHarnessSample) []uint64 {
	return perfHarnessSampleUints(samples, func(sample perfHarnessSample) uint64 { return sample.resultCount })
}

func perfHarnessSampleUints(
	samples []perfHarnessSample,
	value func(perfHarnessSample) uint64,
) []uint64 {
	values := make([]uint64, len(samples))
	for i, sample := range samples {
		values[i] = value(sample)
	}

	return values
}

func perfHarnessInputs(
	spec perfHarnessSpec,
	samples []perfHarnessSample,
	opts PerfHarnessOptions,
) map[string]any {
	inputs := perfHarnessBaseInputs(spec, samples, opts)

	if opts.BaseURL != "" {
		inputs["base_url"] = opts.BaseURL
	}

	if len(spec.command) > 0 {
		inputs["command"] = append([]string(nil), spec.command...)
	}

	if len(samples) > 0 {
		inputs["first_run_wall_ms"] = samples[0].durationMS
	}

	return inputs
}

func perfHarnessBaseInputs(
	spec perfHarnessSpec,
	samples []perfHarnessSample,
	opts PerfHarnessOptions,
) map[string]any {
	return map[string]any{
		"endpoint":             spec.endpoint,
		"query_params":         perfHarnessQueryParams(spec.values),
		"status_codes":         perfHarnessStatusCodes(samples),
		"json_bytes":           perfHarnessJSONBytes(samples),
		"gzip_bytes":           perfHarnessGzipBytes(samples),
		"query_count":          perfHarnessQueryCounts(samples),
		"query_count_source":   perfHarnessSource(opts.QueryCountSource, opts.QueryCount != nil),
		"cache_hits":           perfHarnessCacheHits(samples),
		"cache_misses":         perfHarnessCacheMisses(samples),
		"cache_hit_keys":       perfHarnessCacheHitKeys(samples),
		"cache_counter_source": perfHarnessSource(opts.CacheStatsSource, opts.CacheStats != nil),
		"result_digest":        perfHarnessFirstResultDigest(samples),
	}
}

func perfHarnessQueryParams(values url.Values) map[string]string {
	params := make(map[string]string, len(values))
	for key, value := range values {
		if len(value) > 0 {
			params[key] = value[0]
		}
	}

	return params
}

func perfHarnessStatusCodes(samples []perfHarnessSample) []uint64 {
	return perfHarnessSampleUints(samples, func(sample perfHarnessSample) uint64 { return sample.statusCode })
}

func perfHarnessJSONBytes(samples []perfHarnessSample) []uint64 {
	return perfHarnessSampleUints(samples, func(sample perfHarnessSample) uint64 { return sample.jsonBytes })
}

func perfHarnessGzipBytes(samples []perfHarnessSample) []uint64 {
	return perfHarnessSampleUints(samples, func(sample perfHarnessSample) uint64 { return sample.gzipBytes })
}

func perfHarnessQueryCounts(samples []perfHarnessSample) []uint64 {
	return perfHarnessSampleUints(samples, func(sample perfHarnessSample) uint64 { return sample.queryCount })
}

func perfHarnessSource(source string, configured bool) string {
	if source != "" {
		return source
	}

	if configured {
		return "harness_option"
	}

	return "not_configured"
}

func perfHarnessCacheHits(samples []perfHarnessSample) []uint64 {
	return perfHarnessSampleUints(samples, func(sample perfHarnessSample) uint64 { return sample.cacheHits })
}

func perfHarnessCacheMisses(samples []perfHarnessSample) []uint64 {
	return perfHarnessSampleUints(samples, func(sample perfHarnessSample) uint64 { return sample.cacheMisses })
}

func perfHarnessCacheHitKeys(samples []perfHarnessSample) []string {
	keys := make([]string, 0)
	for _, sample := range samples {
		keys = append(keys, sample.cacheHitKeys...)
	}

	return keys
}

func perfHarnessFirstResultDigest(samples []perfHarnessSample) string {
	if len(samples) == 0 {
		return ""
	}

	return samples[0].resultDigest
}

type perfHarnessSpec struct {
	name        string
	endpoint    string
	values      url.Values
	command     []string
	handler     func(*gin.Context)
	resultCount func([]byte) uint64
}

func (s *Server) perfHarnessTreeSpec(path string) perfHarnessSpec {
	return perfHarnessSpec{
		name:        "rest_tree",
		endpoint:    EndPointAuthTree,
		values:      url.Values{queryParamPath: {path}},
		handler:     s.getTree,
		resultCount: restTreeResultCount,
	}
}

func (s *Server) perfHarnessWhereSpec(values url.Values) perfHarnessSpec {
	return perfHarnessSpec{
		name:        "rest_where",
		endpoint:    EndPointWhere,
		values:      values,
		handler:     s.getWhere,
		resultCount: restWhereResultCount,
	}
}

func (s *Server) runPerfHarnessWarmups(spec perfHarnessSpec, warmup int) error {
	for range warmup {
		if _, err := s.runPerfHarnessRequest(spec); err != nil {
			return err
		}
	}

	return nil
}

type perfHarnessResponse struct {
	statusCode int
	body       []byte
	gzipBody   []byte
}

func (s *Server) runPerfHarnessRequest(spec perfHarnessSpec) (perfHarnessResponse, error) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		spec.endpoint+"?"+spec.values.Encode(),
		nil,
	)

	spec.handler(ctx)

	body := recorder.Body.Bytes()

	gzipBody, err := compressGzip(body)
	if err != nil {
		return perfHarnessResponse{}, err
	}

	return perfHarnessResponse{
		statusCode: recorder.Code,
		body:       append([]byte(nil), body...),
		gzipBody:   gzipBody,
	}, nil
}

type perfHarnessSample struct {
	durationMS   float64
	statusCode   uint64
	jsonBytes    uint64
	gzipBytes    uint64
	queryCount   uint64
	cacheHits    uint64
	cacheMisses  uint64
	cacheHitKeys []string
	resultCount  uint64
	resultDigest string
}

type activeSetIDProvider interface {
	ActiveSetID() string
}

func normalisePerfHarnessTreePaths(treePath string, treePaths []string) []string {
	paths := make([]string, 0, max(1, len(treePaths)))
	for _, path := range treePaths {
		if strings.TrimSpace(path) != "" {
			paths = append(paths, path)
		}
	}

	if len(paths) == 0 {
		paths = append(paths, treePath)
	}

	return paths
}

func responseSplitsKey(splits string) string {
	splitsN, err := parseSplitsValue(splits)
	if err != nil {
		return queryParamSplits + "=" + defaultSplitsStr
	}

	return queryParamSplits + "=" + strconv.FormatUint(splitsN, 10)
}

// convertSplitsValue returns a split.SplitFn that always returns the value
// specified. If the given value fails to be parsed as a Uint, the default value
// of 2 will be used.
func convertSplitsValue(splits string) split.SplitFn {
	splitsN, err := parseSplitsValue(splits)
	if err != nil {
		return split.SplitsToSplitFn(defaultSplits)
	}

	return split.SplitsToSplitFn(int(splitsN))
}

func parseSplitsValue(splits string) (uint64, error) {
	return strconv.ParseUint(splits, 10, 8)
}

func restTreeResultCount(body []byte) uint64 {
	var tree TreeElement
	if err := json.Unmarshal(body, &tree); err != nil {
		return 0
	}

	return uint64(len(tree.Children))
}

func restWhereResultCount(body []byte) uint64 {
	var summaries []*DirSummary
	if err := json.Unmarshal(body, &summaries); err != nil {
		return 0
	}

	return uint64(len(summaries))
}

// getWhere responds with a list of directory stats describing where data is on
// disks. LoadDGUTADB() must already have been called. This is called when there
// is a GET on /rest/v1/where or /rest/v1/auth/where.
func (s *Server) getWhere(c *gin.Context) {
	dir := c.DefaultQuery(queryParamDir, defaultDir)
	splits := c.DefaultQuery(queryParamSplits, defaultSplitsStr)

	filter, err := s.makeRestrictedFilterFromContext(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	allowedGIDs, err := s.allowedGIDs(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cacheKey, cacheable := s.responseCacheKeyLocked(
		whereResponseEndpointVersion,
		dir,
		responseSplitsKey(splits),
		filter,
		allowedGIDs,
	)
	if cacheable && s.serveCachedJSON(c, cacheKey) {
		return
	}

	dcss, err := s.tree.Where(dir, filter, convertSplitsValue(splits))
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	summaries := s.dcssToSummaries(dcss)
	if cacheable {
		if err := s.cacheAndServeJSON(c, cacheKey, summaries); err != nil {
			c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck
		}

		return
	}

	c.IndentedJSON(http.StatusOK, summaries)
}
