/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

package clickhouse

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	activeMetadataQueryVersion         uint32 = 1
	activePrefixDirSummaryQueryVersion uint32 = 1

	treeActiveMetadataCacheMaxEntries      = 256
	treeActivePrefixSummaryCacheMaxEntries = 32768
	treeChildrenCacheMaxEntries            = 65536
	treeDGUTACacheMaxEntries               = 8192
	treeDirSummaryCacheMaxEntries          = 32768
	treeMountSummaryCacheMaxEntries        = 1024
	treeQueryCacheMaxNamespaces            = 16
	treeQueryCacheHitKeyMaxEntries         = 128
)

var sharedTreeQueryCaches = newTreeQueryCacheRegistry() //nolint:gochecknoglobals // process-wide provider cache

type treeCacheKey struct {
	mountPath  string
	snapshotID string
	dir        string
}

func newTreeCacheKey(mountPath, snapshotID, dir string) treeCacheKey {
	return treeCacheKey{
		mountPath:  mountPath,
		snapshotID: snapshotID,
		dir:        ensureTrailingSlash(dir),
	}
}

type treeDirSummaryCacheKey struct {
	treeCacheKey
	age  db.DirGUTAge
	mode mountDirSummaryMode
}

func newTreeDirSummaryCacheKey(
	mountPath, snapshotID, dir string,
	age db.DirGUTAge,
	mode mountDirSummaryMode,
) treeDirSummaryCacheKey {
	return treeDirSummaryCacheKey{
		treeCacheKey: newTreeCacheKey(mountPath, snapshotID, dir),
		age:          age,
		mode:         mode,
	}
}

type treeMountCacheKey struct {
	mountPath  string
	snapshotID string
}

func newTreeMountCacheKey(mountPath, snapshotID string) treeMountCacheKey {
	return treeMountCacheKey{
		mountPath:  ensureTrailingSlash(mountPath),
		snapshotID: snapshotID,
	}
}

type treeQueryVersionKey struct {
	schemaVersion uint32
	queryVersion  uint32
}

func newTreeQueryVersionKey(schemaVersion, queryVersion uint32) treeQueryVersionKey {
	return treeQueryVersionKey{
		schemaVersion: schemaVersion,
		queryVersion:  queryVersion,
	}
}

type treePermissionCacheInputs struct {
	uid  uint32
	gids []uint32
}

type treePermissionCacheKey struct {
	uid  uint32
	gids string
}

func newTreePermissionCacheKey(inputs treePermissionCacheInputs) treePermissionCacheKey {
	return treePermissionCacheKey{
		uid:  inputs.uid,
		gids: uint32SetCacheKey(inputs.gids),
	}
}

type treeFilterCacheKey struct {
	nilFilter bool
	gids      string
	uids      string
	ft        uint16
	age       uint8
}

func newTreeFilterCacheKey(filter *db.Filter) treeFilterCacheKey {
	if filter == nil {
		return treeFilterCacheKey{nilFilter: true}
	}

	return treeFilterCacheKey{
		gids: uint32SetCacheKey(filter.GIDs),
		uids: uint32SetCacheKey(filter.UIDs),
		ft:   uint16(filter.FT),
		age:  uint8(filter.Age),
	}
}

func treeFilterCacheKeyString(key treeFilterCacheKey) string {
	return "nil=" + strconv.FormatBool(key.nilFilter) +
		",gids=" + key.gids +
		",uids=" + key.uids +
		",ft=" + strconv.FormatUint(uint64(key.ft), 10) +
		",age=" + strconv.FormatUint(uint64(key.age), 10)
}

func (c *treeQueryCache) recordActivePrefixSummaryHitKey(key treeActivePrefixSummaryCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.activePrefixSummaryHitKeys = appendBoundedCacheHitKey(
		c.activePrefixSummaryHitKeys,
		activePrefixSummaryCacheHitKey(key),
	)
}

func appendBoundedCacheHitKey(keys []string, key string) []string {
	keys = append(keys, key)
	if len(keys) <= treeQueryCacheHitKeyMaxEntries {
		return keys
	}

	return append([]string(nil), keys[len(keys)-treeQueryCacheHitKeyMaxEntries:]...)
}

func activePrefixSummaryCacheHitKey(key treeActivePrefixSummaryCacheKey) string {
	return "active_prefix_summary:path=" + key.dir +
		";filter=" + treeFilterCacheKeyString(key.filter) +
		";active_set_id=" + key.activeSetID +
		";schema_version=" + strconv.FormatUint(uint64(key.version.schemaVersion), 10) +
		";query_version=" + strconv.FormatUint(uint64(key.version.queryVersion), 10)
}

func (c *treeQueryCache) recordActiveMetadataHitKey(key treeActiveMetadataCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.activeMetadataHitKeys = appendBoundedCacheHitKey(
		c.activeMetadataHitKeys,
		activeMetadataCacheHitKey(key),
	)
}

func activeMetadataCacheHitKey(key treeActiveMetadataCacheKey) string {
	return "active_metadata:active_set_id=" + key.activeSetID +
		";schema_version=" + strconv.FormatUint(uint64(key.version.schemaVersion), 10) +
		";query_version=" + strconv.FormatUint(uint64(key.version.queryVersion), 10)
}

func (c *treeQueryCache) resetStatsLocked() {
	c.activePrefixSummaryHits.Store(0)
	c.activePrefixSummaryMisses.Store(0)
	c.activeMetadataHits.Store(0)
	c.activeMetadataMisses.Store(0)
	c.activePrefixSummaryHitKeys = nil
	c.activeMetadataHitKeys = nil
}

func uint32SetCacheKey(values []uint32) string {
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

type treeActivePrefixSummaryCacheKey struct {
	activeSetID string
	dir         string
	filter      treeFilterCacheKey
	permission  treePermissionCacheKey
	version     treeQueryVersionKey
}

func newTreeActivePrefixSummaryCacheKey(
	activeSetID string,
	dir string,
	filter *db.Filter,
	permission treePermissionCacheInputs,
	schemaVersion uint32,
	queryVersion uint32,
) treeActivePrefixSummaryCacheKey {
	return treeActivePrefixSummaryCacheKey{
		activeSetID: activeSetID,
		dir:         ensureTrailingSlash(dir),
		filter:      newTreeFilterCacheKey(filter),
		permission:  newTreePermissionCacheKey(permission),
		version:     newTreeQueryVersionKey(schemaVersion, queryVersion),
	}
}

type treeActiveMetadataCacheKey struct {
	activeSetID string
	version     treeQueryVersionKey
}

func newTreeActiveMetadataCacheKey(
	activeSetID string,
	schemaVersion uint32,
	queryVersion uint32,
) treeActiveMetadataCacheKey {
	return treeActiveMetadataCacheKey{
		activeSetID: activeSetID,
		version:     newTreeQueryVersionKey(schemaVersion, queryVersion),
	}
}

type treeActiveMetadata struct {
	activeSetID string
	mounts      []activeMount
}

func newTreeActiveMetadata(activeSetID string, mounts []activeMount) treeActiveMetadata {
	return treeActiveMetadata{
		activeSetID: activeSetID,
		mounts:      slices.Clone(mounts),
	}
}

func (m treeActiveMetadata) clone() treeActiveMetadata {
	return newTreeActiveMetadata(m.activeSetID, m.mounts)
}

type treeQueryCache struct {
	mu sync.RWMutex

	children      map[treeCacheKey][]string
	childrenOrder []treeCacheKey

	dgutas     map[treeCacheKey]db.GUTAs
	dgutaOrder []treeCacheKey

	dirSummaries      map[treeDirSummaryCacheKey]*db.DirSummary
	dirSummaryCounts  map[treeDirSummaryCacheKey]uint64
	dirSummaryOrder   []treeDirSummaryCacheKey
	mountSummaries    map[treeMountCacheKey]bool
	mountSummaryOrder []treeMountCacheKey
	mountVectors      map[treeMountCacheKey]bool
	mountVectorOrder  []treeMountCacheKey
	mountAgeAll       map[treeMountCacheKey]bool
	mountAgeAllOrder  []treeMountCacheKey

	activePrefixSummaries      map[treeActivePrefixSummaryCacheKey]*db.DirSummary
	activePrefixSummaryOrder   []treeActivePrefixSummaryCacheKey
	activeMetadata             map[treeActiveMetadataCacheKey]treeActiveMetadata
	activeMetadataOrder        []treeActiveMetadataCacheKey
	activePrefixSummaryHits    atomic.Uint64
	activePrefixSummaryMisses  atomic.Uint64
	activeMetadataHits         atomic.Uint64
	activeMetadataMisses       atomic.Uint64
	activePrefixSummaryHitKeys []string
	activeMetadataHitKeys      []string
}

func treeQueryCacheForConfig(cfg Config) *treeQueryCache {
	return sharedTreeQueryCaches.cache(treeQueryCacheNamespace(cfg))
}

func newTreeQueryCache() *treeQueryCache {
	return &treeQueryCache{
		children:         make(map[treeCacheKey][]string),
		dgutas:           make(map[treeCacheKey]db.GUTAs),
		dirSummaries:     make(map[treeDirSummaryCacheKey]*db.DirSummary),
		dirSummaryCounts: make(map[treeDirSummaryCacheKey]uint64),
		mountSummaries:   make(map[treeMountCacheKey]bool),
		mountVectors:     make(map[treeMountCacheKey]bool),
		mountAgeAll:      make(map[treeMountCacheKey]bool),
		activePrefixSummaries: make(
			map[treeActivePrefixSummaryCacheKey]*db.DirSummary,
		),
		activeMetadata: make(map[treeActiveMetadataCacheKey]treeActiveMetadata),
	}
}

func (c *treeQueryCache) getChildren(key treeCacheKey) ([]string, bool) {
	c.mu.RLock()
	children, ok := c.children[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	return cloneStrings(children), true
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}

	out := make([]string, len(in))
	copy(out, in)

	return out
}

func (c *treeQueryCache) putChildren(key treeCacheKey, children []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.children[key]; !exists {
		c.childrenOrder = append(c.childrenOrder, key)
		c.evictOldestChildren()
	}

	c.children[key] = cloneStrings(children)
}

func (c *treeQueryCache) evictOldestChildren() {
	for len(c.childrenOrder) > treeChildrenCacheMaxEntries {
		oldest := c.childrenOrder[0]
		c.childrenOrder = c.childrenOrder[1:]
		delete(c.children, oldest)
	}
}

func (c *treeQueryCache) getGUTAs(key treeCacheKey) (db.GUTAs, bool) {
	c.mu.RLock()
	gutas, ok := c.dgutas[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	return cloneGUTAs(gutas), true
}

func cloneGUTAs(in db.GUTAs) db.GUTAs {
	if len(in) == 0 {
		return nil
	}

	out := make(db.GUTAs, len(in))
	for i, guta := range in {
		if guta == nil {
			continue
		}

		cp := *guta
		out[i] = &cp
	}

	return out
}

func (c *treeQueryCache) putGUTAs(key treeCacheKey, gutas db.GUTAs) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.dgutas[key]; !exists {
		c.dgutaOrder = append(c.dgutaOrder, key)
		c.evictOldestGUTAs()
	}

	c.dgutas[key] = cloneGUTAs(gutas)
}

func (c *treeQueryCache) evictOldestGUTAs() {
	for len(c.dgutaOrder) > treeDGUTACacheMaxEntries {
		oldest := c.dgutaOrder[0]
		c.dgutaOrder = c.dgutaOrder[1:]
		delete(c.dgutas, oldest)
	}
}

func (c *treeQueryCache) getDirSummary(key treeDirSummaryCacheKey) (*db.DirSummary, bool) {
	c.mu.RLock()
	summary, ok := c.dirSummaries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	return cloneDirSummary(summary), true
}

func cloneDirSummary(in *db.DirSummary) *db.DirSummary {
	if in == nil {
		return nil
	}

	out := *in
	out.UIDs = cloneUint32s(in.UIDs)
	out.GIDs = cloneUint32s(in.GIDs)

	return &out
}

func (c *treeQueryCache) putDirSummary(key treeDirSummaryCacheKey, summary *db.DirSummary) {
	c.putDirSummaryEntry(key, summary, 0, false)
}

func (c *treeQueryCache) getDirSummaryChildCount(key treeDirSummaryCacheKey) (uint64, bool) {
	c.mu.RLock()
	childCount, ok := c.dirSummaryCounts[key]
	c.mu.RUnlock()

	return childCount, ok
}

func (c *treeQueryCache) putDirSummaryWithChildCount(
	key treeDirSummaryCacheKey,
	summary *db.DirSummary,
	childCount uint64,
) {
	c.putDirSummaryEntry(key, summary, childCount, true)
}

func (c *treeQueryCache) putDirSummaryEntry(
	key treeDirSummaryCacheKey,
	summary *db.DirSummary,
	childCount uint64,
	hasChildCount bool,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.dirSummaries[key]; !exists {
		c.dirSummaryOrder = append(c.dirSummaryOrder, key)
		c.evictOldestDirSummaries()
	}

	c.dirSummaries[key] = cloneDirSummary(summary)
	if hasChildCount {
		c.dirSummaryCounts[key] = childCount
	} else {
		delete(c.dirSummaryCounts, key)
	}
}

func (c *treeQueryCache) evictOldestDirSummaries() {
	for len(c.dirSummaryOrder) > treeDirSummaryCacheMaxEntries {
		oldest := c.dirSummaryOrder[0]
		c.dirSummaryOrder = c.dirSummaryOrder[1:]
		delete(c.dirSummaries, oldest)
		delete(c.dirSummaryCounts, oldest)
	}
}

func (c *treeQueryCache) dirSummaryEntryCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.dirSummaries)
}

func (c *treeQueryCache) getActivePrefixDirSummary(
	key treeActivePrefixSummaryCacheKey,
) (*db.DirSummary, bool) {
	c.mu.RLock()
	summary, ok := c.activePrefixSummaries[key]
	c.mu.RUnlock()

	if !ok {
		c.activePrefixSummaryMisses.Add(1)

		return nil, false
	}

	c.activePrefixSummaryHits.Add(1)
	c.recordActivePrefixSummaryHitKey(key)

	return cloneDirSummary(summary), true
}

func (c *treeQueryCache) putActivePrefixDirSummary(
	key treeActivePrefixSummaryCacheKey,
	summary *db.DirSummary,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.activePrefixSummaries[key]; !exists {
		c.activePrefixSummaryOrder = append(c.activePrefixSummaryOrder, key)
		c.evictOldestActivePrefixSummaries()
	}

	c.activePrefixSummaries[key] = cloneDirSummary(summary)
}

func (c *treeQueryCache) evictOldestActivePrefixSummaries() {
	for len(c.activePrefixSummaryOrder) > treeActivePrefixSummaryCacheMaxEntries {
		oldest := c.activePrefixSummaryOrder[0]
		c.activePrefixSummaryOrder = c.activePrefixSummaryOrder[1:]
		delete(c.activePrefixSummaries, oldest)
	}
}

func (c *treeQueryCache) activePrefixSummaryEntryCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.activePrefixSummaries)
}

func (c *treeQueryCache) getActiveMetadata(key treeActiveMetadataCacheKey) (treeActiveMetadata, bool) {
	c.mu.RLock()
	metadata, ok := c.activeMetadata[key]
	c.mu.RUnlock()

	if !ok {
		c.activeMetadataMisses.Add(1)

		return treeActiveMetadata{}, false
	}

	c.activeMetadataHits.Add(1)
	c.recordActiveMetadataHitKey(key)

	return metadata.clone(), true
}

func (c *treeQueryCache) putActiveMetadata(key treeActiveMetadataCacheKey, metadata treeActiveMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.activeMetadata[key]; !exists {
		c.activeMetadataOrder = append(c.activeMetadataOrder, key)
		c.evictOldestActiveMetadata()
	}

	c.activeMetadata[key] = metadata.clone()
}

func (c *treeQueryCache) evictOldestActiveMetadata() {
	for len(c.activeMetadataOrder) > treeActiveMetadataCacheMaxEntries {
		oldest := c.activeMetadataOrder[0]
		c.activeMetadataOrder = c.activeMetadataOrder[1:]
		delete(c.activeMetadata, oldest)
	}
}

func (c *treeQueryCache) recordActiveMetadataMiss() {
	c.activeMetadataMisses.Add(1)
}

func (c *treeQueryCache) getMountDirSummaryReady(key treeMountCacheKey) bool {
	c.mu.RLock()
	ready := c.mountSummaries[key]
	c.mu.RUnlock()

	return ready
}

func (c *treeQueryCache) putMountDirSummaryReady(key treeMountCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mountSummaries[key] {
		return
	}

	c.mountSummaries[key] = true
	c.mountSummaryOrder = append(c.mountSummaryOrder, key)
	c.evictOldestMountSummaries()
}

func (c *treeQueryCache) evictOldestMountSummaries() {
	for len(c.mountSummaryOrder) > treeMountSummaryCacheMaxEntries {
		oldest := c.mountSummaryOrder[0]
		c.mountSummaryOrder = c.mountSummaryOrder[1:]
		delete(c.mountSummaries, oldest)
	}
}

func (c *treeQueryCache) getMountDirDGUTAVectorReady(key treeMountCacheKey) bool {
	c.mu.RLock()
	ready := c.mountVectors[key]
	c.mu.RUnlock()

	return ready
}

func (c *treeQueryCache) putMountDirDGUTAVectorReady(key treeMountCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mountVectors[key] {
		return
	}

	c.mountVectors[key] = true
	c.mountVectorOrder = append(c.mountVectorOrder, key)
	c.evictOldestMountDirDGUTAVectors()
}

func (c *treeQueryCache) evictOldestMountDirDGUTAVectors() {
	for len(c.mountVectorOrder) > treeMountSummaryCacheMaxEntries {
		oldest := c.mountVectorOrder[0]
		c.mountVectorOrder = c.mountVectorOrder[1:]
		delete(c.mountVectors, oldest)
	}
}

func (c *treeQueryCache) getDirFilterAgeAllReady(key treeMountCacheKey) (bool, bool) {
	c.mu.RLock()
	ready, cached := c.mountAgeAll[key]
	c.mu.RUnlock()

	return ready, cached
}

func (c *treeQueryCache) putDirFilterAgeAllReady(key treeMountCacheKey, ready bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, cached := c.mountAgeAll[key]; cached {
		c.mountAgeAll[key] = ready

		return
	}

	c.mountAgeAll[key] = ready
	c.mountAgeAllOrder = append(c.mountAgeAllOrder, key)
	c.evictOldestDirFilterAgeAllReady()
}

func (c *treeQueryCache) evictOldestDirFilterAgeAllReady() {
	for len(c.mountAgeAllOrder) > treeMountSummaryCacheMaxEntries {
		oldest := c.mountAgeAllOrder[0]
		c.mountAgeAllOrder = c.mountAgeAllOrder[1:]
		delete(c.mountAgeAll, oldest)
	}
}

func (c *treeQueryCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.children = make(map[treeCacheKey][]string)
	c.childrenOrder = nil
	c.dgutas = make(map[treeCacheKey]db.GUTAs)
	c.dgutaOrder = nil
	c.dirSummaries = make(map[treeDirSummaryCacheKey]*db.DirSummary)
	c.dirSummaryCounts = make(map[treeDirSummaryCacheKey]uint64)
	c.dirSummaryOrder = nil
	c.mountSummaries = make(map[treeMountCacheKey]bool)
	c.mountSummaryOrder = nil
	c.mountVectors = make(map[treeMountCacheKey]bool)
	c.mountVectorOrder = nil
	c.mountAgeAll = make(map[treeMountCacheKey]bool)
	c.mountAgeAllOrder = nil
	c.activePrefixSummaries = make(map[treeActivePrefixSummaryCacheKey]*db.DirSummary)
	c.activePrefixSummaryOrder = nil
	c.activeMetadata = make(map[treeActiveMetadataCacheKey]treeActiveMetadata)
	c.activeMetadataOrder = nil
	c.resetStatsLocked()
}

type treeQueryCacheStats struct {
	activePrefixSummaryHits    uint64
	activePrefixSummaryMisses  uint64
	activeMetadataHits         uint64
	activeMetadataMisses       uint64
	activePrefixSummaryHitKeys []string
	activeMetadataHitKeys      []string
}

func (c *treeQueryCache) stats() treeQueryCacheStats {
	c.mu.RLock()
	activePrefixSummaryHitKeys := cloneStrings(c.activePrefixSummaryHitKeys)
	activeMetadataHitKeys := cloneStrings(c.activeMetadataHitKeys)
	c.mu.RUnlock()

	return treeQueryCacheStats{
		activePrefixSummaryHits:    c.activePrefixSummaryHits.Load(),
		activePrefixSummaryMisses:  c.activePrefixSummaryMisses.Load(),
		activeMetadataHits:         c.activeMetadataHits.Load(),
		activeMetadataMisses:       c.activeMetadataMisses.Load(),
		activePrefixSummaryHitKeys: activePrefixSummaryHitKeys,
		activeMetadataHitKeys:      activeMetadataHitKeys,
	}
}

func (c *treeQueryCache) resetStats() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.resetStatsLocked()
}

type treeQueryCacheRegistry struct {
	mu sync.Mutex

	caches map[string]*treeQueryCache
	order  []string
}

func newTreeQueryCacheRegistry() *treeQueryCacheRegistry {
	return &treeQueryCacheRegistry{
		caches: make(map[string]*treeQueryCache),
	}
}

func (r *treeQueryCacheRegistry) cache(namespace string) *treeQueryCache {
	r.mu.Lock()
	defer r.mu.Unlock()

	if cache, ok := r.caches[namespace]; ok {
		r.promote(namespace)

		return cache
	}

	cache := newTreeQueryCache()
	r.caches[namespace] = cache
	r.order = append(r.order, namespace)
	r.evictOldest()

	return cache
}

func (r *treeQueryCacheRegistry) promote(namespace string) {
	idx := slices.Index(r.order, namespace)
	if idx < 0 || idx == len(r.order)-1 {
		return
	}

	r.order = slices.Delete(r.order, idx, idx+1)
	r.order = append(r.order, namespace)
}

func (r *treeQueryCacheRegistry) evictOldest() {
	for len(r.order) > treeQueryCacheMaxNamespaces {
		oldest := r.order[0]
		r.order = r.order[1:]
		delete(r.caches, oldest)
	}
}

func (r *treeQueryCacheRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, cache := range r.caches {
		cache.reset()
	}

	r.caches = make(map[string]*treeQueryCache)
	r.order = nil
}

// ResetTreeQueryCaches clears process-local tree query caches used by
// ClickHouse providers.
func ResetTreeQueryCaches() {
	sharedTreeQueryCaches.reset()
}

func resetSharedTreeQueryCachesForTesting() {
	ResetTreeQueryCaches()
}

func cloneUint32s(in []uint32) []uint32 {
	if len(in) == 0 {
		return nil
	}

	out := make([]uint32, len(in))
	copy(out, in)

	return out
}

func treeQueryCacheNamespace(cfg Config) string {
	hash := sha256.Sum256([]byte(cfg.DSN + "\x00" + cfg.Database))

	return hex.EncodeToString(hash[:])
}
