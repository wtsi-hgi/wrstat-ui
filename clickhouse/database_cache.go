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
	"sync"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	treeChildrenCacheMaxEntries     = 65536
	treeDGUTACacheMaxEntries        = 8192
	treeDirSummaryCacheMaxEntries   = 32768
	treeMountSummaryCacheMaxEntries = 1024
	treeQueryCacheMaxNamespaces     = 16
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

type treeQueryCache struct {
	mu sync.RWMutex

	children      map[treeCacheKey][]string
	childrenOrder []treeCacheKey

	dgutas     map[treeCacheKey]db.GUTAs
	dgutaOrder []treeCacheKey

	dirSummaries      map[treeDirSummaryCacheKey]*db.DirSummary
	dirSummaryOrder   []treeDirSummaryCacheKey
	mountSummaries    map[treeMountCacheKey]bool
	mountSummaryOrder []treeMountCacheKey
}

func treeQueryCacheForConfig(cfg Config) *treeQueryCache {
	return sharedTreeQueryCaches.cache(treeQueryCacheNamespace(cfg))
}

func newTreeQueryCache() *treeQueryCache {
	return &treeQueryCache{
		children:       make(map[treeCacheKey][]string),
		dgutas:         make(map[treeCacheKey]db.GUTAs),
		dirSummaries:   make(map[treeDirSummaryCacheKey]*db.DirSummary),
		mountSummaries: make(map[treeMountCacheKey]bool),
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.dirSummaries[key]; !exists {
		c.dirSummaryOrder = append(c.dirSummaryOrder, key)
		c.evictOldestDirSummaries()
	}

	c.dirSummaries[key] = cloneDirSummary(summary)
}

func (c *treeQueryCache) evictOldestDirSummaries() {
	for len(c.dirSummaryOrder) > treeDirSummaryCacheMaxEntries {
		oldest := c.dirSummaryOrder[0]
		c.dirSummaryOrder = c.dirSummaryOrder[1:]
		delete(c.dirSummaries, oldest)
	}
}

func (c *treeQueryCache) dirSummaryEntryCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.dirSummaries)
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

func resetSharedTreeQueryCachesForTesting() {
	sharedTreeQueryCaches = newTreeQueryCacheRegistry()
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
