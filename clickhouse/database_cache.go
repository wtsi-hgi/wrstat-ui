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
	"sync"

	"github.com/wtsi-hgi/wrstat-ui/db"
)

const (
	treeChildrenCacheMaxEntries = 4096
	treeDGUTACacheMaxEntries    = 8192
)

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

type treeQueryCache struct {
	mu sync.RWMutex

	children      map[treeCacheKey][]string
	childrenOrder []treeCacheKey

	dgutas     map[treeCacheKey]db.GUTAs
	dgutaOrder []treeCacheKey
}

func newTreeQueryCache() *treeQueryCache {
	return &treeQueryCache{
		children: make(map[treeCacheKey][]string),
		dgutas:   make(map[treeCacheKey]db.GUTAs),
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
