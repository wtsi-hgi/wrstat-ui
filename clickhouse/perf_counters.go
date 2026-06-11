/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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

package clickhouse

import (
	"context"
	"errors"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const systemEventCountQuery = "SELECT toUInt64(sum(value)) FROM system.events WHERE event = ?"

var errSystemEventNameRequired = errors.New("clickhouse system event name is required")

// SystemEventCounter reads a monotonically increasing ClickHouse system event.
type SystemEventCounter struct {
	cfg   Config
	conn  ch.Conn
	event string
}

// NewSystemEventCounter opens a counter for a ClickHouse system.events entry.
func NewSystemEventCounter(cfg Config, event string) (*SystemEventCounter, error) {
	if event == "" {
		return nil, errSystemEventNameRequired
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	conn, err := connectFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &SystemEventCounter{cfg: cfg, conn: conn, event: event}, nil
}

// Value returns the current system.events value, or zero if the read fails.
func (c *SystemEventCounter) Value() uint64 {
	if c == nil || c.conn == nil {
		return 0
	}

	ctx, cancel := queryContext(context.Background(), queryTimeout(c.cfg))
	defer cancel()

	row := c.conn.QueryRow(ctx, systemEventCountQuery, c.event)

	var count uint64
	if err := row.Scan(&count); err != nil {
		return 0
	}

	return count
}

// Close closes the counter connection.
func (c *SystemEventCounter) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}

	conn := c.conn
	c.conn = nil

	return conn.Close()
}

// TreeQueryCacheStats is a public snapshot of process-local tree cache counters.
type TreeQueryCacheStats struct {
	ParentPacketHits           uint64
	ParentPacketMisses         uint64
	ParentPacketReads          uint64
	ChildFilterAllReads        uint64
	FactVectorReads            uint64
	ParentPacketHitKeys        []string
	ParentPacketMissKeys       []string
	ParentPacketReadKeys       []string
	ChildFilterReadKeys        []string
	ActivePrefixSummaryHits    uint64
	ActivePrefixSummaryMisses  uint64
	ActiveMetadataHits         uint64
	ActiveMetadataMisses       uint64
	ActivePrefixSummaryHitKeys []string
	ActiveMetadataHitKeys      []string
}

// ReadTreeQueryCacheStats reads process-local tree query cache counters.
func ReadTreeQueryCacheStats(cfg Config) TreeQueryCacheStats {
	stats := treeQueryCacheForConfig(cfg).stats()

	return TreeQueryCacheStats{
		ParentPacketHits:           stats.parentPacketHits,
		ParentPacketMisses:         stats.parentPacketMisses,
		ParentPacketReads:          stats.parentPacketReads,
		ChildFilterAllReads:        stats.childFilterAllReads,
		FactVectorReads:            stats.factVectorReads,
		ParentPacketHitKeys:        append([]string(nil), stats.parentPacketHitKeys...),
		ParentPacketMissKeys:       append([]string(nil), stats.parentPacketMissKeys...),
		ParentPacketReadKeys:       append([]string(nil), stats.parentPacketReadKeys...),
		ChildFilterReadKeys:        append([]string(nil), stats.childFilterReadKeys...),
		ActivePrefixSummaryHits:    stats.activePrefixSummaryHits,
		ActivePrefixSummaryMisses:  stats.activePrefixSummaryMisses,
		ActiveMetadataHits:         stats.activeMetadataHits,
		ActiveMetadataMisses:       stats.activeMetadataMisses,
		ActivePrefixSummaryHitKeys: append([]string(nil), stats.activePrefixSummaryHitKeys...),
		ActiveMetadataHitKeys:      append([]string(nil), stats.activeMetadataHitKeys...),
	}
}

// Hits returns the total process-local tree query cache hits.
func (s TreeQueryCacheStats) Hits() uint64 {
	return s.ParentPacketHits + s.ActivePrefixSummaryHits + s.ActiveMetadataHits
}

// Misses returns the total process-local tree query cache misses.
func (s TreeQueryCacheStats) Misses() uint64 {
	return s.ParentPacketMisses + s.ActivePrefixSummaryMisses + s.ActiveMetadataMisses
}

// ResetTreeQueryCacheStats clears process-local tree query cache counters.
func ResetTreeQueryCacheStats(cfg Config) {
	treeQueryCacheForConfig(cfg).resetStats()
}

// ReadSchema3FallbackRoutes reads schema3 fallback route counters.
func ReadSchema3FallbackRoutes() map[string]uint64 {
	return map[string]uint64{
		parentFactsFallbackRouteName(): parentFactsFallbackRoutes(),
	}
}

// ResetSchema3FallbackRoutes clears schema3 fallback route counters.
func ResetSchema3FallbackRoutes() {
	resetParentFactsFallbackRoutesForTest()
}
