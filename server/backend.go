/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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
	"errors"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
)

// Backend captures the operations the server needs from a data backend.
//
// Implementations may be backed by Bolt (db.Tree + basedirs.MultiReader)
// or ClickHouse. This allows the server to switch between storage engines.
type Backend interface {
	// Tree-like queries
	DirInfo(dir string, filter *db.Filter) (*db.DirInfo, error)
	DirHasChildren(dir string, filter *db.Filter) bool
	Where(dir string, filter *db.Filter, recurseCount split.SplitFn) (db.DCSs, error)

	// Basedirs queries
	GroupUsage(age db.DirGUTAge) ([]*basedirs.Usage, error)
	UserUsage(age db.DirGUTAge) ([]*basedirs.Usage, error)
	GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error)
	UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error)
	History(gid uint32, basedir string) ([]basedirs.History, error)
}

// NotImplemented is returned by Backend methods that aren't implemented for
// a particular backend yet.
var ErrNotImplemented = errors.New("not implemented")

// ClickHouseBackend is a Backend backed by ClickHouse.
//
// For Phase 0, only the methods needed by tree summaries will be implemented
// later; other methods return NotImplemented for now.
type ClickHouseBackend struct {
	ch *clickhouse.Clickhouse
}

// NewClickHouseBackend creates a new ClickHouse-backed Backend.
// Caller is responsible for managing the ClickHouse connection lifecycle.
func NewClickHouseBackend(ch *clickhouse.Clickhouse) *ClickHouseBackend {
	return &ClickHouseBackend{ch: ch}
}

// DirInfo returns a directory summary and its immediate children summaries.
// Phase 0: leave unimplemented; server will continue using Bolt unless
// explicitly switched later phases.
func (b *ClickHouseBackend) DirInfo(_ string, _ *db.Filter) (*db.DirInfo, error) {
	return nil, ErrNotImplemented
}

// DirHasChildren reports if dir has any child dirs with matching files.
func (b *ClickHouseBackend) DirHasChildren(_ string, _ *db.Filter) bool {
	return false
}

// Where implements where-splits style aggregation.
func (b *ClickHouseBackend) Where(_ string, _ *db.Filter, _ split.SplitFn) (db.DCSs, error) {
	return nil, ErrNotImplemented
}

// GroupUsage returns usage grouped by group.
func (b *ClickHouseBackend) GroupUsage(_ db.DirGUTAge) ([]*basedirs.Usage, error) {
	return nil, ErrNotImplemented
}

// UserUsage returns usage grouped by user.
func (b *ClickHouseBackend) UserUsage(_ db.DirGUTAge) ([]*basedirs.Usage, error) {
	return nil, ErrNotImplemented
}

// GroupSubDirs returns subdirectory usage for a group.
func (b *ClickHouseBackend) GroupSubDirs(_ uint32, _ string, _ db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return nil, ErrNotImplemented
}

// UserSubDirs returns subdirectory usage for a user.
func (b *ClickHouseBackend) UserSubDirs(_ uint32, _ string, _ db.DirGUTAge) ([]*basedirs.SubDir, error) {
	return nil, ErrNotImplemented
}

// History returns historical usage for a group basedir.
func (b *ClickHouseBackend) History(_ uint32, _ string) ([]basedirs.History, error) {
	return nil, ErrNotImplemented
}
