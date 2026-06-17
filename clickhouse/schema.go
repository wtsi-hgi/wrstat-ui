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
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"sync"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	errNoEmbeddedSchemaFiles   = errors.New("clickhouse: no embedded schema files found")
	errNoSchemaVersionStats    = errors.New("clickhouse: schema version stats returned no rows")
	errUnexpectedSchemaVersion = errors.New("clickhouse: unexpected schema versions")

	//nolint:gochecknoglobals // Serialises same-process bootstrap for legacy empty tables.
	schemaVersionBootstrapMu sync.Mutex
)

const (
	currentSchemaVersion    uint32 = 1
	schemaVersionStatsQuery        = "SELECT count(), min(singleton), max(singleton), " +
		"min(version), max(version) FROM wrstat_schema_version FINAL"
	insertSchemaVersionStmt = "INSERT INTO wrstat_schema_version (singleton, version) VALUES (1, ?)"
	ensureFilesDirIDColumn  = "ALTER TABLE wrstat_files ADD COLUMN IF NOT EXISTS " +
		"dir_id UInt32 CODEC(Delta, LZ4) AFTER snapshot_id"
)

//go:embed schema/*.sql
var schemaFS embed.FS

type schemaVersionStats struct {
	count        uint64
	minSingleton *uint8
	maxSingleton *uint8
	minVersion   *uint32
	maxVersion   *uint32
}

func (s schemaVersionStats) ok() bool {
	if !s.hasValues() {
		return false
	}

	return *s.minSingleton == 1 &&
		*s.maxSingleton == 1 &&
		*s.minVersion == currentSchemaVersion &&
		*s.maxVersion == currentSchemaVersion
}

func (s schemaVersionStats) hasValues() bool {
	return s.count == 1 &&
		s.minSingleton != nil &&
		s.maxSingleton != nil &&
		s.minVersion != nil &&
		s.maxVersion != nil
}

func scanSchemaVersionStats(rows driver.Rows) (uint64, *uint8, *uint8, *uint32, *uint32, error) {
	if !rows.Next() {
		return noSchemaVersionStats(rows.Err())
	}

	var stats schemaVersionStats
	if err := rows.Scan(
		&stats.count,
		&stats.minSingleton,
		&stats.maxSingleton,
		&stats.minVersion,
		&stats.maxVersion,
	); err != nil {
		return 0, nil, nil, nil, nil, fmt.Errorf("clickhouse: failed to scan schema version stats: %w", err)
	}

	return stats.count, stats.minSingleton, stats.maxSingleton, stats.minVersion, stats.maxVersion, nil
}

func noSchemaVersionStats(err error) (uint64, *uint8, *uint8, *uint32, *uint32, error) {
	if err != nil {
		return 0, nil, nil, nil, nil, fmt.Errorf("clickhouse: failed to query schema version stats: %w", err)
	}

	return 0, nil, nil, nil, nil, errNoSchemaVersionStats
}

func ensureSchema(ctx context.Context, execer ch.Conn) error {
	stmts, err := schemaSQL()
	if err != nil {
		return err
	}

	if err := applySchemaDDL(ctx, execer, stmts); err != nil {
		return err
	}

	if err := ensureFilesSchemaReady(ctx, execer); err != nil {
		return err
	}

	return ensureSchemaVersion(ctx, execer)
}

func schemaSQL() ([]string, error) {
	entries, err := fs.Glob(schemaFS, "schema/*.sql")
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to list embedded schema files: %w", err)
	}

	stmts := make([]string, 0, len(entries))
	for _, name := range entries {
		fileStmts, err := readSchemaStatements(name)
		if err != nil {
			return nil, err
		}

		stmts = append(stmts, fileStmts...)
	}

	if len(stmts) == 0 {
		return nil, errNoEmbeddedSchemaFiles
	}

	return stmts, nil
}

func ensureFilesSchemaReady(ctx context.Context, execer ch.Conn) error {
	if err := execer.Exec(ctx, ensureFilesDirIDColumn); err != nil {
		return fmt.Errorf(
			"clickhouse: failed to ensure wrstat_files.dir_id column; please migrate or recreate wrstat_files: %w",
			err,
		)
	}

	return nil
}

func readSchemaStatement(name string) (string, error) {
	stmts, err := readSchemaStatements(name)
	if err != nil {
		return "", err
	}

	return strings.Join(stmts, ";\n"), nil
}

func applySchemaDDL(ctx context.Context, execer ch.Conn, stmts []string) error {
	for _, stmt := range stmts {
		if err := execer.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("clickhouse: failed to execute schema DDL: %w", err)
		}
	}

	return nil
}

func ensureSchemaVersion(ctx context.Context, execer ch.Conn) error {
	schemaVersionBootstrapMu.Lock()
	defer schemaVersionBootstrapMu.Unlock()

	count, minSingleton, maxSingleton, minVersion, maxVersion, err := schemaVersionStatsFromDB(ctx, execer)
	if err != nil {
		return err
	}

	if count == 0 {
		count, minSingleton, maxSingleton, minVersion, maxVersion, err = insertAndReadSchemaVersionStats(ctx, execer)
		if err != nil {
			return err
		}
	}

	return validateSchemaVersionStats(count, minSingleton, maxSingleton, minVersion, maxVersion)
}

func readSchemaStatements(name string) ([]string, error) {
	b, err := schemaFS.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to read embedded schema file %q: %w", name, err)
	}

	parts := strings.Split(string(b), ";")
	stmts := make([]string, 0, len(parts))

	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt != "" {
			stmts = append(stmts, stmt)
		}
	}

	return stmts, nil
}

func insertAndReadSchemaVersionStats(
	ctx context.Context,
	execer ch.Conn,
) (uint64, *uint8, *uint8, *uint32, *uint32, error) {
	if err := insertSchemaVersion(ctx, execer); err != nil {
		return 0, nil, nil, nil, nil, err
	}

	return schemaVersionStatsFromDB(ctx, execer)
}

func insertSchemaVersion(ctx context.Context, execer ch.Conn) error {
	if err := execer.Exec(ctx, insertSchemaVersionStmt, currentSchemaVersion); err != nil {
		return fmt.Errorf("clickhouse: failed to set schema version: %w", err)
	}

	return nil
}

func schemaVersionStatsFromDB(
	ctx context.Context,
	conn ch.Conn,
) (uint64, *uint8, *uint8, *uint32, *uint32, error) {
	rows, err := conn.Query(ctx, schemaVersionStatsQuery)
	if err != nil {
		return 0, nil, nil, nil, nil, fmt.Errorf("clickhouse: failed to query schema version stats: %w", err)
	}

	defer func() { _ = rows.Close() }()

	return scanSchemaVersionStats(rows)
}

func validateSchemaVersionStats(
	count uint64,
	minSingleton, maxSingleton *uint8,
	minVersion, maxVersion *uint32,
) error {
	if schemaVersionStatsOK(count, minSingleton, maxSingleton, minVersion, maxVersion) {
		return nil
	}

	return fmt.Errorf(
		"%w: count=%d singleton_min=%s singleton_max=%s min=%s max=%s; please migrate or drop the database",
		errUnexpectedSchemaVersion,
		count,
		formatNullableUint8(minSingleton),
		formatNullableUint8(maxSingleton),
		formatNullableUint32(minVersion),
		formatNullableUint32(maxVersion),
	)
}

func schemaVersionStatsOK(
	count uint64,
	minSingleton, maxSingleton *uint8,
	minVersion, maxVersion *uint32,
) bool {
	stats := schemaVersionStats{
		count:        count,
		minSingleton: minSingleton,
		maxSingleton: maxSingleton,
		minVersion:   minVersion,
		maxVersion:   maxVersion,
	}

	return stats.ok()
}

func formatNullableUint8(v *uint8) string {
	if v == nil {
		return "NULL"
	}

	return strconv.FormatUint(uint64(*v), 10)
}

func formatNullableUint32(v *uint32) string {
	if v == nil {
		return "NULL"
	}

	return strconv.FormatUint(uint64(*v), 10)
}
