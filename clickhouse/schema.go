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
	"sort"
	"strconv"
	"strings"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

var (
	errNoEmbeddedSchemaFiles   = errors.New("clickhouse: no embedded schema files found")
	errUnexpectedSchemaVersion = errors.New("clickhouse: unexpected schema versions")
)

const (
	schemaVersionStatsQuery = "SELECT count(), min(version), max(version) FROM wrstat_schema_version"
	insertSchemaVersionStmt = "INSERT INTO wrstat_schema_version (version) VALUES (1)"
)

//go:embed schema/*.sql
var schemaFS embed.FS

func ensureSchema(ctx context.Context, execer ch.Conn) error {
	stmts, err := schemaSQL()
	if err != nil {
		return err
	}

	if err := applySchemaDDL(ctx, execer, stmts); err != nil {
		return err
	}

	return ensureSchemaVersion(ctx, execer)
}

func schemaSQL() ([]string, error) {
	entries, err := fs.Glob(schemaFS, "schema/*.sql")
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to list embedded schema files: %w", err)
	}

	sort.Strings(entries)

	stmts := make([]string, 0, len(entries))
	for _, name := range entries {
		stmt, err := readSchemaStatement(name)
		if err != nil {
			return nil, err
		}

		if stmt != "" {
			stmts = append(stmts, stmt)
		}
	}

	if len(stmts) == 0 {
		return nil, errNoEmbeddedSchemaFiles
	}

	return stmts, nil
}

func readSchemaStatement(name string) (string, error) {
	b, err := schemaFS.ReadFile(name)
	if err != nil {
		return "", fmt.Errorf("clickhouse: failed to read embedded schema file %q: %w", name, err)
	}

	s := strings.TrimSpace(string(b))
	for strings.HasSuffix(s, ";") {
		s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	}

	return s, nil
}

func applySchemaDDL(ctx context.Context, execer ch.Conn, stmts []string) error {
	for _, stmt := range stmts {
		execErr := execer.Exec(ctx, stmt)
		if execErr != nil {
			return fmt.Errorf("clickhouse: failed to execute schema DDL: %w", execErr)
		}
	}

	return nil
}

func ensureSchemaVersion(ctx context.Context, execer ch.Conn) error {
	count, minVersion, maxVersion, err := schemaVersionStatsFromDB(ctx, execer)
	if err != nil {
		return err
	}

	if count != 0 {
		return validateSchemaVersionStats(count, minVersion, maxVersion)
	}

	if insertErr := insertSchemaVersion(ctx, execer); insertErr != nil {
		return insertErr
	}

	count, minVersion, maxVersion, err = schemaVersionStatsFromDB(ctx, execer)
	if err != nil {
		return err
	}

	return validateSchemaVersionStats(count, minVersion, maxVersion)
}

func schemaVersionStatsFromDB(ctx context.Context, q ch.Conn) (uint64, *uint32, *uint32, error) {
	rows, err := q.Query(ctx, schemaVersionStatsQuery)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("clickhouse: failed to query schema version stats: %w", err)
	}

	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return 0, nil, nil, fmt.Errorf("clickhouse: failed to query schema version stats: %w", rows.Err())
	}

	var (
		count      uint64
		minVersion *uint32
		maxVersion *uint32
	)

	if err := rows.Scan(&count, &minVersion, &maxVersion); err != nil {
		return 0, nil, nil, fmt.Errorf("clickhouse: failed to scan schema version stats: %w", err)
	}

	return count, minVersion, maxVersion, nil
}

func validateSchemaVersionStats(count uint64, minVersion, maxVersion *uint32) error {
	if schemaVersionStatsOK(count, minVersion, maxVersion) {
		return nil
	}

	return fmt.Errorf(
		"%w: count=%d min=%s max=%s; please migrate or drop the database",
		errUnexpectedSchemaVersion,
		count,
		formatNullableUint32(minVersion),
		formatNullableUint32(maxVersion),
	)
}

func schemaVersionStatsOK(count uint64, minVersion, maxVersion *uint32) bool {
	return count == 1 &&
		minVersion != nil && maxVersion != nil &&
		*minVersion == 1 && *maxVersion == 1
}

func formatNullableUint32(v *uint32) string {
	if v == nil {
		return "NULL"
	}

	return strconv.FormatUint(uint64(*v), 10)
}

func insertSchemaVersion(ctx context.Context, execer ch.Conn) error {
	execErr := execer.Exec(ctx, insertSchemaVersionStmt)
	if execErr != nil {
		return fmt.Errorf("clickhouse: failed to set schema version: %w", execErr)
	}

	return nil
}
