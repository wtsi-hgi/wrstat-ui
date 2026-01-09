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
	"strings"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

var (
	errNoEmbeddedSchemaFiles   = errors.New("clickhouse: no embedded schema files found")
	errUnexpectedSchemaVersion = errors.New("clickhouse: unexpected schema versions")
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
	versions, err := ensureSchemaVersionRow(ctx, execer)
	if err != nil {
		return err
	}

	if len(versions) != 1 || versions[0] != 1 {
		return fmt.Errorf("%w: %v", errUnexpectedSchemaVersion, versions)
	}

	return nil
}

func ensureSchemaVersionRow(ctx context.Context, execer ch.Conn) ([]uint32, error) {
	versions, err := schemaVersionsFromDB(ctx, execer)
	if err != nil {
		return nil, err
	}

	if len(versions) != 0 {
		return versions, nil
	}

	execErr := execer.Exec(ctx, "INSERT INTO wrstat_schema_version (version) VALUES (1)")
	if execErr != nil {
		return nil, fmt.Errorf("clickhouse: failed to set schema version: %w", execErr)
	}

	return schemaVersionsFromDB(ctx, execer)
}

func schemaVersionsFromDB(ctx context.Context, q ch.Conn) ([]uint32, error) {
	rows, err := q.Query(ctx, "SELECT version FROM wrstat_schema_version ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query schema versions: %w", err)
	}

	defer func() { _ = rows.Close() }()

	versions := make([]uint32, 0, 1)

	for rows.Next() {
		var v uint32
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("clickhouse: failed to scan schema version: %w", err)
		}

		versions = append(versions, v)
	}

	return versions, nil
}
