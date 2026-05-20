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
	"errors"
	"fmt"
	"os"
	"strings"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
)

const (
	cleanHistoryMutationQuery = "ALTER TABLE wrstat_basedirs_history DELETE WHERE NOT startsWith(mount_path, ?) " +
		"SETTINGS mutations_sync = 2"

	findInvalidHistoryQuery = "SELECT DISTINCT gid, mount_path FROM wrstat_basedirs_history " +
		"WHERE NOT startsWith(mount_path, ?) " +
		"ORDER BY mount_path ASC, gid ASC"

	testDatabaseNamePrefix = "wrstat_ui_test_"
)

var errUnsafeHistoryCleanup = errors.New("clickhouse: refusing to clean basedirs history in test environment")

type historyMaintainer struct {
	cfg  Config
	opts ch.Options
}

func (m *historyMaintainer) CleanHistoryForMount(prefix string) error {
	if err := m.refuseUnsafeCleanInTestEnv(); err != nil {
		return err
	}

	ctx, cancel := m.queryContext()
	defer cancel()

	conn, err := m.openConn()
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	if err := conn.Exec(ctx, cleanHistoryMutationQuery, prefix); err != nil {
		return fmt.Errorf("clickhouse: failed to clean basedirs history: %w", err)
	}

	return nil
}

func (m *historyMaintainer) refuseUnsafeCleanInTestEnv() error {
	if os.Getenv("WRSTAT_ENV") != "test" {
		return nil
	}

	if strings.HasPrefix(m.cfg.Database, testDatabaseNamePrefix) {
		return nil
	}

	return fmt.Errorf(
		"%w for database %q; expected prefix %q",
		errUnsafeHistoryCleanup,
		m.cfg.Database,
		testDatabaseNamePrefix,
	)
}

func (m *historyMaintainer) FindInvalidHistory(prefix string) ([]basedirs.HistoryIssue, error) {
	ctx, cancel := m.queryContext()
	defer cancel()

	conn, err := m.openConn()
	if err != nil {
		return nil, err
	}

	defer func() { _ = conn.Close() }()

	rows, err := conn.Query(ctx, findInvalidHistoryQuery, prefix)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to query invalid history: %w", err)
	}

	return scanInvalidHistoryRows(rows)
}

func (m *historyMaintainer) queryContext() (context.Context, context.CancelFunc) {
	return configQueryContext(m.cfg)
}

type invalidHistoryRows interface {
	rowsScanner
	Close() error
	Err() error
}

func scanInvalidHistoryRows(rows invalidHistoryRows) ([]basedirs.HistoryIssue, error) {
	defer func() { _ = rows.Close() }()

	out := make([]basedirs.HistoryIssue, 0)

	for rows.Next() {
		issue, err := scanInvalidHistoryIssue(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, issue)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: invalid history iteration error: %w", err)
	}

	return out, nil
}

func scanInvalidHistoryIssue(rows rowsScanner) (basedirs.HistoryIssue, error) {
	var issue basedirs.HistoryIssue
	if err := rows.Scan(&issue.GID, &issue.MountPath); err != nil {
		return basedirs.HistoryIssue{}, fmt.Errorf("clickhouse: failed to scan invalid history: %w", err)
	}

	return issue, nil
}

func (m *historyMaintainer) openConn() (ch.Conn, error) {
	opts := m.opts

	conn, err := ch.Open(&opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to connect: %w", err)
	}

	return conn, nil
}

// NewHistoryMaintainer returns a ClickHouse-backed basedirs.HistoryMaintainer.
func NewHistoryMaintainer(cfg Config) (basedirs.HistoryMaintainer, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	conn, err := connectFromOptions(cfg, opts)
	if err != nil {
		return nil, err
	}

	_ = conn.Close()

	return &historyMaintainer{cfg: cfg, opts: *opts}, nil
}
