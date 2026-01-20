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
	"net/url"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
)

var (
	errDSNRequired         = errors.New("clickhouse: DSN is required")
	errDatabaseRequired    = errors.New("clickhouse: Database is required")
	errDatabaseInvalid     = errors.New("clickhouse: Database contains invalid characters")
	errDSNMissingDatabase  = errors.New("clickhouse: DSN must include database=")
	errDSNDatabaseMismatch = errors.New("clickhouse: DSN database does not match Database")
)

const defaultQueryTimeout = 10 * time.Second

const createDatabaseStmtPrefix = "CREATE DATABASE IF NOT EXISTS "

// Client is the public ClickHouse-backed client for the extra-goal file APIs.
//
// It intentionally does not expose any clickhouse-go types.
type Client struct {
	cfg Config

	conn ch.Conn

	mountPoints basedirs.MountPoints
}

// NewClient returns a new Client configured to use the ClickHouse database.
func NewClient(cfg Config) (*Client, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	mountPoints, err := mountPointsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout(cfg))
	defer cancel()

	conn, err := connectAndBootstrap(ctx, opts, cfg.Database)
	if err != nil {
		return nil, err
	}

	return &Client{cfg: cfg, conn: conn, mountPoints: mountPoints}, nil
}

func mountPointsFromConfig(cfg Config) (basedirs.MountPoints, error) {
	if len(cfg.MountPoints) > 0 {
		return basedirs.ValidateMountPoints(cfg.MountPoints), nil
	}

	mountPoints, err := basedirs.GetMountPoints()
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to auto-discover mountpoints: %w", err)
	}

	return mountPoints, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}

	return c.conn.Close()
}

func connectAndBootstrap(ctx context.Context, opts *ch.Options, database string) (ch.Conn, error) {
	bootErr := ensureDatabaseExists(ctx, opts, database)
	if bootErr != nil {
		return nil, bootErr
	}

	conn, err := ch.Open(opts)
	if err != nil {
		return nil, err
	}

	bootErr = ensureSchema(ctx, conn)
	if bootErr != nil {
		_ = conn.Close()

		return nil, bootErr
	}

	return conn, nil
}

func ensureDatabaseExists(ctx context.Context, opts *ch.Options, database string) error {
	if database == "default" {
		return nil
	}

	adminOpts := *opts
	adminOpts.Auth.Database = "default"

	conn, err := ch.Open(&adminOpts)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to connect for bootstrap: %w", err)
	}

	defer func() { _ = conn.Close() }()

	stmt := createDatabaseStmtPrefix + quoteIdent(database)
	if err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("clickhouse: failed to create database %q: %w", database, err)
	}

	return nil
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func validateConfig(cfg Config) error {
	if cfg.DSN == "" {
		return errDSNRequired
	}

	if cfg.Database == "" {
		return errDatabaseRequired
	}

	if strings.ContainsAny(cfg.Database, "`\x00") {
		return errDatabaseInvalid
	}

	dsnDB, err := databaseFromDSN(cfg.DSN)
	if err != nil {
		return err
	}

	if dsnDB != cfg.Database {
		return fmt.Errorf(
			"%w: DSN database %q does not match Database %q",
			errDSNDatabaseMismatch,
			dsnDB,
			cfg.Database,
		)
	}

	return nil
}

func databaseFromDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("clickhouse: invalid DSN: %w", err)
	}

	db := u.Query().Get("database")
	if db == "" {
		return "", errDSNMissingDatabase
	}

	return db, nil
}

func optionsFromConfig(cfg Config) (*ch.Options, error) {
	opts, err := ch.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid DSN: %w", err)
	}

	opts.Auth.Database = cfg.Database

	if cfg.MaxOpenConns > 0 {
		opts.MaxOpenConns = cfg.MaxOpenConns
		if cfg.MaxIdleConns <= 0 {
			opts.MaxIdleConns = cfg.MaxOpenConns
		}
	}

	if cfg.MaxIdleConns > 0 {
		opts.MaxIdleConns = cfg.MaxIdleConns
	}

	return opts, nil
}

func queryTimeout(cfg Config) time.Duration {
	if cfg.QueryTimeout > 0 {
		return cfg.QueryTimeout
	}

	return defaultQueryTimeout
}
