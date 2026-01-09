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
	"errors"
	"fmt"
	"net/url"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

var (
	errDSNRequired         = errors.New("clickhouse: DSN is required")
	errDatabaseRequired    = errors.New("clickhouse: Database is required")
	errDSNMissingDatabase  = errors.New("clickhouse: DSN must include database=")
	errDSNDatabaseMismatch = errors.New("clickhouse: DSN database does not match Database")
)

// Client is the public ClickHouse-backed client for the extra-goal file APIs.
//
// It intentionally does not expose any clickhouse-go types.
type Client struct {
	conn ch.Conn
}

func NewClient(cfg Config) (*Client, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	opts, err := ch.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid DSN: %w", err)
	}

	if cfg.MaxOpenConns > 0 {
		opts.MaxOpenConns = cfg.MaxOpenConns

		if cfg.MaxIdleConns <= 0 {
			opts.MaxIdleConns = cfg.MaxOpenConns
		}
	}

	if cfg.MaxIdleConns > 0 {
		opts.MaxIdleConns = cfg.MaxIdleConns
	}

	conn, err := ch.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}

	return c.conn.Close()
}

func validateConfig(cfg Config) error {
	if cfg.DSN == "" {
		return errDSNRequired
	}

	if cfg.Database == "" {
		return errDatabaseRequired
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
