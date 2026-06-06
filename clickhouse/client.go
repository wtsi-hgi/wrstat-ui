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
	"net"
	"net/url"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
)

var (
	errDSNRequired         = errors.New("clickhouse: DSN is required")
	errDatabaseRequired    = errors.New("clickhouse: Database is required")
	errDatabaseInvalid     = errors.New("clickhouse: Database contains invalid characters")
	errDSNNativeProtocol   = errors.New("clickhouse: DSN must use the native clickhouse:// protocol")
	errDSNMissingDatabase  = errors.New("clickhouse: DSN must include database=")
	errDSNDatabaseMismatch = errors.New("clickhouse: DSN database does not match Database")
)

//nolint:gochecknoglobals // overridden in tests to simulate mount autodiscovery failures.
var discoverMountPoints = basedirs.GetMountPoints

const defaultQueryTimeout = 10 * time.Second

const defaultMaxOpenConns = 10

const (
	minImportDialTimeout      = 30 * time.Second
	minImportReadTimeout      = time.Hour
	minImportConnMaxLifetime  = 24 * time.Hour
	minLongLivedImportBatches = 4
	minImportControlConns     = 1
	minImportOpenConns        = minLongLivedImportBatches + minImportControlConns
)

const maxConnectionSetupAttempts = 4

const (
	firstConnectionSetupRetryDelay  = 100 * time.Millisecond
	secondConnectionSetupRetryDelay = 250 * time.Millisecond
	laterConnectionSetupRetryDelay  = 500 * time.Millisecond
)

const createDatabaseStmtPrefix = "CREATE DATABASE IF NOT EXISTS "

const defaultDatabaseName = "default"

const unknownDatabaseCode int32 = 81

type clickHouseOpener func(*ch.Options) (ch.Conn, error)

func connectAndBootstrapWith(
	ctx context.Context,
	opts *ch.Options,
	database string,
	queryTO time.Duration,
	open clickHouseOpener,
	ensureSchema schemaEnsurer,
) (ch.Conn, error) {
	conn, err := openAndPingWithTimeout(ctx, opts, open, queryTO)
	if err == nil {
		return ensureSchemaReady(ctx, conn, ensureSchema)
	}

	if !shouldBootstrapMissingDatabase(err, database) {
		return nil, err
	}

	if ensureErr := ensureDatabaseExists(ctx, opts, database, queryTO, open); ensureErr != nil {
		return nil, ensureErr
	}

	conn, err = openAndPingWithTimeout(ctx, opts, open, queryTO)
	if err != nil {
		return nil, err
	}

	return ensureSchemaReady(ctx, conn, ensureSchema)
}

func ensureSchemaReady(
	ctx context.Context,
	conn ch.Conn,
	ensureSchema schemaEnsurer,
) (ch.Conn, error) {
	if err := ensureSchema(ctx, conn); err != nil {
		_ = conn.Close()

		return nil, err
	}

	return conn, nil
}

func shouldBootstrapMissingDatabase(err error, database string) bool {
	return database != defaultDatabaseName && isMissingDatabaseError(err)
}

func isMissingDatabaseError(err error) bool {
	var exception *chproto.Exception
	if errors.As(err, &exception) {
		return exception.Code == unknownDatabaseCode || exception.Name == "UNKNOWN_DATABASE"
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "unknown database") ||
		strings.Contains(msg, "database does not exist")
}

func openAndPingWithRetry(ctx context.Context, opts *ch.Options, open clickHouseOpener) (ch.Conn, error) {
	var err error

	for attempt := range maxConnectionSetupAttempts {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		conn, attemptErr := openAndPing(ctx, opts, open)
		if attemptErr == nil {
			return conn, nil
		}

		err = attemptErr

		if shouldStopConnectionSetupRetry(err, attempt) {
			return nil, err
		}

		if waitErr := waitForConnectionSetupRetry(ctx, connectionSetupRetryDelay(attempt)); waitErr != nil {
			return nil, waitErr
		}
	}

	return nil, err
}

func shouldStopConnectionSetupRetry(err error, attempt int) bool {
	return !isTransientConnectionSetupError(err) || attempt == maxConnectionSetupAttempts-1
}

func isTransientConnectionSetupError(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, context.Canceled):
		return false
	}

	var exception *chproto.Exception
	if errors.As(err, &exception) {
		return false
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return isTransientDNSError(dnsErr)
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Timeout()
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return isTimeoutError(err)
}

func isTransientDNSError(err *net.DNSError) bool {
	return err.IsTimeout || err.IsTemporary
}

func isTimeoutError(err error) bool {
	var netErr net.Error

	return errors.As(err, &netErr) && netErr.Timeout()
}

func waitForConnectionSetupRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func connectionSetupRetryDelay(attempt int) time.Duration {
	switch attempt {
	case 0:
		return firstConnectionSetupRetryDelay
	case 1:
		return secondConnectionSetupRetryDelay
	default:
		return laterConnectionSetupRetryDelay
	}
}

func openAndPing(ctx context.Context, opts *ch.Options, open clickHouseOpener) (ch.Conn, error) {
	conn, err := open(opts)
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()

		return nil, err
	}

	return conn, nil
}

func ensureDatabaseExists(
	ctx context.Context,
	opts *ch.Options,
	database string,
	queryTO time.Duration,
	open clickHouseOpener,
) error {
	if database == defaultDatabaseName {
		return nil
	}

	adminOpts := *opts
	adminOpts.Auth.Database = defaultDatabaseName

	conn, err := openAndPingWithTimeout(ctx, &adminOpts, open, queryTO)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to connect for bootstrap: %w", err)
	}

	defer func() { _ = conn.Close() }()

	stmt := createDatabaseStmtPrefix + quoteIdent(database)

	queryCtx, cancel := queryContext(ctx, queryTO)
	defer cancel()

	if err := conn.Exec(queryCtx, stmt); err != nil {
		return fmt.Errorf("clickhouse: failed to create database %q: %w", database, err)
	}

	return nil
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func queryContext(parent context.Context, queryTO time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}

	if queryTO <= 0 {
		queryTO = defaultQueryTimeout
	}

	return context.WithTimeout(parent, queryTO)
}

func openAndPingWithTimeout(
	parent context.Context,
	opts *ch.Options,
	open clickHouseOpener,
	queryTO time.Duration,
) (ch.Conn, error) {
	ctx, cancel := queryContext(parent, queryTO)
	defer cancel()

	return openAndPingWithRetry(ctx, opts, open)
}

type schemaEnsurer func(context.Context, ch.Conn) error

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

	conn, err := connectFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &Client{cfg: cfg, conn: conn, mountPoints: mountPoints}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}

	conn := c.conn
	c.conn = nil

	return conn.Close()
}

func configQueryContext(cfg Config) (context.Context, context.CancelFunc) {
	return queryContext(context.Background(), queryTimeout(cfg))
}

func connectFromConfig(cfg Config) (ch.Conn, error) {
	return connectFromConfigContext(context.Background(), cfg)
}

func connectFromConfigContext(ctx context.Context, cfg Config) (ch.Conn, error) {
	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return connectFromOptionsContext(ctx, cfg, opts)
}

func connectForImportFromConfig(cfg Config) (ch.Conn, error) {
	return connectForImportFromConfigContext(context.Background(), cfg)
}

func connectForImportFromConfigContext(ctx context.Context, cfg Config) (ch.Conn, error) {
	opts, err := importOptionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return connectFromOptionsContext(ctx, cfg, opts)
}

func importOptionsFromConfig(cfg Config) (*ch.Options, error) {
	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	normalizeImportConnectionOptions(opts)

	return opts, nil
}

func optionsFromConfig(cfg Config) (*ch.Options, error) {
	opts, err := ch.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid DSN: %w", err)
	}

	opts.Auth.Database = cfg.Database
	opts.Compression = &ch.Compression{Method: ch.CompressionLZ4}
	opts.MaxOpenConns, opts.MaxIdleConns = effectiveConnectionLimits(cfg)

	return opts, nil
}

func effectiveConnectionLimits(cfg Config) (int, int) {
	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = defaultMaxOpenConns
	}

	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = maxOpen
	}

	return maxOpen, maxIdle
}

func normalizeImportConnectionOptions(opts *ch.Options) {
	opts.DialTimeout = maxDuration(opts.DialTimeout, minImportDialTimeout)
	opts.ReadTimeout = maxDuration(opts.ReadTimeout, minImportReadTimeout)
	opts.ConnMaxLifetime = maxDuration(opts.ConnMaxLifetime, minImportConnMaxLifetime)

	if opts.MaxOpenConns < minImportOpenConns {
		opts.MaxOpenConns = minImportOpenConns
	}

	if opts.MaxIdleConns < minImportOpenConns {
		opts.MaxIdleConns = minImportOpenConns
	}
}

func maxDuration(current, floor time.Duration) time.Duration {
	if current >= floor {
		return current
	}

	return floor
}

func connectFromOptions(cfg Config, opts *ch.Options) (ch.Conn, error) {
	return connectFromOptionsContext(context.Background(), cfg, opts)
}

func connectFromOptionsContext(ctx context.Context, cfg Config, opts *ch.Options) (ch.Conn, error) {
	return connectAndBootstrap(ctx, opts, cfg.Database, queryTimeout(cfg))
}

func connectAndBootstrap(
	ctx context.Context,
	opts *ch.Options,
	database string,
	queryTO time.Duration,
) (ch.Conn, error) {
	return connectAndBootstrapWith(ctx, opts, database, queryTO, ch.Open, func(ctx context.Context, conn ch.Conn) error {
		return ensureSchemaWithBootstrapLock(ctx, conn, opts, database, queryTO)
	})
}

func queryTimeout(cfg Config) time.Duration {
	if cfg.QueryTimeout > 0 {
		return cfg.QueryTimeout
	}

	return defaultQueryTimeout
}

func mountPointsFromConfig(cfg Config) (basedirs.MountPoints, error) {
	if len(cfg.MountPoints) > 0 {
		return basedirs.ValidateMountPoints(cfg.MountPoints), nil
	}

	mountPoints, err := discoverMountPoints()
	if err != nil {
		return nil, fmt.Errorf("clickhouse: failed to auto-discover mountpoints: %w", err)
	}

	return mountPoints, nil
}

func validateConfig(cfg Config) error {
	if err := validateRequiredConfig(cfg); err != nil {
		return err
	}

	dsnURL, err := parseNativeDSN(cfg.DSN)
	if err != nil {
		return err
	}

	return validateDSNDatabase(cfg, dsnURL)
}

func validateRequiredConfig(cfg Config) error {
	if cfg.DSN == "" {
		return errDSNRequired
	}

	if cfg.Database == "" {
		return errDatabaseRequired
	}

	if strings.ContainsAny(cfg.Database, "`\x00") {
		return errDatabaseInvalid
	}

	return nil
}

func parseNativeDSN(dsn string) (*url.URL, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: invalid DSN: %w", err)
	}

	if u.Scheme != "clickhouse" {
		return nil, errDSNNativeProtocol
	}

	return u, nil
}

func validateDSNDatabase(cfg Config, dsnURL *url.URL) error {
	dsnDB, err := databaseFromDSNURL(dsnURL)
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

func databaseFromDSNURL(u *url.URL) (string, error) {
	db := u.Query().Get("database")
	if db == "" {
		return "", errDSNMissingDatabase
	}

	return db, nil
}
