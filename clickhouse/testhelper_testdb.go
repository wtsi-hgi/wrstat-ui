/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Test helper utilities for provisioning ephemeral ClickHouse databases.
 ******************************************************************************/

package clickhouse

import (
	"context"
	"fmt"
	"os"
	osuser "os/user"
	"time"
)

// NewUserEphemeralForTests attempts to provision a temporary database named after
// the current user. It drops any existing DB with that name (best-effort),
// creates it, calls CreateSchema, and returns (client, ctx, cleanup, error).
// If ClickHouse is unavailable, returns a non-nil error so callers can t.Skip.
func NewUserEphemeralForTests() (*Clickhouse, context.Context, func(), error) {
	ctx := context.Background()

	host := getenv("TEST_CLICKHOUSE_HOST", "127.0.0.1")
	port := getenv("TEST_CLICKHOUSE_PORT", "9000")
	user := getenv("TEST_CLICKHOUSE_USERNAME", "default")
	pass := getenv("TEST_CLICKHOUSE_PASSWORD", "")

	u, uErr := osuser.Current()
	uname := "nouser"
	if uErr == nil && u != nil && u.Username != "" {
		uname = u.Username
	}

	dbName := fmt.Sprintf("test_wrstatui_%s_%d", uname, time.Now().UnixNano())

	// Best-effort drop via admin connection to default
	admin, err := New(ConnectionParams{Host: host, Port: port, Database: "default", Username: user, Password: pass})
	if err == nil {
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
		_ = admin.Close()
	}

	// Connect (may attach to default if target DB missing) and let CreateSchema ensure DB creation
	ch, err := New(ConnectionParams{Host: host, Port: port, Database: dbName, Username: user, Password: pass})
	if err != nil {
		return nil, nil, func() {}, fmt.Errorf("connect db: %w", err)
	}

	if err := ch.CreateSchema(ctx); err != nil {
		_ = ch.Close()
		// Best-effort cleanup
		if admin2, aerr := New(ConnectionParams{Host: host, Port: port, Database: "default", Username: user, Password: pass}); aerr == nil {
			_ = admin2.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
			_ = admin2.Close()
		}

		return nil, nil, func() {}, fmt.Errorf("schema: %w", err)
	}

	cleanup := func() {
		_ = ch.Close()
		if admin3, aerr := New(ConnectionParams{Host: host, Port: port, Database: "default", Username: user, Password: pass}); aerr == nil {
			_ = admin3.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
			_ = admin3.Close()
		}
	}

	return ch, ctx, cleanup, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
