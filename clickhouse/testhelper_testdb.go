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

	u, _ := osuser.Current()

	uname := "nouser"
	if u != nil && u.Username != "" {
		uname = u.Username
	}

	dbName := fmt.Sprintf("test_wrstatui_%s_%d", uname, time.Now().UnixNano())

	// Admin connection to default for setup/teardown
	admin, err := New(ConnectionParams{Host: host, Port: port, Database: "default", Username: user, Password: pass})
	if err != nil {
		return nil, nil, func() {}, fmt.Errorf("admin connect: %w", err)
	}

	// Best-effort drop then create
	_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
	if err := admin.ExecuteQuery(ctx, "CREATE DATABASE "+dbName); err != nil {
		_ = admin.Close()

		return nil, nil, func() {}, fmt.Errorf("create db: %w", err)
	}

	// Connect to new DB and create schema
	ch, err := New(ConnectionParams{Host: host, Port: port, Database: dbName, Username: user, Password: pass})
	if err != nil {
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
		_ = admin.Close()

		return nil, nil, func() {}, fmt.Errorf("connect db: %w", err)
	}

	if err := ch.CreateSchema(ctx); err != nil {
		_ = ch.Close()
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
		_ = admin.Close()

		return nil, nil, func() {}, fmt.Errorf("schema: %w", err)
	}

	cleanup := func() {
		_ = ch.Close()
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
		_ = admin.Close()
	}

	return ch, ctx, cleanup, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
