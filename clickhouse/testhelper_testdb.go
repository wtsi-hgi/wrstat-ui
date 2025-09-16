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
//
//nolint:gocyclo,funlen
func NewUserEphemeralForTests() (*Clickhouse, context.Context, func(), error) {
	ctx := context.Background()

	// Force test environment for dotenv/env resolution
	_ = os.Setenv("WRSTATUI_ENV", "test")

	// Load connection params from env/.env with maintained defaults
	base := ConnectionParamsFromEnv()

	u, uErr := osuser.Current()

	uname := "nouser"
	if uErr == nil && u != nil && u.Username != "" {
		uname = u.Username
	}

	dbName := fmt.Sprintf("test_wrstatui_%s_%d", uname, time.Now().UnixNano())

	// Best-effort drop via admin connection to default
	admin, err := New(ConnectionParams{
		Host:     base.Host,
		Port:     base.Port,
		Database: "default",
		Username: base.Username,
		Password: base.Password,
	})
	if err == nil {
		// Best-effort cleanup; ignore errors
		//nolint:errcheck
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
		_ = admin.Close()
	}

	// Connect (may attach to default if target DB missing) and let CreateSchema ensure DB creation
	ch, err := New(ConnectionParams{
		Host:     base.Host,
		Port:     base.Port,
		Database: dbName,
		Username: base.Username,
		Password: base.Password,
	})
	if err != nil {
		return nil, nil, func() {}, fmt.Errorf("connect db: %w", err)
	}

	if err := ch.CreateSchema(ctx); err != nil {
		_ = ch.Close()
		// Best-effort cleanup
		if admin2, aerr := New(ConnectionParams{
			Host:     base.Host,
			Port:     base.Port,
			Database: "default",
			Username: base.Username,
			Password: base.Password,
		}); aerr == nil {
			//nolint:errcheck // best-effort test cleanup
			_ = admin2.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
			_ = admin2.Close()
		}

		return nil, nil, func() {}, fmt.Errorf("schema: %w", err)
	}

	cleanup := func() {
		_ = ch.Close()
		if admin3, aerr := New(ConnectionParams{
			Host:     base.Host,
			Port:     base.Port,
			Database: "default",
			Username: base.Username,
			Password: base.Password,
		}); aerr == nil {
			//nolint:errcheck // best-effort test cleanup
			_ = admin3.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
			_ = admin3.Close()
		}
	}

	return ch, ctx, cleanup, nil
}
