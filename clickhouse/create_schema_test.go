// Copyright (c) 2025 Genome Research Ltd.
//
// Test for CreateSchema creating the database if it does not exist.
package clickhouse_test

import (
	"context"
	"os"
	"os/user"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

func TestCreateSchemaCreatesDatabase(t *testing.T) {
	// Force test environment for dotenv/env resolution
	_ = os.Setenv("WRSTATUI_ENV", "test")

	u, uErr := user.Current()

	uname := "nouser"
	if uErr == nil && u != nil && u.Username != "" {
		uname = u.Username
	}

	dbName := "test_wrstatui_schema_" + uname + "_" + time.Now().Format("20060102150405")

	params := clickhouse.ConnectionParamsFromEnv()
	params.Database = dbName

	ch, err := clickhouse.New(params)
	if err != nil {
		t.Skipf("ClickHouse unavailable: %v", err)
	}
	defer ch.Close()

	ctx := context.Background()

	err = ch.CreateSchema(ctx)
	require.NoError(t, err, "CreateSchema should create the database if missing")

	// Try again to ensure idempotency
	err = ch.CreateSchema(ctx)
	require.NoError(t, err, "CreateSchema should be idempotent")

	// Clean up
	adminParams := params
	adminParams.Database = "default"

	admin, err := clickhouse.New(adminParams)
	if err == nil {
		// Best-effort cleanup in tests; ignore errors
		//nolint:errcheck
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
		_ = admin.Close()
	}
}

// Note: env resolution now handled by ConnectionParamsFromEnv.
