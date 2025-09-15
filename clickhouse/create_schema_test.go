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
	host := getenv("TEST_CLICKHOUSE_HOST", "127.0.0.1")
	port := getenv("TEST_CLICKHOUSE_PORT", "9000")
	chUser := getenv("TEST_CLICKHOUSE_USERNAME", "default")
	pass := getenv("TEST_CLICKHOUSE_PASSWORD", "")

	u, uErr := user.Current()

	uname := "nouser"
	if uErr == nil && u != nil && u.Username != "" {
		uname = u.Username
	}

	dbName := "test_wrstatui_schema_" + uname + "_" + time.Now().Format("20060102150405")

	params := clickhouse.ConnectionParams{
		Host:     host,
		Port:     port,
		Database: dbName,
		Username: chUser,
		Password: pass,
	}

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
		_ = admin.ExecuteQuery(ctx, "DROP DATABASE IF EXISTS "+dbName)
		_ = admin.Close()
	}
}

// getenv returns the value of the environment variable named by the key,
// or the provided default if not set. Only for tests in package clickhouse_test.
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
