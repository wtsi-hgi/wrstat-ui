/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/pflag"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/provider"
)

const (
	envClickhouseDSN      = "WRSTAT_CLICKHOUSE_DSN"
	envClickhouseDatabase = "WRSTAT_CLICKHOUSE_DATABASE"
	envPollInterval       = "WRSTAT_POLL_INTERVAL"
	envQueryTimeout       = "WRSTAT_QUERY_TIMEOUT"

	clickhouseDSNFlagHelp      = "ClickHouse DSN (default $WRSTAT_CLICKHOUSE_DSN)"
	clickhouseDatabaseFlagHelp = "ClickHouse database name (default $WRSTAT_CLICKHOUSE_DATABASE)"
	clickhouseQueryTOFlagHelp  = "per-query timeout (default $WRSTAT_QUERY_TIMEOUT or 30s)"
	clickhouseNavIndexFlagHelp = "build an optional in-process navigation index for active snapshots"
	mountpointsFlagHelp        = "path to a file containing a list of quoted mountpoints"
)

var (
	errClickhouseDSNRequired      = errors.New("clickhouse DSN required")
	errClickhouseDatabaseRequired = errors.New("clickhouse database required")
)

var clickhouseDotEnvKeys = []string{
	envClickhouseDSN,
	envClickhouseDatabase,
	envPollInterval,
	envQueryTimeout,
}

func addClickhouseConnectionFlags(flags *pflag.FlagSet, dsn, database *string) {
	flags.StringVarP(dsn, "clickhouse-dsn", "C", "", clickhouseDSNFlagHelp)
	flags.StringVarP(database, "clickhouse-database", "D", "", clickhouseDatabaseFlagHelp)
}

func addClickhouseQueryTimeoutFlag(flags *pflag.FlagSet, queryTimeout *string) {
	flags.StringVar(queryTimeout, "query-timeout", "", clickhouseQueryTOFlagHelp)
}

func addClickhouseNavIndexFlag(flags *pflag.FlagSet, navIndex *bool) {
	flags.BoolVar(navIndex, "nav-index", false, clickhouseNavIndexFlagHelp)
}

func addMountpointsFlag(flags *pflag.FlagSet, mountpoints *string) {
	flags.StringVarP(mountpoints, "mounts", "m", "", mountpointsFlagHelp)
}

func openClickhouseProvider(cfg clickhouse.Config) (provider.Provider, error) {
	p, err := clickhouse.OpenProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse provider: %w", err)
	}

	return p, nil
}

func parseMountpointsFlag(path string) ([]string, error) {
	mountpoints, err := summariseutil.ParseMountpointsFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse mounts file: %w", err)
	}

	return mountpoints, nil
}

func loadClickhouseConfig(input clickhouseConfigInput) (clickhouse.Config, error) {
	loadClickhouseDotEnv()

	return clickhouseConfigFromEnvAndFlags(input)
}

func loadClickhouseConfigWithMountpoints(
	input clickhouseConfigInput,
	mountpointsPath string,
) (clickhouse.Config, error) {
	mountpoints, err := parseMountpointsFlag(mountpointsPath)
	if err != nil {
		return clickhouse.Config{}, err
	}

	input.mountpoints = mountpoints

	return loadClickhouseConfig(input)
}

type clickhouseConfigInput struct {
	dsnFlag             string
	databaseFlag        string
	ownersPath          string
	mountpoints         []string
	pollIntervalFlag    string
	pollIntervalDefault time.Duration
	queryTimeoutFlag    string
	navIndex            bool
}

func clickhouseDurationsFromEnvAndFlags(
	input clickhouseConfigInput,
) (time.Duration, time.Duration, error) {
	pollInterval, err := parseDurationFlagOrEnv(
		input.pollIntervalFlag,
		"poll-interval",
		envPollInterval,
		input.pollIntervalDefault,
	)
	if err != nil {
		return 0, 0, err
	}

	queryTimeout, err := parseDurationFlagOrEnv(
		input.queryTimeoutFlag,
		"query-timeout",
		envQueryTimeout,
		defaultQueryTimeout,
	)
	if err != nil {
		return 0, 0, err
	}

	return pollInterval, queryTimeout, nil
}

func loadClickhouseDotEnv() {
	original := originalEnvKeys(clickhouseDotEnvKeys)

	for _, path := range [...]string{".env", ".env.local"} {
		loadClickhouseDotEnvFile(path, original)
	}
}

func originalEnvKeys(keys []string) map[string]struct{} {
	orig := make(map[string]struct{}, len(keys))

	for _, key := range keys {
		if _, ok := os.LookupEnv(key); ok {
			orig[key] = struct{}{}
		}
	}

	return orig
}

func loadClickhouseDotEnvFile(path string, orig map[string]struct{}) {
	env, err := godotenv.Read(path)
	if err != nil {
		return
	}

	for _, key := range clickhouseDotEnvKeys {
		if _, ok := orig[key]; ok {
			continue
		}

		if val, ok := env[key]; ok {
			_ = os.Setenv(key, val)
		}
	}
}

func clickhouseConfigFromEnvAndFlags(input clickhouseConfigInput) (clickhouse.Config, error) {
	dsn, err := requiredFlagOrEnv(input.dsnFlag, envClickhouseDSN, errClickhouseDSNRequired)
	if err != nil {
		return clickhouse.Config{}, err
	}

	database, err := requiredFlagOrEnv(input.databaseFlag, envClickhouseDatabase, errClickhouseDatabaseRequired)
	if err != nil {
		return clickhouse.Config{}, err
	}

	pollInterval, queryTimeout, err := clickhouseDurationsFromEnvAndFlags(input)
	if err != nil {
		return clickhouse.Config{}, err
	}

	return clickhouse.Config{
		DSN:           dsn,
		Database:      database,
		OwnersCSVPath: input.ownersPath,
		MountPoints:   input.mountpoints,
		PollInterval:  pollInterval,
		QueryTimeout:  queryTimeout,
		NavIndex:      input.navIndex,
	}, nil
}

func requiredFlagOrEnv(flagValue string, envKey string, missing error) (string, error) {
	v := strings.TrimSpace(flagValue)
	if v != "" {
		return v, nil
	}

	v = strings.TrimSpace(os.Getenv(envKey))
	if v == "" {
		return "", missing
	}

	return v, nil
}

func parseDurationFlagOrEnv(
	flagValue string,
	flagName string,
	envKey string,
	defaultValue time.Duration,
) (time.Duration, error) {
	if v := strings.TrimSpace(flagValue); v != "" {
		return parseConfiguredDuration(v, "for --"+flagName)
	}

	v := strings.TrimSpace(os.Getenv(envKey))
	if v == "" {
		return defaultValue, nil
	}

	return parseConfiguredDuration(v, "in "+envKey)
}

func parseConfiguredDuration(value, source string) (time.Duration, error) {
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %s: %w", source, err)
	}

	return d, nil
}
