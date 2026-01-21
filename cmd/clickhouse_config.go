package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

const (
	envClickhouseDSN      = "WRSTAT_CLICKHOUSE_DSN"
	envClickhouseDatabase = "WRSTAT_CLICKHOUSE_DATABASE"
	envPollInterval       = "WRSTAT_POLL_INTERVAL"
	envQueryTimeout       = "WRSTAT_QUERY_TIMEOUT"
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

func loadClickhouseDotEnv() {
	orig := originalEnvKeys(clickhouseDotEnvKeys)

	loadClickhouseDotEnvFile(".env", orig)
	loadClickhouseDotEnvFile(".env.local", orig)
}

func originalEnvKeys(keys []string) map[string]struct{} {
	orig := map[string]struct{}{}

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
		val, ok := env[key]
		if !ok {
			continue
		}

		if _, ok := orig[key]; ok {
			continue
		}

		_ = os.Setenv(key, val)
	}
}

func clickhouseConfigFromEnvAndFlags(
	dsnFlag string,
	databaseFlag string,
	ownersPath string,
	mountpoints []string,
	pollIntervalFlag string,
	pollIntervalDefault time.Duration,
	queryTimeoutFlag string,
	queryTimeoutDefault time.Duration,
) (clickhouse.Config, error) {
	dsn, database, pollInterval, queryTimeout, err := resolveClickhouseConfigInputs(
		dsnFlag,
		databaseFlag,
		pollIntervalFlag,
		pollIntervalDefault,
		queryTimeoutFlag,
		queryTimeoutDefault,
	)
	if err != nil {
		return clickhouse.Config{}, err
	}

	return clickhouseConfig(dsn, database, ownersPath, mountpoints, pollInterval, queryTimeout), nil
}

func resolveClickhouseConfigInputs(
	dsnFlag string,
	databaseFlag string,
	pollIntervalFlag string,
	pollIntervalDefault time.Duration,
	queryTimeoutFlag string,
	queryTimeoutDefault time.Duration,
) (string, string, time.Duration, time.Duration, error) {
	dsn, err := requiredFlagOrEnv(dsnFlag, envClickhouseDSN, errClickhouseDSNRequired)
	if err != nil {
		return "", "", 0, 0, err
	}

	database, err := requiredFlagOrEnv(databaseFlag, envClickhouseDatabase, errClickhouseDatabaseRequired)
	if err != nil {
		return "", "", 0, 0, err
	}

	pollInterval, queryTimeout, err := clickhouseDurationsFromFlagsAndEnv(
		pollIntervalFlag,
		pollIntervalDefault,
		queryTimeoutFlag,
		queryTimeoutDefault,
	)
	if err != nil {
		return "", "", 0, 0, err
	}

	return dsn, database, pollInterval, queryTimeout, nil
}

func clickhouseConfig(
	dsn string,
	database string,
	ownersPath string,
	mountpoints []string,
	pollInterval time.Duration,
	queryTimeout time.Duration,
) clickhouse.Config {
	return clickhouse.Config{
		DSN:           dsn,
		Database:      database,
		OwnersCSVPath: ownersPath,
		MountPoints:   mountpoints,
		PollInterval:  pollInterval,
		QueryTimeout:  queryTimeout,
	}
}

func clickhouseDurationsFromFlagsAndEnv(
	pollIntervalFlag string,
	pollIntervalDefault time.Duration,
	queryTimeoutFlag string,
	queryTimeoutDefault time.Duration,
) (time.Duration, time.Duration, error) {
	pollInterval, err := parseDurationFlagOrEnv(pollIntervalFlag, envPollInterval, pollIntervalDefault)
	if err != nil {
		return 0, 0, err
	}

	queryTimeout, err := parseDurationFlagOrEnv(queryTimeoutFlag, envQueryTimeout, queryTimeoutDefault)
	if err != nil {
		return 0, 0, err
	}

	return pollInterval, queryTimeout, nil
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

func parseDurationFlagOrEnv(flagValue string, envKey string, defaultValue time.Duration) (time.Duration, error) {
	if strings.TrimSpace(flagValue) != "" {
		d, err := time.ParseDuration(flagValue)
		if err != nil {
			return 0, fmt.Errorf("invalid duration for %q: %w", envKey, err)
		}

		return d, nil
	}

	v := strings.TrimSpace(os.Getenv(envKey))
	if v == "" {
		return defaultValue, nil
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid duration in %s: %w", envKey, err)
	}

	return d, nil
}
