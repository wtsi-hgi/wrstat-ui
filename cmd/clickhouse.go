/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

var (
	defaultDir string

	quotaPath      string
	basedirsConfig string
	mounts         string

	// ClickHouse connection settings.
	chHost     string
	chPort     string
	chDatabase string
	chUsername string
	chPassword string
)

// clickhouseCmd represents the clickhouse command.
//
// This command ingests a single mount's scan into ClickHouse with atomic
// promotion semantics (loading -> ready), maintains only the latest ready scan
// per mount, and provides precomputed subtree rollups.
var clickhouseCmd = &cobra.Command{
	Use:   "clickhouse <mount_path> <stats_file|->",
	Short: "Load and summarise stat data into ClickHouse",
	Long: `Load and summarise stat data into ClickHouse for fast, interactive queries.

This command ingests a single mount's scan into ClickHouse with atomic promotion
semantics (loading -> ready), maintains only the latest ready scan per mount,
and provides precomputed subtree rollups.`,
	Run: func(_ *cobra.Command, args []string) {
		if err := Run(args); err != nil {
			die("%s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(clickhouseCmd)

	// Add ClickHouse connection settings
	clickhouseCmd.Flags().StringVar(&chHost, "host", "127.0.0.1", "ClickHouse host")
	clickhouseCmd.Flags().StringVar(&chPort, "port", "9000", "ClickHouse port")
	clickhouseCmd.Flags().StringVar(&chDatabase, "database", "default", "ClickHouse database")
	clickhouseCmd.Flags().StringVar(&chUsername, "username", "default", "ClickHouse username")
	clickhouseCmd.Flags().StringVar(&chPassword, "password", "", "ClickHouse password")
}

// Run executes the clickhouse command with the given arguments.
func Run(args []string) (err error) {
	mountPath, statsPath, err := checkArgs(args)
	if err != nil {
		return err
	}

	r, _, err := clickhouse.OpenStatsFile(statsPath)
	if err != nil {
		return err
	}

	defer r.Close()

	ctx := context.Background()

	ch, err := setupClickHouseConnection()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.CreateSchema(ctx); err != nil {
		return fmt.Errorf("createSchema: %w", err)
	}

	return ch.UpdateClickhouse(ctx, mountPath, r)
}

func checkArgs(args []string) (string, string, error) {
	// Command line arguments.
	const expectedArgCount = 2

	if len(args) != expectedArgCount {
		return "", "", errors.New("usage: clickhouse <mount_path> <stats_file|->") //nolint:err113
	}

	mountPath := clickhouse.NormalizeMount(args[0])
	statsPath := args[1]

	if mountPath == "/" {
		return "", "", errors.New("mount_path must not be '/' — use the real mount point path") //nolint:err113
	}

	return mountPath, statsPath, nil
}

// setupClickHouseConnection creates and configures a new ClickHouse connection.
func setupClickHouseConnection() (*clickhouse.Clickhouse, error) {
	params := clickhouse.ConnectionParams{
		Host:     chHost,
		Port:     chPort,
		Database: chDatabase,
		Username: chUsername,
		Password: chPassword,
	}

	return clickhouse.New(params)
}
