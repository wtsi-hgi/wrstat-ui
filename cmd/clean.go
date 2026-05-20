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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
)

var (
	cleanPrefix   string
	cleanViewOnly bool
)

var cleancmd = &cobra.Command{
	Use:   "clean",
	Short: "clean basedir history of invalid history",
	Long: `clean filters basedir history to only have paths with a certain prefix

Legacy basedirectory databases may have history relating to other mountpoints
which is no longer valid with the new database structures.

This command will remove all history except for those whose paths match the
prefix supplied in the --prefix/-p flag.

The --view/-v flag can be used to view the keys that would have be removed if
the flag were not supplied.
`,
	Run: func(_ *cobra.Command, args []string) {
		if len(args) > 0 {
			warn("clean: ignoring legacy basedirs DB path argument")
		}

		if cleanPrefix == "" {
			die("need to specify a path prefix to keep")
		}

		m, err := openCleanHistoryMaintainer()
		if err != nil {
			die("%s", err)
		}

		if err := cleanHistory(m, cleanPrefix, cleanViewOnly); err != nil {
			die("%s", err)
		}
	},
}

func openCleanHistoryMaintainer() (basedirs.HistoryMaintainer, error) {
	cfg, err := loadClickhouseConfig(clickhouseConfigInput{
		dsnFlag:          clickhouseDSN,
		databaseFlag:     clickhouseDatabase,
		queryTimeoutFlag: clickhouseQueryTO,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build ClickHouse config: %w", err)
	}

	m, err := clickhouse.NewHistoryMaintainer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse history maintainer: %w", err)
	}

	return m, nil
}

func cleanHistory(m basedirs.HistoryMaintainer, prefix string, viewOnly bool) error {
	if viewOnly {
		return printInvalidHistory(m, prefix)
	}

	if err := m.CleanHistoryForMount(prefix); err != nil {
		return fmt.Errorf("error cleaning ClickHouse history: %w", err)
	}

	return nil
}

func printInvalidHistory(m basedirs.HistoryMaintainer, prefix string) error {
	toRemove, err := m.FindInvalidHistory(prefix)
	if err != nil {
		return fmt.Errorf("failed to read ClickHouse history: %w", err)
	}

	for _, k := range toRemove {
		fmt.Printf("%d:%s\n", k.GID, k.MountPath)
	}

	return nil
}

func init() {
	RootCmd.AddCommand(cleancmd)

	cleancmd.Flags().StringVarP(&cleanPrefix, "prefix", "p", "", "path prefix to keep in history")
	cleancmd.Flags().BoolVarP(
		&cleanViewOnly,
		"view",
		"v",
		false,
		"show the keys that will be removed without deleting them",
	)
	addClickhouseConnectionFlags(cleancmd.Flags(), &clickhouseDSN, &clickhouseDatabase)
	addClickhouseQueryTimeoutFlag(cleancmd.Flags(), &clickhouseQueryTO)
}
