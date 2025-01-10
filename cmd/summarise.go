/*******************************************************************************
 * Copyright (c) 2021-2022 Genome Research Ltd.
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
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
	"github.com/wtsi-hgi/wrstat-ui/summary/groupuser"
	"github.com/wtsi-hgi/wrstat-ui/summary/usergroup"
)

var (
	defaultDir string
	userGroup  string
	groupUser  string
	basedirsDB string
	dirgutaDB  string

	quotaPath      string
	basedirsConfig string
)

// summariseCmd represents the stat command.
var summariseCmd = &cobra.Command{
	Use:   "summarise",
	Short: "Summarise stat data",
	Long: `Summarise state data in to dirguta database, basedirs database, ` +
		`and usergroup/groupuser files.
	`,
	Run: func(_ *cobra.Command, args []string) {
		if err := run(args); err != nil {
			die("%s", err)
		}
	},
}

func run(args []string) error {
	if len(args) != 1 {
		return errors.New("exactly 1 input file should be provided")
	}

	if defaultDir == "" && userGroup == "" && groupUser == "" && basedirsDB == "" && dirgutaDB == "" {
		return errors.New("no output files specified")
	}

	f, err := os.Open(args[1])
	if err != nil {
		return fmt.Errorf("failed to open stats file: %w", err)
	}

	defer f.Close()

	s := summary.NewSummariser(stats.NewStatsParser(f))

	if defaultDir != "" {
		if userGroup == "" {
			userGroup = filepath.Join(defaultDir, "usergroup")
		}

		if groupUser == "" {
			groupUser = filepath.Join(defaultDir, "groupuser")
		}

		if basedirsDB == "" {
			basedirsDB = filepath.Join(defaultDir, "basedirs")
		}

		if dirgutaDB == "" {
			dirgutaDB = filepath.Join(defaultDir, "dirguta")
			// mkdir??
		}
	}

	if userGroup != "" {
		uf, err := os.Create(userGroup)
		if err != nil {
			return fmt.Errorf("failed to create usergroup file: %w", err)
		}

		s.AddDirectoryOperation(usergroup.NewByUserGroup(uf))
	}

	if groupUser != "" {
		gf, err := os.Create(groupUser)
		if err != nil {
			return fmt.Errorf("failed to create groupuser file: %w", err)
		}

		s.AddGlobalOperation(groupuser.NewByGroupUser(gf))
	}

	if basedirsDB != "" {
		quotas, err := basedirs.ParseQuotas(quotaPath)
		if err != nil {
			return fmt.Errorf("error parsing quotas file: %w", err)
		}

		cf, err := os.Open(basedirsConfig)
		if err != nil {
			return fmt.Errorf("error opening basedirs config: %w", err)
		}

		config, err := basedirs.ParseConfig(cf)
		if err != nil {
			return fmt.Errorf("error parsing basedirs config: %w", err)
		}

		cf.Close()

		bd, err := basedirs.NewCreator(basedirsDB, quotas)
		if err != nil {
			return fmt.Errorf("failed to create new basedirs creator: %w", err)
		}

		s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))
	}

	if dirgutaDB != "" {
		db := db.NewDB(dirgutaDB)
		defer db.Close()

		s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(db))
	}

	return s.Summarise()
}

func init() {
	RootCmd.AddCommand(summariseCmd)

	summariseCmd.Flags().StringVarP(&defaultDir, "defaultDir", "d", "", "output all summarisers to here")
	summariseCmd.Flags().StringVarP(&userGroup, "userGroup", "u", "", "usergroup output file")
	summariseCmd.Flags().StringVarP(&userGroup, "groupUser", "g", "", "groupUser output file")
	summariseCmd.Flags().StringVarP(&userGroup, "basedirsDB", "b", "", "basedirs output file")
	summariseCmd.Flags().StringVarP(&userGroup, "tree", "t", "", "tree output dir")

	summariseCmd.Flags().StringVarP(&quotaPath, "quota", "q", "", "csv of gid,disk,size_quota,inode_quota")
	summariseCmd.Flags().StringVarP(&ownersPath, "owners", "o", "", "gid,owner csv file")
	summariseCmd.Flags().StringVarP(&basedirsConfig, "config", "b", "", "path to basedirs config file")
}
