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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/bolt"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
	"github.com/wtsi-hgi/wrstat-ui/summary/groupuser"
	"github.com/wtsi-hgi/wrstat-ui/summary/usergroup"
)

const (
	summariseDBBatchSize = 10000
	summariseDirPerm     = 0o755
)

var (
	defaultDir        string
	userGroup         string
	groupUser         string
	basedirsDB        string
	basedirsHistoryDB string
	dirgutaDB         string

	quotaPath      string
	basedirsConfig string
	mounts         string
)

var (
	errSummariseExactlyOneInput = errors.New("exactly 1 input file should be provided")
	errSummariseNoOutput        = errors.New("no output files specified")
)

// summariseCmd represents the stat command.
var summariseCmd = &cobra.Command{
	Use:   "summarise",
	Short: "Summarise stat data",
	Long: `Summarise stat data in to dirguta database, basedirs database, ` +
		`and usergroup/groupuser files.

Summarise processes stat files from the output of 'wrstat multi' into different
summaries.

Summarise takes the following arguments

  --defaultDir,-d
	output all summarisers to here with the default names.

  --userGroup,-u
	usergroup output file. Defaults to DEFAULTDIR/byusergroup.gz, if --defaultDir is set.
	If filename ends in '.gz' the file will be gzip compressed.

  --groupUser,-g
	groupUser output file. Defaults to DEFAULTDIR/bygroup, if --defaultDir is set.
	If filename ends in '.gz' the file will be gzip compressed.

  --basedirsDB,-b
	basedirs output file. Defaults to DEFAULTDIR/basedirs.db, if --defaultDir is set.

  --tree,-t
	tree output dir. Defaults to DEFAULTDIR/dguta.dbs, if --defaultDir is set.

  --basedirsHistoryDB,-s
	basedirs file containing previous history.

  --quota,-q
	Required for basedirs, format is a csv of gid,disk,size_quota,inode_quota

  --config,-c
	Required for basedirs, path to basedirs config file.

  --mounts,-m
	Provide a file containing quoted mount points, one-per-line, instead of
	relying on automatically discovered mount points.
	The following is an example command that can be used to generate an
	appropriate file:
		findmnt -ln --real -o target | sed -e 's/^/"/' -e 's/$/"/' > mounts

NB: All existing output files will be deleted or truncated during initialisation.

An example command would be the following:

	wrstat-ui summarise -d /path/to/output -s /path/to/previous/basedirs.db -q ` +
		`/path/to/quota.file -c /path/to/basedirs.config /path/to/stats.file
`,
	Run: func(_ *cobra.Command, args []string) {
		if err := run(args); err != nil {
			die("%s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(summariseCmd)

	summariseCmd.Flags().StringVarP(&defaultDir, "defaultDir", "d", "", "output all summarisers to here")
	summariseCmd.Flags().StringVarP(&userGroup, "userGroup", "u", "", "usergroup output file")
	summariseCmd.Flags().StringVarP(&groupUser, "groupUser", "g", "", "groupUser output file")
	summariseCmd.Flags().StringVarP(&basedirsDB, "basedirsDB", "b", "", "basedirs output file")
	summariseCmd.Flags().StringVarP(&basedirsHistoryDB, "basedirsHistoryDB", "s", "",
		"basedirs file containing previous history")
	summariseCmd.Flags().StringVarP(&dirgutaDB, "tree", "t", "", "tree output dir")
	summariseCmd.Flags().StringVarP(&quotaPath, "quota", "q", "", "csv of gid,disk,size_quota,inode_quota")
	summariseCmd.Flags().StringVarP(&basedirsConfig, "config", "c", "", "path to basedirs config file")
	summariseCmd.Flags().StringVarP(&mounts, "mounts", "m", "", "path to a file containing a list of quoted mountpoints")
}

type compressedFile struct {
	*pgzip.Writer
	file *os.File
}

func (c *compressedFile) Close() error {
	err := c.Writer.Close()
	errr := c.file.Close()

	if err != nil {
		return err
	}

	return errr
}

func wrapCompressed(wc *os.File) io.WriteCloser {
	if !strings.HasSuffix(wc.Name(), ".gz") {
		return wc
	}

	return &compressedFile{
		Writer: pgzip.NewWriter(wc),
		file:   wc,
	}
}

func addBasedirsSummariser(s *summary.Summariser, basedirsDB, basedirsHistoryDB,
	quotaPath, basedirsConfig, mountpoints string, modtime time.Time) (func() error, error) {
	quotas, config, err := summariseutil.ParseBasedirConfig(quotaPath, basedirsConfig)
	if err != nil {
		return nil, err
	}

	if err = removeIfExists(basedirsDB); err != nil {
		return nil, err
	}

	mps, err := summariseutil.ParseMountpointsFromFile(mountpoints)
	if err != nil {
		return nil, err
	}

	store, err := createBasedirsStore(basedirsDB, basedirsHistoryDB, modtime)
	if err != nil {
		return nil, err
	}

	bd, err := configureBaseDirsCreator(store, quotas, mps, modtime)
	if err != nil {
		_ = store.Close()

		return nil, err
	}

	s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return store.Close, nil
}

func removeIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
}

func createBasedirsStore(basedirsDB, basedirsHistoryDB string, modtime time.Time) (basedirs.Store, error) {
	store, err := bolt.NewBaseDirsStore(basedirsDB, basedirsHistoryDB)
	if err != nil {
		return nil, fmt.Errorf("failed to create basedirs store: %w", err)
	}

	mountPath := summariseutil.DeriveMountPathFromOutputDir(basedirsDB)
	store.SetMountPath(mountPath)
	store.SetUpdatedAt(modtime)

	return store, nil
}

func configureBaseDirsCreator(
	store basedirs.Store,
	quotas *basedirs.Quotas,
	mountpoints []string,
	modtime time.Time,
) (*basedirs.BaseDirs, error) {
	bd, err := basedirs.NewCreator(store, quotas)
	if err != nil {
		return nil, fmt.Errorf("failed to create new basedirs creator: %w", err)
	}

	if len(mountpoints) > 0 {
		bd.SetMountPoints(mountpoints)
	}

	bd.SetModTime(modtime)

	return bd, nil
}

func run(args []string) (err error) {
	if err = checkArgs(args); err != nil {
		return err
	}

	r, modtime, err := openStatsFile(args[0])
	if err != nil {
		return err
	}

	s := summary.NewSummariser(stats.NewStatsParser(r))

	setArgsDefaults()

	if fn, err := setSummarisers(s, mounts, modtime); err != nil { //nolint:nestif
		return err
	} else if fn != nil {
		defer func() {
			if errr := fn(); err == nil {
				err = errr
			}
		}()
	}

	return s.Summarise()
}

func checkArgs(args []string) error {
	if len(args) != 1 {
		return errSummariseExactlyOneInput
	}

	if defaultDir == "" && userGroup == "" && groupUser == "" && basedirsDB == "" && dirgutaDB == "" {
		return errSummariseNoOutput
	}

	return nil
}

func openStatsFile(statsFile string) (io.Reader, time.Time, error) {
	if statsFile == "-" {
		return os.Stdin, time.Now(), nil
	}

	f, err := os.Open(statsFile)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to open stats file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}

	var r io.Reader = f

	if strings.HasSuffix(statsFile, ".gz") {
		if r, err = pgzip.NewReader(f); err != nil {
			return nil, time.Time{}, fmt.Errorf("failed to decompress stats file: %w", err)
		}
	}

	return r, fi.ModTime(), nil
}

func setArgsDefaults() {
	if defaultDir == "" {
		return
	}

	if userGroup == "" {
		userGroup = filepath.Join(defaultDir, "byusergroup.gz")
	}

	if groupUser == "" {
		groupUser = filepath.Join(defaultDir, "bygroup")
	}

	if basedirsDB == "" {
		basedirsDB = filepath.Join(defaultDir, basedirBasename)
	}

	if dirgutaDB == "" {
		dirgutaDB = filepath.Join(defaultDir, dgutaDBsSuffix)
	}
}

func setSummarisers(s *summary.Summariser, mountpoints string, //nolint:gocognit,gocyclo
	modtime time.Time) (func() error, error) {
	var closers []func() error

	if userGroup != "" {
		if err := addUserGroupSummariser(s, userGroup); err != nil {
			return nil, err
		}
	}

	if groupUser != "" {
		if err := addGroupUserSummariser(s, groupUser); err != nil {
			return nil, err
		}
	}

	if basedirsDB != "" {
		c, err := addBasedirsSummariser(s, basedirsDB, basedirsHistoryDB,
			quotaPath, basedirsConfig, mountpoints, modtime)
		if err != nil {
			return nil, err
		}

		if c != nil {
			closers = append(closers, c)
		}
	}

	if dirgutaDB != "" {
		c, err := addDirgutaSummariser(s, dirgutaDB, modtime)
		if err != nil {
			return nil, err
		}

		if c != nil {
			closers = append(closers, c)
		}
	}

	if len(closers) == 0 {
		return nil, nil //nolint:nilnil
	}

	return func() error {
		var err error

		for _, c := range closers {
			if c == nil {
				continue
			}

			err = errors.Join(err, c())
		}

		return err
	}, nil
}

func addUserGroupSummariser(s *summary.Summariser, userGroup string) error {
	uf, err := os.Create(userGroup)
	if err != nil {
		return fmt.Errorf("failed to create usergroup file: %w", err)
	}

	s.AddDirectoryOperation(usergroup.NewByUserGroup(wrapCompressed(uf)))

	return nil
}

func addGroupUserSummariser(s *summary.Summariser, groupUser string) error {
	gf, err := os.Create(groupUser)
	if err != nil {
		return fmt.Errorf("failed to create groupuser file: %w", err)
	}

	s.AddGlobalOperation(groupuser.NewByGroupUser(wrapCompressed(gf)))

	return nil
}

func addDirgutaSummariser(s *summary.Summariser, dirgutaDB string,
	modtime time.Time) (func() error, error) {
	if err := os.RemoveAll(dirgutaDB); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	if err := os.MkdirAll(dirgutaDB, summariseDirPerm); err != nil {
		return nil, err
	}

	writer, err := bolt.NewDGUTAWriter(dirgutaDB)
	if err != nil {
		return nil, fmt.Errorf("failed to create dguta writer: %w", err)
	}

	mountPath := summariseutil.DeriveMountPathFromOutputDir(dirgutaDB)

	writer.SetMountPath(mountPath)
	writer.SetUpdatedAt(modtime)
	writer.SetBatchSize(summariseDBBatchSize)

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(writer))

	return writer.Close, nil
}
