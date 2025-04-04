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
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/pgzip"
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

const dbBatchSize = 10000

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
		return errors.New("exactly 1 input file should be provided") //nolint:err113
	}

	if defaultDir == "" && userGroup == "" && groupUser == "" && basedirsDB == "" && dirgutaDB == "" {
		return errors.New("no output files specified") //nolint:err113
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
		if err := addBasedirsSummariser(s, basedirsDB, basedirsHistoryDB,
			quotaPath, basedirsConfig, mountpoints, modtime); err != nil {
			return nil, err
		}
	}

	if dirgutaDB != "" {
		return addDirgutaSummariser(s, dirgutaDB)
	}

	return nil, nil //nolint:nilnil
}

func addUserGroupSummariser(s *summary.Summariser, userGroup string) error {
	uf, err := os.Create(userGroup)
	if err != nil {
		return fmt.Errorf("failed to create usergroup file: %w", err)
	}

	s.AddDirectoryOperation(usergroup.NewByUserGroup(wrapCompressed(uf)))

	return nil
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

func addGroupUserSummariser(s *summary.Summariser, groupUser string) error {
	gf, err := os.Create(groupUser)
	if err != nil {
		return fmt.Errorf("failed to create groupuser file: %w", err)
	}

	s.AddGlobalOperation(groupuser.NewByGroupUser(wrapCompressed(gf)))

	return nil
}

func addBasedirsSummariser(s *summary.Summariser, basedirsDB, basedirsHistoryDB,
	quotaPath, basedirsConfig, mountpoints string, modtime time.Time) error {
	quotas, config, err := parseBasedirConfig(quotaPath, basedirsConfig)
	if err != nil {
		return err
	}

	if err = os.Remove(basedirsDB); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	bd, err := basedirs.NewCreator(basedirsDB, quotas)
	if err != nil {
		return fmt.Errorf("failed to create new basedirs creator: %w", err)
	} else if mps, errr := parseMountpointsFromFile(mountpoints); errr != nil {
		return errr
	} else if len(mps) > 0 {
		bd.SetMountPoints(mps)
	}

	bd.SetModTime(modtime)

	if basedirsHistoryDB != "" {
		if err = copyHistory(bd, basedirsHistoryDB); err != nil {
			return err
		}
	}

	s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return nil
}

func parseMountpointsFromFile(mountpoints string) ([]string, error) {
	if mountpoints == "" {
		return nil, nil
	}

	data, err := os.ReadFile(mountpoints)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	mounts := make([]string, 0, len(lines))

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		mountpoint, err := strconv.Unquote(line)
		if err != nil {
			return nil, err
		}

		mounts = append(mounts, mountpoint)
	}

	return mounts, nil
}

func parseBasedirConfig(quotaPath, basedirsConfig string) (*basedirs.Quotas, basedirs.Config, error) {
	quotas, err := basedirs.ParseQuotas(quotaPath)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing quotas file: %w", err)
	}

	cf, err := os.Open(basedirsConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening basedirs config: %w", err)
	}

	defer cf.Close()

	config, err := basedirs.ParseConfig(cf)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing basedirs config: %w", err)
	}

	cf.Close()

	return quotas, config, nil
}

func copyHistory(bd *basedirs.BaseDirs, basedirsHistoryDB string) error {
	db, err := basedirs.OpenDBRO(basedirsHistoryDB)
	if err != nil {
		return err
	}

	defer db.Close()

	return bd.CopyHistoryFrom(db)
}

func addDirgutaSummariser(s *summary.Summariser, dirgutaDB string) (func() error, error) {
	if err := os.RemoveAll(dirgutaDB); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	if err := os.MkdirAll(dirgutaDB, 0755); err != nil { //nolint:mnd
		return nil, err
	}

	db := db.NewDB(dirgutaDB)

	if err := db.CreateDB(); err != nil {
		return nil, err
	}

	db.SetBatchSize(dbBatchSize)

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(db))

	return db.Close, nil
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
