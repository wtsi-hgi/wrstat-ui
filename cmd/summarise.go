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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
	"github.com/wtsi-hgi/wrstat-ui/summary/groupuser"
	"github.com/wtsi-hgi/wrstat-ui/summary/usergroup"
)

const (
	summariseDBBatchSize   = 100_000
	summariseDirPerm       = 0o755
	summariseMarkerPerm    = 0o600
	maxMountPathCandidates = 4
	clickhouseRecoverFlag  = "clickhouse-recover"
	completionMarkerName   = ".wrstat-ui-summarise-complete"
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

	clickhouseRecover bool
)

var (
	errSummariseExactlyOneInput = errors.New(
		"exactly 1 input file should be provided",
	)
	errSummariseNoOutput = errors.New(
		"no output files specified",
	)
	errNoOutputDir = errors.New(
		"no output directory available for mount-path derivation",
	)
	errSummariseClickHouseSnapshotAlreadyActive = errors.New(
		"clickhouse snapshot already active",
	)
	errSummariseClickHouseActiveSnapshotRewrite = errors.New(
		"clickhouse: refusing to rewrite active snapshot",
	)
)

var (
	clickHouseSnapshotIsActive           = clickhouse.ActiveSnapshotMatches
	clickHouseCleanActiveSnapshotAttempt = clickhouse.CleanActiveSnapshotAttempt
	wireSummariseClickHouseOperations    = wireClickHouseOperations
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

	// ClickHouse flags (must override env vars if specified).
	summariseCmd.Flags().StringVarP(&clickhouseDSN, "clickhouse-dsn", "C", "",
		"ClickHouse DSN (default $WRSTAT_CLICKHOUSE_DSN)")
	summariseCmd.Flags().StringVarP(&clickhouseDatabase, "clickhouse-database", "D", "",
		"ClickHouse database name (default $WRSTAT_CLICKHOUSE_DATABASE)")
	summariseCmd.Flags().StringVar(&clickhouseQueryTO, "query-timeout", "",
		"Per-query timeout (default $WRSTAT_QUERY_TIMEOUT)")
	summariseCmd.Flags().BoolVar(&clickhouseRecover, clickhouseRecoverFlag, false,
		"recover a failed ClickHouse summarise retry")
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

func wireClickHouseOperations( //nolint:funlen
	s *summary.Summariser,
	cfg clickhouse.Config,
	mountPath, mountpoints string,
	modtime time.Time,
) (func(bool) error, error) {
	dw, err := clickhouse.NewDGUTAWriter(cfg)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create dguta writer: %w", err,
		)
	}

	fi, fiCloser, err := clickhouse.NewFileIngestOperation(
		cfg, mountPath, modtime,
	)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("failed to create file ingest operation: %w", err),
			closeSummariseDGUTAWriter(dw, false),
		)
	}

	dw.SetMountPath(mountPath)
	dw.SetUpdatedAt(modtime)
	setClickHouseBatchSize(summariseDBBatchSize, dw, fiCloser)

	bs, err := setupBasedirsStore(
		s, cfg, mountPath, mountpoints, modtime,
	)
	if err != nil {
		return nil, errors.Join(
			err,
			fiCloser.Close(),
			closeSummariseDGUTAWriter(dw, false),
		)
	}

	setClickHouseBatchSize(summariseDBBatchSize, bs)

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(dw))
	s.AddGlobalOperation(fi)

	var basedirsCloser func(bool) error

	if bs != nil {
		basedirsCloser = func(publish bool) error {
			return closeSummariseBasedirsStore(bs, publish)
		}
	}

	return composeSummariseCloser(fiCloser, basedirsCloser, dw), nil
}

func setClickHouseBatchSize(batchSize int, targets ...any) {
	if batchSize <= 0 {
		return
	}

	for _, target := range targets {
		setter, ok := target.(batchSizeSetter)
		if !ok {
			continue
		}

		setter.SetBatchSize(batchSize)
	}
}

func composeSummariseCloser(
	fileCloser io.Closer,
	basedirsCloser func(bool) error,
	dgutaCloser io.Closer,
) func(bool) error {
	return summariseutil.ComposePublishCloser(fileCloser, basedirsCloser, dgutaCloser)
}

func closeSummariseDGUTAWriter(writer io.Closer, publish bool) error {
	return summariseutil.CloseOrAbort(writer, publish)
}

func setupBasedirsStore(
	s *summary.Summariser,
	cfg clickhouse.Config,
	mountPath, mountpoints string,
	modtime time.Time,
) (basedirs.Store, error) {
	if basedirsDB == "" {
		return nil, nil //nolint:nilnil
	}

	bs, err := clickhouse.NewBaseDirsStore(cfg)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create basedirs store: %w", err,
		)
	}

	bs.SetMountPath(mountPath)
	bs.SetUpdatedAt(modtime)

	if err := addBasedirsSummariser(
		s, bs, quotaPath, basedirsConfig,
		mountpoints, modtime,
	); err != nil {
		_ = bs.Close()

		return nil, err
	}

	return bs, nil
}

func addBasedirsSummariser(
	s *summary.Summariser,
	store basedirs.Store,
	quotaPath string,
	basedirsConfig string,
	mountpoints string,
	modtime time.Time,
) error {
	quotas, config, err := summariseutil.ParseBasedirConfig(quotaPath, basedirsConfig)
	if err != nil {
		return err
	}

	mps, err := parseOptionalMountpoints(mountpoints)
	if err != nil {
		return err
	}

	bd, err := summariseutil.NewBaseDirsCreator(store, quotas, mps, modtime)
	if err != nil {
		return err
	}

	s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return nil
}

func closeSummariseBasedirsStore(store basedirs.Store, publish bool) error {
	return summariseutil.CloseOrAbort(store, publish)
}

type batchSizeSetter interface {
	SetBatchSize(batchSize int)
}

type clickHouseSummariseTarget struct {
	cfg         clickhouse.Config
	mountPath   string
	mountpoints string
	modtime     time.Time
	outputDir   string
}

func prepareClickHouseSummariseTarget(
	mountpoints string,
	modtime time.Time,
) (*clickHouseSummariseTarget, error) {
	if basedirsDB == "" && dirgutaDB == "" {
		return nil, nil //nolint:nilnil
	}

	loadClickhouseDotEnv()

	mountPath, err := deriveMountPathForClickHouseSummarise(
		basedirsDB, dirgutaDB, defaultDir,
	)
	if err != nil {
		return nil, err
	}

	cfg, err := clickhouseSummariserConfig(mountpoints)
	if err != nil {
		return nil, err
	}

	return &clickHouseSummariseTarget{
		cfg:         cfg,
		mountPath:   mountPath,
		mountpoints: mountpoints,
		modtime:     modtime,
		outputDir:   defaultDir,
	}, nil
}

func preflightClickHouseActiveSnapshot(target clickHouseSummariseTarget) error {
	active, err := clickHouseSnapshotIsActive(
		target.cfg,
		target.mountPath,
		target.modtime,
	)
	if err != nil {
		return err
	}

	if !active {
		return nil
	}

	if clickhouseRecover {
		return preflightClickHouseActiveSnapshotRetry(target)
	}

	return summariseActiveSnapshotRewriteError(target)
}

func preflightClickHouseActiveSnapshotRetry(target clickHouseSummariseTarget) error {
	markerMatches, err := summariseCompletionMarkerMatches(target)
	if err != nil {
		return err
	}

	if markerMatches {
		return errSummariseClickHouseSnapshotAlreadyActive
	}

	if err := clickHouseCleanActiveSnapshotAttempt(target.cfg, target.mountPath, target.modtime); err != nil {
		return err
	}

	return nil
}

func summariseCompletionMarkerMatches(target clickHouseSummariseTarget) (bool, error) {
	if target.outputDir == "" {
		return false, nil
	}

	data, err := os.ReadFile(summariseCompletionMarkerPath(target.outputDir))
	if os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to read summarise completion marker: %w", err)
	}

	if !json.Valid(data) {
		return false, nil
	}

	var marker summariseCompletionMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return false, fmt.Errorf("failed to parse summarise completion marker: %w", err)
	}

	expected := newSummariseCompletionMarker(target)

	return marker == expected, nil
}

func summariseCompletionMarkerPath(outputDir string) string {
	return filepath.Join(outputDir, completionMarkerName)
}

func newSummariseCompletionMarker(target clickHouseSummariseTarget) summariseCompletionMarker {
	return summariseCompletionMarker{
		MountPath: target.mountPath,
		UpdatedAt: target.modtime.UTC().Format(time.RFC3339Nano),
	}
}

func summariseActiveSnapshotRewriteError(target clickHouseSummariseTarget) error {
	return fmt.Errorf(
		"%w: mount_path=%s updated_at=%s",
		errSummariseClickHouseActiveSnapshotRewrite,
		target.mountPath,
		target.modtime.UTC().Format(time.RFC3339Nano),
	)
}

func writeSummariseCompletionMarker(target *clickHouseSummariseTarget) error {
	if target == nil || target.outputDir == "" {
		return nil
	}

	data, err := json.Marshal(newSummariseCompletionMarker(*target))
	if err != nil {
		return err
	}

	data = append(data, '\n')

	if err := os.WriteFile(
		summariseCompletionMarkerPath(target.outputDir),
		data,
		summariseMarkerPerm,
	); err != nil {
		return fmt.Errorf("failed to write summarise completion marker: %w", err)
	}

	return nil
}

type summariseCompletionMarker struct {
	MountPath string `json:"mount_path"`
	UpdatedAt string `json:"updated_at"`
}

type summariseRunHooks struct {
	close            func(bool) error
	completionTarget *clickHouseSummariseTarget
}

func addClickHouseSummariseHooks(
	s *summary.Summariser,
	chTarget *clickHouseSummariseTarget,
) (*summariseRunHooks, error) {
	closer, err := addClickHouseSummarisers(s, *chTarget)
	if err != nil {
		return nil, err
	}

	return &summariseRunHooks{
		close:            closer,
		completionTarget: chTarget,
	}, nil
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

	hooks, err := setSummarisers(s, mounts, modtime)
	if errors.Is(err, errSummariseClickHouseSnapshotAlreadyActive) {
		return nil
	}

	if err != nil {
		return err
	}

	if hooks == nil {
		return s.Summarise()
	}

	err = s.Summarise()
	if hooks.close != nil {
		err = errors.Join(err, hooks.close(err == nil))
	}

	if err != nil {
		return err
	}

	return writeSummariseCompletionMarker(hooks.completionTarget)
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

func setSummarisers(
	s *summary.Summariser,
	mountpoints string,
	modtime time.Time,
) (*summariseRunHooks, error) {
	chTarget, err := prepareClickHouseSummariseTarget(mountpoints, modtime)
	if err != nil {
		return nil, err
	}

	if chTarget != nil {
		err = preflightClickHouseActiveSnapshot(*chTarget)
		if err != nil {
			return nil, err
		}
	}

	err = addOutputSummarisers(s)
	if err != nil {
		return nil, err
	}

	if chTarget == nil {
		return nil, nil //nolint:nilnil
	}

	return addClickHouseSummariseHooks(s, chTarget)
}

func addOutputSummarisers(s *summary.Summariser) error {
	if userGroup != "" {
		if err := addUserGroupSummariser(s, userGroup); err != nil {
			return err
		}
	}

	if groupUser != "" {
		if err := addGroupUserSummariser(s, groupUser); err != nil {
			return err
		}
	}

	return nil
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

func addClickHouseSummarisers(
	s *summary.Summariser,
	target clickHouseSummariseTarget,
) (func(bool) error, error) {
	return wireSummariseClickHouseOperations(
		s, target.cfg, target.mountPath, target.mountpoints, target.modtime,
	)
}

func deriveMountPathForClickHouseSummarise(
	basedirsDB, dirgutaDB, defaultDir string,
) (string, error) {
	candidates := mountPathCandidates(
		basedirsDB, dirgutaDB, defaultDir,
	)

	return resolveFirstMountPath(candidates)
}

func mountPathCandidates(
	basedirsDB, dirgutaDB, defaultDir string,
) []string {
	candidates := make([]string, 0, maxMountPathCandidates)

	if defaultDir != "" {
		candidates = append(candidates, defaultDir)
	}

	if dirgutaDB != "" {
		candidates = append(candidates, dirgutaDB)
		candidates = append(candidates, filepath.Dir(dirgutaDB))
	}

	if basedirsDB != "" {
		candidates = append(candidates, filepath.Dir(basedirsDB))
	}

	return candidates
}

func resolveFirstMountPath(
	candidates []string,
) (string, error) {
	var lastErr error

	for _, c := range candidates {
		mp, err := mountpath.FromOutputDir(c)
		if err == nil {
			return mp, nil
		}

		lastErr = err
	}

	if lastErr == nil {
		lastErr = errNoOutputDir
	}

	return "", lastErr
}

func clickhouseSummariserConfig(
	mountpoints string,
) (clickhouse.Config, error) {
	mps, err := parseOptionalMountpoints(mountpoints)
	if err != nil {
		return clickhouse.Config{}, err
	}

	return clickhouseConfigFromEnvAndFlags(
		clickhouseDSN,
		clickhouseDatabase,
		"",
		mps,
		"",
		0,
		clickhouseQueryTO,
	)
}
