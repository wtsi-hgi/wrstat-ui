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
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/mountpath"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/internal/watchenv"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
	"github.com/wtsi-hgi/wrstat-ui/summary/groupuser"
	"github.com/wtsi-hgi/wrstat-ui/summary/usergroup"
)

const (
	summariseDBBatchSize                 = 100_000
	summariseProgressEveryRows           = 1_000_000
	bytesPerMiB                          = 1024 * 1024
	summariseDirPerm                     = 0o755
	summariseMarkerPerm                  = 0o600
	clickhouseRecoverFlag                = "clickhouse-recover"
	completionMarkerName                 = ".wrstat-ui-summarise-complete"
	defaultSummariseFilesInsertBytes     = 16 * bytesPerMiB
	defaultSummariseFilterInsertBytes    = 8 * bytesPerMiB
	defaultSummariseOtherInsertBytes     = 32 * bytesPerMiB
	defaultSummarisePressureActiveParts  = 1000
	defaultSummarisePressureMerges       = 16
	defaultSummarisePressureMemoryBytes  = 64 * 1024 * bytesPerMiB
	defaultSummarisePressureQueryLatency = time.Second
	defaultSummarisePressurePollInterval = 5 * time.Second
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

	clickhouseRecover                bool
	summariseFilesInsertBytes        int64
	summariseFilterInsertBytes       int64
	summariseOtherInsertBytes        int64
	summarisePressureMaxActiveParts  int64
	summarisePressureMaxMerges       int64
	summarisePressureMaxMemoryBytes  int64
	summarisePressureMaxQueryLatency time.Duration
	summarisePressurePollInterval    time.Duration
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
	errSummariseInsertByteTarget = errors.New(
		"clickhouse summarise insert byte targets must be greater than zero",
	)
	errSummarisePressureThreshold = errors.New(
		"clickhouse pressure thresholds must not be negative",
	)
	errSummarisePressurePollInterval = errors.New(
		"clickhouse pressure poll interval must be greater than zero and at most 1m",
	)
	errSummarisePressureQueryLatency = errors.New(
		"clickhouse pressure query latency must not be negative",
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
		warnIfSummariseUnguarded()

		if err := run(args); err != nil {
			die("%s", err)
		}
	},
}

func validateSummariseClickHouseInsertFlags() error {
	if err := validateSummariseInsertByteFlags(); err != nil {
		return err
	}

	if err := validateSummarisePressureThresholdFlags(); err != nil {
		return err
	}

	return validateSummarisePressureDurationFlags()
}

func validateSummariseInsertByteFlags() error {
	if summariseFilesInsertBytes <= 0 || summariseFilterInsertBytes <= 0 || summariseOtherInsertBytes <= 0 {
		return errSummariseInsertByteTarget
	}

	return nil
}

func validateSummarisePressureThresholdFlags() error {
	thresholds := [...]int64{
		summarisePressureMaxActiveParts,
		summarisePressureMaxMerges,
		summarisePressureMaxMemoryBytes,
	}
	for _, threshold := range thresholds {
		if threshold < 0 {
			return errSummarisePressureThreshold
		}
	}

	return nil
}

func validateSummarisePressureDurationFlags() error {
	if summarisePressurePollInterval <= 0 || summarisePressurePollInterval > time.Minute {
		return errSummarisePressurePollInterval
	}

	if summarisePressureMaxQueryLatency < 0 {
		return errSummarisePressureQueryLatency
	}

	return nil
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
	addMountpointsFlag(summariseCmd.Flags(), &mounts)

	// ClickHouse flags (must override env vars if specified).
	addClickhouseConnectionFlags(summariseCmd.Flags(), &clickhouseDSN, &clickhouseDatabase)
	addClickhouseQueryTimeoutFlag(summariseCmd.Flags(), &clickhouseQueryTO)
	summariseCmd.Flags().BoolVar(&clickhouseRecover, clickhouseRecoverFlag, false,
		"recover a failed ClickHouse summarise retry")
	addSummariseClickHouseInsertFlags(summariseCmd)
}

func warnIfSummariseUnguarded() {
	if os.Getenv(watchenv.Name) == watchenv.Value {
		return
	}

	warn("summarise is not protected by the watch scheduler concurrency limit; " +
		"concurrent manual jobs may overload ClickHouse and shared storage")
}

type compressedFile struct {
	*pgzip.Writer
	file *os.File
}

func (c *compressedFile) Close() error {
	err := c.Writer.Close()
	fileErr := c.file.Close()

	if err != nil {
		return err
	}

	return fileErr
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

type compressedStatsFile struct {
	*pgzip.Reader
	file *os.File
}

func (c *compressedStatsFile) Close() error {
	return errors.Join(c.Reader.Close(), c.file.Close())
}

func wireClickHouseOperations( //nolint:funlen
	s *summary.Summariser,
	cfg clickhouse.Config,
	mountPath, _ string,
	modtime time.Time,
	diag *summariseDiagnostics,
) (func(bool) error, error) {
	idAllocator := summary.NewDirIDAllocator()
	if err := idAllocator.SetMountPath(mountPath); err != nil {
		return nil, fmt.Errorf("failed to reserve directory ids: %w", err)
	}

	dw, err := clickhouse.NewDGUTAWriter(cfg)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create dguta writer: %w", err,
		)
	}

	fi, fiCloser, err := clickhouse.NewFileIngestOperation(
		cfg, mountPath, modtime, idAllocator,
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
	setSummariseImportPhaseRecorder(diag.recordImportPhase, dw, fiCloser)

	bs, err := setupBasedirsStore(
		s, cfg, mountPath, cfg.MountPoints, modtime,
	)
	if err != nil {
		return nil, errors.Join(
			err,
			fiCloser.Close(),
			closeSummariseDGUTAWriter(dw, false),
		)
	}

	setClickHouseBatchSize(summariseDBBatchSize, bs)
	setSummariseImportPhaseRecorder(diag.recordImportPhase, bs)

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(dw, idAllocator))
	s.AddGlobalOperation(fi)

	var basedirsCloser func(bool) error

	if bs != nil {
		basedirsCloser = func(publish bool) error {
			return summariseutil.CloseOrAbort(bs, publish)
		}
	}

	return composeSummariseCloser(fiCloser, basedirsCloser, dw), nil
}

func setupBasedirsStore(
	s *summary.Summariser,
	cfg clickhouse.Config,
	mountPath string,
	mountpoints []string,
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
		return nil, errors.Join(err, summariseutil.CloseOrAbort(bs, false))
	}

	return bs, nil
}

func addBasedirsSummariser(
	s *summary.Summariser,
	store basedirs.Store,
	quotaPath string,
	basedirsConfig string,
	mountpoints []string,
	modtime time.Time,
) error {
	quotas, config, err := summariseutil.ParseBasedirConfig(quotaPath, basedirsConfig)
	if err != nil {
		return err
	}

	bd, err := summariseutil.NewBaseDirsCreator(store, quotas, mountpoints, modtime)
	if err != nil {
		return err
	}

	s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return nil
}

func setClickHouseBatchSize(batchSize int, targets ...any) {
	summariseutil.SetBatchSize(batchSize, targets...)
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

func setSummariseProgress(s *summary.Summariser, diag *summariseDiagnostics) {
	diag.setProgress(s)
}

func bytesToMiB(bytes uint64) uint64 {
	return bytes / bytesPerMiB
}

func setupClickHouseSummariseHooks(
	s *summary.Summariser,
	chTarget *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) (*summariseRunHooks, error) {
	if chTarget == nil {
		return nil, nil //nolint:nilnil
	}

	if err := preflightClickHouseActiveSnapshot(*chTarget); err != nil {
		return nil, err
	}

	return addClickHouseSummariseHooks(s, chTarget, diag)
}

func preflightClickHouseActiveSnapshotForSpool(
	target clickHouseSummariseTarget,
	spoolDir string,
	manifest *chspool.Manifest,
) error {
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
		return preflightClickHouseActiveSnapshotRetry(target, spoolDir, manifest)
	}

	return summariseActiveSnapshotRewriteError(target)
}

func summariseSpoolPublishCanResume(spoolDir string, manifest *chspool.Manifest) (bool, error) {
	if manifest == nil {
		return false, nil
	}

	return clickhouse.SummariseSpoolPublishCanResume(spoolDir, manifest)
}

func abortSummariseHooks(hooks *summariseRunHooks) error {
	if hooks == nil || hooks.close == nil {
		return nil
	}

	return hooks.close(false)
}

func summariseWithHooks(
	s *summary.Summariser,
	hooks *summariseRunHooks,
	diag *summariseDiagnostics,
) error {
	diag.setCurrentPhase("parse")

	parseCounter := addSummariseParseCounter(s)

	err := s.Summarise()
	diag.logParseResult(parseCounter.Count(), err)

	err = errors.Join(err, closeSummariseHooksWithDiagnostics(hooks, diag, err == nil))
	if err != nil {
		diag.logFailure(err)

		return err
	}

	return writeSummariseCompletionMarkerWithDiagnostics(hooks, diag)
}

func closeSummariseHooksWithDiagnostics(
	hooks *summariseRunHooks,
	diag *summariseDiagnostics,
	publish bool,
) error {
	if hooks == nil || hooks.close == nil {
		return nil
	}

	diag.logCloseStart(publish)
	err := hooks.close(publish)
	diag.logCloseResult(publish, err)

	return err
}

func writeSummariseCompletionMarkerWithDiagnostics(
	hooks *summariseRunHooks,
	diag *summariseDiagnostics,
) error {
	if hooks == nil {
		diag.logCompletionMarkerResult(nil)

		return nil
	}

	err := writeSummariseCompletionMarker(hooks.completionTarget)
	diag.logCompletionMarkerResult(err)

	return err
}

type clickHouseSummariseTarget struct {
	cfg             clickhouse.Config
	mountPath       string
	mountpointsPath string
	modtime         time.Time
	outputDir       string
}

func prepareClickHouseSummariseTarget(
	mountpointsPath string,
	modtime time.Time,
) (*clickHouseSummariseTarget, error) {
	if basedirsDB == "" && dirgutaDB == "" {
		return nil, nil //nolint:nilnil
	}

	mountPath, err := deriveMountPathForClickHouseSummarise(
		basedirsDB, dirgutaDB, defaultDir,
	)
	if err != nil {
		return nil, err
	}

	outputDir, err := summariseSpoolOutputDir()
	if err != nil {
		return nil, err
	}

	cfg, err := clickhouseSummariserConfig(mountpointsPath)
	if err != nil {
		return nil, err
	}

	return &clickHouseSummariseTarget{
		cfg:             cfg,
		mountPath:       mountPath,
		mountpointsPath: mountpointsPath,
		modtime:         modtime,
		outputDir:       outputDir,
	}, nil
}

func preflightClickHouseActiveSnapshot(target clickHouseSummariseTarget) error {
	return preflightClickHouseActiveSnapshotForSpool(target, "", nil)
}

func preflightClickHouseActiveSnapshotRetry(
	target clickHouseSummariseTarget,
	spoolDir string,
	manifest *chspool.Manifest,
) error {
	markerMatches, err := summariseCompletionMarkerMatches(target)
	if err != nil {
		return err
	}

	if markerMatches {
		return errSummariseClickHouseSnapshotAlreadyActive
	}

	resumable, err := summariseSpoolPublishCanResume(spoolDir, manifest)
	if err != nil {
		return err
	}

	if resumable {
		return nil
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
	diag *summariseDiagnostics,
) (*summariseRunHooks, error) {
	closer, err := wireSummariseClickHouseOperations(
		s, chTarget.cfg, chTarget.mountPath, chTarget.mountpointsPath, chTarget.modtime, diag,
	)
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

	setArgsDefaults()

	modtime, err := statsFileModtime(args[0])
	if err != nil {
		return err
	}

	diag := newSummariseDiagnostics(args[0])
	diag.setOutputDir(defaultDir)

	defer diag.stopSignalHandler()

	chTarget, err := prepareClickHouseSummariseTarget(mounts, modtime)
	if err != nil {
		return err
	}

	handled, err := maybeRunClickHouseSpoolSummarise(args[0], chTarget, diag)
	if handled {
		return err
	}

	r, modtime, err := openStatsFile(args[0])
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, r.Close())
	}()

	s := summary.NewSummariser(stats.NewStatsParser(r))

	setSummariseProgress(s, diag)

	hooks, err := setSummarisers(s, mounts, modtime, diag)
	if errors.Is(err, errSummariseClickHouseSnapshotAlreadyActive) {
		return nil
	}

	if err != nil {
		diag.logFailure(err)

		return err
	}

	return summariseWithHooks(s, hooks, diag)
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

func openStatsFile(statsFile string) (io.ReadCloser, time.Time, error) {
	if statsFile == "-" {
		return io.NopCloser(os.Stdin), time.Now(), nil
	}

	f, err := os.Open(statsFile)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to open stats file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()

		return nil, time.Time{}, err
	}

	if strings.HasSuffix(statsFile, ".gz") {
		r, err := pgzip.NewReader(f)
		if err != nil {
			_ = f.Close()

			return nil, time.Time{}, fmt.Errorf("failed to decompress stats file: %w", err)
		}

		return &compressedStatsFile{Reader: r, file: f}, fi.ModTime(), nil
	}

	return f, fi.ModTime(), nil
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
	diag *summariseDiagnostics,
) (*summariseRunHooks, error) {
	chTarget, err := prepareClickHouseSummariseTarget(mountpoints, modtime)
	if err != nil {
		return nil, err
	}

	diag.setTarget(chTarget)
	diag.logStart()
	diag.startSignalHandler()

	hooks, err := setupClickHouseSummariseHooks(s, chTarget, diag)
	if err != nil {
		return nil, err
	}

	err = addOutputSummarisers(s)
	if err != nil {
		return nil, errors.Join(err, abortSummariseHooks(hooks))
	}

	return hooks, nil
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

func deriveMountPathForClickHouseSummarise(
	basedirsDB, dirgutaDB, defaultDir string,
) (string, error) {
	var lastErr error

	for _, c := range mountPathCandidates(basedirsDB, dirgutaDB, defaultDir) {
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

func mountPathCandidates(
	basedirsDB, dirgutaDB, defaultDir string,
) []string {
	var candidates []string

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

func clickhouseSummariserConfig(
	mountpoints string,
) (clickhouse.Config, error) {
	cfg, err := loadClickhouseConfigWithMountpoints(clickhouseConfigInput{
		dsnFlag:          clickhouseDSN,
		databaseFlag:     clickhouseDatabase,
		queryTimeoutFlag: clickhouseQueryTO,
	}, mountpoints)
	if err != nil {
		return clickhouse.Config{}, err
	}

	if err := validateSummariseClickHouseInsertFlags(); err != nil {
		return clickhouse.Config{}, err
	}

	cfg.SummariseFilesInsertBytes = summariseFilesInsertBytes
	cfg.SummariseFilterInsertBytes = summariseFilterInsertBytes
	cfg.SummariseOtherInsertBytes = summariseOtherInsertBytes
	cfg.SummarisePressureMaxActiveParts = summarisePressureMaxActiveParts
	cfg.SummarisePressureMaxMerges = summarisePressureMaxMerges
	cfg.SummarisePressureMaxMemoryBytes = summarisePressureMaxMemoryBytes
	cfg.SummarisePressureMaxQueryLatency = summarisePressureMaxQueryLatency
	cfg.SummarisePressurePollInterval = summarisePressurePollInterval

	return cfg, nil
}

func addSummariseClickHouseInsertFlags(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.Int64Var(&summariseFilesInsertBytes, "clickhouse-files-insert-bytes",
		defaultSummariseFilesInsertBytes, "maximum estimated uncompressed bytes per files insert batch")
	flags.Int64Var(&summariseFilterInsertBytes, "clickhouse-filter-insert-bytes",
		defaultSummariseFilterInsertBytes, "maximum estimated uncompressed bytes per filter insert batch")
	flags.Int64Var(&summariseOtherInsertBytes, "clickhouse-other-insert-bytes",
		defaultSummariseOtherInsertBytes, "maximum estimated uncompressed bytes per other insert batch")
	flags.Int64Var(&summarisePressureMaxActiveParts, "clickhouse-pressure-active-parts",
		defaultSummarisePressureActiveParts, "pause inserts above this active-part count; zero disables")
	flags.Int64Var(&summarisePressureMaxMerges, "clickhouse-pressure-merges",
		defaultSummarisePressureMerges, "pause inserts above this active-merge count; zero disables")
	flags.Int64Var(&summarisePressureMaxMemoryBytes, "clickhouse-pressure-memory-bytes",
		defaultSummarisePressureMemoryBytes, "pause inserts above this server memory use; zero disables")
	flags.DurationVar(&summarisePressureMaxQueryLatency, "clickhouse-pressure-query-latency",
		defaultSummarisePressureQueryLatency, "pause inserts above this pressure-probe latency; zero disables")
	flags.DurationVar(&summarisePressurePollInterval, "clickhouse-pressure-poll-interval",
		defaultSummarisePressurePollInterval, "poll interval while ClickHouse is under pressure")
}
