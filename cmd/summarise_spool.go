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
	"cmp"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
	"github.com/wtsi-hgi/wrstat-ui/internal/summariseutil"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	clickHouseSpoolDirName    = ".wrstat-ui-clickhouse-spool"
	clickHouseSpoolSchemaMark = "wrstat-ui-clickhouse-summarise-spool-v1"
)

var loadSummariseClickHouseSpool = clickhouse.LoadSummariseSpool

var (
	errSummariseSpoolFileDirRequired       = errors.New("clickhouse spool: file row requires directory path")
	errSummariseSpoolFileNameRequired      = errors.New("clickhouse spool: file row requires entry name")
	errSummariseSpoolFileNonNegative       = errors.New("clickhouse spool: file row requires non-negative numeric fields")
	errSummariseSpoolChildrenParent        = errors.New("clickhouse spool: child rows require parent")
	errSummariseSpoolDirFactsDir           = errors.New("clickhouse spool: dir facts require dir")
	errSummariseSpoolBasedirsNotReset      = errors.New("clickhouse spool: basedirs store not reset")
	errSummariseSpoolSubdirPositionInvalid = errors.New("clickhouse spool: basedirs subdir position overflows UInt32")
)

type summariseSpoolDataset struct {
	set         *chspool.Set
	mountPath   string
	updatedAt   time.Time
	snapshotID  string
	refreshedAt time.Time
}

type summariseFileSpoolOperation struct {
	ds *summariseSpoolDataset
}

type summariseDGUTASpoolWriter struct {
	ds                   *summariseSpoolDataset
	mountPath            string
	updatedAt            time.Time
	snapshotID           string
	refreshedAt          time.Time
	previousDGUTARows    summariseDGUTARecordRows
	closed               bool
	projectionSetWritten bool
}

type summariseBasedirsSpoolStore struct {
	ds                       *summariseSpoolDataset
	mountPath                string
	updatedAt                time.Time
	snapshotID               string
	reset                    bool
	bufferedAgeAllGroupUsage map[uint32][]*basedirs.Usage
}

type summariseDGUTARowKey struct {
	dir         string
	gid         uint32
	uid         uint32
	ft          uint16
	age         uint8
	count       uint64
	size        uint64
	atime       int64
	mtime       int64
	aTimeRanges [9]uint64
	mTimeRanges [9]uint64
}

type summariseDGUTARecordRows struct {
	rawDir       string
	canonicalDir string
	keys         map[summariseDGUTARowKey]struct{}
}

type summariseMountDirRecordSummary struct {
	count        uint64
	size         uint64
	atimeMin     int64
	mtimeMax     int64
	atimeBuckets summary.AgeBuckets
	mtimeBuckets summary.AgeBuckets
	uids         []uint32
	gids         []uint32
	ft           db.DirGUTAFileType
}

type summariseMountDirProjectionVectorColumns struct {
	gids         []uint32
	uids         []uint32
	fts          []uint16
	ages         []uint8
	counts       []uint64
	sizes        []uint64
	atimeMins    []int64
	mtimeMaxs    []int64
	atimeBuckets [][]uint64
	mtimeBuckets [][]uint64
}

func maybeRunClickHouseSpoolSummarise( //nolint:funlen
	statsPath string,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) (bool, error) {
	if target == nil {
		return false, nil
	}

	diag.setTarget(target)
	diag.logStart()
	diag.startSignalHandler()

	if err := preflightClickHouseActiveSnapshot(*target); err != nil {
		if errors.Is(err, errSummariseClickHouseSnapshotAlreadyActive) {
			return true, nil
		}

		return true, err
	}

	spoolDir := summariseClickHouseSpoolDir(target.outputDir)

	expected, err := newSummariseSpoolManifest(statsPath, target)
	if err != nil {
		return true, err
	}

	if manifest, ok := completeSummariseSpool(spoolDir, expected); ok {
		return true, publishSummariseSpool(spoolDir, manifest, target, diag)
	}

	manifest, err := buildSummariseSpool(statsPath, spoolDir, expected, target, diag)
	if err != nil {
		diag.logFailure(err)

		return true, err
	}

	return true, publishSummariseSpool(spoolDir, manifest, target, diag)
}

func summariseClickHouseSpoolDir(outputDir string) string {
	return filepath.Join(outputDir, clickHouseSpoolDirName)
}

func completeSummariseSpool(spoolDir string, expected chspool.Manifest) (*chspool.Manifest, bool) {
	manifest, err := chspool.ReadManifest(spoolDir)
	if err != nil {
		return nil, false
	}

	if err := chspool.VerifyManifest(spoolDir, manifest, expected); err != nil {
		return nil, false
	}

	return manifest, true
}

func newSummariseSpoolManifest( //nolint:funlen
	statsPath string,
	target *clickHouseSummariseTarget,
) (chspool.Manifest, error) {
	statsIdentity := chspool.FileIdentity{Path: statsPath}
	if statsPath != "-" {
		var err error

		statsIdentity, err = chspool.IdentifyExistingPath(statsPath, false)
		if err != nil {
			return chspool.Manifest{}, err
		}
	}

	mountsIdentity, err := chspool.IdentifyExistingPath(target.mountpointsPath, true)
	if err != nil {
		return chspool.Manifest{}, err
	}

	quotaIdentity, err := chspool.IdentifyExistingPath(quotaPath, true)
	if err != nil {
		return chspool.Manifest{}, err
	}

	configIdentity, err := chspool.IdentifyExistingPath(basedirsConfig, true)
	if err != nil {
		return chspool.Manifest{}, err
	}

	return chspool.Manifest{
		Version:         chspool.Version,
		Format:          chspool.Format,
		State:           chspool.Complete,
		MountPath:       target.mountPath,
		SnapshotID:      clickhouse.SnapshotID(target.mountPath, target.modtime),
		UpdatedAt:       target.modtime.UTC().Format(time.RFC3339Nano),
		OutputDir:       target.outputDir,
		SchemaMarker:    clickHouseSpoolSchemaMark,
		Stats:           statsIdentity,
		Mounts:          mountsIdentity,
		Quota:           quotaIdentity,
		BasedirsConfig:  configIdentity,
		BasedirsEnabled: basedirsDB != "",
	}, nil
}

func buildSummariseSpool( //nolint:funlen,gocyclo
	statsPath string,
	spoolDir string,
	expected chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) (*chspool.Manifest, error) {
	partialDir := spoolDir + ".partial"

	if err := os.RemoveAll(partialDir); err != nil {
		return nil, err
	}

	set, err := chspool.CreateSet(partialDir)
	if err != nil {
		return nil, err
	}

	ds := &summariseSpoolDataset{
		set:         set,
		mountPath:   target.mountPath,
		updatedAt:   target.modtime,
		snapshotID:  expected.SnapshotID,
		refreshedAt: time.Now().UTC(),
	}

	err = parseSummariseToSpool(statsPath, ds, diag)

	closeErr := set.Close()
	if err != nil || closeErr != nil {
		_ = os.RemoveAll(partialDir)

		return nil, errors.Join(err, closeErr)
	}

	manifest := expected
	manifest.Tables = set.TableManifests()
	manifest.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)

	if err := chspool.WriteManifestAtomic(partialDir, &manifest); err != nil {
		_ = os.RemoveAll(partialDir)

		return nil, err
	}

	if err := os.RemoveAll(spoolDir); err != nil {
		_ = os.RemoveAll(partialDir)

		return nil, err
	}

	if err := os.Rename(partialDir, spoolDir); err != nil {
		_ = os.RemoveAll(partialDir)

		return nil, err
	}

	return &manifest, nil
}

func parseSummariseToSpool( //nolint:funlen
	statsPath string,
	ds *summariseSpoolDataset,
	diag *summariseDiagnostics,
) (err error) {
	r, _, err := openStatsFile(statsPath)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, r.Close())
	}()

	s := summary.NewSummariser(stats.NewStatsParser(r))
	setSummariseProgress(s, diag)
	addSummariseSpoolOperations(s, ds)

	if addErr := addSummariseSpoolBasedirs(s, ds); addErr != nil {
		return addErr
	}

	if addErr := addOutputSummarisers(s); addErr != nil {
		return addErr
	}

	diag.setCurrentPhase("parse")

	err = s.Summarise()
	diag.logParseResult(err)

	if err != nil {
		return err
	}

	return closeSummariseSpoolOperations(ds)
}

func addSummariseSpoolOperations(s *summary.Summariser, ds *summariseSpoolDataset) {
	dw := &summariseDGUTASpoolWriter{ds: ds}
	dw.SetMountPath(ds.mountPath)
	dw.SetUpdatedAt(ds.updatedAt)

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(dw))
	s.AddGlobalOperation((&summariseFileSpoolOperation{ds: ds}).operation)
}

func addSummariseSpoolBasedirs(s *summary.Summariser, ds *summariseSpoolDataset) error {
	if basedirsDB == "" {
		return nil
	}

	quotas, config, err := summariseutil.ParseBasedirConfig(quotaPath, basedirsConfig)
	if err != nil {
		return err
	}

	mountpoints, err := summariseutil.ParseMountpointsFromFile(mounts)
	if err != nil {
		return err
	}

	store := &summariseBasedirsSpoolStore{ds: ds}
	store.SetMountPath(ds.mountPath)
	store.SetUpdatedAt(ds.updatedAt)

	creator, err := summariseutil.NewBaseDirsCreator(store, quotas, mountpoints, ds.updatedAt)
	if err != nil {
		return err
	}

	s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, creator))

	return nil
}

func closeSummariseSpoolOperations(ds *summariseSpoolDataset) error {
	return (&summariseDGUTASpoolWriter{
		ds:          ds,
		mountPath:   ds.mountPath,
		updatedAt:   ds.updatedAt,
		snapshotID:  ds.snapshotID,
		refreshedAt: ds.refreshedAt,
	}).Close()
}

func publishSummariseSpool(
	spoolDir string,
	manifest *chspool.Manifest,
	target *clickHouseSummariseTarget,
	diag *summariseDiagnostics,
) error {
	diag.logCloseStart(true)
	err := loadSummariseClickHouseSpool(context.Background(), target.cfg, spoolDir, manifest, diag.recordImportPhase)
	diag.logCloseResult(true, err)

	if err != nil {
		diag.logFailure(err)

		return err
	}

	return writeSummariseCompletionMarkerWithDiagnostics(
		&summariseRunHooks{completionTarget: target},
		diag,
	)
}

func (f *summariseFileSpoolOperation) operation() summary.Operation {
	return f
}

func (f *summariseFileSpoolOperation) Add(info *summary.FileInfo) error { //nolint:funlen
	if info == nil {
		return nil
	}

	if err := validateSummariseFileInfo(info); err != nil {
		return err
	}

	size, apparentSize, inode, nlink, err := summariseSpoolUnsignedFileInfoValues(info)
	if err != nil {
		return err
	}

	parentDir := string(info.Path.AppendTo(make([]byte, 0, info.Path.Len())))
	name := string(info.Name)

	parentDir, name, keep := summariseCanonicalFileIngestPath(f.ds.mountPath, parentDir, name)
	if !keep {
		return nil
	}

	return f.ds.set.WriteFile(chspool.FileRow{
		MountPath:    f.ds.mountPath,
		SnapshotID:   f.ds.snapshotID,
		ParentDir:    parentDir,
		Name:         name,
		Ext:          summariseExtFromName(name),
		EntryType:    info.EntryType,
		Size:         size,
		ApparentSize: apparentSize,
		UID:          info.UID,
		GID:          info.GID,
		ATime:        time.Unix(info.ATime, 0),
		MTime:        time.Unix(info.MTime, 0),
		CTime:        time.Unix(info.CTime, 0),
		Inode:        inode,
		Nlink:        nlink,
	})
}

func validateSummariseFileInfo(info *summary.FileInfo) error {
	if info.Path == nil {
		return errSummariseSpoolFileDirRequired
	}

	if len(info.Name) == 0 {
		return errSummariseSpoolFileNameRequired
	}

	if info.Size < 0 || info.ApparentSize < 0 || info.Inode < 0 || info.Nlink < 0 {
		return errSummariseSpoolFileNonNegative
	}

	return nil
}

func summariseSpoolUnsignedFileInfoValues(info *summary.FileInfo) (uint64, uint64, uint64, uint64, error) {
	size, err := summariseNonNegativeInt64ToUint64(info.Size, errSummariseSpoolFileNonNegative)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	apparentSize, err := summariseNonNegativeInt64ToUint64(info.ApparentSize, errSummariseSpoolFileNonNegative)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	inode, err := summariseNonNegativeInt64ToUint64(info.Inode, errSummariseSpoolFileNonNegative)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	nlink, err := summariseNonNegativeInt64ToUint64(info.Nlink, errSummariseSpoolFileNonNegative)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	return size, apparentSize, inode, nlink, nil
}

func summariseNonNegativeInt64ToUint64(value int64, negativeErr error) (uint64, error) {
	if value < 0 {
		return 0, negativeErr
	}

	return uint64(value), nil
}

func (f *summariseFileSpoolOperation) Output() error {
	return nil
}

func (w *summariseDGUTASpoolWriter) SetBatchSize(_ int) {}

func (w *summariseDGUTASpoolWriter) SetMountPath(mountPath string) {
	w.mountPath = summariseNormalizeImportMountPath(mountPath)
	w.refreshIDs()
}

func (w *summariseDGUTASpoolWriter) SetUpdatedAt(updatedAt time.Time) {
	w.updatedAt = updatedAt
	w.refreshIDs()
}

func (w *summariseDGUTASpoolWriter) refreshIDs() {
	if w.mountPath != "" && !w.updatedAt.IsZero() {
		w.snapshotID = clickhouse.SnapshotID(w.mountPath, w.updatedAt)
	}

	if w.refreshedAt.IsZero() && w.ds != nil {
		w.refreshedAt = w.ds.refreshedAt
	}
}

func (w *summariseDGUTASpoolWriter) AddChildren(parent *summary.DirectoryPath, children []string) error {
	if parent == nil {
		return errSummariseSpoolChildrenParent
	}

	w.refreshIDs()

	parentDir := string(parent.AppendTo(make([]byte, 0, parent.Len())))
	parentDir = summariseCanonicalPathForMount(w.mountPath, parentDir)

	return w.appendChildrenRows(children, parentDir)
}

func (w *summariseDGUTASpoolWriter) Add(dguta db.RecordDGUTA) error { //nolint:funlen,gocyclo
	if dguta.Dir == nil {
		return errSummariseSpoolDirFactsDir
	}

	w.refreshIDs()

	rawParentDir := string(dguta.Dir.AppendTo(make([]byte, 0, dguta.Dir.Len())))
	parentDir := summariseCanonicalPathForMount(w.mountPath, rawParentDir)
	children := w.canonicalChildrenForParent(parentDir, dguta.Children)
	childCount := max(dguta.ChildCount, uint64(len(children)))
	appendedGUTAs := make(db.GUTAs, 0, len(dguta.GUTAs))

	tracker := w.newDGUTARecordTracker(len(dguta.GUTAs))
	for _, guta := range dguta.GUTAs {
		key, keep, project := w.appendDGUTARow(rawParentDir, parentDir, guta, tracker.keys != nil)
		if keep && tracker.keys != nil {
			tracker.keys[key] = struct{}{}
		}

		if project {
			appendedGUTAs = append(appendedGUTAs, guta)
		}
	}

	if err := w.writeDirFactRow(parentDir, appendedGUTAs, childCount); err != nil {
		return err
	}

	if err := w.appendChildrenRows(children, parentDir); err != nil {
		return err
	}

	w.previousDGUTARows = summariseDGUTARecordRows{
		rawDir:       rawParentDir,
		canonicalDir: parentDir,
		keys:         tracker.keys,
	}

	return nil
}

func (w *summariseDGUTASpoolWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}

	w.closed = true

	return w.writeProjectionSetRow()
}

func (w *summariseDGUTASpoolWriter) Abort() error {
	w.closed = true

	return nil
}

func (w *summariseDGUTASpoolWriter) newDGUTARecordTracker(numGUTAs int) summariseDGUTARecordRows {
	if w.mountPath != "/" {
		return summariseDGUTARecordRows{}
	}

	return summariseDGUTARecordRows{keys: make(map[summariseDGUTARowKey]struct{}, numGUTAs)}
}

func (w *summariseDGUTASpoolWriter) appendDGUTARow(
	rawParentDir string,
	parentDir string,
	guta *db.GUTA,
	trackDuplicateKeys bool,
) (summariseDGUTARowKey, bool, bool) {
	if guta == nil {
		return summariseDGUTARowKey{}, false, false
	}

	if !trackDuplicateKeys {
		return summariseDGUTARowKey{}, true, true
	}

	rowKey := newSummariseDGUTARowKey(parentDir, guta)
	if w.isConsecutiveCanonicalDGUTADuplicate(rawParentDir, parentDir, rowKey) {
		return rowKey, true, false
	}

	return rowKey, true, true
}

func (w *summariseDGUTASpoolWriter) isConsecutiveCanonicalDGUTADuplicate(
	rawDir string,
	canonicalDir string,
	key summariseDGUTARowKey,
) bool {
	prev := w.previousDGUTARows
	if prev.keys == nil || prev.rawDir == rawDir || prev.canonicalDir != canonicalDir {
		return false
	}

	if prev.rawDir == prev.canonicalDir && rawDir == canonicalDir {
		return false
	}

	_, ok := prev.keys[key]

	return ok
}

func newSummariseDGUTARowKey(dir string, guta *db.GUTA) summariseDGUTARowKey {
	return summariseDGUTARowKey{
		dir:         dir,
		gid:         guta.GID,
		uid:         guta.UID,
		ft:          uint16(guta.FT),
		age:         uint8(guta.Age),
		count:       guta.Count,
		size:        guta.Size,
		atime:       guta.Atime,
		mtime:       guta.Mtime,
		aTimeRanges: guta.ATimeRanges,
		mTimeRanges: guta.MTimeRanges,
	}
}

func (w *summariseDGUTASpoolWriter) canonicalChildrenForParent(
	parentDir string,
	children []string,
) []string {
	out := make([]string, 0, len(children))

	for _, child := range children {
		child = summariseChildPathForParent(parentDir, child)
		child = summariseCanonicalPathForMount(w.mountPath, child)

		if child != "" {
			out = append(out, child)
		}
	}

	return out
}

func (w *summariseDGUTASpoolWriter) appendChildrenRows(children []string, parentDir string) error {
	for _, child := range children {
		child = summariseChildPathForParent(parentDir, child)
		child = summariseCanonicalPathForMount(w.mountPath, child)

		if child == "" {
			continue
		}

		if err := w.ds.set.WriteChild(chspool.ChildRow{
			MountPath:  w.mountPath,
			SnapshotID: w.snapshotID,
			ParentDir:  parentDir,
			Child:      child,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (w *summariseDGUTASpoolWriter) writeDirFactRow( //nolint:funlen
	dir string,
	gutas db.GUTAs,
	childCount uint64,
) error {
	allSummary, fileSummary := summariseMountDirRecordSummaries(gutas)
	if allSummary == nil {
		allSummary = newSummariseMountDirRecordSummary()
	}

	columns := summariseMountDirProjectionVectorColumnsFor(gutas)

	return w.ds.set.WriteDirFact(chspool.DirFactRow{
		MountPath:        w.mountPath,
		SnapshotID:       w.snapshotID,
		Dir:              dir,
		UpdatedAt:        w.updatedAt,
		AllCount:         allSummary.count,
		AllSize:          allSummary.size,
		AllAtimeMin:      allSummary.atimeMin,
		AllMtimeMax:      allSummary.mtimeMax,
		AllAtimeBuckets:  summariseAgeBucketsSlice(&allSummary.atimeBuckets),
		AllMtimeBuckets:  summariseAgeBucketsSlice(&allSummary.mtimeBuckets),
		AllUIDs:          allSummary.sortedUIDs(),
		AllGIDs:          allSummary.sortedGIDs(),
		AllFT:            uint16(allSummary.ft),
		FileCount:        summariseRecordSummaryCount(fileSummary),
		FileSize:         summariseRecordSummarySize(fileSummary),
		FileAtimeMin:     summariseRecordSummaryAtimeMin(fileSummary),
		FileMtimeMax:     summariseRecordSummaryMtimeMax(fileSummary),
		FileAtimeBuckets: summariseRecordSummaryATimeBuckets(fileSummary),
		FileMtimeBuckets: summariseRecordSummaryMTimeBuckets(fileSummary),
		FileUIDs:         summariseRecordSummaryUIDs(fileSummary),
		FileGIDs:         summariseRecordSummaryGIDs(fileSummary),
		FileFT:           summariseRecordSummaryFT(fileSummary),
		GIDs:             columns.gids,
		UIDs:             columns.uids,
		FTs:              columns.fts,
		Ages:             columns.ages,
		Counts:           columns.counts,
		Sizes:            columns.sizes,
		AtimeMins:        columns.atimeMins,
		MtimeMaxs:        columns.mtimeMaxs,
		AtimeBuckets:     columns.atimeBuckets,
		MtimeBuckets:     columns.mtimeBuckets,
		ChildCount:       childCount,
		RefreshedAt:      w.refreshedAt,
	})
}

func (w *summariseDGUTASpoolWriter) writeProjectionSetRow() error {
	if w.projectionSetWritten {
		return nil
	}

	w.projectionSetWritten = true

	return w.ds.set.WriteDirProjectionSet(chspool.DirProjectionSetRow{
		MountPath:   w.mountPath,
		SnapshotID:  w.snapshotID,
		UpdatedAt:   w.updatedAt,
		RefreshedAt: w.refreshedAt,
	})
}

func (s *summariseBasedirsSpoolStore) SetMountPath(mountPath string) {
	s.mountPath = summariseNormalizeImportMountPath(mountPath)
	s.refreshIDs()
}

func (s *summariseBasedirsSpoolStore) SetUpdatedAt(updatedAt time.Time) {
	s.updatedAt = updatedAt
	s.refreshIDs()
}

func (s *summariseBasedirsSpoolStore) refreshIDs() {
	if s.mountPath != "" && !s.updatedAt.IsZero() {
		s.snapshotID = clickhouse.SnapshotID(s.mountPath, s.updatedAt)
	}
}

func (s *summariseBasedirsSpoolStore) Reset() error {
	s.refreshIDs()
	s.reset = true
	s.bufferedAgeAllGroupUsage = map[uint32][]*basedirs.Usage{}

	return nil
}

func (s *summariseBasedirsSpoolStore) ensureReady() error {
	if !s.reset {
		return errSummariseSpoolBasedirsNotReset
	}

	return nil
}

func (s *summariseBasedirsSpoolStore) PutGroupUsage(u *basedirs.Usage) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	if u == nil {
		return nil
	}

	if u.Age == db.DGUTAgeAll {
		s.bufferedAgeAllGroupUsage[u.GID] = append(s.bufferedAgeAllGroupUsage[u.GID], u)

		return nil
	}

	return s.writeGroupUsage(u, summariseUnixEpochUTC(), summariseUnixEpochUTC())
}

func (s *summariseBasedirsSpoolStore) PutUserUsage(u *basedirs.Usage) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	if u == nil {
		return nil
	}

	return s.ds.set.WriteBasedirsUserUsage(chspool.BasedirsUserUsageRow{
		MountPath:   s.mountPath,
		SnapshotID:  s.snapshotID,
		UID:         u.UID,
		BaseDir:     u.BaseDir,
		Age:         uint8(u.Age),
		GIDs:        summariseEnsureNonNilUInt32s(u.GIDs),
		UsageSize:   u.UsageSize,
		QuotaSize:   u.QuotaSize,
		UsageInodes: u.UsageInodes,
		QuotaInodes: u.QuotaInodes,
		Mtime:       u.Mtime,
	})
}

func (s *summariseBasedirsSpoolStore) PutGroupSubDirs(
	key basedirs.SubDirKey,
	subdirs []*basedirs.SubDir,
) error {
	return s.writeSubDirs(chspool.TableBasedirsGroupSubdirs, key, subdirs)
}

func (s *summariseBasedirsSpoolStore) PutUserSubDirs(
	key basedirs.SubDirKey,
	subdirs []*basedirs.SubDir,
) error {
	return s.writeSubDirs(chspool.TableBasedirsUserSubdirs, key, subdirs)
}

func (s *summariseBasedirsSpoolStore) writeSubDirs( //nolint:funlen,gocognit,gocyclo
	table string,
	key basedirs.SubDirKey,
	subdirs []*basedirs.SubDir,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	for pos, sd := range subdirs {
		if sd == nil {
			continue
		}

		if uint64(pos) > math.MaxUint32 {
			return errSummariseSpoolSubdirPositionInvalid
		}

		row := chspool.BasedirsSubdirRow{
			MountPath:    s.mountPath,
			SnapshotID:   s.snapshotID,
			ID:           key.ID,
			BaseDir:      key.BaseDir,
			Age:          uint8(key.Age),
			Pos:          uint32(pos),
			SubDir:       sd.SubDir,
			NumFiles:     sd.NumFiles,
			SizeFiles:    sd.SizeFiles,
			LastModified: sd.LastModified,
			FileUsage:    summariseUsageBreakdownToCHMap(sd.FileUsage),
		}

		if table == chspool.TableBasedirsGroupSubdirs { //nolint:nestif
			if err := s.ds.set.WriteBasedirsGroupSubdir(row); err != nil {
				return err
			}
		} else if err := s.ds.set.WriteBasedirsUserSubdir(row); err != nil {
			return err
		}
	}

	return nil
}

func (s *summariseBasedirsSpoolStore) AppendGroupHistory(
	key basedirs.HistoryKey,
	point basedirs.History,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	return s.ds.set.WriteBasedirsHistory(chspool.BasedirsHistoryRow{
		MountPath:   key.MountPath,
		GID:         key.GID,
		Date:        point.Date,
		UsageSize:   point.UsageSize,
		QuotaSize:   point.QuotaSize,
		UsageInodes: point.UsageInodes,
		QuotaInodes: point.QuotaInodes,
	})
}

func (s *summariseBasedirsSpoolStore) Finalise() error { //nolint:gocognit
	if err := s.ensureReady(); err != nil {
		return err
	}

	for _, usages := range s.bufferedAgeAllGroupUsage {
		for _, u := range usages {
			if u == nil {
				continue
			}

			if err := s.writeGroupUsage(u, time.Time{}, time.Time{}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *summariseBasedirsSpoolStore) Close() error {
	return nil
}

func (s *summariseBasedirsSpoolStore) writeGroupUsage(
	u *basedirs.Usage,
	dateNoSpace time.Time,
	dateNoFiles time.Time,
) error {
	return s.ds.set.WriteBasedirsGroupUsage(chspool.BasedirsGroupUsageRow{
		MountPath:   s.mountPath,
		SnapshotID:  s.snapshotID,
		GID:         u.GID,
		BaseDir:     u.BaseDir,
		Age:         uint8(u.Age),
		UIDs:        summariseEnsureNonNilUInt32s(u.UIDs),
		UsageSize:   u.UsageSize,
		QuotaSize:   u.QuotaSize,
		UsageInodes: u.UsageInodes,
		QuotaInodes: u.QuotaInodes,
		Mtime:       u.Mtime,
		DateNoSpace: dateNoSpace,
		DateNoFiles: dateNoFiles,
	})
}

func summariseMountDirRecordSummaries(
	gutas db.GUTAs,
) (*summariseMountDirRecordSummary, *summariseMountDirRecordSummary) {
	var allSummary, fileSummary *summariseMountDirRecordSummary

	for _, guta := range gutas {
		allSummary, fileSummary = addSummariseAgeAllMountDirRecordSummary(allSummary, fileSummary, guta)
	}

	return allSummary, fileSummary
}

func addSummariseAgeAllMountDirRecordSummary(
	allSummary *summariseMountDirRecordSummary,
	fileSummary *summariseMountDirRecordSummary,
	guta *db.GUTA,
) (*summariseMountDirRecordSummary, *summariseMountDirRecordSummary) {
	if guta == nil || guta.Age != db.DGUTAgeAll {
		return allSummary, fileSummary
	}

	if allSummary == nil {
		allSummary = newSummariseMountDirRecordSummary()
	}

	allSummary.add(guta)

	if guta.FT&db.AllTypesExceptDirectories == 0 {
		return allSummary, fileSummary
	}

	if fileSummary == nil {
		fileSummary = newSummariseMountDirRecordSummary()
	}

	fileSummary.add(guta)

	return allSummary, fileSummary
}

func newSummariseMountDirRecordSummary() *summariseMountDirRecordSummary {
	return &summariseMountDirRecordSummary{}
}

func (s *summariseMountDirRecordSummary) add(guta *db.GUTA) {
	s.count += guta.Count
	s.size += guta.Size

	if guta.Atime != 0 && (s.atimeMin == 0 || guta.Atime < s.atimeMin) {
		s.atimeMin = guta.Atime
	}

	if guta.Mtime > s.mtimeMax {
		s.mtimeMax = guta.Mtime
	}

	for i, count := range guta.ATimeRanges {
		s.atimeBuckets[i] += count
	}

	for i, count := range guta.MTimeRanges {
		s.mtimeBuckets[i] += count
	}

	s.uids = summariseAppendUniqueUint32(s.uids, guta.UID)
	s.gids = summariseAppendUniqueUint32(s.gids, guta.GID)
	s.ft |= guta.FT
}

func summariseAppendUniqueUint32(values []uint32, value uint32) []uint32 {
	if slices.Contains(values, value) {
		return values
	}

	return append(values, value)
}

func (s *summariseMountDirRecordSummary) sortedUIDs() []uint32 {
	return summariseSortedUint32Slice(s.uids)
}

func (s *summariseMountDirRecordSummary) sortedGIDs() []uint32 {
	return summariseSortedUint32Slice(s.gids)
}

func summariseSortedUint32Slice(values []uint32) []uint32 {
	slices.Sort(values)

	return values
}

//nolint:funlen
func summariseMountDirProjectionVectorColumnsFor(
	gutas db.GUTAs,
) summariseMountDirProjectionVectorColumns {
	slices.SortFunc(gutas, summariseCompareProjectionGUTAs)

	columns := summariseMountDirProjectionVectorColumns{
		gids:         make([]uint32, 0, len(gutas)),
		uids:         make([]uint32, 0, len(gutas)),
		fts:          make([]uint16, 0, len(gutas)),
		ages:         make([]uint8, 0, len(gutas)),
		counts:       make([]uint64, 0, len(gutas)),
		sizes:        make([]uint64, 0, len(gutas)),
		atimeMins:    make([]int64, 0, len(gutas)),
		mtimeMaxs:    make([]int64, 0, len(gutas)),
		atimeBuckets: make([][]uint64, 0, len(gutas)),
		mtimeBuckets: make([][]uint64, 0, len(gutas)),
	}

	for _, guta := range gutas {
		if guta == nil {
			continue
		}

		columns.gids = append(columns.gids, guta.GID)
		columns.uids = append(columns.uids, guta.UID)
		columns.fts = append(columns.fts, uint16(guta.FT))
		columns.ages = append(columns.ages, uint8(guta.Age))
		columns.counts = append(columns.counts, guta.Count)
		columns.sizes = append(columns.sizes, guta.Size)
		columns.atimeMins = append(columns.atimeMins, guta.Atime)
		columns.mtimeMaxs = append(columns.mtimeMaxs, guta.Mtime)
		columns.atimeBuckets = append(columns.atimeBuckets, summariseAgeBucketsSlice(&guta.ATimeRanges))
		columns.mtimeBuckets = append(columns.mtimeBuckets, summariseAgeBucketsSlice(&guta.MTimeRanges))
	}

	return columns
}

func summariseCompareProjectionGUTAs(a, b *db.GUTA) int { //nolint:gocyclo
	if a == nil && b == nil {
		return 0
	}

	if a == nil {
		return -1
	}

	if b == nil {
		return 1
	}

	if diff := cmp.Compare(a.Age, b.Age); diff != 0 {
		return diff
	}

	if diff := cmp.Compare(a.GID, b.GID); diff != 0 {
		return diff
	}

	if diff := cmp.Compare(a.UID, b.UID); diff != 0 {
		return diff
	}

	return cmp.Compare(a.FT, b.FT)
}

func summariseRecordSummaryCount(summary *summariseMountDirRecordSummary) uint64 {
	if summary == nil {
		return 0
	}

	return summary.count
}

func summariseRecordSummarySize(summary *summariseMountDirRecordSummary) uint64 {
	if summary == nil {
		return 0
	}

	return summary.size
}

func summariseRecordSummaryAtimeMin(summary *summariseMountDirRecordSummary) int64 {
	if summary == nil {
		return 0
	}

	return summary.atimeMin
}

func summariseRecordSummaryMtimeMax(summary *summariseMountDirRecordSummary) int64 {
	if summary == nil {
		return 0
	}

	return summary.mtimeMax
}

func summariseRecordSummaryATimeBuckets(summary *summariseMountDirRecordSummary) []uint64 {
	if summary == nil {
		return summariseAgeBucketsSlice(nil)
	}

	return summariseAgeBucketsSlice(&summary.atimeBuckets)
}

func summariseRecordSummaryMTimeBuckets(summary *summariseMountDirRecordSummary) []uint64 {
	if summary == nil {
		return summariseAgeBucketsSlice(nil)
	}

	return summariseAgeBucketsSlice(&summary.mtimeBuckets)
}

func summariseRecordSummaryUIDs(summary *summariseMountDirRecordSummary) []uint32 {
	if summary == nil {
		return nil
	}

	return summary.sortedUIDs()
}

func summariseRecordSummaryGIDs(summary *summariseMountDirRecordSummary) []uint32 {
	if summary == nil {
		return nil
	}

	return summary.sortedGIDs()
}

func summariseRecordSummaryFT(summary *summariseMountDirRecordSummary) uint16 {
	if summary == nil {
		return 0
	}

	return uint16(summary.ft)
}

func summariseAgeBucketsSlice(buckets *summary.AgeBuckets) []uint64 {
	if buckets == nil {
		return make([]uint64, len(summary.AgeBuckets{}))
	}

	return buckets[:]
}

func summariseCanonicalFileIngestPath(mountPath, parentDir, name string) (string, string, bool) {
	parentDir = summariseCanonicalPathForMount(mountPath, parentDir)

	selfNamedDir := strings.HasSuffix(name, "/") && strings.HasSuffix(parentDir, name)
	if !selfNamedDir {
		return parentDir, name, true
	}

	if parentDir == "/" && name == "/" {
		return "", "", false
	}

	return strings.TrimSuffix(parentDir, name), name, true
}

func summariseCanonicalPathForMount(mountPath, path string) string {
	if mountPath != "/" || path == "" {
		return path
	}

	trimmed := strings.TrimLeft(path, "/")
	if trimmed == "" {
		return "/"
	}

	return "/" + trimmed
}

func summariseChildPathForParent(parentDir, child string) string {
	child = strings.TrimSuffix(child, "/")
	if child == "" {
		return ""
	}

	if strings.HasPrefix(child, "/") {
		return child
	}

	return parentDir + child
}

func summariseNormalizeImportMountPath(mountPath string) string {
	if mountPath == "" || mountPath == "/" || strings.HasSuffix(mountPath, "/") {
		return mountPath
	}

	return mountPath + "/"
}

func summariseExtFromName(name string) string {
	if strings.HasSuffix(name, "/") {
		return ""
	}

	idx := strings.LastIndexByte(name, '.')
	if idx <= 0 || idx == len(name)-1 {
		return ""
	}

	return strings.ToLower(name[idx+1:])
}

func summariseEnsureNonNilUInt32s(values []uint32) []uint32 {
	if values == nil {
		return []uint32{}
	}

	return values
}

func summariseUsageBreakdownToCHMap(in basedirs.UsageBreakdownByType) map[uint16]uint64 {
	if in == nil {
		return map[uint16]uint64{}
	}

	out := make(map[uint16]uint64, len(in))
	for ft, v := range in {
		out[uint16(ft)] = v
	}

	return out
}

func summariseUnixEpochUTC() time.Time {
	return time.Unix(0, 0).UTC()
}

func summariseSpoolOutputDir() (string, error) {
	if defaultDir != "" {
		return defaultDir, nil
	}

	if basedirsDB != "" {
		return filepath.Dir(basedirsDB), nil
	}

	if dirgutaDB != "" {
		return filepath.Dir(dirgutaDB), nil
	}

	return "", errNoOutputDir
}

func statsFileModtime(statsPath string) (time.Time, error) {
	if statsPath == "-" {
		return time.Now(), nil
	}

	st, err := os.Stat(statsPath)
	if err != nil {
		return time.Time{}, err
	}

	return st.ModTime(), nil
}
