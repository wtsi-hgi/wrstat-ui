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

package chspool

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const (
	ManifestName = "manifest.json"
	Version      = 1
	Complete     = "complete"

	// Format is intentionally isolated from the retry/publish logic. The first
	// production format is typed gzip+gob because it is small, standard-library
	// only, and low-risk; RowBinary can be added behind the same table boundary.
	Format = "gob-gzip-v1"

	TableFiles                = "wrstat_files"
	TableChildren             = "wrstat_children"
	TableDirFacts             = "wrstat_dir_facts"
	TableDirFilterAgeAll      = "wrstat_dir_filter_ageall"
	TableParentFacts          = "wrstat_parent_facts"
	TableDirProjectionSets    = "wrstat_dir_projection_sets"
	TableBasedirsHistory      = "wrstat_basedirs_history"
	TableBasedirsGroupUsage   = "wrstat_basedirs_group_usage"
	TableBasedirsUserUsage    = "wrstat_basedirs_user_usage"
	TableBasedirsGroupSubdirs = "wrstat_basedirs_group_subdirs"
	TableBasedirsUserSubdirs  = "wrstat_basedirs_user_subdirs"
)

var ErrManifestMismatch = errors.New("clickhouse spool manifest mismatch")

var errUnknownTable = errors.New("unknown clickhouse spool table")

var tableOrder = []string{ //nolint:gochecknoglobals
	TableFiles,
	TableDirFacts,
	TableDirFilterAgeAll,
	TableParentFacts,
	TableChildren,
	TableDirProjectionSets,
	TableBasedirsHistory,
	TableBasedirsGroupUsage,
	TableBasedirsUserUsage,
	TableBasedirsGroupSubdirs,
	TableBasedirsUserSubdirs,
}

func TableOrder() []string {
	return slices.Clone(tableOrder)
}

func countDecodedRows[T any](path string, table string) (uint64, error) {
	var rows uint64

	err := decodeRowsFromPath[T](path, table, func(T) error {
		rows++

		return nil
	})

	return rows, err
}

type FileIdentity struct {
	Path    string `json:"path,omitempty"`
	Size    int64  `json:"size,omitempty"`
	ModTime string `json:"mtime,omitempty"`
	SHA256  string `json:"sha256,omitempty"`
}

func IdentifyExistingPath(path string, includeHash bool) (FileIdentity, error) {
	if path == "" {
		return FileIdentity{}, nil
	}

	st, err := os.Stat(path)
	if err != nil {
		return FileIdentity{}, err
	}

	identity := FileIdentity{
		Path:    path,
		Size:    st.Size(),
		ModTime: st.ModTime().UTC().Format(time.RFC3339Nano),
	}

	if includeHash {
		_, sum, err := HashFile(path)
		if err != nil {
			return FileIdentity{}, err
		}

		identity.SHA256 = sum
	}

	return identity, nil
}

type TableManifest struct {
	Table  string `json:"table"`
	Path   string `json:"path"`
	Rows   uint64 `json:"rows"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func VerifyTables(dir string, tables map[string]TableManifest) error { //nolint:gocognit,gocyclo
	for _, table := range tableOrder {
		tm, ok := tables[table]
		if !ok {
			return fmt.Errorf("%w: missing table %s", ErrManifestMismatch, table)
		}

		if tm.Table != table {
			return fmt.Errorf("%w: table key %s contains %s", ErrManifestMismatch, table, tm.Table)
		}

		path := filepath.Join(dir, tm.Path)

		bytes, sum, err := HashFile(path)
		if err != nil {
			return err
		}

		if bytes != tm.Bytes || sum != tm.SHA256 {
			return fmt.Errorf("%w: table %s checksum or size changed", ErrManifestMismatch, table)
		}

		rows, err := countRows(path, table)
		if err != nil {
			return err
		}

		if rows != tm.Rows {
			return fmt.Errorf(
				"%w: table %s row count changed decoded=%d expected=%d",
				ErrManifestMismatch,
				table,
				rows,
				tm.Rows,
			)
		}
	}

	return nil
}

func HashFile(path string) (int64, string, error) {
	fh, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = fh.Close() }()

	h := sha256.New()

	n, err := io.Copy(h, fh)
	if err != nil {
		return 0, "", err
	}

	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func countRows(path string, table string) (uint64, error) { //nolint:gocyclo,cyclop
	switch table {
	case TableFiles:
		return countDecodedRows[FileRow](path, table)
	case TableChildren:
		return countDecodedRows[ChildRow](path, table)
	case TableDirFacts:
		return countDecodedRows[DirFactRow](path, table)
	case TableDirFilterAgeAll:
		return countDecodedRows[DirFilterAgeAllRow](path, table)
	case TableParentFacts:
		return countDecodedRows[ParentFactRow](path, table)
	case TableDirProjectionSets:
		return countDecodedRows[DirProjectionSetRow](path, table)
	case TableBasedirsHistory:
		return countDecodedRows[BasedirsHistoryRow](path, table)
	case TableBasedirsGroupUsage:
		return countDecodedRows[BasedirsGroupUsageRow](path, table)
	case TableBasedirsUserUsage:
		return countDecodedRows[BasedirsUserUsageRow](path, table)
	case TableBasedirsGroupSubdirs, TableBasedirsUserSubdirs:
		return countDecodedRows[BasedirsSubdirRow](path, table)
	default:
		return 0, fmt.Errorf("%w: %q", errUnknownTable, table)
	}
}

type Manifest struct {
	Version         int                      `json:"version"`
	Format          string                   `json:"format"`
	State           string                   `json:"state"`
	MountPath       string                   `json:"mount_path"`
	SnapshotID      string                   `json:"snapshot_id"`
	UpdatedAt       string                   `json:"updated_at"`
	OutputDir       string                   `json:"output_dir"`
	SchemaMarker    string                   `json:"schema_marker"`
	Stats           FileIdentity             `json:"stats"`
	Mounts          FileIdentity             `json:"mounts,omitempty"`
	Quota           FileIdentity             `json:"quota,omitempty"`
	BasedirsConfig  FileIdentity             `json:"basedirs_config,omitempty"`
	BasedirsEnabled bool                     `json:"basedirs_enabled"`
	Tables          map[string]TableManifest `json:"tables"`
	CompletedAt     string                   `json:"completed_at"`
}

func ReadManifest(dir string) (*Manifest, error) {
	data, err := os.ReadFile(ManifestPath(dir))
	if err != nil {
		return nil, err
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

func WriteManifestAtomic(dir string, manifest *Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	data = append(data, '\n')

	path := ManifestPath(dir)
	partial := path + ".partial"

	if err := os.WriteFile(partial, data, 0o600); err != nil {
		return err
	}

	return os.Rename(partial, path)
}

func ManifestPath(dir string) string {
	return filepath.Join(dir, ManifestName)
}

func VerifyManifest(
	dir string,
	got *Manifest,
	expected Manifest,
) error {
	if got == nil {
		return fmt.Errorf("%w: missing manifest", ErrManifestMismatch)
	}

	if err := verifyIdentity(got, expected); err != nil {
		return err
	}

	return VerifyTables(dir, got.Tables)
}

func verifyIdentity(got *Manifest, expected Manifest) error {
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"version", got.Version, Version},
		{"format", got.Format, Format},
		{"state", got.State, Complete},
		{"schema", got.SchemaMarker, expected.SchemaMarker},
		{"mount_path", got.MountPath, expected.MountPath},
		{"snapshot_id", got.SnapshotID, expected.SnapshotID},
		{"updated_at", got.UpdatedAt, expected.UpdatedAt},
		{"output_dir", got.OutputDir, expected.OutputDir},
		{"stats", got.Stats, expected.Stats},
		{"mounts", got.Mounts, expected.Mounts},
		{"quota", got.Quota, expected.Quota},
		{"basedirs_config", got.BasedirsConfig, expected.BasedirsConfig},
		{"basedirs_enabled", got.BasedirsEnabled, expected.BasedirsEnabled},
	}

	for _, check := range checks {
		if check.got != check.want {
			return fmt.Errorf(
				"%w: %s got %v want %v",
				ErrManifestMismatch,
				check.name,
				check.got,
				check.want,
			)
		}
	}

	return nil
}

type FileRow struct {
	MountPath    string
	SnapshotID   string
	ParentDir    string
	Name         string
	Ext          string
	EntryType    uint8
	Size         uint64
	ApparentSize uint64
	UID          uint32
	GID          uint32
	ATime        time.Time
	MTime        time.Time
	CTime        time.Time
	Inode        uint64
	Nlink        uint64
}

type ChildRow struct {
	MountPath  string
	SnapshotID string
	ParentDir  string
	Child      string
}

type DirFactRow struct {
	MountPath        string
	SnapshotID       string
	Dir              string
	UpdatedAt        time.Time
	AllCount         uint64
	AllSize          uint64
	AllAtimeMin      int64
	AllMtimeMax      int64
	AllAtimeBuckets  []uint64
	AllMtimeBuckets  []uint64
	AllUIDs          []uint32
	AllGIDs          []uint32
	AllFT            uint16
	FileCount        uint64
	FileSize         uint64
	FileAtimeMin     int64
	FileMtimeMax     int64
	FileAtimeBuckets []uint64
	FileMtimeBuckets []uint64
	FileUIDs         []uint32
	FileGIDs         []uint32
	FileFT           uint16
	GIDs             []uint32
	UIDs             []uint32
	FTs              []uint16
	Ages             []uint8
	Counts           []uint64
	Sizes            []uint64
	AtimeMins        []int64
	MtimeMaxs        []int64
	AtimeBuckets     [][]uint64
	MtimeBuckets     [][]uint64
	ChildCount       uint64
	RefreshedAt      time.Time
}

type DirFilterAgeAllRow struct {
	MountPath    string
	SnapshotID   string
	GID          uint32
	UID          uint32
	FT           uint16
	Dir          string
	Count        uint64
	Size         uint64
	AtimeMin     int64
	MtimeMax     int64
	AtimeBuckets []uint64
	MtimeBuckets []uint64
	RefreshedAt  time.Time
}

type ParentFactRow struct {
	MountPath        string
	SnapshotID       string
	ParentDir        string
	Dir              string
	UpdatedAt        time.Time
	AllCount         uint64
	AllSize          uint64
	AllAtimeMin      int64
	AllMtimeMax      int64
	AllAtimeBuckets  []uint64
	AllMtimeBuckets  []uint64
	AllUIDs          []uint32
	AllGIDs          []uint32
	AllFT            uint16
	FileCount        uint64
	FileSize         uint64
	FileAtimeMin     int64
	FileMtimeMax     int64
	FileAtimeBuckets []uint64
	FileMtimeBuckets []uint64
	FileUIDs         []uint32
	FileGIDs         []uint32
	FileFT           uint16
	GIDs             []uint32
	UIDs             []uint32
	FTs              []uint16
	Ages             []uint8
	Counts           []uint64
	Sizes            []uint64
	AtimeMins        []int64
	MtimeMaxs        []int64
	AtimeBuckets     [][]uint64
	MtimeBuckets     [][]uint64
	ChildCount       uint64
	HasChildren      uint8
	RefreshedAt      time.Time
}

type DirProjectionSetRow struct {
	MountPath   string
	SnapshotID  string
	UpdatedAt   time.Time
	RefreshedAt time.Time
}

type BasedirsHistoryRow struct {
	MountPath   string
	GID         uint32
	Date        time.Time
	UsageSize   uint64
	QuotaSize   uint64
	UsageInodes uint64
	QuotaInodes uint64
}

type BasedirsGroupUsageRow struct {
	MountPath   string
	SnapshotID  string
	GID         uint32
	BaseDir     string
	Age         uint8
	UIDs        []uint32
	UsageSize   uint64
	QuotaSize   uint64
	UsageInodes uint64
	QuotaInodes uint64
	Mtime       time.Time
	DateNoSpace time.Time
	DateNoFiles time.Time
}

type BasedirsUserUsageRow struct {
	MountPath   string
	SnapshotID  string
	UID         uint32
	BaseDir     string
	Age         uint8
	GIDs        []uint32
	UsageSize   uint64
	QuotaSize   uint64
	UsageInodes uint64
	QuotaInodes uint64
	Mtime       time.Time
}

type BasedirsSubdirRow struct {
	MountPath    string
	SnapshotID   string
	ID           uint32
	BaseDir      string
	Age          uint8
	Pos          uint32
	SubDir       string
	NumFiles     uint64
	SizeFiles    uint64
	LastModified time.Time
	FileUsage    map[uint16]uint64
}

type tableWriter struct {
	table string
	path  string
	file  *os.File
	gzip  *gzip.Writer
	enc   *gob.Encoder
	hash  hash.Hash
	bytes int64
	rows  uint64
}

func newTableWriter(dir, table string) (*tableWriter, error) {
	path := filepath.Join(dir, table+".gob.gz")

	fh, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	h := sha256.New()
	tw := &tableWriter{table: table, path: path, file: fh, hash: h}

	gz, err := gzip.NewWriterLevel(&countingHashWriter{file: fh, hash: h, n: &tw.bytes}, gzip.BestSpeed)
	if err != nil {
		_ = fh.Close()

		return nil, err
	}

	tw.gzip = gz
	tw.enc = gob.NewEncoder(gz)

	return tw, nil
}

func (w *tableWriter) close() error {
	if w == nil || w.file == nil {
		return nil
	}

	err := w.gzip.Close()
	err = errors.Join(err, w.file.Close())
	w.file = nil

	return err
}

type Set struct {
	dir    string
	tables map[string]*tableWriter
}

func CreateSet(dir string) (*Set, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	set := &Set{dir: dir, tables: make(map[string]*tableWriter, len(tableOrder))}

	for _, table := range tableOrder {
		tw, err := newTableWriter(dir, table)
		if err != nil {
			return nil, errors.Join(err, set.Close())
		}

		set.tables[table] = tw
	}

	return set, nil
}

func (s *Set) Close() error {
	if s == nil {
		return nil
	}

	var out error

	for _, table := range tableOrder {
		out = errors.Join(out, s.tables[table].close())
	}

	return out
}

func (s *Set) WriteFile(row FileRow) error {
	return s.encode(TableFiles, row)
}

func (s *Set) WriteChild(row ChildRow) error {
	return s.encode(TableChildren, row)
}

func (s *Set) WriteDirFact(row DirFactRow) error {
	return s.encode(TableDirFacts, row)
}

func (s *Set) WriteDirFilterAgeAll(row DirFilterAgeAllRow) error {
	return s.encode(TableDirFilterAgeAll, row)
}

func (s *Set) WriteParentFact(row ParentFactRow) error {
	return s.encode(TableParentFacts, row)
}

func (s *Set) WriteDirProjectionSet(row DirProjectionSetRow) error {
	return s.encode(TableDirProjectionSets, row)
}

func (s *Set) WriteBasedirsHistory(row BasedirsHistoryRow) error {
	return s.encode(TableBasedirsHistory, row)
}

func (s *Set) WriteBasedirsGroupUsage(row BasedirsGroupUsageRow) error {
	return s.encode(TableBasedirsGroupUsage, row)
}

func (s *Set) WriteBasedirsUserUsage(row BasedirsUserUsageRow) error {
	return s.encode(TableBasedirsUserUsage, row)
}

func (s *Set) WriteBasedirsGroupSubdir(row BasedirsSubdirRow) error {
	return s.encode(TableBasedirsGroupSubdirs, row)
}

func (s *Set) WriteBasedirsUserSubdir(row BasedirsSubdirRow) error {
	return s.encode(TableBasedirsUserSubdirs, row)
}

func (s *Set) encode(table string, row any) error {
	tw := s.tables[table]
	if tw == nil {
		return fmt.Errorf("%w: %q", errUnknownTable, table)
	}

	if err := tw.enc.Encode(row); err != nil {
		return fmt.Errorf("write clickhouse spool table %s: %w", table, err)
	}

	tw.rows++

	return nil
}

func (s *Set) TableManifests() map[string]TableManifest {
	out := make(map[string]TableManifest, len(tableOrder))

	for _, table := range tableOrder {
		tw := s.tables[table]
		out[table] = TableManifest{
			Table:  table,
			Path:   filepath.Base(tw.path),
			Rows:   tw.rows,
			Bytes:  tw.bytes,
			SHA256: hex.EncodeToString(tw.hash.Sum(nil)),
		}
	}

	return out
}

func decodeRowsFromPath[T any](path string, table string, fn func(T) error) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = fh.Close() }()

	gz, err := gzip.NewReader(fh)
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()

	dec := gob.NewDecoder(gz)

	for {
		var row T

		err := dec.Decode(&row)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("decode clickhouse spool table %s: %w", table, err)
		}

		if err := fn(row); err != nil {
			return err
		}
	}
}

type countingHashWriter struct {
	file *os.File
	hash hash.Hash
	n    *int64
}

func (w *countingHashWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	if n > 0 {
		_, _ = w.hash.Write(p[:n])
		*w.n += int64(n)
	}

	return n, err
}

func DecodeRows[T any](dir string, table string, fn func(T) error) error {
	path := filepath.Join(dir, table+".gob.gz")

	return decodeRowsFromPath(path, table, fn)
}
