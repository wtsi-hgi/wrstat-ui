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

package clickhouse

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/internal/chspool"
)

const (
	summariseSpoolPublishStateVersion       = 2
	summariseSpoolPublishStateLegacyVersion = 1
	summariseSpoolPublishStateName          = "post_spool_publish_state.json"
	summariseSpoolPublishStatePerm          = 0o600

	summariseSpoolPublishPhaseSnapshotPrepared         = "snapshot_prepared"
	summariseSpoolPublishPhaseTablesLoaded             = "tables_loaded"
	summariseSpoolPublishPhaseActiveVirtualReady       = "active_virtual_ready"
	summariseSpoolPublishPhaseSwitchPlanned            = "switch_planned"
	summariseSpoolPublishPhaseMountSwitched            = "mount_switched"
	summariseSpoolPublishPhaseOldSnapshotDropped       = "old_snapshot_dropped"
	summariseSpoolPublishPhaseOldActiveVirtualDropped  = "old_active_virtual_dropped"
	summariseSpoolPublishPhaseTreeSummaryRefreshed     = "tree_summary_refreshed"
	summariseSpoolPublishPhaseActivePrefixRefreshed    = "active_prefix_refreshed"
	summariseSpoolPublishPhasePostSpoolPublishComplete = "post_spool_publish_complete"

	summariseSpoolPublishOperationChildFilterAll = "derive_child_filter_all"
)

type summariseSpoolVerifiedCheckpoint struct {
	Rows       uint64 `json:"rows"`
	VerifiedAt string `json:"verified_at"`
}

type summariseSpoolSwitchPlan struct {
	HasPrevious         bool   `json:"has_previous"`
	PreviousSnapshotID  string `json:"previous_snapshot_id,omitempty"`
	PreviousActiveSetID string `json:"previous_active_set_id,omitempty"`
	NextActiveSetID     string `json:"next_active_set_id,omitempty"`
}

type summariseSpoolPublishState struct {
	Version            int                                         `json:"version"`
	ManifestKey        string                                      `json:"manifest_key"`
	MountPath          string                                      `json:"mount_path"`
	SnapshotID         string                                      `json:"snapshot_id"`
	UpdatedAt          string                                      `json:"updated_at"`
	CompletedPhases    map[string]string                           `json:"completed_phases"`
	VerifiedTables     map[string]summariseSpoolVerifiedCheckpoint `json:"verified_tables,omitempty"`
	VerifiedOperations map[string]summariseSpoolVerifiedCheckpoint `json:"verified_operations,omitempty"`
	NextActiveSetID    string                                      `json:"next_active_set_id,omitempty"`
	SwitchPlan         *summariseSpoolSwitchPlan                   `json:"switch_plan,omitempty"`
	LastCompletedUTC   string                                      `json:"last_completed_utc,omitempty"`
}

type summariseSpoolPublishTracker struct {
	path               string
	key                string
	state              summariseSpoolPublishState
	legacyCoarseTables bool
	onMark             func(string)
}

func newSummariseSpoolPublishTracker(
	spoolDir string,
	manifest *chspool.Manifest,
) (*summariseSpoolPublishTracker, error) {
	key := summariseSpoolPublishManifestKey(manifest)
	tracker := newEmptySummariseSpoolPublishTracker(spoolDir, manifest, key)

	if spoolDir == "" {
		return tracker, nil
	}

	if err := tracker.loadExisting(manifest); err != nil {
		return nil, err
	}

	return tracker, nil
}

func newEmptySummariseSpoolPublishTracker(
	spoolDir string,
	manifest *chspool.Manifest,
	key string,
) *summariseSpoolPublishTracker {
	return &summariseSpoolPublishTracker{
		path: filepath.Join(spoolDir, summariseSpoolPublishStateName),
		key:  key,
		state: summariseSpoolPublishState{
			Version:            summariseSpoolPublishStateVersion,
			ManifestKey:        key,
			MountPath:          manifest.MountPath,
			SnapshotID:         manifest.SnapshotID,
			UpdatedAt:          manifest.UpdatedAt,
			CompletedPhases:    map[string]string{},
			VerifiedTables:     map[string]summariseSpoolVerifiedCheckpoint{},
			VerifiedOperations: map[string]summariseSpoolVerifiedCheckpoint{},
		},
	}
}

func (t *summariseSpoolPublishTracker) loadExisting(manifest *chspool.Manifest) error {
	data, err := os.ReadFile(t.path)
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("clickhouse: failed to read summarise spool publish state: %w", err)
	}

	var existing summariseSpoolPublishState
	if err := json.Unmarshal(data, &existing); err != nil {
		return fmt.Errorf("clickhouse: failed to parse summarise spool publish state: %w", err)
	}

	if err := validateSummariseSpoolPublishStateVersion(existing.Version); err != nil {
		return err
	}

	if !summariseSpoolPublishStateIdentityMatches(existing, manifest, t.key) {
		return nil
	}

	t.loadMatchedState(existing, manifest)

	return nil
}

func validateSummariseSpoolPublishStateVersion(version int) error {
	if summariseSpoolPublishStateVersionSupported(version) {
		return nil
	}

	return fmt.Errorf(
		"%w: version=%d supported=%d,%d",
		errUnsupportedSummariseSpoolPublishStateVersion,
		version,
		summariseSpoolPublishStateLegacyVersion,
		summariseSpoolPublishStateVersion,
	)
}

func summariseSpoolPublishStateIdentityMatches(
	state summariseSpoolPublishState,
	manifest *chspool.Manifest,
	key string,
) bool {
	return state.ManifestKey == key &&
		state.MountPath == manifest.MountPath &&
		state.SnapshotID == manifest.SnapshotID &&
		state.UpdatedAt == manifest.UpdatedAt
}

func (t *summariseSpoolPublishTracker) loadMatchedState(
	existing summariseSpoolPublishState,
	manifest *chspool.Manifest,
) {
	if existing.CompletedPhases == nil {
		existing.CompletedPhases = map[string]string{}
	}

	if existing.VerifiedTables == nil {
		existing.VerifiedTables = map[string]summariseSpoolVerifiedCheckpoint{}
	}

	if existing.VerifiedOperations == nil {
		existing.VerifiedOperations = map[string]summariseSpoolVerifiedCheckpoint{}
	}

	t.state = existing
	if existing.Version == summariseSpoolPublishStateLegacyVersion {
		t.migrateLegacyTableCheckpoint()
	}

	t.sanitiseCheckpoints(manifest)
}

func (t *summariseSpoolPublishTracker) migrateLegacyTableCheckpoint() {
	t.state.Version = summariseSpoolPublishStateVersion
	if !t.done(summariseSpoolPublishPhaseTablesLoaded) {
		return
	}

	verifiedAt := t.state.CompletedPhases[summariseSpoolPublishPhaseTablesLoaded]
	t.state.CompletedPhases[summariseSpoolPublishPhaseSnapshotPrepared] = verifiedAt
	t.legacyCoarseTables = true
}

func (t *summariseSpoolPublishTracker) sanitiseCheckpoints(manifest *chspool.Manifest) {
	for table, checkpoint := range t.state.VerifiedTables {
		tableManifest, ok := manifest.Tables[table]
		if !ok || !validSummariseSpoolVerifiedCheckpoint(checkpoint, tableManifest.Rows) {
			delete(t.state.VerifiedTables, table)
		}
	}

	expected := manifest.Tables[chspool.TableDirFilterAll].Rows
	for operation, checkpoint := range t.state.VerifiedOperations {
		valid := operation == summariseSpoolPublishOperationChildFilterAll &&
			validSummariseSpoolVerifiedCheckpoint(checkpoint, expected)
		if !valid {
			delete(t.state.VerifiedOperations, operation)
		}
	}
}

func validSummariseSpoolVerifiedCheckpoint(checkpoint summariseSpoolVerifiedCheckpoint, rows uint64) bool {
	if checkpoint.Rows != rows || checkpoint.VerifiedAt == "" {
		return false
	}

	_, err := time.Parse(time.RFC3339Nano, checkpoint.VerifiedAt)

	return err == nil
}

func (t *summariseSpoolPublishTracker) done(phase string) bool {
	_, ok := t.state.CompletedPhases[phase]

	return ok
}

func (t *summariseSpoolPublishTracker) mark(phase string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	t.state.CompletedPhases[phase] = now
	t.state.LastCompletedUTC = now

	if err := t.save(); err != nil {
		return err
	}

	if t.onMark != nil {
		t.onMark(phase)
	}

	return nil
}

func (t *summariseSpoolPublishTracker) tableDone(table string, rows uint64) bool {
	checkpoint, ok := t.state.VerifiedTables[table]

	return ok && validSummariseSpoolVerifiedCheckpoint(checkpoint, rows)
}

func (t *summariseSpoolPublishTracker) markTable(table string, rows uint64) error {
	return t.markVerifiedCheckpoint(t.state.VerifiedTables, table, rows, "table:"+table)
}

func (t *summariseSpoolPublishTracker) clearTable(table string) error {
	if _, ok := t.state.VerifiedTables[table]; !ok {
		return nil
	}

	delete(t.state.VerifiedTables, table)

	return t.saveAndNotifyCurrentCheckpoint()
}

func (t *summariseSpoolPublishTracker) operationDone(rows uint64) bool {
	checkpoint, ok := t.state.VerifiedOperations[summariseSpoolPublishOperationChildFilterAll]

	return ok && validSummariseSpoolVerifiedCheckpoint(checkpoint, rows)
}

func (t *summariseSpoolPublishTracker) markOperation(rows uint64) error {
	operation := summariseSpoolPublishOperationChildFilterAll

	return t.markVerifiedCheckpoint(t.state.VerifiedOperations, operation, rows, "operation:"+operation)
}

func (t *summariseSpoolPublishTracker) clearOperation(operation string) error {
	if _, ok := t.state.VerifiedOperations[operation]; !ok {
		return nil
	}

	delete(t.state.VerifiedOperations, operation)

	return t.saveAndNotifyCurrentCheckpoint()
}

func (t *summariseSpoolPublishTracker) markVerifiedCheckpoint(
	checkpoints map[string]summariseSpoolVerifiedCheckpoint,
	name string,
	rows uint64,
	telemetryName string,
) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	checkpoints[name] = summariseSpoolVerifiedCheckpoint{Rows: rows, VerifiedAt: now}
	t.state.LastCompletedUTC = now

	if err := t.save(); err != nil {
		return err
	}

	if t.onMark != nil {
		t.onMark(telemetryName)
	}

	return nil
}

func (t *summariseSpoolPublishTracker) currentCheckpoint() string {
	var checkpoint, completedAt string
	for phase, timestamp := range t.state.CompletedPhases {
		if timestamp > completedAt {
			checkpoint = phase
			completedAt = timestamp
		}
	}

	for table, verified := range t.state.VerifiedTables {
		if verified.VerifiedAt > completedAt {
			checkpoint = "table:" + table
			completedAt = verified.VerifiedAt
		}
	}

	for operation, verified := range t.state.VerifiedOperations {
		if verified.VerifiedAt > completedAt {
			checkpoint = "operation:" + operation
			completedAt = verified.VerifiedAt
		}
	}

	return checkpoint
}

func (t *summariseSpoolPublishTracker) setNextActiveSetID(activeSetID string) error {
	if activeSetID == "" || t.state.NextActiveSetID == activeSetID {
		return nil
	}

	t.state.NextActiveSetID = activeSetID

	return t.save()
}

func (t *summariseSpoolPublishTracker) nextActiveSetID() string {
	return t.state.NextActiveSetID
}

func (t *summariseSpoolPublishTracker) setSwitchPlan(plan summariseSpoolSwitchPlan) error {
	t.state.SwitchPlan = &plan
	if plan.NextActiveSetID != "" {
		t.state.NextActiveSetID = plan.NextActiveSetID
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	t.state.CompletedPhases[summariseSpoolPublishPhaseSwitchPlanned] = now
	t.state.LastCompletedUTC = now

	if err := t.save(); err != nil {
		return err
	}

	if t.onMark != nil {
		t.onMark(summariseSpoolPublishPhaseSwitchPlanned)
	}

	return nil
}

func (t *summariseSpoolPublishTracker) switchPlan() (summariseSpoolSwitchPlan, bool) {
	if t.state.SwitchPlan == nil {
		return summariseSpoolSwitchPlan{}, false
	}

	return *t.state.SwitchPlan, true
}

func (t *summariseSpoolPublishTracker) hasSwitchPlan() bool {
	return t.state.SwitchPlan != nil
}

func (t *summariseSpoolPublishTracker) reusesPreSwitchState() bool {
	if t.done(summariseSpoolPublishPhaseMountSwitched) {
		return false
	}

	return t.done(summariseSpoolPublishPhaseActiveVirtualReady) || t.hasSwitchPlan()
}

func (t *summariseSpoolPublishTracker) clearPreSwitchPlan() error {
	changed := false

	for _, phase := range []string{
		summariseSpoolPublishPhaseActiveVirtualReady,
		summariseSpoolPublishPhaseSwitchPlanned,
	} {
		if _, ok := t.state.CompletedPhases[phase]; ok {
			delete(t.state.CompletedPhases, phase)

			changed = true
		}
	}

	if t.state.NextActiveSetID != "" {
		t.state.NextActiveSetID = ""
		changed = true
	}

	if t.state.SwitchPlan != nil {
		t.state.SwitchPlan = nil
		changed = true
	}

	if !changed {
		return nil
	}

	return t.saveAndNotifyCurrentCheckpoint()
}

func (t *summariseSpoolPublishTracker) saveAndNotifyCurrentCheckpoint() error {
	if err := t.save(); err != nil {
		return err
	}

	if t.onMark != nil {
		t.onMark(t.currentCheckpoint())
	}

	return nil
}

func (t *summariseSpoolPublishTracker) save() error {
	if t.path == summariseSpoolPublishStateName {
		return nil
	}

	data, err := t.marshal()
	if err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(t.path), filepath.Base(t.path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("clickhouse: failed to write summarise spool publish state: %w", err)
	}

	tmp := tmpFile.Name()
	defer func() { _ = os.Remove(tmp) }()

	if err := writeSummariseSpoolPublishStateTemp(tmpFile, data); err != nil {
		return err
	}

	if err := os.Rename(tmp, t.path); err != nil {
		return fmt.Errorf("clickhouse: failed to replace summarise spool publish state: %w", err)
	}

	if err := syncSummariseSpoolPublishStateDir(filepath.Dir(t.path)); err != nil {
		return err
	}

	return nil
}

func writeSummariseSpoolPublishStateTemp(file *os.File, data []byte) error {
	if err := file.Chmod(summariseSpoolPublishStatePerm); err != nil {
		_ = file.Close()

		return fmt.Errorf("clickhouse: failed to secure summarise spool publish state: %w", err)
	}

	if _, err := file.Write(data); err != nil {
		_ = file.Close()

		return fmt.Errorf("clickhouse: failed to write summarise spool publish state: %w", err)
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()

		return fmt.Errorf("clickhouse: failed to sync summarise spool publish state: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("clickhouse: failed to close summarise spool publish state: %w", err)
	}

	return nil
}

func syncSummariseSpoolPublishStateDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("clickhouse: failed to open summarise spool publish state directory: %w", err)
	}
	defer func() { _ = file.Close() }()

	if err := file.Sync(); err != nil {
		return fmt.Errorf("clickhouse: failed to sync summarise spool publish state directory: %w", err)
	}

	return nil
}

func (t *summariseSpoolPublishTracker) marshal() ([]byte, error) {
	data, err := json.MarshalIndent(t.state, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(data, '\n'), nil
}

// SummariseSpoolPublishCanResume reports whether a completed spool has durable
// post-spool publish state that can resume an already-active snapshot.
func SummariseSpoolPublishCanResume(spoolDir string, manifest *chspool.Manifest) (bool, error) {
	if spoolDir == "" || manifest == nil {
		return false, nil
	}

	tracker, err := newSummariseSpoolPublishTracker(spoolDir, manifest)
	if err != nil {
		return false, err
	}

	return tracker.done(summariseSpoolPublishPhaseMountSwitched) || tracker.hasSwitchPlan(), nil
}

func summariseSpoolPublishStateVersionSupported(version int) bool {
	return version == summariseSpoolPublishStateVersion || version == summariseSpoolPublishStateLegacyVersion
}

func summariseSpoolPublishManifestKey(manifest *chspool.Manifest) string {
	if manifest == nil {
		return ""
	}

	hash := sha256.New()
	writeChecksumFields(
		hash,
		manifest.Version,
		manifest.Format,
		manifest.State,
		manifest.MountPath,
		manifest.SnapshotID,
		manifest.UpdatedAt,
		manifest.OutputDir,
		manifest.SchemaMarker,
		manifest.BasedirsEnabled,
	)

	for _, table := range summariseSpoolPublishTableNames(manifest.Tables) {
		tm := manifest.Tables[table]
		writeChecksumFields(hash, table, tm.Table, tm.Path, tm.Rows, tm.Bytes, tm.SHA256)
	}

	return hex.EncodeToString(hash.Sum(nil))
}

func summariseSpoolPublishTableNames(tables map[string]chspool.TableManifest) []string {
	names := make([]string, 0, len(tables))
	for table := range tables {
		names = append(names, table)
	}

	sort.Strings(names)

	return names
}
