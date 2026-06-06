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
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	chspoolTestMountPath  = "/mnt/test/"
	chspoolTestSnapshotID = "00000000-0000-0000-0000-000000000001"
)

func TestVerifyManifest(t *testing.T) {
	Convey("VerifyManifest rejects a completed spool for a different output dir", t, func() {
		dir := t.TempDir()
		tables := writeChspoolTestSet(dir)
		expected := newChspoolTestManifest("/out/expected", tables)
		got := newChspoolTestManifest("/out/other", tables)

		err := VerifyManifest(dir, got, *expected)

		So(errors.Is(err, ErrManifestMismatch), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, "output_dir")
	})

	Convey("VerifyTables rejects manifest-only row count tampering", t, func() {
		dir := t.TempDir()
		tables := writeChspoolTestSet(dir)

		So(VerifyTables(dir, tables), ShouldBeNil)

		tampered := cloneChspoolTestTables(tables)
		table := tampered[TableFiles]
		table.Rows = 0
		tampered[TableFiles] = table

		err := VerifyTables(dir, tampered)

		So(errors.Is(err, ErrManifestMismatch), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, "row count")
	})
}

func writeChspoolTestSet(dir string) map[string]TableManifest {
	set, err := CreateSet(dir)
	So(err, ShouldBeNil)

	So(set.WriteFile(FileRow{
		MountPath:  chspoolTestMountPath,
		SnapshotID: chspoolTestSnapshotID,
		ParentDir:  chspoolTestMountPath,
		Name:       "file.txt",
		ATime:      time.Unix(1, 0).UTC(),
		MTime:      time.Unix(2, 0).UTC(),
		CTime:      time.Unix(3, 0).UTC(),
	}), ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	return set.TableManifests()
}

func newChspoolTestManifest(outputDir string, tables map[string]TableManifest) *Manifest {
	return &Manifest{
		Version:         Version,
		Format:          Format,
		State:           Complete,
		MountPath:       chspoolTestMountPath,
		SnapshotID:      chspoolTestSnapshotID,
		UpdatedAt:       time.Unix(4, 0).UTC().Format(time.RFC3339Nano),
		OutputDir:       outputDir,
		SchemaMarker:    "test-schema",
		BasedirsConfig:  FileIdentity{Path: "/config", Size: 1},
		BasedirsEnabled: true,
		Tables:          tables,
		CompletedAt:     time.Unix(5, 0).UTC().Format(time.RFC3339Nano),
	}
}

func cloneChspoolTestTables(tables map[string]TableManifest) map[string]TableManifest {
	out := make(map[string]TableManifest, len(tables))
	for table, manifest := range tables {
		out[table] = manifest
	}

	return out
}
