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
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	chspoolTestMountPath  = "/mnt/test/"
	chspoolTestSnapshotID = "00000000-0000-0000-0000-000000000001"
	chspoolTestActiveSet  = "active-set"
	chspoolTestVirtualDir = "/mnt/"

	chspoolFieldBaseDir         = "BaseDir"
	chspoolFieldBaseDirExternal = "BaseDirExternal"
	chspoolFieldBaseDirID       = "BaseDirID"
	chspoolFieldDir             = "Dir"
	chspoolFieldDirID           = "DirID"
	chspoolFieldParentDir       = "ParentDir"
	chspoolFieldSubtreeEnd      = "SubtreeEnd"
)

func TestVerifyManifest(t *testing.T) {
	Convey("H1 table order starts with the directory catalog and omits deleted path-edge streams", t, func() {
		order := TableOrder()

		So(order[0], ShouldEqual, TableDirs)
		So(order, ShouldNotContain, "wrstat_children")
		So(order, ShouldNotContain, "wrstat_parent_facts")
	})

	Convey("H1 row structs carry ids instead of repeated path strings", t, func() {
		assertChspoolFields(t, FileRow{}, []string{chspoolFieldDirID, "Name"}, []string{chspoolFieldParentDir})
		assertChspoolFields(
			t,
			DirFactRow{},
			[]string{chspoolFieldDirID, "ParentID", chspoolFieldSubtreeEnd},
			[]string{chspoolFieldDir},
		)
		assertChspoolFields(
			t,
			ChildFilterAllRow{},
			[]string{"ParentID", chspoolFieldDirID},
			[]string{chspoolFieldParentDir, chspoolFieldDir},
		)
		assertChspoolFields(
			t,
			DirFilterAllRow{},
			[]string{chspoolFieldDirID, chspoolFieldSubtreeEnd},
			[]string{chspoolFieldParentDir, chspoolFieldDir},
		)
		assertChspoolFields(
			t,
			DirFilterAgeAllRow{},
			[]string{chspoolFieldDirID, chspoolFieldSubtreeEnd},
			[]string{chspoolFieldDir},
		)
		assertChspoolFields(
			t,
			BasedirsGroupUsageRow{},
			[]string{chspoolFieldBaseDirID, chspoolFieldBaseDirExternal},
			[]string{chspoolFieldBaseDir},
		)
		assertChspoolFields(
			t,
			BasedirsUserUsageRow{},
			[]string{chspoolFieldBaseDirID, chspoolFieldBaseDirExternal},
			[]string{chspoolFieldBaseDir},
		)
		assertChspoolFields(
			t,
			BasedirsSubdirRow{},
			[]string{chspoolFieldBaseDirID, "SubDirID", chspoolFieldBaseDirExternal, "SubDirExternal"},
			[]string{chspoolFieldBaseDir, "SubDir"},
		)
		assertChspoolFields(
			t,
			ActiveVirtualSummaryRow{},
			[]string{"VirtualID", "SnapshotID", "MountRootDirID"},
			[]string{chspoolFieldDir},
		)
		assertChspoolFields(t, ActiveVirtualFilterAllRow{}, []string{"VirtualID"}, []string{chspoolFieldDir})
		assertChspoolFields(
			t,
			ActiveVirtualChildRow{},
			[]string{"ParentVirtualID", "ChildVirtualID", "SnapshotID", "MountRootDirID"},
			[]string{"ParentDir", "ChildDir"},
		)
	})

	Convey("D2.1 closed spool manifests all schema3 tables in order with verified metadata", t, func() {
		dir := t.TempDir()
		tables := writeChspoolTestSet(dir)
		expected := newChspoolTestManifest("/out/expected", tables)

		So(WriteManifestAtomic(dir, expected), ShouldBeNil)

		got, err := ReadManifest(dir)
		So(err, ShouldBeNil)
		So(VerifyManifest(dir, got, *expected), ShouldBeNil)

		for _, table := range TableOrder() {
			tm, ok := got.Tables[table]
			So(ok, ShouldBeTrue)
			So(tm.Table, ShouldEqual, table)
			So(tm.Path, ShouldEqual, table+".gob.gz")
			So(tm.Bytes, ShouldBeGreaterThan, int64(0))
			So(tm.SHA256, ShouldNotBeBlank)
		}

		So(got.Tables[TableDirs].Rows, ShouldEqual, 1)
		So(got.Tables[TableFiles].Rows, ShouldEqual, 1)
	})

	Convey("D2.2 VerifyManifest rejects a changed schema3 spool file", t, func() {
		dir := t.TempDir()
		tables := writeChspoolTestSet(dir)
		expected := newChspoolTestManifest("/out/expected", tables)
		So(WriteManifestAtomic(dir, expected), ShouldBeNil)

		path := filepath.Join(dir, TableChildFilterAll+".gob.gz")
		fh, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		So(err, ShouldBeNil)
		_, err = fh.WriteString("changed")
		So(err, ShouldBeNil)
		So(fh.Close(), ShouldBeNil)

		got, err := ReadManifest(dir)
		So(err, ShouldBeNil)
		err = VerifyManifest(dir, got, *expected)

		So(errors.Is(err, ErrManifestMismatch), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, TableChildFilterAll)
	})

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

	Convey("D2.5 VerifyManifest rejects active virtual set decoded row-count mismatches", t, func() {
		dir := t.TempDir()
		tables := writeChspoolTestSet(dir)
		expected := newChspoolTestManifest("/out/expected", tables)
		tampered := cloneChspoolTestTables(tables)
		table := tampered[TableActiveVirtualSets]
		table.Rows = 0
		tampered[TableActiveVirtualSets] = table
		got := newChspoolTestManifest("/out/expected", tampered)

		err := VerifyManifest(dir, got, *expected)

		So(errors.Is(err, ErrManifestMismatch), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, TableActiveVirtualSets)
		So(err.Error(), ShouldContainSubstring, "row count")
	})

	Convey("D2.6 VerifyManifest rejects active virtual child decoded row-count mismatches", t, func() {
		dir := t.TempDir()
		tables := writeChspoolTestSet(dir)
		expected := newChspoolTestManifest("/out/expected", tables)
		tampered := cloneChspoolTestTables(tables)
		table := tampered[TableActiveVirtualChildren]
		table.Rows = 0
		tampered[TableActiveVirtualChildren] = table
		got := newChspoolTestManifest("/out/expected", tampered)

		err := VerifyManifest(dir, got, *expected)

		So(errors.Is(err, ErrManifestMismatch), ShouldBeTrue)
		So(err.Error(), ShouldContainSubstring, TableActiveVirtualChildren)
		So(err.Error(), ShouldContainSubstring, "row count")
	})
}

func assertChspoolFields(t *testing.T, row any, present []string, absent []string) {
	t.Helper()

	typ := reflect.TypeOf(row)

	for _, field := range present {
		_, ok := typ.FieldByName(field)
		So(ok, ShouldBeTrue)
	}

	for _, field := range absent {
		_, ok := typ.FieldByName(field)
		So(ok, ShouldBeFalse)
	}
}

func writeChspoolTestSet(dir string) map[string]TableManifest {
	set, err := CreateSet(dir)
	So(err, ShouldBeNil)

	So(set.WriteDir(DirRow{
		MountPath:      chspoolTestMountPath,
		SnapshotID:     chspoolTestSnapshotID,
		DirID:          7,
		ParentID:       6,
		SubtreeEnd:     8,
		Depth:          3,
		Name:           "test/",
		FullPath:       chspoolTestMountPath,
		ChildDirCount:  1,
		ChildFileCount: 2,
		PathHash:       99,
	}), ShouldBeNil)
	So(set.WriteFile(FileRow{
		MountPath:  chspoolTestMountPath,
		SnapshotID: chspoolTestSnapshotID,
		DirID:      7,
		Name:       "file.txt",
		ATime:      time.Unix(1, 0).UTC(),
		MTime:      time.Unix(2, 0).UTC(),
		CTime:      time.Unix(3, 0).UTC(),
	}), ShouldBeNil)
	So(writeChspoolSchema3TestRows(set), ShouldBeNil)
	So(set.Close(), ShouldBeNil)

	return set.TableManifests()
}

func writeChspoolSchema3TestRows(set *Set) error {
	refreshedAt := time.Unix(6, 0).UTC()
	buckets := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9}

	if err := set.WriteChildFilterAll(ChildFilterAllRow{
		MountPath:         chspoolTestMountPath,
		SnapshotID:        chspoolTestSnapshotID,
		ParentID:          7,
		Age:               255,
		GID:               7,
		UID:               17,
		FT:                1,
		DirID:             8,
		Count:             2,
		Size:              3,
		AtimeMin:          4,
		MtimeMax:          5,
		AtimeBuckets:      buckets,
		MtimeBuckets:      buckets,
		FilterChildCount:  1,
		ChildCount:        1,
		HasFilterChildren: 1,
		HasChildren:       1,
		RefreshedAt:       refreshedAt,
	}); err != nil {
		return err
	}

	if err := set.WriteDirFilterAll(DirFilterAllRow{
		MountPath:         chspoolTestMountPath,
		SnapshotID:        chspoolTestSnapshotID,
		Age:               255,
		GID:               7,
		UID:               17,
		FT:                1,
		DirID:             8,
		SubtreeEnd:        9,
		Count:             2,
		Size:              3,
		AtimeMin:          4,
		MtimeMax:          5,
		AtimeBuckets:      buckets,
		MtimeBuckets:      buckets,
		FilterChildCount:  1,
		ChildCount:        1,
		HasFilterChildren: 1,
		HasChildren:       1,
		RefreshedAt:       refreshedAt,
	}); err != nil {
		return err
	}

	if err := set.WriteSchema3SnapshotSet(Schema3SnapshotSetRow{
		MountPath:          chspoolTestMountPath,
		SnapshotID:         chspoolTestSnapshotID,
		Schema3Version:     1,
		DirsRows:           1,
		DirFactsRows:       1,
		ChildFilterAllRows: 1,
		DirFilterAllRows:   1,
		ManifestSHA256:     "snapshot-manifest",
		RefreshedAt:        refreshedAt,
	}); err != nil {
		return err
	}

	return writeChspoolActiveVirtualTestRows(set, refreshedAt, buckets)
}

func writeChspoolActiveVirtualTestRows(set *Set, refreshedAt time.Time, buckets []uint64) error {
	if err := set.WriteActiveVirtualSummary(ActiveVirtualSummaryRow{
		ActiveSetID:     chspoolTestActiveSet,
		VirtualID:       1,
		MountPath:       chspoolTestMountPath,
		SnapshotID:      chspoolTestSnapshotID,
		MountRootDirID:  7,
		IsMountRootBox:  1,
		UpdatedAt:       time.Unix(7, 0).UTC(),
		AllAtimeBuckets: buckets,
		AllMtimeBuckets: buckets,
		AllUIDs:         []uint32{17},
		AllGIDs:         []uint32{7},
		ChildCount:      1,
		RefreshedAt:     refreshedAt,
	}); err != nil {
		return err
	}

	if err := set.WriteActiveVirtualFilterAll(ActiveVirtualFilterAllRow{
		ActiveSetID:      chspoolTestActiveSet,
		VirtualID:        1,
		Age:              255,
		AtimeBuckets:     buckets,
		MtimeBuckets:     buckets,
		FilterChildCount: 1,
		ChildCount:       1,
		RefreshedAt:      refreshedAt,
	}); err != nil {
		return err
	}

	if err := set.WriteActiveVirtualChild(ActiveVirtualChildRow{
		ActiveSetID:     chspoolTestActiveSet,
		ParentVirtualID: 0,
		ChildVirtualID:  1,
		MountPath:       chspoolTestMountPath,
		SnapshotID:      chspoolTestSnapshotID,
		MountRootDirID:  7,
		IsMountRootBox:  1,
		ChildCount:      1,
		RefreshedAt:     refreshedAt,
	}); err != nil {
		return err
	}

	return set.WriteActiveVirtualSet(ActiveVirtualSetRow{
		ActiveSetID:      chspoolTestActiveSet,
		Schema3Version:   1,
		MountsSHA256:     chspoolTestActiveSet,
		ActiveMountCount: 1,
		SummaryRows:      1,
		FilterRows:       1,
		ChildRows:        1,
		ManifestSHA256:   "active-manifest",
		Ready:            1,
		RefreshedAt:      refreshedAt,
	})
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
