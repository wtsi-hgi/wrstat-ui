/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
 *	- Michael Grace <mg38@sanger.ac.uk>
 *	- Michael Woolnough <mw31@sanger.ac.uk>
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

package internaldb

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/fs"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	DirPerms                    = fs.DirPerms
	ExampleDgutaDirParentSuffix = "dguta.dbs"
	minGIDsForExampleDgutaDB    = 2
	exampleDBBatchSize          = 20
)

// CreateExampleDGUTADBCustomIDs creates a temporary dguta.db from some example
// data that uses the given uid and gids, and returns the path to the database
// directory.
func CreateExampleDGUTADBCustomIDs(t *testing.T, uid, gidA, gidB string, refTime int64) (string, error) {
	t.Helper()

	dgutaData := exampleDGUTAData(t, uid, gidA, gidB, refTime)

	return CreateCustomDGUTADB(t, dgutaData)
}

// CreateCustomDGUTADB creates a dguta database in a temp directory using the
// given dguta data, and returns the database directory.
func CreateCustomDGUTADB(t *testing.T, data io.Reader) (string, error) {
	t.Helper()

	dir, err := createExampleDgutaDir(t)
	if err != nil {
		return dir, err
	}

	d := db.NewDB(dir)
	d.SetBatchSize(exampleDBBatchSize)

	if err = d.CreateDB(); err != nil {
		return dir, err
	}

	s := summary.NewSummariser(stats.NewStatsParser(data))
	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(d))

	if err := s.Summarise(); err != nil {
		return dir, err
	}

	return dir, d.Close()
}

// createExampleDgutaDir creates a temp directory structure to hold dguta db
// files in the same way that 'wrstat tidy' organises them.
func createExampleDgutaDir(t *testing.T) (string, error) {
	t.Helper()

	tdir := t.TempDir()
	dir := filepath.Join(tdir, "orig."+ExampleDgutaDirParentSuffix, "0")
	err := os.MkdirAll(dir, DirPerms)

	return dir, err
}

// exampleDGUTAData is some example DGUTA data that uses the given uid and gids,
// along with root's uid.
func exampleDGUTAData(t *testing.T, uidStr, gidAStr, gidBStr string, refTime int64) io.Reader {
	t.Helper()

	uid, err := strconv.ParseUint(uidStr, 10, 32)
	if err != nil {
		t.Fatal(err)
	}

	gidA, err := strconv.ParseUint(gidAStr, 10, 32)
	if err != nil {
		t.Fatal(err)
	}

	gidB, err := strconv.ParseUint(gidBStr, 10, 32)
	if err != nil {
		t.Fatal(err)
	}

	return internaldata.CreateDefaultTestData(uint32(gidA), uint32(gidB), 0, uint32(uid), 0, refTime).AsReader()
}

// func CreateDGUTADBFromFakeFiles(t *testing.T, files []internaldata.TestFile,
// 	modtime ...time.Time,
// ) (*db.Tree, string, error) {
// 	t.Helper()

// 	dgutaData := internaldata.TestDGUTAData(t, files)

// 	dbPath, err := CreateCustomDGUTADB(t, dgutaData)
// 	if err != nil {
// 		t.Fatalf("could not create dguta db: %s", err)
// 	}

// 	if len(modtime) == 1 {
// 		if err = fs.Touch(dbPath, modtime[0]); err != nil {
// 			return nil, "", err
// 		}
// 	}

// 	tree, err := db.NewTree(dbPath)

// 	return tree, dbPath, err
// }
