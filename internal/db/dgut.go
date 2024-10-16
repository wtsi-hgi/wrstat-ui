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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/fs"
	"github.com/wtsi-ssg/wrstat/v5/dguta"
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
func CreateExampleDGUTADBCustomIDs(t *testing.T, uid, gidA, gidB string) (string, error) {
	t.Helper()

	dgutaData := exampleDGUTAData(t, uid, gidA, gidB)

	return CreateCustomDGUTADB(t, dgutaData)
}

// CreateCustomDGUTADB creates a dguta database in a temp directory using the
// given dguta data, and returns the database directory.
func CreateCustomDGUTADB(t *testing.T, dgutaData string) (string, error) {
	t.Helper()

	dir, err := createExampleDgutaDir(t)
	if err != nil {
		return dir, err
	}

	data := strings.NewReader(dgutaData)
	db := dguta.NewDB(dir)

	err = db.Store(data, exampleDBBatchSize)

	return dir, err
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
func exampleDGUTAData(t *testing.T, uidStr, gidAStr, gidBStr string) string {
	t.Helper()

	uid, err := strconv.ParseUint(uidStr, 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	gidA, err := strconv.ParseUint(gidAStr, 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	gidB, err := strconv.ParseUint(gidBStr, 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	return internaldata.TestDGUTAData(t, internaldata.CreateDefaultTestData(int(gidA), int(gidB), 0, int(uid), 0))
}

func CreateDGUTADBFromFakeFiles(t *testing.T, files []internaldata.TestFile,
	modtime ...time.Time,
) (*dguta.Tree, string, error) {
	t.Helper()

	dgutaData := internaldata.TestDGUTAData(t, files)

	dbPath, err := CreateCustomDGUTADB(t, dgutaData)
	if err != nil {
		t.Fatalf("could not create dguta db: %s", err)
	}

	if len(modtime) == 1 {
		if err = fs.Touch(dbPath, modtime[0]); err != nil {
			return nil, "", err
		}
	}

	tree, err := dguta.NewTree(dbPath)

	return tree, dbPath, err
}
