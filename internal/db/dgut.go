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

	"github.com/wtsi-ssg/wrstat/v4/dgut"
	internaldata "github.com/wtsi-ssg/wrstat/v4/internal/data"
)

const DirPerms = 0755
const ExampleDgutDirParentSuffix = "dgut.dbs"
const minGIDsForExampleDgutDB = 2
const exampleDBBatchSize = 20

// CreateExampleDGUTDB creates a temporary dgut.db from some example data that
// uses your uid and 2 of your gids, and returns the path to the database
// directory. For use when testing something that needs a Tree.
func CreateExampleDGUTDB(t *testing.T) (string, error) {
	t.Helper()

	_, uid, gids := GetUserAndGroups(t)
	if len(gids) < minGIDsForExampleDgutDB {
		gids = append(gids, "0")
	}

	return CreateExampleDGUTDBCustomIDs(t, uid, gids[0], gids[1])
}

// CreateExampleDGUTDBCustomIDs creates a temporary dgut.db from some example
// data that uses the given uid and gids, and returns the path to the database
// directory.
func CreateExampleDGUTDBCustomIDs(t *testing.T, uid, gidA, gidB string) (string, error) {
	t.Helper()

	dgutData := exampleDGUTData(t, uid, gidA, gidB)

	return CreateCustomDGUTDB(t, dgutData)
}

// CreateCustomDGUTDB creates a dgut database in a temp directory using the
// given dgut data, and returns the database directory.
func CreateCustomDGUTDB(t *testing.T, dgutData string) (string, error) {
	t.Helper()

	dir, err := createExampleDgutDir(t)
	if err != nil {
		return dir, err
	}

	data := strings.NewReader(dgutData)
	db := dgut.NewDB(dir)

	err = db.Store(data, exampleDBBatchSize)

	return dir, err
}

// createExampleDgutDir creates a temp directory structure to hold dgut db files
// in the same way that 'wrstat tidy' organises them.
func createExampleDgutDir(t *testing.T) (string, error) {
	t.Helper()

	tdir := t.TempDir()
	dir := filepath.Join(tdir, "orig."+ExampleDgutDirParentSuffix, "0")
	err := os.MkdirAll(dir, DirPerms)

	return dir, err
}

// exampleDGUTData is some example DGUT data that uses the given uid and gids,
// along with root's uid.
func exampleDGUTData(t *testing.T, uidStr, gidAStr, gidBStr string) string {
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

	return internaldata.TestDGUTData(t, internaldata.CreateDefaultTestData(int(gidA), int(gidB), 0, int(uid), 0))
}

func CreateDGUTDBFromFakeFiles(t *testing.T, files []internaldata.TestFile) (*dgut.Tree, error) {
	t.Helper()

	dgutData := internaldata.TestDGUTData(t, files)

	dbPath, err := CreateCustomDGUTDB(t, dgutData)
	if err != nil {
		t.Fatalf("could not create dgut db: %s", err)
	}

	tree, err := dgut.NewTree(dbPath)

	return tree, err
}
