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
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/internal/fs"
	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const (
	DirPerms                    = fs.DirPerms
	ExampleDgutaDirParentSuffix = "dguta.dbs"
	minGIDsForExampleDgutaDB    = 2
	exampleDBBatchSize          = 20
)

// CreateExampleDBsCustomIDs creates a temporary dguta.db from some example
// data that uses the given uid and gids, and returns the path to the database
// directory.
func CreateExampleDBsCustomIDs(t *testing.T, uid, gidA, gidB string, refTime int64) (string, error) {
	t.Helper()

	dir, err := createExampleDgutaDir(t)
	if err != nil {
		return dir, err
	}

	return dir, CreateExampleDBsCustomIDsWithDir(t, dir, uid, gidA, gidB, refTime)
}

// CreateExampleDBsCustomIDsWithDir creates a temporary dguta.db, in the path
// provided, from some example data that uses the given uid and gids.
func CreateExampleDBsCustomIDsWithDir(t *testing.T, dir, uid, gidA, gidB string, refTime int64) error {
	t.Helper()

	dbData := exampleDBData(t, uid, gidA, gidB, refTime)
	s := summary.NewSummariser(stats.NewStatsParser(dbData))

	fn, err := addDirgutaSummariser(s, dir)
	if err != nil {
		return err
	}

	err = addBasedirsSummariser(t, s, dir)
	if err != nil {
		return err
	}

	if err := s.Summarise(); err != nil {
		return err
	}

	return fn()
}

func addDirgutaSummariser(s *summary.Summariser, path string) (func() error, error) {
	path = filepath.Join(path, "dirguta")

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	d := db.NewDB(path)
	d.SetBatchSize(exampleDBBatchSize)

	if err := d.CreateDB(); err != nil {
		return nil, err
	}

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(d))

	return d.Close, nil
}

func addBasedirsSummariser(t *testing.T, s *summary.Summariser, path string) error {
	t.Helper()

	csvPath := internaldata.CreateQuotasCSV(t, internaldata.ExampleQuotaCSV)

	quotas, err := basedirs.ParseQuotas(csvPath)
	if err != nil {
		return err
	}

	dbPath := filepath.Join(path, "basedir.db")
	config := basedirs.Config{
		{
			Prefix:  split.SplitPath("/a"),
			Splits:  2,
			MinDirs: 1,
		},
		{
			Splits:  2,
			MinDirs: 2,
		},
	}

	bd, err := basedirs.NewCreator(dbPath, quotas)
	if err != nil {
		return err
	}

	bd.SetMountPoints([]string{
		"/a/",
		"/k/",
	})

	s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return nil
}

// createExampleDgutaDir creates a temp directory structure to hold dguta db
// files in the same way that 'wrstat tidy' organises them.
func createExampleDgutaDir(t *testing.T) (string, error) {
	t.Helper()

	tmp := t.TempDir()
	name := strconv.FormatInt(time.Now().Unix()-10, 10) + "_test"
	dir := filepath.Join(tmp, name)
	err := os.MkdirAll(dir, DirPerms)

	return dir, err
}

// exampleDBData is some example DGUTA data that uses the given uid and gids,
// along with root's uid.
func exampleDBData(t *testing.T, uidStr, gidAStr, gidBStr string, refTime int64) io.Reader {
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
