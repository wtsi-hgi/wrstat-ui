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

package syscalls

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/wtsi-hgi/wrstat-ui/server"
	"vimagination.zapto.org/httpfile"
)

func StartServer(serverBind string, dbs ...string) error {
	if len(dbs) == 0 {
		return errors.New("no db paths given")
	}

	paths, err := getDBPaths(dbs)
	if err != nil {
		return fmt.Errorf("error during inital log discovery: %w", err)
	}

	l := newLogAnalyzer()

	go l.loadDirs(paths)

	http.Handle("/", index)
	http.Handle("/data.json", l)

	return http.ListenAndServe(serverBind, nil)
}

func getDBPaths(dbs []string) ([]string, error) {
	var dbDirs []string

	for _, db := range dbs {
		if err := fs.WalkDir(os.DirFS(db), ".", func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if server.IsValidDBDir(entry, db, "logs.gz") || server.IsValidDBDir(entry, db, "walk.log") {
				dbDirs = append(dbDirs, filepath.Join(db, path))
			}

			return nil
		}); err != nil {
			return nil, err
		}
	}

	sort.Strings(dbDirs)

	return dbDirs, nil
}

type logAnalyzer struct {
	mu    sync.RWMutex
	stats map[string]json.RawMessage
	*httpfile.File
}

func newLogAnalyzer() *logAnalyzer {
	return &logAnalyzer{
		stats: make(map[string]json.RawMessage),
		File:  httpfile.NewWithData("data.json", []byte{'{', '}'}),
	}
}

func (l *logAnalyzer) loadDirs(dirs []string) {
	for _, dir := range dirs {
		if err := l.loadDir(dir); err != nil {
			slog.Info("error loading log directory", "err", err)
		}

		slog.Info("loaded logs from path", "path", dir)
	}

	w := l.File.Create()

	json.NewEncoder(w).Encode(l.stats)
	w.Close()
}

func (l *logAnalyzer) loadDir(dir string) error {
	name := filepath.Base(dir)

	l.mu.RLock()
	_, ok := l.stats[name]
	l.mu.RUnlock()

	if ok {
		return nil
	}

	d := &data{hosts: make(map[string]string)}
	files, _ := filepath.Glob(filepath.Join(dir, "*log*"))

	for _, file := range files {
		if d.lastFile = strings.TrimSuffix(filepath.Base(file), ".log"); d.lastFile == "logs.gz" {
			d.Complete = true
			d.lastFile = "walk"
		}

		if err := d.loadFile(file); err != nil {
			return fmt.Errorf("error loading file (%s): %w", file, err)
		}
	}

	if len(d.Events) == 0 && len(d.Errors) == 0 {
		return nil
	}

	var buf bytes.Buffer

	json.NewEncoder(&buf).Encode(d)

	l.mu.Lock()
	l.stats[name] = json.RawMessage(buf.Bytes())
	l.mu.Unlock()

	slog.Info("loaded logs", "name", name)

	return nil
}
