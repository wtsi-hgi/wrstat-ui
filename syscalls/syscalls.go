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
	"time"

	"github.com/wtsi-hgi/wrstat-ui/server"
	"vimagination.zapto.org/httpfile"
)

var (
	errNoDBPath = errors.New("no db paths given")
	errNotFound = errors.New("not found")

	completeTrue  = json.RawMessage(`{"complete":true}`)  //nolint:gochecknoglobals
	completeFalse = json.RawMessage(`{"complete":false}`) //nolint:gochecknoglobals
)

type logAnalyzer struct {
	mu        sync.RWMutex
	stats     map[string]json.RawMessage
	summaries map[string]json.RawMessage
	*httpfile.File
}

func newLogAnalyzer() *logAnalyzer {
	return &logAnalyzer{
		stats:     make(map[string]json.RawMessage),
		summaries: make(map[string]json.RawMessage),
		File:      httpfile.NewWithData("data.json", []byte{'{', '}'}),
	}
}

func (l *logAnalyzer) load(dbs []string, reload uint) error {
	paths, err := getDBPaths(dbs)
	if err != nil {
		return err
	}

	if reload > 0 {
		go func() {
			l.loadDirs(paths)
			time.Sleep(time.Duration(reload) * time.Minute) //nolint:gosec
			l.load(dbs, reload)                             //nolint:errcheck
		}()
	} else {
		go l.loadDirs(paths)
	}

	return nil
}

func getDBPaths(dbs []string) ([]string, error) { //nolint:gocognit
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

func (l *logAnalyzer) loadDirs(dirs []string) {
	var wg sync.WaitGroup

	wg.Add(len(dirs))

	for _, dir := range dirs {
		go func() {
			if err := l.loadDir(dir); err != nil {
				slog.Info("error loading log directory", "err", err)
			}

			wg.Done()
		}()
	}

	wg.Wait()

	w := l.File.Create()

	json.NewEncoder(w).Encode(l.summaries) //nolint:errcheck,errchkjson
	w.Close()

	slog.Info("done loading logs")
}

func (l *logAnalyzer) loadDir(dir string) error {
	name := filepath.Base(dir)

	l.mu.RLock()
	_, ok := l.stats[name]
	l.mu.RUnlock()

	if ok {
		return nil
	}

	slog.Info("loading logs from path", "path", dir)

	d := &data{hosts: make(map[string]string)}
	files, _ := filepath.Glob(filepath.Join(dir, "*log*")) //nolint:errcheck

	if err := d.loadFiles(files); err != nil {
		return err
	}

	if len(d.Events) == 0 && len(d.Errors) == 0 {
		l.setNull(name)

		return nil
	}

	if jobs, err := os.Open(filepath.Join(dir, "jobs")); err == nil {
		defer jobs.Close()

		if err = d.loadJobsData(jobs); err != nil {
			return err
		}
	}

	l.setData(name, d)

	complete := len(d.Errors) == 0

	l.setComplete(name, complete)

	slog.Info("loaded logs", "path", name)

	return nil
}

func (l *logAnalyzer) handleRunRequest(w http.ResponseWriter, r *http.Request) {
	runName := strings.TrimPrefix(r.URL.Path, "/logs/")

	dataBytes, err := l.getRunData(runName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	w.Header().Set("Content-Type", "application/json")

	w.Write(dataBytes) //nolint:errcheck
}

func (l *logAnalyzer) getRunData(runName string) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	dataBytes, ok := l.stats[runName]
	if !ok {
		return nil, errNotFound
	}

	return dataBytes, nil
}

func (l *logAnalyzer) setNull(name string) {
	l.mu.Lock()
	l.stats[name] = nil
	l.mu.Unlock()
}

func (l *logAnalyzer) setComplete(name string, complete bool) {
	l.mu.Lock()
	if complete {
		l.summaries[name] = completeTrue
	} else {
		l.summaries[name] = completeFalse
	}
	l.mu.Unlock()
}

func (l *logAnalyzer) setData(name string, data any) {
	var buf bytes.Buffer

	json.NewEncoder(&buf).Encode(data) //nolint:errcheck,errchkjson

	l.mu.Lock()
	l.stats[name] = buf.Bytes()
	l.mu.Unlock()
}

// StartServer starts a webserver that displays wrstat logged syscall data in
// order to analyse potential issues.
//
// The reload param represents the delay, in minutes, between the checks for new
// logs. If set to zero, no log checks are performed after the server has
// started.
//
// The dbs should be the 'wrstat multi -f' output directories that should be
// scanned for logs.
func StartServer(serverBind string, reload uint, dbs ...string) error {
	if len(dbs) == 0 {
		return errNoDBPath
	}

	l := newLogAnalyzer()

	if err := l.load(dbs, reload); err != nil {
		return fmt.Errorf("error during initial log discovery: %w", err)
	}

	http.Handle("/", index)
	http.Handle("/data.json", l)
	http.HandleFunc("/logs/", l.handleRunRequest)

	return http.ListenAndServe(serverBind, nil) //nolint:gosec
}
