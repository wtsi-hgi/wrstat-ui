/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

package bolt

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/datasets"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/provider"
)

var (
	ErrInvalidDatasetDirName = errors.New("invalid dataset dir name")
)

const splitN = 2

type providerState struct {
	datasetDirs []string
	toDelete    []string

	database db.Database
	tree     *db.Tree
	basedirs basedirs.Reader

	closers []func() error
}

type timestampOverrideReader struct {
	basedirs.Reader

	mountKey  string
	mountPath string
	updatedAt time.Time
}

func (r timestampOverrideReader) MountTimestamps() (map[string]time.Time, error) {
	mt, err := r.Reader.MountTimestamps()
	if err != nil {
		return nil, err
	}

	// If the underlying DB had proper meta, use it.
	if t, ok := mt[r.mountKey]; ok && !t.IsZero() {
		return map[string]time.Time{r.mountKey: t}, nil
	}

	// Backwards-compat: legacy datasets didn't persist updatedAt.
	if !r.updatedAt.IsZero() {
		return map[string]time.Time{r.mountKey: r.updatedAt}, nil
	}

	return nil, nil //nolint: nilnil
}

type boltProvider struct {
	cfg Config

	mu    sync.RWMutex
	state *providerState
	cb    func()
	errCb func(error)

	stopCh chan struct{}
	wg     sync.WaitGroup

	cbMu sync.Mutex
	cbWG sync.WaitGroup
}

func (p *boltProvider) Tree() *db.Tree {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.state == nil {
		return nil
	}

	return p.state.tree
}

func (p *boltProvider) BaseDirs() basedirs.Reader {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.state == nil {
		return nil
	}

	return p.state.basedirs
}

func (p *boltProvider) OnUpdate(cb func()) {
	p.mu.Lock()
	p.cb = cb
	p.mu.Unlock()
}

func (p *boltProvider) OnError(cb func(error)) {
	p.mu.Lock()
	p.errCb = cb
	p.mu.Unlock()
}

func (p *boltProvider) Close() error {
	if p.stopCh != nil {
		close(p.stopCh)
		p.stopCh = nil
	}

	p.wg.Wait()
	p.cbWG.Wait()

	p.mu.Lock()
	st := p.state
	p.state = nil
	p.cb = nil
	p.mu.Unlock()

	if st != nil {
		return closeState(st)
	}

	return nil
}

func (p *boltProvider) pollLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.maybeReload()
		}
	}
}

func (p *boltProvider) maybeReload() {
	st, err := p.loadOnce()
	if err != nil {
		// Nothing better to do here without a logger; server still has old state.
		return
	}

	p.mu.RLock()
	old := p.state
	cb := p.cb
	errCb := p.errCb
	p.mu.RUnlock()

	if old != nil && slices.Equal(old.datasetDirs, st.datasetDirs) {
		closeErr := closeState(st)
		_ = closeErr

		return
	}

	// Swap state and handle post-swap actions via a helper to reduce the
	// length and complexity of this function.
	p.swapStateAndHandle(st, cb, errCb, old)
}

func (p *boltProvider) swapStateAndHandle(st *providerState, cb func(), errCb func(error), old *providerState) {
	p.mu.Lock()
	p.state = st
	p.mu.Unlock()

	p.runCallbackAndCleanup(cb, errCb, st, old)
}

func (p *boltProvider) runCallbackAndCleanup(cb func(), errCb func(error), st *providerState, old *providerState) {
	cleanup := func() {
		p.cleanupAfterCallback(errCb, st, old)
	}

	if cb == nil {
		cleanup()

		return
	}

	p.cbWG.Add(1)

	go func() {
		defer p.cbWG.Done()

		p.cbMu.Lock()
		defer p.cbMu.Unlock()
		defer cleanup()

		cb()
	}()
}

func (p *boltProvider) cleanupAfterCallback(errCb func(error), st *providerState, old *providerState) {
	if p.cfg.RemoveOldPaths && st != nil {
		if err := removeDatasetDirs(p.cfg.BasePath, st.toDelete); err != nil {
			p.reportError(errCb, err)
		}
	}

	if old != nil {
		if err := closeState(old); err != nil {
			p.reportError(errCb, err)
		}
	}
}

func removeDatasetDirs(baseDirectory string, toDelete []string) error {
	for _, path := range toDelete {
		// Create marker to avoid the watch subcommand re-running a summarise.
		f, err := os.Create(filepath.Join(baseDirectory, "."+path))
		if err != nil {
			return err
		}

		if err := f.Close(); err != nil {
			return err
		}

		if err := os.RemoveAll(filepath.Join(baseDirectory, path)); err != nil {
			return err
		}
	}

	return nil
}

func closeState(st *providerState) error {
	var err error

	for _, c := range st.closers {
		if c == nil {
			continue
		}

		err = errors.Join(err, c())
	}

	return err
}

func (p *boltProvider) reportError(errCb func(error), err error) {
	if err == nil || errCb == nil {
		return
	}

	errCb(err)
}

func (p *boltProvider) loadOnce() (*providerState, error) {
	datasetDirs, toDelete, err := datasets.FindDatasetDirs(p.cfg.BasePath, p.cfg.DGUTADBName, p.cfg.BaseDirDBName)
	if err != nil {
		return nil, err
	}

	if len(datasetDirs) == 0 {
		return nil, provider.ErrNoPaths
	}

	return p.createStateFromDatasets(datasetDirs, toDelete)
}

func (p *boltProvider) createStateFromDatasets(datasetDirs, toDelete []string) (*providerState, error) {
	dgutaDirs, basedirsReaders, closers, err := p.openAllDatasets(datasetDirs)
	if err != nil {
		return nil, errors.Join(err, closeAll(closers))
	}

	database, err := OpenDatabase(dgutaDirs...)
	if err != nil {
		return nil, errors.Join(err, closeAll(closers))
	}

	closers = append(closers, database.Close)

	state := &providerState{
		datasetDirs: datasetDirs,
		toDelete:    toDelete,
		database:    database,
		tree:        db.NewTree(database),
		basedirs:    multiBaseDirsReader(basedirsReaders),
		closers:     closers,
	}

	return state, nil
}

func closeAll(closers []func() error) error {
	var err error

	for _, c := range closers {
		if c == nil {
			continue
		}

		err = errors.Join(err, c())
	}

	return err
}

func (p *boltProvider) openAllDatasets(datasetDirs []string) ([]string, []basedirs.Reader, []func() error, error) {
	dgutaDirs := make([]string, 0, len(datasetDirs))
	basedirsReaders := make([]basedirs.Reader, 0, len(datasetDirs))
	closers := make([]func() error, 0, 1+len(datasetDirs))

	mountPoints, err := p.mountPoints()
	if err != nil {
		return nil, nil, nil, err
	}

	for _, dsDir := range datasetDirs {
		dgutaDirs = append(dgutaDirs, filepath.Join(dsDir, p.cfg.DGUTADBName))

		wrapped, closer, openErr := p.openWrappedBaseDirs(dsDir, mountPoints)
		if openErr != nil {
			return nil, nil, closers, openErr
		}

		basedirsReaders = append(basedirsReaders, wrapped)
		closers = append(closers, closer)
	}

	return dgutaDirs, basedirsReaders, closers, nil
}

func (p *boltProvider) openWrappedBaseDirs(dsDir string, mountPoints []string) (basedirs.Reader, func() error, error) {
	mountKey, mountPath, fallbackUpdatedAt, deriveErr := deriveMountInfoFromDatasetDir(dsDir)
	if deriveErr != nil {
		return nil, nil, deriveErr
	}

	bdPath := filepath.Join(dsDir, p.cfg.BaseDirDBName)

	r, openErr := OpenBaseDirsReader(bdPath, p.cfg.OwnersCSVPath)
	if openErr != nil {
		return nil, nil, openErr
	}

	r.SetMountPoints(mountPoints)

	wrapped := timestampOverrideReader{
		Reader:    r,
		mountKey:  mountKey,
		mountPath: mountPath,
		updatedAt: fallbackUpdatedAt,
	}

	return wrapped, wrapped.Close, nil
}

func deriveMountInfoFromDatasetDir(datasetDir string) (mountKey, mountPath string, updatedAt time.Time, err error) {
	base := filepath.Base(datasetDir)

	parts := strings.SplitN(base, "_", splitN)
	if len(parts) != splitN {
		return "", "", time.Time{}, fmt.Errorf("%w: %q", ErrInvalidDatasetDirName, base)
	}

	mountKey = parts[1]

	mountPath = strings.ReplaceAll(mountKey, "／", "/")
	if !strings.HasSuffix(mountPath, "/") {
		mountPath += "/"
	}

	if st, statErr := os.Stat(datasetDir); statErr == nil {
		updatedAt = st.ModTime()
	}

	return mountKey, mountPath, updatedAt, nil
}

func (p *boltProvider) mountPoints() ([]string, error) {
	if len(p.cfg.MountPoints) > 0 {
		return p.cfg.MountPoints, nil
	}

	mps, err := basedirs.GetMountPoints()
	if err != nil {
		return nil, err
	}

	return []string(mps), nil
}

func (p *boltProvider) maybeStartPoll() {
	if p.cfg.PollInterval > 0 {
		p.wg.Add(1)

		go p.pollLoop()
	}
}

// OpenProvider constructs a backend bundle that implements provider.Provider.
// When cfg.PollInterval > 0, the backend starts an internal goroutine that
// watches cfg.BasePath for new databases and triggers OnUpdate callbacks.
func OpenProvider(cfg Config) (provider.Provider, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	p := &boltProvider{cfg: cfg, stopCh: make(chan struct{})}

	st, err := p.loadOnce()
	if err != nil {
		return nil, err
	}

	p.state = st

	p.maybeStartPoll()

	return p, nil
}

func validateConfig(cfg Config) error {
	if cfg.BasePath == "" {
		return ErrInvalidConfig
	}

	if cfg.DGUTADBName == "" {
		return ErrInvalidConfig
	}

	if cfg.BaseDirDBName == "" {
		return ErrInvalidConfig
	}

	if cfg.OwnersCSVPath == "" {
		return ErrInvalidConfig
	}

	return nil
}
