package bolt

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/server"
)

var validDatasetDir = regexp.MustCompile(`^[^.][^_]*_.`)

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

type nameVersion struct {
	name    string
	version string
}

func addEntryToLatest(entry fs.DirEntry, latest map[string]nameVersion, toDelete []string) []string {
	parts := strings.SplitN(entry.Name(), "_", splitN)
	if len(parts) != splitN {
		return toDelete
	}

	key, version := parts[1], parts[0]

	if prev, ok := latest[key]; ok {
		if prev.version > version {
			return append(toDelete, entry.Name())
		}

		toDelete = append(toDelete, prev.name)
	}

	latest[key] = nameVersion{entry.Name(), version}

	return toDelete
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

	return map[string]time.Time{}, nil
}

type provider struct {
	cfg Config

	mu    sync.RWMutex
	state *providerState
	cb    func()

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func (p *provider) Tree() *db.Tree {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.state == nil {
		return nil
	}

	return p.state.tree
}

func (p *provider) BaseDirs() basedirs.Reader {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.state == nil {
		return nil
	}

	return p.state.basedirs
}

func (p *provider) OnUpdate(cb func()) {
	p.mu.Lock()
	p.cb = cb
	p.mu.Unlock()
}

func (p *provider) Close() error {
	close(p.stopCh)
	p.wg.Wait()

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

func (p *provider) pollLoop() {
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

func (p *provider) maybeReload() {
	st, err := p.loadOnce()
	if err != nil {
		// Nothing better to do here without a logger; server still has old state.
		return
	}

	p.mu.RLock()
	old := p.state
	cb := p.cb
	p.mu.RUnlock()

	if old != nil && slices.Equal(old.datasetDirs, st.datasetDirs) {
		closeErr := closeState(st)
		_ = closeErr

		return
	}

	// Swap state and handle post-swap actions via a helper to reduce the
	// length and complexity of this function.
	p.swapStateAndHandle(st, cb, old)
}

func (p *provider) swapStateAndHandle(st *providerState, cb func(), old *providerState) {
	p.mu.Lock()
	p.state = st
	p.mu.Unlock()

	if cb != nil {
		cb()
	}

	if p.cfg.RemoveOldPaths {
		if err := removeDatasetDirs(p.cfg.BasePath, st.toDelete); err != nil {
			// Intentionally ignore removal errors; nothing better to do here
			_ = err
		}
	}

	if old != nil {
		if err := closeState(old); err != nil {
			_ = err
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

func (p *provider) loadOnce() (*providerState, error) {
	datasetDirs, toDelete, err := findDatasetDirs(p.cfg.BasePath, p.cfg.DGUTADBName, p.cfg.BaseDirDBName)
	if err != nil {
		return nil, err
	}

	if len(datasetDirs) == 0 {
		return nil, server.ErrNoPaths
	}

	return p.createStateFromDatasets(datasetDirs, toDelete)
}

func (p *provider) createStateFromDatasets(datasetDirs, toDelete []string) (*providerState, error) {
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

func findDatasetDirs(basepath string, required ...string) ([]string, []string, error) {
	entries, err := os.ReadDir(basepath)
	if err != nil {
		return nil, nil, err
	}

	latest := make(map[string]nameVersion)

	var toDelete []string

	for _, entry := range entries {
		if !isValidDatasetDir(entry, basepath, required...) {
			continue
		}

		toDelete = addEntryToLatest(entry, latest, toDelete)
	}

	dirs := make([]string, 0, len(latest))
	for _, v := range latest {
		dirs = append(dirs, filepath.Join(basepath, v.name))
	}

	slices.Sort(dirs)

	return dirs, toDelete, nil
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

func (p *provider) openAllDatasets(datasetDirs []string) ([]string, []basedirs.Reader, []func() error, error) {
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

func (p *provider) openWrappedBaseDirs(dsDir string, mountPoints []string) (basedirs.Reader, func() error, error) {
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

func (p *provider) mountPoints() ([]string, error) {
	if len(p.cfg.MountPoints) > 0 {
		return p.cfg.MountPoints, nil
	}

	mps, err := basedirs.GetMountPoints()
	if err != nil {
		return nil, err
	}

	return []string(mps), nil
}

func (p *provider) maybeStartPoll() {
	if p.cfg.PollInterval > 0 {
		p.wg.Add(1)

		go p.pollLoop()
	}
}

// OpenProvider constructs a backend bundle that implements server.Provider.
// When cfg.PollInterval > 0, the backend starts an internal goroutine that
// watches cfg.BasePath for new databases and triggers OnUpdate callbacks.
func OpenProvider(cfg Config) (server.Provider, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	p := &provider{cfg: cfg, stopCh: make(chan struct{})}

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

func isValidDatasetDir(entry fs.DirEntry, basepath string, required ...string) bool {
	name := entry.Name()
	if !entry.IsDir() || !validDatasetDir.MatchString(name) {
		return false
	}

	for _, req := range required {
		if _, err := os.Stat(filepath.Join(basepath, name, req)); err != nil {
			return false
		}
	}

	return true
}
