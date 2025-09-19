package bolt

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Reloader watches a basepath for latest versioned db dirs matching required entries.
// On changes, it invokes the provided callback with the new directories and a deletion list.
// If the callback returns true, older directories may be removed when removeOld is true.
// The callback is responsible for opening dbs and loading them into the server.
// The stop channel is closed to terminate the loop.

type Reloader struct {
	stopCh chan struct{}
}

// NewReloader constructs a new Reloader.
func NewReloader() *Reloader { return &Reloader{stopCh: make(chan struct{})} }

// OnChange is invoked with the new directories and the list of directories to delete.
// It should return true if the change was successfully applied (e.g., server reloaded),
// so the reloader can proceed with deletion of old directories when configured.
// required lists the filenames that must exist in each directory (e.g., "dguta.dbs", "basedirs.db").
func (r *Reloader) Start(basepath string, pollFrequency time.Duration, removeOld bool, required []string, onChange func(dirs, toDelete []string) bool) error {
	// initial scan
	dirs, toDelete, err := FindDBDirs(basepath, required...)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return os.ErrNotExist
	}
	if ok := onChange(dirs, toDelete); ok && removeOld {
		_ = removeAll(basepath, toDelete) // best-effort
	}

	prev := slices.Clone(dirs)
	go r.loop(basepath, pollFrequency, removeOld, required, prev, onChange)
	return nil
}

// Stop terminates the reloader.
func (r *Reloader) Stop() { close(r.stopCh) }

func (r *Reloader) loop(basepath string, pollFrequency time.Duration, removeOld bool, required, prev []string, onChange func([]string, []string) bool) {
	for {
		select {
		case <-time.After(pollFrequency):
		case <-r.stopCh:
			return
		}
		dirs, toDelete, err := FindDBDirs(basepath, required...)
		if err != nil || slices.Equal(dirs, prev) {
			continue
		}
		if ok := onChange(dirs, toDelete); ok {
			prev = dirs
			if removeOld {
				_ = removeAll(basepath, toDelete)
			}
		}
	}
}

// removeAll deletes the listed directories from baseDirectory after writing a marker file.
func removeAll(baseDirectory string, toDelete []string) error {
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

// TimestampsFromDirs computes a timestamps map keyed by the suffix after the first underscore.
// If parsing fails, falls back to using the whole basename as key.
func TimestampsFromDirs(dirs []string) (map[string]int64, error) {
	ts := make(map[string]int64, len(dirs))
	for _, d := range dirs {
		fi, err := os.Stat(d)
		if err != nil {
			return nil, err
		}
		base := filepath.Base(d)
		parts := strings.SplitN(base, "_", 2)
		key := base
		if len(parts) == 2 {
			key = parts[1]
		}
		ts[key] = fi.ModTime().Unix()
	}
	return ts, nil
}
