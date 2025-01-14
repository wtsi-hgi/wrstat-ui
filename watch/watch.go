/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

// package watch is used to watch single files for changes to their mtimes, for
// filesystems that don't support inotify.

package watch

import (
	"os"
	"sync"
	"time"
)

// WatcherCallback, once supplied to New(), will be called each time your
// Watcher path changes (that is, the file's mtime changes), and is supplied the
// new mtime of that path.
type WatcherCallback func(mtime time.Time)

// Watcher is used to watch a file on a filesystem, and let you do something
// whenever it's mtime changes.
type Watcher struct {
	path          string
	cb            WatcherCallback
	pollFrequency time.Duration
	previous      time.Time
	stop          chan bool
	stopped       bool
	sync.RWMutex
}

// New returns a new Watcher that will call your cb with path's mtime whenever
// its mtime changes in the future.
//
// It also immediately gets the path's mtime, available via Mtime().
//
// This is intended for use on filesystems that don't support inotify, so the
// watcher will check for changes to path's mtime every pollFrequency.
func New(path string, cb WatcherCallback, pollFrequency time.Duration) (*Watcher, error) {
	mtime, err := getFileMtime(path)
	if err != nil {
		return nil, err
	}

	w := &Watcher{
		path:          path,
		cb:            cb,
		pollFrequency: pollFrequency,
		previous:      mtime,
	}

	w.startWatching()

	return w, nil
}

// getFileMtime returns the mtime of the given file.
func getFileMtime(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}

	return info.ModTime(), nil
}

// startWatching will start ticking at our pollFrequency, and call our cb if
// our path's mtime changes.
func (w *Watcher) startWatching() {
	ticker := time.NewTicker(w.pollFrequency)

	stopTicking := make(chan bool)

	w.stop = stopTicking

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-stopTicking:
				return
			case <-ticker.C:
				w.callCBIfMtimeChanged()
			}
		}
	}()
}

// callCBIfMtimeChanged calls our cb if the mtime of our path has changed since
// the last time this method was called. Errors in trying to get the mtime are
// ignored, in the hopes a future attempt will succeed.
func (w *Watcher) callCBIfMtimeChanged() {
	w.Lock()
	defer w.Unlock()

	mtime, err := getFileMtime(w.path)
	if err != nil || mtime == w.previous {
		return
	}

	w.cb(mtime)

	w.previous = mtime
}

// Mtime returns the latest mtime of our path, captured during New() or the last
// time we polled.
func (w *Watcher) Mtime() time.Time {
	w.RLock()
	defer w.RUnlock()

	return w.previous
}

// Stop will stop watching our path for changes.
func (w *Watcher) Stop() {
	w.Lock()
	defer w.Unlock()

	if w.stopped {
		return
	}

	close(w.stop)
	w.stopped = true
}
