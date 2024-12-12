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

package watch

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

// testMtimeTracker lets us test mtime updates in our WatcherCallback in a
// thread-safe way.
type testMtimeTracker struct {
	sync.RWMutex
	calls  int
	latest time.Time
}

// update increments calls and sets latest to the given mtime.
func (m *testMtimeTracker) update(mtime time.Time) {
	m.Lock()
	defer m.Unlock()

	m.calls++
	m.latest = mtime
}

// report returns the latest mtime and true if there has both been a call to
// update() since the last call to report(), and the given time is older than
// the latest mtime.
func (m *testMtimeTracker) report(previous time.Time) (time.Time, bool) {
	m.Lock()
	defer m.Unlock()

	calls := m.calls
	m.calls = 0

	return m.latest, calls == 1 && previous.Before(m.latest)
}

// numCalls tells you how many calls to update() there have been since the last
// call to report().
func (m *testMtimeTracker) numCalls() int {
	m.RLock()
	defer m.RUnlock()

	return m.calls
}

func TestWatch(t *testing.T) {
	pollFrequency := 10 * time.Millisecond

	Convey("Given a file to watch", t, func() {
		// lustre can record mtimes earlier than local time, so our 'before'
		// time has to be even more before
		before := time.Now().Add(-1 * time.Second)

		path := createTestFile(t)

		Convey("You can create a watcher, which immediately finds the file's mtime", func() {
			tracker := &testMtimeTracker{}

			cb := func(mtime time.Time) {
				tracker.update(mtime)
			}

			w, err := New(path, cb, pollFrequency)
			So(err, ShouldBeNil)
			defer w.Stop()

			calls := tracker.numCalls()
			So(calls, ShouldEqual, 0)

			_, ok := tracker.report(before)
			So(ok, ShouldBeFalse)

			latest := w.Mtime()
			So(latest.After(before), ShouldBeTrue)

			Convey("Changing the file's mtime calls cb after some time", func() {
				<-time.After(2 * pollFrequency)

				calls := tracker.numCalls()
				So(calls, ShouldEqual, 0)

				_, ok = tracker.report(latest)
				So(ok, ShouldBeFalse)

				touchTestFile(path)
				<-time.After(2 * pollFrequency)

				latest, ok = tracker.report(latest)
				So(ok, ShouldBeTrue)

				Convey("Stop() ends the polling", func() {
					w.Stop()
					tracker.report(latest)
					calls := tracker.numCalls()
					So(calls, ShouldEqual, 0)
					<-time.After(100 * time.Millisecond)

					touchTestFile(path)
					<-time.After(2 * pollFrequency)

					calls = tracker.numCalls()
					So(calls, ShouldEqual, 0)

					_, ok = tracker.report(latest)
					So(ok, ShouldBeFalse)
				})
			})
		})
	})

	Convey("You can't create a watcher with a bad file", t, func() {
		w, err := New("/foo£@£$%", func(time.Time) {}, pollFrequency)
		So(err, ShouldNotBeNil)
		So(w, ShouldBeNil)
	})
}

// createTestFile creates a file to test with that will be auto-cleaned up after
// the test. Returns its path.
func createTestFile(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "file")
	f, err := os.Create(path)
	So(err, ShouldBeNil)
	err = f.Close()
	So(err, ShouldBeNil)

	return path
}

// touchTestFile modifies path's a and mtime to the current time.
func touchTestFile(path string) {
	now := time.Now().Local()
	err := os.Chtimes(path, now, now)
	So(err, ShouldBeNil)
}
