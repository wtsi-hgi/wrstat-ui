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
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestOpenProvider(t *testing.T) {
	Convey("OpenProvider validates config", t, func() {
		Convey("it errors when BasePath is empty", func() {
			p, err := OpenProvider(Config{})
			So(err, ShouldNotBeNil)
			So(p, ShouldBeNil)
		})
	})
}

func TestProviderOnUpdateContract(t *testing.T) {
	Convey("swapStateAndHandle runs callbacks asynchronously and defers cleanup until callbacks finish", t, func() {
		p := &boltProvider{cfg: Config{}}

		cbStarted := make(chan struct{})
		cbRelease := make(chan struct{})
		oldClosed := make(chan struct{})

		old := &providerState{closers: []func() error{
			func() error {
				close(oldClosed)

				return nil
			},
		}}

		st := &providerState{}

		done := make(chan struct{})

		go func() {
			p.swapStateAndHandle(st, func() {
				close(cbStarted)
				<-cbRelease
			}, nil, old)
			close(done)
		}()

		So(waitWithTimeout(cbStarted, 200*time.Millisecond), ShouldBeTrue)
		So(waitWithTimeout(done, 200*time.Millisecond), ShouldBeTrue)

		select {
		case <-oldClosed:
			So("", ShouldEqual, "old state closed too early")
		default:
		}

		close(cbRelease)
		So(waitWithTimeout(oldClosed, 200*time.Millisecond), ShouldBeTrue)
	})

	Convey("callbacks are not invoked concurrently", t, func() {
		p := &boltProvider{cfg: Config{}}

		firstStarted := make(chan struct{})
		firstRelease := make(chan struct{})
		secondStarted := make(chan struct{})

		go func() {
			p.swapStateAndHandle(&providerState{}, func() {
				close(firstStarted)
				<-firstRelease
			}, nil, nil)
		}()

		So(waitWithTimeout(firstStarted, 200*time.Millisecond), ShouldBeTrue)

		go func() {
			p.swapStateAndHandle(&providerState{}, func() {
				close(secondStarted)
			}, nil, nil)
		}()

		So(waitWithTimeout(secondStarted, 100*time.Millisecond), ShouldBeFalse)

		close(firstRelease)
		So(waitWithTimeout(secondStarted, 200*time.Millisecond), ShouldBeTrue)
	})
}

func waitWithTimeout(ch <-chan struct{}, d time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}
