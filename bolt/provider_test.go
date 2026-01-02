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
			}, old)
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
			}, nil)
		}()

		So(waitWithTimeout(firstStarted, 200*time.Millisecond), ShouldBeTrue)

		go func() {
			p.swapStateAndHandle(&providerState{}, func() {
				close(secondStarted)
			}, nil)
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
