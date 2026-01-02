package bolt

import (
	"testing"

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
