package boltperf

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPercentilesMS(t *testing.T) {
	Convey("PercentilesMS computes p50/p95/p99", t, func() {
		p50, p95, p99 := PercentilesMS([]float64{10, 1, 5, 20})
		So(p50, ShouldEqual, 5)
		So(p95, ShouldEqual, 20)
		So(p99, ShouldEqual, 20)
	})

	Convey("PercentilesMS on empty slice", t, func() {
		p50, p95, p99 := PercentilesMS(nil)
		So(p50, ShouldEqual, 0)
		So(p95, ShouldEqual, 0)
		So(p99, ShouldEqual, 0)
	})
}
