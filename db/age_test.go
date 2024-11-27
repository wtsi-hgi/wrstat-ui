package db

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDirGUTAge(t *testing.T) {
	Convey("You can go from a string to a DirGUTAge", t, func() {
		age, err := AgeStringToDirGUTAge("0")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeAll)

		age, err = AgeStringToDirGUTAge("1")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeA1M)

		age, err = AgeStringToDirGUTAge("2")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeA2M)

		age, err = AgeStringToDirGUTAge("3")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeA6M)

		age, err = AgeStringToDirGUTAge("4")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeA1Y)

		age, err = AgeStringToDirGUTAge("5")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeA2Y)

		age, err = AgeStringToDirGUTAge("6")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeA3Y)

		age, err = AgeStringToDirGUTAge("7")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeA5Y)

		age, err = AgeStringToDirGUTAge("8")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeA7Y)

		age, err = AgeStringToDirGUTAge("9")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeM1M)

		age, err = AgeStringToDirGUTAge("10")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeM2M)

		age, err = AgeStringToDirGUTAge("11")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeM6M)

		age, err = AgeStringToDirGUTAge("12")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeM1Y)

		age, err = AgeStringToDirGUTAge("13")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeM2Y)

		age, err = AgeStringToDirGUTAge("14")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeM3Y)

		age, err = AgeStringToDirGUTAge("15")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeM5Y)

		age, err = AgeStringToDirGUTAge("16")
		So(err, ShouldBeNil)
		So(age, ShouldEqual, DGUTAgeM7Y)

		_, err = AgeStringToDirGUTAge("17")
		So(err, ShouldNotBeNil)

		_, err = AgeStringToDirGUTAge("incorrect")
		So(err, ShouldNotBeNil)
	})
}
