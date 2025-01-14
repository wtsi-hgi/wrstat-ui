/*******************************************************************************
 * Copyright (c) 2022, 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *         Michael Woolnough <mw31@sanger.ac.uk>
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

package db

import (
	"testing"
	"time"

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

func TestFitsAgeInterval(t *testing.T) {
	refTime := time.Now().Unix()

	Convey("You can check whether atime or mtime fit within an interval ", t, func() {
		So(DGUTAgeAll.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeAll.FitsAgeInterval(refTime, refTime, refTime), ShouldBeTrue)

		So(DGUTAgeA1M.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA1M.FitsAgeInterval(refTime, refTime, refTime), ShouldBeFalse)
		So(DGUTAgeA1M.FitsAgeInterval(refTime-SecondsInAMonth, refTime-SecondsInAMonth, refTime), ShouldBeTrue)
		So(DGUTAgeA1M.FitsAgeInterval(refTime-SecondsInAMonth, refTime, refTime), ShouldBeTrue)
		So(DGUTAgeA1M.FitsAgeInterval(refTime-SecondsInAMonth+1, refTime, refTime), ShouldBeFalse)

		So(DGUTAgeM1M.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeM1M.FitsAgeInterval(refTime, refTime, refTime), ShouldBeFalse)
		So(DGUTAgeM1M.FitsAgeInterval(refTime-SecondsInAMonth, refTime-SecondsInAMonth, refTime), ShouldBeTrue)
		So(DGUTAgeM1M.FitsAgeInterval(refTime, refTime-SecondsInAMonth, refTime), ShouldBeTrue)
		So(DGUTAgeM1M.FitsAgeInterval(refTime, refTime-SecondsInAMonth+1, refTime), ShouldBeFalse)

		So(DGUTAgeA2M.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA2M.FitsAgeInterval(refTime, 0, refTime), ShouldBeFalse)
		So(DGUTAgeA2M.FitsAgeInterval(refTime-2*SecondsInAMonth, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA2M.FitsAgeInterval(refTime-2*SecondsInAMonth+1, 0, refTime), ShouldBeFalse)

		So(DGUTAgeM2M.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeM2M.FitsAgeInterval(0, refTime, refTime), ShouldBeFalse)
		So(DGUTAgeM2M.FitsAgeInterval(0, refTime-2*SecondsInAMonth, refTime), ShouldBeTrue)
		So(DGUTAgeM2M.FitsAgeInterval(0, refTime-2*SecondsInAMonth+1, refTime), ShouldBeFalse)

		So(DGUTAgeA6M.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA6M.FitsAgeInterval(refTime, 0, refTime), ShouldBeFalse)
		So(DGUTAgeA6M.FitsAgeInterval(refTime-6*SecondsInAMonth, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA6M.FitsAgeInterval(refTime-6*SecondsInAMonth+1, 0, refTime), ShouldBeFalse)

		So(DGUTAgeM6M.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeM6M.FitsAgeInterval(0, refTime, refTime), ShouldBeFalse)
		So(DGUTAgeM6M.FitsAgeInterval(0, refTime-6*SecondsInAMonth, refTime), ShouldBeTrue)
		So(DGUTAgeM6M.FitsAgeInterval(0, refTime-6*SecondsInAMonth+1, refTime), ShouldBeFalse)

		So(DGUTAgeA1Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA1Y.FitsAgeInterval(refTime, 0, refTime), ShouldBeFalse)
		So(DGUTAgeA1Y.FitsAgeInterval(refTime-SecondsInAYear, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA1Y.FitsAgeInterval(refTime-SecondsInAYear+1, 0, refTime), ShouldBeFalse)

		So(DGUTAgeM1Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeM1Y.FitsAgeInterval(0, refTime, refTime), ShouldBeFalse)
		So(DGUTAgeM1Y.FitsAgeInterval(0, refTime-SecondsInAYear, refTime), ShouldBeTrue)
		So(DGUTAgeM1Y.FitsAgeInterval(0, refTime-SecondsInAYear+1, refTime), ShouldBeFalse)

		So(DGUTAgeA2Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA2Y.FitsAgeInterval(refTime, 0, refTime), ShouldBeFalse)
		So(DGUTAgeA2Y.FitsAgeInterval(refTime-2*SecondsInAYear, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA2Y.FitsAgeInterval(refTime-2*SecondsInAYear+1, 0, refTime), ShouldBeFalse)

		So(DGUTAgeM2Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeM2Y.FitsAgeInterval(0, refTime, refTime), ShouldBeFalse)
		So(DGUTAgeM2Y.FitsAgeInterval(0, refTime-2*SecondsInAYear, refTime), ShouldBeTrue)
		So(DGUTAgeM2Y.FitsAgeInterval(0, refTime-2*SecondsInAYear+1, refTime), ShouldBeFalse)

		So(DGUTAgeA3Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA3Y.FitsAgeInterval(refTime, 0, refTime), ShouldBeFalse)
		So(DGUTAgeA3Y.FitsAgeInterval(refTime-3*SecondsInAYear, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA3Y.FitsAgeInterval(refTime-3*SecondsInAYear+1, 0, refTime), ShouldBeFalse)

		So(DGUTAgeM3Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeM3Y.FitsAgeInterval(0, refTime, refTime), ShouldBeFalse)
		So(DGUTAgeM3Y.FitsAgeInterval(0, refTime-3*SecondsInAYear, refTime), ShouldBeTrue)
		So(DGUTAgeM3Y.FitsAgeInterval(0, refTime-3*SecondsInAYear+1, refTime), ShouldBeFalse)

		So(DGUTAgeA5Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA5Y.FitsAgeInterval(refTime, 0, refTime), ShouldBeFalse)
		So(DGUTAgeA5Y.FitsAgeInterval(refTime-5*SecondsInAYear, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA5Y.FitsAgeInterval(refTime-5*SecondsInAYear+1, 0, refTime), ShouldBeFalse)

		So(DGUTAgeM5Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeM5Y.FitsAgeInterval(0, refTime, refTime), ShouldBeFalse)
		So(DGUTAgeM5Y.FitsAgeInterval(0, refTime-5*SecondsInAYear, refTime), ShouldBeTrue)
		So(DGUTAgeM5Y.FitsAgeInterval(0, refTime-5*SecondsInAYear+1, refTime), ShouldBeFalse)

		So(DGUTAgeA7Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA7Y.FitsAgeInterval(refTime, 0, refTime), ShouldBeFalse)
		So(DGUTAgeA7Y.FitsAgeInterval(refTime-7*SecondsInAYear, 0, refTime), ShouldBeTrue)
		So(DGUTAgeA7Y.FitsAgeInterval(refTime-7*SecondsInAYear+1, 0, refTime), ShouldBeFalse)

		So(DGUTAgeM7Y.FitsAgeInterval(0, 0, refTime), ShouldBeTrue)
		So(DGUTAgeM7Y.FitsAgeInterval(0, refTime, refTime), ShouldBeFalse)
		So(DGUTAgeM7Y.FitsAgeInterval(0, refTime-7*SecondsInAYear, refTime), ShouldBeTrue)
		So(DGUTAgeM7Y.FitsAgeInterval(0, refTime-7*SecondsInAYear+1, refTime), ShouldBeFalse)
	})
}
