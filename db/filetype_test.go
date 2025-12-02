/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDirGUTAFileType(t *testing.T) {
	Convey("DGUTAFileType* consts are ints that can be stringified", t, func() {
		So(DGUTAFileTypeOther.String(), ShouldEqual, "other")
		So(DGUTAFileTypeTemp.String(), ShouldEqual, "temp")
		So(DGUTAFileTypeVCF.String(), ShouldEqual, "vcf")
		So(DGUTAFileTypeVCFGz.String(), ShouldEqual, "vcf.gz")
		So(DGUTAFileTypeBCF.String(), ShouldEqual, "bcf")
		So(DGUTAFileTypeSam.String(), ShouldEqual, "sam")
		So(DGUTAFileTypeBam.String(), ShouldEqual, "bam")
		So(DGUTAFileTypeCram.String(), ShouldEqual, "cram")
		So(DGUTAFileTypeFasta.String(), ShouldEqual, "fasta")
		So(DGUTAFileTypeFastq.String(), ShouldEqual, "fastq")
		So(DGUTAFileTypeFastqGz.String(), ShouldEqual, "fastq.gz")
		So(DGUTAFileTypePedBed.String(), ShouldEqual, "ped/bed")
		So(DGUTAFileTypeCompressed.String(), ShouldEqual, "compressed")
		So(DGUTAFileTypeText.String(), ShouldEqual, "text")
		So(DGUTAFileTypeLog.String(), ShouldEqual, "log")

		So(int(DGUTAFileTypeTemp), ShouldEqual, 1)
	})

	Convey("String() returns multiple types joined with '|'", t, func() {
		ft := DGUTAFileTypeTemp | DGUTAFileTypeFastq

		s := ft.String()
		parts := strings.Split(s, "|")

		So(len(parts), ShouldEqual, 2)
		So(parts, ShouldContain, "temp")
		So(parts, ShouldContain, "fastq")
	})

	Convey("String() ordering is based on provided bit checks order", t, func() {
		ft := DGUTAFileTypeCram | DGUTAFileTypeTemp | DGUTAFileTypeText

		So(ft.String(), ShouldEqual, "temp|cram|text")
	})

	Convey("Multiple flags include all mapped names", t, func() {
		ft := DGUTAFileTypeFastq | DGUTAFileTypeFastqGz | DGUTAFileTypeCompressed

		s := ft.String()
		parts := strings.Split(s, "|")

		So(parts, ShouldContain, "fastq")
		So(parts, ShouldContain, "fastq.gz")
		So(parts, ShouldContain, "compressed")
	})

	Convey("You can go from a string to a DGUTAFileType", t, func() {
		ft, err := FileTypeStringToDirGUTAFileType("other")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeOther)

		ft, err = FileTypeStringToDirGUTAFileType("temp")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeTemp)

		ft, err = FileTypeStringToDirGUTAFileType("vcf")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeVCF)

		ft, err = FileTypeStringToDirGUTAFileType("vcf.gz")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeVCFGz)

		ft, err = FileTypeStringToDirGUTAFileType("bcf")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeBCF)

		ft, err = FileTypeStringToDirGUTAFileType("sam")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeSam)

		ft, err = FileTypeStringToDirGUTAFileType("bam")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeBam)

		ft, err = FileTypeStringToDirGUTAFileType("cram")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeCram)

		ft, err = FileTypeStringToDirGUTAFileType("fasta")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeFasta)

		ft, err = FileTypeStringToDirGUTAFileType("fastq")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeFastq)

		ft, err = FileTypeStringToDirGUTAFileType("fastq.gz")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeFastqGz)

		ft, err = FileTypeStringToDirGUTAFileType("ped/bed")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypePedBed)

		ft, err = FileTypeStringToDirGUTAFileType("compressed")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeCompressed)

		ft, err = FileTypeStringToDirGUTAFileType("text")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeText)

		ft, err = FileTypeStringToDirGUTAFileType("log")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTAFileTypeLog)

		ft, err = FileTypeStringToDirGUTAFileType("foo")
		So(err, ShouldNotBeNil)
		So(err, ShouldEqual, ErrInvalidType)
		So(ft, ShouldEqual, DGUTAFileTypeOther)
	})
}
