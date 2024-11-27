package db

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDirGUTAFileType(t *testing.T) {
	Convey("DGUTAFileType* consts are ints that can be stringified", t, func() {
		So(DirGUTAFileType(0).String(), ShouldEqual, "other")
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
