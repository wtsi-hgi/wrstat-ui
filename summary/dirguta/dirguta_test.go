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

package dirguta

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
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

	Convey("isTemp lets you know if a path is a temporary file", t, func() {
		So(isTempFile(".tmp.cram"), ShouldBeTrue)
		So(isTempFile("tmp.cram"), ShouldBeTrue)
		So(isTempFile("xtmp.cram"), ShouldBeFalse)
		So(isTempFile("tmpx.cram"), ShouldBeFalse)

		So(isTempFile(".temp.cram"), ShouldBeTrue)
		So(isTempFile("temp.cram"), ShouldBeTrue)
		So(isTempFile("xtemp.cram"), ShouldBeFalse)
		So(isTempFile("tempx.cram"), ShouldBeFalse)

		So(isTempFile("a.cram.tmp"), ShouldBeTrue)
		So(isTempFile("xtmp"), ShouldBeFalse)
		So(isTempFile("a.cram.temp"), ShouldBeTrue)
		So(isTempFile("xtemp"), ShouldBeFalse)

		d := internaltest.NewDirectoryPathCreator()

		So(isTempDir(d.ToDirectoryPath("/foo/tmp/bar.cram")), ShouldBeTrue)
		So(isTempDir(d.ToDirectoryPath("/foo/temp/bar.cram")), ShouldBeTrue)
		So(isTempDir(d.ToDirectoryPath("/foo/TEMP/bar.cram")), ShouldBeTrue)
		So(isTempDir(d.ToDirectoryPath("/foo/bar.cram")), ShouldBeFalse)
	})

	Convey("isVCF lets you know if a path is a vcf file", t, func() {
		So(isVCF("bar.vcf"), ShouldBeTrue)
		So(isVCF("bar.VCF"), ShouldBeTrue)
		So(isVCF("vcf.bar"), ShouldBeFalse)
		So(isVCF("bar.fcv"), ShouldBeFalse)
	})

	Convey("isVCFGz lets you know if a path is a vcf.gz file", t, func() {
		So(isVCFGz("bar.vcf.gz"), ShouldBeTrue)
		So(isVCFGz("vcf.gz.bar"), ShouldBeFalse)
		So(isVCFGz("bar.vcf"), ShouldBeFalse)
	})

	Convey("isBCF lets you know if a path is a bcf file", t, func() {
		So(isBCF("bar.bcf"), ShouldBeTrue)
		So(isBCF("bcf.bar"), ShouldBeFalse)
		So(isBCF("bar.vcf"), ShouldBeFalse)
	})

	Convey("isSam lets you know if a path is a sam file", t, func() {
		So(isSam("bar.sam"), ShouldBeTrue)
		So(isSam("bar.bam"), ShouldBeFalse)
	})

	Convey("isBam lets you know if a path is a bam file", t, func() {
		So(isBam("bar.bam"), ShouldBeTrue)
		So(isBam("bar.sam"), ShouldBeFalse)
	})

	Convey("isCram lets you know if a path is a cram file", t, func() {
		So(isCram("bar.cram"), ShouldBeTrue)
		So(isCram("bar.bam"), ShouldBeFalse)
	})

	Convey("isFasta lets you know if a path is a fasta file", t, func() {
		So(isFasta("bar.fasta"), ShouldBeTrue)
		So(isFasta("bar.fa"), ShouldBeTrue)
		So(isFasta("bar.fastq"), ShouldBeFalse)
	})

	Convey("isFastq lets you know if a path is a fastq file", t, func() {
		So(isFastq("bar.fastq"), ShouldBeTrue)
		So(isFastq("bar.fq"), ShouldBeTrue)
		So(isFastq("bar.fasta"), ShouldBeFalse)
		So(isFastq("bar.fastq.gz"), ShouldBeFalse)
	})

	Convey("isFastqGz lets you know if a path is a fastq.gz file", t, func() {
		So(isFastqGz("bar.fastq.gz"), ShouldBeTrue)
		So(isFastqGz("bar.fq.gz"), ShouldBeTrue)
		So(isFastqGz("bar.fastq"), ShouldBeFalse)
		So(isFastqGz("bar.fq"), ShouldBeFalse)
	})

	Convey("isPedBed lets you know if a path is a ped/bed related file", t, func() {
		So(isPedBed("bar.ped"), ShouldBeTrue)
		So(isPedBed("bar.map"), ShouldBeTrue)
		So(isPedBed("bar.bed"), ShouldBeTrue)
		So(isPedBed("bar.bim"), ShouldBeTrue)
		So(isPedBed("bar.fam"), ShouldBeTrue)
		So(isPedBed("bar.asd"), ShouldBeFalse)
	})

	Convey("isCompressed lets you know if a path is a compressed file", t, func() {
		So(isCompressed("bar.bzip2"), ShouldBeTrue)
		So(isCompressed("bar.gz"), ShouldBeTrue)
		So(isCompressed("bar.tgz"), ShouldBeTrue)
		So(isCompressed("bar.zip"), ShouldBeTrue)
		So(isCompressed("bar.xz"), ShouldBeTrue)
		So(isCompressed("bar.bgz"), ShouldBeTrue)
		So(isCompressed("bar.bcf"), ShouldBeFalse)
		So(isCompressed("bar.asd"), ShouldBeFalse)
		So(isCompressed("bar.vcf.gz"), ShouldBeFalse)
		So(isCompressed("bar.fastq.gz"), ShouldBeFalse)
	})

	Convey("isText lets you know if a path is a text file", t, func() {
		So(isText("bar.csv"), ShouldBeTrue)
		So(isText("bar.tsv"), ShouldBeTrue)
		So(isText("bar.txt"), ShouldBeTrue)
		So(isText("bar.text"), ShouldBeTrue)
		So(isText("bar.md"), ShouldBeTrue)
		So(isText("bar.dat"), ShouldBeTrue)
		So(isText("bar.README"), ShouldBeTrue)
		So(isText("READme"), ShouldBeTrue)
		So(isText("bar.sam"), ShouldBeFalse)
		So(isText("bar.out"), ShouldBeFalse)
		So(isText("bar.asd"), ShouldBeFalse)
	})

	Convey("isLog lets you know if a path is a log file", t, func() {
		So(isLog("bar.log"), ShouldBeTrue)
		So(isLog("bar.o"), ShouldBeTrue)
		So(isLog("bar.out"), ShouldBeTrue)
		So(isLog("bar.e"), ShouldBeTrue)
		So(isLog("bar.err"), ShouldBeTrue)
		So(isLog("bar.oe"), ShouldBeTrue)
		So(isLog("bar.txt"), ShouldBeFalse)
		So(isLog("bar.asd"), ShouldBeFalse)
	})

	Convey("infoToType lets you know the filetypes of a file", t, func() {
		d := internaltest.NewDirectoryPathCreator()

		for _, test := range [...]struct {
			Path     string
			IsDir    bool
			FileType DirGUTAFileType
			IsTmp    bool
		}{
			{"/some/path/", true, DGUTAFileTypeDir, false},
			{"/foo/bar.asd", false, DGUTAFileTypeOther, false},
			{"/foo/.tmp.asd", false, DGUTAFileTypeOther, true},
			{"/foo/bar.vcf", false, DGUTAFileTypeVCF, false},
			{"/foo/bar.vcf.gz", false, DGUTAFileTypeVCFGz, false},
			{"/foo/bar.bcf", false, DGUTAFileTypeBCF, false},
			{"/foo/bar.sam", false, DGUTAFileTypeSam, false},
			{"/foo/bar.bam", false, DGUTAFileTypeBam, false},
			{"/foo/.tmp.cram", false, DGUTAFileTypeCram, true},
			{"/foo/bar.fa", false, DGUTAFileTypeFasta, false},
			{"/foo/bar.fq", false, DGUTAFileTypeFastq, false},
			{"/foo/bar.fq.gz", false, DGUTAFileTypeFastqGz, false},
			{"/foo/bar.bzip2", false, DGUTAFileTypeCompressed, false},
			{"/foo/bar.csv", false, DGUTAFileTypeText, false},
			{"/foo/bar.o", false, DGUTAFileTypeLog, false},
		} {
			ft, tmp := infoToType(internaltest.NewMockInfo(d.ToDirectoryPath(test.Path), 0, 0, 0, test.IsDir))
			So(ft, ShouldEqual, test.FileType)
			So(tmp, ShouldEqual, test.IsTmp)
		}
	})
}

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

type mockDB struct {
	gutas map[string]GUTAs
}

func (m *mockDB) Add(dguta recordDGUTA) error {
	m.gutas[string(dguta.Dir.AppendTo(nil))] = dguta.GUTAs

	return nil
}

func (m *mockDB) has(dir string, gid, uid uint32, ft DirGUTAFileType, age DirGUTAge, count, size uint64, atime, mtime int64) bool {
	dgutas, ok := m.gutas[dir]
	if !ok {
		return false
	}

	expected := GUTA{
		GID:   gid,
		UID:   uid,
		FT:    ft,
		Age:   age,
		Count: count,
		Size:  size,
		Atime: atime,
		Mtime: mtime,
	}

	for _, dguta := range dgutas {
		if *dguta == expected {
			return true
		}
	}

	return false
}

func (m *mockDB) hasNot(dir string, gid, uid uint32, ft DirGUTAFileType, age DirGUTAge) bool {
	dgutas, ok := m.gutas[dir]
	if !ok {
		return true
	}

	for _, dguta := range dgutas {
		if dguta.GID == gid && dguta.UID == uid && dguta.FT == ft && dguta.Age == age {
			return false
		}
	}

	return true
}

func TestDirGUTA(t *testing.T) {
	gid, uid, _, _, err := internaluser.RealGIDAndUID()
	if err != nil {
		t.Fatal(err)
	}

	refTime := time.Now().Unix()

	Convey("You can summarise data with a range of Atimes", t, func() {
		f := statsdata.NewRoot("/", 0)
		f.UID = uid
		f.GID = gid

		atime1 := refTime - (SecondsInAMonth*2 + 100000)
		mtime1 := refTime - (SecondsInAMonth * 3)
		addFile(f, "a/b/c/1.bam", uid, gid, 2, atime1, mtime1)

		atime2 := refTime - (SecondsInAMonth * 7)
		mtime2 := refTime - (SecondsInAMonth * 8)
		addFile(f, "a/b/c/2.bam", uid, gid, 3, atime2, mtime2)

		atime3 := refTime - (SecondsInAYear + SecondsInAMonth)
		mtime3 := refTime - (SecondsInAYear + SecondsInAMonth*6)
		addFile(f, "a/b/c/3.txt", uid, gid, 4, atime3, mtime3)

		atime4 := refTime - (SecondsInAYear * 4)
		mtime4 := refTime - (SecondsInAYear * 6)
		addFile(f, "a/b/c/4.bam", uid, gid, 5, atime4, mtime4)

		atime5 := refTime - (SecondsInAYear*5 + SecondsInAMonth)
		mtime5 := refTime - (SecondsInAYear*7 + SecondsInAMonth)
		addFile(f, "a/b/c/5.cram", uid, gid, 6, atime5, mtime5)

		atime6 := refTime - (SecondsInAYear*7 + SecondsInAMonth)
		mtime6 := refTime - (SecondsInAYear*7 + SecondsInAMonth)
		addFile(f, "a/b/c/6.cram", uid, gid, 7, atime6, mtime6)

		addFile(f, "a/b/c/6.tmp", uid, gid, 8, mtime3, mtime3)

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{make(map[string]GUTAs)}
		op := newDirGroupUserTypeAge(m, refTime)
		s.AddDirectoryOperation(op)

		err := s.Summarise()
		So(err, ShouldBeNil)

		dir := "/a/b/c/"
		ft, count, size := DGUTAFileTypeBam, uint64(3), uint64(10)
		testAtime, testMtime := atime4, mtime1

		So(m.has(dir, gid, uid, ft, DGUTAgeAll, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA6M, count-1, size-2, testAtime, mtime2), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA1Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA2Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA3Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA7Y), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM6M, count-1, size-2, testAtime, mtime2), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM1Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM2Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM3Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM5Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeM7Y), ShouldBeTrue)

		ft, count, size = DGUTAFileTypeCram, 2, 13
		testAtime, testMtime = atime6, mtime5

		So(m.has(dir, gid, uid, ft, DGUTAgeAll, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA2Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA3Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA5Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA7Y, count-1, size-6, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM2Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM3Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM5Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM7Y, count, size, testAtime, testMtime), ShouldBeTrue)

		ft, count, size = DGUTAFileTypeText, 1, 4
		testAtime, testMtime = atime3, mtime3

		So(m.has(dir, gid, uid, ft, DGUTAgeAll, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA2Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA3Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA7Y), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeM2Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeM3Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeM5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeM7Y), ShouldBeTrue)

		ft, count, size = DGUTAFileTypeTemp, 1, 8
		testAtime, testMtime = mtime3, mtime3

		So(m.has(dir, gid, uid, ft, DGUTAgeAll, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeA1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA2Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA3Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeA7Y), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, DGUTAgeM1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeM2Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeM3Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeM5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, DGUTAgeM7Y), ShouldBeTrue)
	})

	Convey("You can summarise data with different groups and users", t, func() {
		f := statsdata.NewRoot("/", 0)

		atime1 := int64(100)
		mtime1 := int64(0)
		addFile(f, "a/b/c/3.bam", 2, 2, 1, atime1, mtime1)

		atime2 := int64(250)
		mtime2 := int64(250)
		addFile(f, "a/b/c/7.cram", 10, 2, 2, atime2, mtime2)

		atime3 := int64(201)
		mtime3 := int64(200)
		addFile(f, "a/b/c/d/9.cram", 10, 2, 3, atime3, mtime3)

		atime4 := int64(300)
		mtime4 := int64(301)
		addFile(f, "a/b/c/8.cram", 2, 10, 4, atime4, mtime4)

		dDir := f.AddDirectory("a").AddDirectory("b").AddDirectory("c").AddDirectory("d")
		dDir.UID = 10
		dDir.GID = 2
		dDir.ATime = 50
		dDir.Size = 8192

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{make(map[string]GUTAs)}
		op := newDirGroupUserTypeAge(m, refTime)
		s.AddDirectoryOperation(op)

		err := s.Summarise()
		So(err, ShouldBeNil)

		for _, age := range DirGUTAges {
			So(m.has("/a/b/c/d/", 2, 10, DGUTAFileTypeCram, age, 1, 3, atime3, mtime3), ShouldBeTrue)
		}

		So(m.has("/a/b/c/", 2, 2, DGUTAFileTypeBam, DGUTAgeAll, 1, 1, atime1, mtime1), ShouldBeTrue)
		So(m.hasNot("/a/b/c/", 2, 2, DGUTAFileTypeCram, DGUTAgeAll), ShouldBeTrue)
		So(m.has("/a/b/c/", 2, 10, DGUTAFileTypeCram, DGUTAgeAll, 2, 5, atime3, mtime2), ShouldBeTrue)
		So(m.has("/a/b/c/", 10, 2, DGUTAFileTypeCram, DGUTAgeAll, 1, 4, atime4, mtime4), ShouldBeTrue)
	})
}

func addFile(d *statsdata.Directory, path string, uid, gid uint32, size, atime, mtime int64) {
	for _, part := range strings.Split(filepath.Dir(path), "/") {
		d = d.AddDirectory(part)
	}

	file := d.AddFile(filepath.Base(path))
	file.UID = uid
	file.GID = gid
	file.Size = size
	file.ATime = atime
	file.MTime = mtime
}
