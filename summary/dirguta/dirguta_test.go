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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
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
		So(isTempFile("/foo/.tmp.cram"), ShouldBeTrue)
		So(isTempFile("/foo/tmp.cram"), ShouldBeTrue)
		So(isTempFile("/foo/xtmp.cram"), ShouldBeFalse)
		So(isTempFile("/foo/tmpx.cram"), ShouldBeFalse)

		So(isTempFile("/foo/.temp.cram"), ShouldBeTrue)
		So(isTempFile("/foo/temp.cram"), ShouldBeTrue)
		So(isTempFile("/foo/xtemp.cram"), ShouldBeFalse)
		So(isTempFile("/foo/tempx.cram"), ShouldBeFalse)

		So(isTempFile("/foo/a.cram.tmp"), ShouldBeTrue)
		So(isTempFile("/foo/xtmp"), ShouldBeFalse)
		So(isTempFile("/foo/a.cram.temp"), ShouldBeTrue)
		So(isTempFile("/foo/xtemp"), ShouldBeFalse)

		So(isTempFile("/foo/tmp/bar.cram"), ShouldBeTrue)
		So(isTempFile("/foo/temp/bar.cram"), ShouldBeTrue)
		So(isTempFile("/foo/TEMP/bar.cram"), ShouldBeTrue)
		So(isTempFile("/foo/bar.cram"), ShouldBeFalse)
	})

	Convey("isVCF lets you know if a path is a vcf file", t, func() {
		So(isVCF("/foo/bar.vcf"), ShouldBeTrue)
		So(isVCF("/foo/bar.VCF"), ShouldBeTrue)
		So(isVCF("/foo/vcf.bar"), ShouldBeFalse)
		So(isVCF("/foo/bar.fcv"), ShouldBeFalse)
	})

	Convey("isVCFGz lets you know if a path is a vcf.gz file", t, func() {
		So(isVCFGz("/foo/bar.vcf.gz"), ShouldBeTrue)
		So(isVCFGz("/foo/vcf.gz.bar"), ShouldBeFalse)
		So(isVCFGz("/foo/bar.vcf"), ShouldBeFalse)
	})

	Convey("isBCF lets you know if a path is a bcf file", t, func() {
		So(isBCF("/foo/bar.bcf"), ShouldBeTrue)
		So(isBCF("/foo/bcf.bar"), ShouldBeFalse)
		So(isBCF("/foo/bar.vcf"), ShouldBeFalse)
	})

	Convey("isSam lets you know if a path is a sam file", t, func() {
		So(isSam("/foo/bar.sam"), ShouldBeTrue)
		So(isSam("/foo/bar.bam"), ShouldBeFalse)
	})

	Convey("isBam lets you know if a path is a bam file", t, func() {
		So(isBam("/foo/bar.bam"), ShouldBeTrue)
		So(isBam("/foo/bar.sam"), ShouldBeFalse)
	})

	Convey("isCram lets you know if a path is a cram file", t, func() {
		So(isCram("/foo/bar.cram"), ShouldBeTrue)
		So(isCram("/foo/bar.bam"), ShouldBeFalse)
	})

	Convey("isFasta lets you know if a path is a fasta file", t, func() {
		So(isFasta("/foo/bar.fasta"), ShouldBeTrue)
		So(isFasta("/foo/bar.fa"), ShouldBeTrue)
		So(isFasta("/foo/bar.fastq"), ShouldBeFalse)
	})

	Convey("isFastq lets you know if a path is a fastq file", t, func() {
		So(isFastq("/foo/bar.fastq"), ShouldBeTrue)
		So(isFastq("/foo/bar.fq"), ShouldBeTrue)
		So(isFastq("/foo/bar.fasta"), ShouldBeFalse)
		So(isFastq("/foo/bar.fastq.gz"), ShouldBeFalse)
	})

	Convey("isFastqGz lets you know if a path is a fastq.gz file", t, func() {
		So(isFastqGz("/foo/bar.fastq.gz"), ShouldBeTrue)
		So(isFastqGz("/foo/bar.fq.gz"), ShouldBeTrue)
		So(isFastqGz("/foo/bar.fastq"), ShouldBeFalse)
		So(isFastqGz("/foo/bar.fq"), ShouldBeFalse)
	})

	Convey("isPedBed lets you know if a path is a ped/bed related file", t, func() {
		So(isPedBed("/foo/bar.ped"), ShouldBeTrue)
		So(isPedBed("/foo/bar.map"), ShouldBeTrue)
		So(isPedBed("/foo/bar.bed"), ShouldBeTrue)
		So(isPedBed("/foo/bar.bim"), ShouldBeTrue)
		So(isPedBed("/foo/bar.fam"), ShouldBeTrue)
		So(isPedBed("/foo/bar.asd"), ShouldBeFalse)
	})

	Convey("isCompressed lets you know if a path is a compressed file", t, func() {
		So(isCompressed("/foo/bar.bzip2"), ShouldBeTrue)
		So(isCompressed("/foo/bar.gz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.tgz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.zip"), ShouldBeTrue)
		So(isCompressed("/foo/bar.xz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.bgz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.bcf"), ShouldBeFalse)
		So(isCompressed("/foo/bar.asd"), ShouldBeFalse)
		So(isCompressed("/foo/bar.vcf.gz"), ShouldBeFalse)
		So(isCompressed("/foo/bar.fastq.gz"), ShouldBeFalse)
	})

	Convey("isText lets you know if a path is a text file", t, func() {
		So(isText("/foo/bar.csv"), ShouldBeTrue)
		So(isText("/foo/bar.tsv"), ShouldBeTrue)
		So(isText("/foo/bar.txt"), ShouldBeTrue)
		So(isText("/foo/bar.text"), ShouldBeTrue)
		So(isText("/foo/bar.md"), ShouldBeTrue)
		So(isText("/foo/bar.dat"), ShouldBeTrue)
		So(isText("/foo/bar.README"), ShouldBeTrue)
		So(isText("/foo/READme"), ShouldBeTrue)
		So(isText("/foo/bar.sam"), ShouldBeFalse)
		So(isText("/foo/bar.out"), ShouldBeFalse)
		So(isText("/foo/bar.asd"), ShouldBeFalse)
	})

	Convey("isLog lets you know if a path is a log file", t, func() {
		So(isLog("/foo/bar.log"), ShouldBeTrue)
		So(isLog("/foo/bar.o"), ShouldBeTrue)
		So(isLog("/foo/bar.out"), ShouldBeTrue)
		So(isLog("/foo/bar.e"), ShouldBeTrue)
		So(isLog("/foo/bar.err"), ShouldBeTrue)
		So(isLog("/foo/bar.oe"), ShouldBeTrue)
		So(isLog("/foo/bar.txt"), ShouldBeFalse)
		So(isLog("/foo/bar.asd"), ShouldBeFalse)
	})

	Convey("DirGroupUserTypeAge.pathToTypes lets you know the filetypes of a path", t, func() {
		// var w internaltest.StringBuilder
		// dGen := NewDirGroupUserTypeAge(&w)

		// d := dGen().(*DirGroupUserTypeAge)

		// So(d.pathToTypes("/foo/bar.asd"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeOther})
		// So(pathToTypesMap(d, "/foo/.tmp.asd"), ShouldResemble, map[DirGUTAFileType]bool{
		// 	DGUTAFileTypeOther: true, DGUTAFileTypeTemp: true,
		// })

		// So(d.pathToTypes("/foo/bar.vcf"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeVCF})
		// So(d.pathToTypes("/foo/bar.vcf.gz"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeVCFGz})
		// So(d.pathToTypes("/foo/bar.bcf"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeBCF})

		// So(d.pathToTypes("/foo/bar.sam"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeSam})
		// So(d.pathToTypes("/foo/bar.bam"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeBam})
		// So(pathToTypesMap(d, "/foo/.tmp.cram"), ShouldResemble, map[DirGUTAFileType]bool{
		// 	DGUTAFileTypeCram: true, DGUTAFileTypeTemp: true,
		// })

		// So(d.pathToTypes("/foo/bar.fa"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeFasta})
		// So(d.pathToTypes("/foo/bar.fq"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeFastq})
		// So(d.pathToTypes("/foo/bar.fq.gz"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeFastqGz})

		// So(d.pathToTypes("/foo/bar.bzip2"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeCompressed})
		// So(d.pathToTypes("/foo/bar.csv"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeText})
		// So(d.pathToTypes("/foo/bar.o"), ShouldResemble, []DirGUTAFileType{DGUTAFileTypeLog})
	})
}

// pathToTypesMap is used in tests to help ignore the order of types returned by
// DirGroupUserTypeAge.pathToTypes, for test comparison purposes.
// func pathToTypesMap(d *DirGroupUserTypeAge, path string) map[DirGUTAFileType]bool {
// 	types := d.pathToTypes(path)
// 	m := make(map[DirGUTAFileType]bool, len(types))

// 	for _, ftype := range types {
// 		m[ftype] = true
// 	}

// 	return m
// }

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

func TestDirGUTA(t *testing.T) {
	// _, cuid, _, _, err := internaluser.RealGIDAndUID()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// Convey("Given a DirGroupUserTypeAge", t, func() {
	// 	var w internaltest.StringBuilder
	// 	dgutaGen := NewDirGroupUserTypeAge(&w)
	// 	So(dgutaGen, ShouldNotBeNil)

	// 	dguta := dgutaGen().(*DirGroupUserTypeAge)

	// 	Convey("You can add file info with a range of Atimes to it", func() {
	// 		paths := internaltest.NewDirectoryPathCreator()
	// 		atime1 := dguta.store.refTime - (SecondsInAMonth*2 + 100000)
	// 		mtime1 := dguta.store.refTime - (SecondsInAMonth * 3)
	// 		mi := internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/1.bam"), 10, 2, 2, false, atime1)
	// 		mi.MTime = mtime1
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		atime2 := dguta.store.refTime - (SecondsInAMonth * 7)
	// 		mtime2 := dguta.store.refTime - (SecondsInAMonth * 8)
	// 		mi = internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/2.bam"), 10, 2, 3, false, atime2)
	// 		mi.MTime = mtime2
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		atime3 := dguta.store.refTime - (SecondsInAYear + SecondsInAMonth)
	// 		mtime3 := dguta.store.refTime - (SecondsInAYear + SecondsInAMonth*6)
	// 		mi = internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/3.txt"), 10, 2, 4, false, atime3)
	// 		mi.MTime = mtime3
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		atime4 := dguta.store.refTime - (SecondsInAYear * 4)
	// 		mtime4 := dguta.store.refTime - (SecondsInAYear * 6)
	// 		mi = internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/4.bam"), 10, 2, 5, false, atime4)
	// 		mi.MTime = mtime4
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		atime5 := dguta.store.refTime - (SecondsInAYear*5 + SecondsInAMonth)
	// 		mtime5 := dguta.store.refTime - (SecondsInAYear*7 + SecondsInAMonth)
	// 		mi = internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/5.cram"), 10, 2, 6, false, atime5)
	// 		mi.MTime = mtime5
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		atime6 := dguta.store.refTime - (SecondsInAYear*7 + SecondsInAMonth)
	// 		mtime6 := dguta.store.refTime - (SecondsInAYear*7 + SecondsInAMonth)
	// 		mi = internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/6.cram"), 10, 2, 7, false, atime6)
	// 		mi.MTime = mtime6
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		mi = internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/6.tmp"), 10, 2, 8, false, mtime3)
	// 		mi.MTime = mtime3
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		Convey("You can output the summaries to file", func() {
	// 			err = dguta.Output()
	// 			So(err, ShouldBeNil)

	// 			output := w.String()

	// 			buildExpectedOutputLine := func(
	// 				dir string, gid, uid int, ft DirGUTAFileType, age DirGUTAge,
	// 				count, size int, atime, mtime int64,
	// 			) string {
	// 				return fmt.Sprintf("%q\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
	// 					dir, gid, uid, ft, age, count, size, atime, mtime)
	// 			}

	// 			buildExpectedEmptyOutputLine := func(dir string, gid, uid int, ft DirGUTAFileType, age DirGUTAge) string {
	// 				return fmt.Sprintf("%s\t%d\t%d\t%d\t%d",
	// 					strconv.Quote(dir), gid, uid, ft, age)
	// 			}

	// 			dir := "/a/b/c"
	// 			gid, uid, ft, count, size := 2, 10, DGUTAFileTypeBam, 3, 10
	// 			testAtime, testMtime := atime4, mtime1

	// 			So(output, ShouldNotContainSubstring, "0\t0\t0\t0\n")

	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeAll, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA1M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA2M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA6M, count-1, size-2, testAtime, mtime2))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA1Y, count-2, size-5, testAtime, mtime4))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA2Y, count-2, size-5, testAtime, mtime4))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA3Y, count-2, size-5, testAtime, mtime4))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA5Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA7Y))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM1M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM2M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM6M, count-1, size-2, testAtime, mtime2))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM1Y, count-2, size-5, testAtime, mtime4))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM2Y, count-2, size-5, testAtime, mtime4))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM3Y, count-2, size-5, testAtime, mtime4))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM5Y, count-2, size-5, testAtime, mtime4))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeM7Y))

	// 			gid, uid, ft, count, size = 2, 10, DGUTAFileTypeCram, 2, 13
	// 			testAtime, testMtime = atime6, mtime5

	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeAll, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA1M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA2M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA6M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA1Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA2Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA3Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA5Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA7Y, count-1, size-6, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM1M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM2M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM6M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM1Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM2Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM3Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM5Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM7Y, count, size, testAtime, testMtime))

	// 			gid, uid, ft, count, size = 2, 10, DGUTAFileTypeText, 1, 4
	// 			testAtime, testMtime = atime3, mtime3

	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeAll, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA1M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA2M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA6M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA1Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA2Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA3Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA5Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA7Y))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM1M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM2M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM6M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM1Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeM2Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeM3Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeM5Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeM7Y))

	// 			gid, uid, ft, count, size = 2, 10, DGUTAFileTypeTemp, 1, 8
	// 			testAtime, testMtime = mtime3, mtime3

	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeAll, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA1M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA2M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA6M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeA1Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA2Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA3Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA5Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeA7Y))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM1M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM2M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM6M, count, size, testAtime, testMtime))
	// 			So(output, ShouldContainSubstring,
	// 				buildExpectedOutputLine(dir, gid, uid, ft, DGUTAgeM1Y, count, size, testAtime, testMtime))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeM2Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeM3Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeM5Y))
	// 			So(output, ShouldNotContainSubstring, buildExpectedEmptyOutputLine(dir, gid, uid, ft, DGUTAgeM7Y))
	// 		})
	// 	})

	// 	Convey("You can add file info to it which accumulates the info", func() {
	// 		addTestData(dguta, cuid)

	// 		paths := internaltest.NewDirectoryPathCreator()

	// 		err = dguta.Add(internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/3.bam"), 2, 2, 3, false, 100))
	// 		So(err, ShouldBeNil)

	// 		mi := internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/7.cram"), 10, 2, 2, false, 250)
	// 		mi.MTime = 250
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		mi = internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/d/9.cram"), 10, 2, 2, false, 199)
	// 		mi.MTime = 200
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		mi = internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/8.cram"), 2, 10, 2, false, 300)
	// 		mi.CTime = 301
	// 		err = dguta.Add(mi)
	// 		So(err, ShouldBeNil)

	// 		// before := time.Now().Unix()
	// 		err = dguta.Add(internaltest.NewMockInfoWithAtime(paths.ToDirectoryPath("/a/b/c/d"), 10, 2, 4096, true, 50))
	// 		So(err, ShouldBeNil)

	// 		// So(dguta.store.gsMap["/a/b/c"], ShouldNotBeNil)
	// 		// So(dguta.store.gsMap["/a/b"], ShouldNotBeNil)
	// 		// So(dguta.store.gsMap["/a"], ShouldNotBeNil)
	// 		// So(dguta.store.gsMap["/"], ShouldNotBeNil)
	// 		// So(dguta.store.gsMap[""], ShouldBeZeroValue)

	// 		// cuidKey := fmt.Sprintf("2\t%d\t13\t0", cuid)

	// 		// swa := dguta.store.gsMap["/a/b"].sumMap[GUTAKey{2, 10, 15, 0}.String()]
	// 		// if swa.atime >= before {
	// 		// 	swa.atime = 18
	// 		// }

	// 		// So(swa, ShouldResemble, &summaryWithTimes{
	// 		// 	summary{1, 4096},
	// 		// 	dguta.store.refTime, 18, 0,
	// 		// })

	// 		// swa = dguta.store.gsMap["/a/b/c"].sumMap[GUTAKey{2, 10, 15, 0}.String()]
	// 		// if swa.atime >= before {
	// 		// 	swa.atime = 18
	// 		// }

	// 		// So(swa, ShouldResemble, &summaryWithTimes{
	// 		// 	summary{1, 4096},
	// 		// 	dguta.store.refTime, 18, 0,
	// 		// })
	// 		// So(dguta.store.gsMap["/a/b/c/d"].sumMap[GUTAKey{2, 10, 15, 0}.String()], ShouldNotBeNil)

	// 		Convey("You can output the summaries to file", func() {
	// 			err = dguta.Output()
	// 			So(err, ShouldBeNil)

	// 			output := w.String()

	// 			for i := range len(DirGUTAges) - 1 {
	// 				So(output, ShouldContainSubstring, strconv.Quote("/a/b/c/d")+
	// 					fmt.Sprintf("\t2\t10\t7\t%d\t1\t2\t200\t200\n", i))
	// 			}

	// 			cuidKey := fmt.Sprintf("2\t%d\t13\t0", cuid)

	// 			// these are based on files added with newMockInfo and
	// 			// don't have a/mtime set, so show up as 0 a/mtime and are
	// 			// treated as ancient
	// 			So(output, ShouldContainSubstring, strconv.Quote("/a/b/c")+
	// 				"\t"+cuidKey+"\t2\t30\t0\t0\n")
	// 			So(output, ShouldContainSubstring, strconv.Quote("/a/b/c")+
	// 				"\t"+fmt.Sprintf("2\t%d\t13\t1", cuid)+"\t2\t30\t0\t0\n")
	// 			So(output, ShouldContainSubstring, strconv.Quote("/a/b/c")+
	// 				"\t"+fmt.Sprintf("2\t%d\t13\t16", cuid)+"\t2\t30\t0\t0\n")
	// 			So(output, ShouldContainSubstring, strconv.Quote("/a/b")+
	// 				"\t"+cuidKey+"\t3\t60\t0\t0\n")
	// 			So(output, ShouldContainSubstring, strconv.Quote("/a/b")+
	// 				"\t2\t2\t13\t0\t1\t5\t0\t0\n")
	// 			So(output, ShouldContainSubstring, strconv.Quote("/a/b")+
	// 				"\t2\t2\t6\t0\t1\t3\t100\t0\n")
	// 			So(output, ShouldContainSubstring, strconv.Quote("/")+
	// 				"\t3\t2\t13\t0\t1\t6\t0\t0\n")

	// 			So(internaltest.CheckDataIsSorted(output, 1), ShouldBeTrue)
	// 		})
	// 	})
	// })
}

func TestOldFile(t *testing.T) {
	Convey("Given an real old file and a dguta", t, func() {
		// 	var w internaltest.StringBuilder
		// 	dgutaGen := NewDirGroupUserTypeAge(&w)
		// 	So(dgutaGen, ShouldNotBeNil)

		// 	dguta := dgutaGen().(*DirGroupUserTypeAge)

		// 	tempDir := t.TempDir()
		// 	path := filepath.Join(tempDir, "oldFile.txt")
		// 	f, err := os.Create(path)
		// 	So(err, ShouldBeNil)

		// 	amtime := dguta.store.refTime - (SecondsInAYear*5 + SecondsInAMonth)

		// 	formattedTime := time.Unix(amtime, 0).Format("200601021504.05")

		// 	size, err := f.WriteString("test")
		// 	So(err, ShouldBeNil)

		// 	size64 := int64(size)

		// 	err = f.Close()
		// 	So(err, ShouldBeNil)

		// 	cmd := exec.Command("touch", "-t", formattedTime, path)
		// 	err = cmd.Run()
		// 	So(err, ShouldBeNil)

		// 	fileInfo, err := os.Stat(path)
		// 	So(err, ShouldBeNil)

		// 	statt, ok := fileInfo.Sys().(*syscall.Stat_t)
		// 	So(ok, ShouldBeTrue)

		// 	UID := statt.Uid
		// 	GID := statt.Gid

		// 	Convey("adding it results in correct a and m age sizes", func() {
		// 		paths := internaltest.NewDirectoryPathCreator()

		// 		err = dguta.Add(&FileInfo{
		// 			Path:      paths.ToDirectoryPath(path),
		// 			Size:      statt.Size,
		// 			UID:       UID,
		// 			GID:       GID,
		// 			MTime:     amtime,
		// 			ATime:     amtime,
		// 			CTime:     amtime,
		// 			EntryType: stats.FileType,
		// 		})

		// 		So(dguta.store.sumMap[GUTAKey{GID, UID, DGUTAFileTypeText, DGUTAgeA1M}],
		// 			ShouldResemble, &summary.SummaryWithTimes{
		// 				summary.Summary{1, size64},
		// 				dguta.store.refTime,
		// 				amtime, amtime,
		// 			})
		// 		So(dguta.store.sumMap[GUTAKey{GID, UID, DGUTAFileTypeText, DGUTAgeA2M}],
		// 			ShouldResemble, &summary.SummaryWithTimes{
		// 				summary.Summary{1, size64},
		// 				dguta.store.refTime,
		// 				amtime, amtime,
		// 			})
		// 		So(dguta.store.sumMap[GUTAKey{GID, UID, DGUTAFileTypeText, DGUTAgeA6M}],
		// 			ShouldResemble, &summary.SummaryWithTimes{
		// 				summary.Summary{1, size64},
		// 				dguta.store.refTime,
		// 				amtime, amtime,
		// 			})
		// 		So(dguta.store.sumMap[GUTAKey{GID, UID, DGUTAFileTypeText, DGUTAgeA1Y}],
		// 			ShouldResemble, &summary.SummaryWithTimes{
		// 				summary.Summary{1, size64},
		// 				dguta.store.refTime,
		// 				amtime, amtime,
		// 			})
		// 		So(dguta.store.sumMap[GUTAKey{GID, UID, DGUTAFileTypeText, DGUTAgeA2Y}],
		// 			ShouldResemble, &summary.SummaryWithTimes{
		// 				summary.Summary{1, size64},
		// 				dguta.store.refTime,
		// 				amtime, amtime,
		// 			})
		// 		So(dguta.store.sumMap[GUTAKey{GID, UID, DGUTAFileTypeText, DGUTAgeA3Y}],
		// 			ShouldResemble, &summary.SummaryWithTimes{
		// 				summary.Summary{1, size64},
		// 				dguta.store.refTime,
		// 				amtime, amtime,
		// 			})
		// 		So(dguta.store.sumMap[GUTAKey{GID, UID, DGUTAFileTypeText, DGUTAgeA5Y}],
		// 			ShouldResemble, &summary.SummaryWithTimes{
		// 				summary.Summary{1, size64},
		// 				dguta.store.refTime,
		// 				amtime, amtime,
		// 			})
		// 		So(dguta.store.sumMap[GUTAKey{GID, UID, DGUTAFileTypeText, DGUTAgeA7Y}],
		// 			ShouldBeNil)
		// 	})
	})
}

func addTestData(a summary.Operation, cuid uint32) {
	paths := internaltest.NewDirectoryPathCreator()

	err := a.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/6.txt"), cuid, 2, 30, false))
	So(err, ShouldBeNil)
	err = a.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/c/1.txt"), cuid, 2, 10, false))
	So(err, ShouldBeNil)
	err = a.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/c/2.txt"), cuid, 2, 20, false))
	So(err, ShouldBeNil)
	err = a.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/c/3.txt"), 2, 2, 5, false))
	So(err, ShouldBeNil)
	err = a.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/c/4.txt"), 2, 3, 6, false))
	So(err, ShouldBeNil)
	err = a.Add(internaltest.NewMockInfo(paths.ToDirectoryPath("/a/b/c/5"), 2, 3, 1, true))
	So(err, ShouldBeNil)
}
