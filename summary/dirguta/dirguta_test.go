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
	"fmt"
	"testing"
	"time"
	"unsafe"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	internaltest "github.com/wtsi-hgi/wrstat-ui/internal/test"
	internaluser "github.com/wtsi-hgi/wrstat-ui/internal/user"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

func TestDirGUTAFileType(t *testing.T) {
	Convey("isTemp lets you know if a path is a temporary file", t, func() {
		So(IsTemp(strToBS(".tmp.cram")), ShouldBeTrue)
		So(IsTemp(strToBS("tmp.cram")), ShouldBeTrue)
		So(IsTemp(strToBS("xtmp.cram")), ShouldBeFalse)
		So(IsTemp(strToBS("tmpx.cram")), ShouldBeFalse)

		So(IsTemp(strToBS(".temp.cram")), ShouldBeTrue)
		So(IsTemp(strToBS("temp.cram")), ShouldBeTrue)
		So(IsTemp(strToBS("xtemp.cram")), ShouldBeFalse)
		So(IsTemp(strToBS("tempx.cram")), ShouldBeFalse)

		So(IsTemp(strToBS("a.cram.tmp")), ShouldBeTrue)
		So(IsTemp(strToBS("xtmp")), ShouldBeFalse)
		So(IsTemp(strToBS("a.cram.temp")), ShouldBeTrue)
		So(IsTemp(strToBS("xtemp")), ShouldBeFalse)

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

	Convey("FilenameToType lets you know the filetypes of a file", t, func() {
		for _, test := range [...]struct {
			Name     string
			IsDir    bool
			FileType db.DirGUTAFileType
			IsTmp    bool
		}{
			{"path/", true, db.DGUTAFileTypeDir, false},
			{"bar.asd", false, db.DGUTAFileTypeOther, false},
			{".tmp.asd", false, db.DGUTAFileTypeTemp | db.DGUTAFileTypeOther, false},
			{"bar.vcf", false, db.DGUTAFileTypeVCF, false},
			{"bar.vcf.gz", false, db.DGUTAFileTypeVCFGz, false},
			{"bar.bcf", false, db.DGUTAFileTypeBCF, false},
			{"bar.sam", false, db.DGUTAFileTypeSam, false},
			{"bar.bam", false, db.DGUTAFileTypeBam, false},
			{".tmp.cram", false, db.DGUTAFileTypeCram | db.DGUTAFileTypeTemp, false},
			{"bar.fa", false, db.DGUTAFileTypeFasta | db.DGUTAFileTypeTemp, true},
			{"bar.fq", false, db.DGUTAFileTypeFastq, false},
			{"bar.fq.gz", false, db.DGUTAFileTypeFastqGz, false},
			{"bar.bzip2", false, db.DGUTAFileTypeCompressed, false},
			{"bar.csv", false, db.DGUTAFileTypeText, false},
			{"bar.o", false, db.DGUTAFileTypeLog, false},
			{".tmp", false, db.DGUTAFileTypeTemp | db.DGUTAFileTypeOther, true},
			{".tmp", false, db.DGUTAFileTypeTemp | db.DGUTAFileTypeOther, false},
		} {
			ft := FileTypeWithTemp(strToBS(test.Name), test.IsTmp)
			So(ft, ShouldEqual, test.FileType)
		}
	})
}

type mockDB struct {
	gutas map[string]db.GUTAs
}

func (m *mockDB) Add(dguta db.RecordDGUTA) error {
	m.gutas[string(dguta.Dir.AppendTo(nil))] = dguta.GUTAs

	return nil
}

func (m *mockDB) has(dir string, gid, uid uint32, ft db.DirGUTAFileType,
	age db.DirGUTAge, count, size uint64, atime, mtime int64) bool {
	dgutas, ok := m.gutas[dir]
	if !ok {
		return false
	}

	expected := db.GUTA{
		GID:   gid,
		UID:   uid,
		FT:    ft,
		Age:   age,
		Count: count,
		Size:  size,
		Atime: atime,
		Mtime: mtime,
	}
	fmt.Println("expected:", expected)
	for _, dguta := range dgutas {
		fmt.Println("dguta:", dguta)

		if *dguta == expected {
			return true
		}
	}

	return false
}

func (m *mockDB) hasNot(dir string, gid, uid uint32, ft db.DirGUTAFileType, age db.DirGUTAge) bool { //nolint:unparam
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

		atime1 := refTime - (db.SecondsInAMonth*2 + 100000)
		mtime1 := refTime - (db.SecondsInAMonth * 3)
		statsdata.AddFileWithInode(f, "a/b/c/1.bam", uid, gid, 2, atime1, mtime1, 1, 1)

		atime2 := refTime - (db.SecondsInAMonth * 7)
		mtime2 := refTime - (db.SecondsInAMonth * 8)
		statsdata.AddFileWithInode(f, "a/b/c/2.bam", uid, gid, 3, atime2, mtime2, 2, 1)

		atime3 := refTime - (db.SecondsInAYear + db.SecondsInAMonth)
		mtime3 := refTime - (db.SecondsInAYear + db.SecondsInAMonth*6)
		statsdata.AddFileWithInode(f, "a/b/c/3.txt", uid, gid, 4, atime3, mtime3, 3, 1)

		atime4 := refTime - (db.SecondsInAYear * 4)
		mtime4 := refTime - (db.SecondsInAYear * 6)
		statsdata.AddFileWithInode(f, "a/b/c/4.bam", uid, gid, 5, atime4, mtime4, 4, 1)

		atime5 := refTime - (db.SecondsInAYear*5 + db.SecondsInAMonth)
		mtime5 := refTime - (db.SecondsInAYear*7 + db.SecondsInAMonth)
		statsdata.AddFileWithInode(f, "a/b/c/5.cram", uid, gid, 6, atime5, mtime5, 5, 1)

		atime6 := refTime - (db.SecondsInAYear*7 + db.SecondsInAMonth)
		mtime6 := refTime - (db.SecondsInAYear*7 + db.SecondsInAMonth)
		statsdata.AddFileWithInode(f, "a/b/c/6.cram", uid, gid, 7, atime6, mtime6, 6, 1)

		statsdata.AddFileWithInode(f, "a/b/c/6.tmp", uid, gid, 8, mtime3, mtime3, 7, 1)

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{make(map[string]db.GUTAs)}
		op := newDirGroupUserTypeAge(m, refTime)
		s.AddDirectoryOperation(op)

		err := s.Summarise()
		So(err, ShouldBeNil)

		dir := "/a/b/c/"
		ft, count, size := db.DGUTAFileTypeBam, uint64(3), uint64(10)
		testAtime, testMtime := atime4, mtime1

		So(m.has(dir, gid, uid, ft, db.DGUTAgeAll, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA6M, count-1, size-2, testAtime, mtime2), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA1Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA2Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA3Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA7Y), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM6M, count-1, size-2, testAtime, mtime2), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM1Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM2Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM3Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM5Y, count-2, size-5, testAtime, mtime4), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeM7Y), ShouldBeTrue)

		ft, count, size = db.DGUTAFileTypeCram, 2, 13
		testAtime, testMtime = atime6, mtime5

		So(m.has(dir, gid, uid, ft, db.DGUTAgeAll, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA2Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA3Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA5Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA7Y, count-1, size-6, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM2Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM3Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM5Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM7Y, count, size, testAtime, testMtime), ShouldBeTrue)

		ft, count, size = db.DGUTAFileTypeText, 1, 4
		testAtime, testMtime = atime3, mtime3

		So(m.has(dir, gid, uid, ft, db.DGUTAgeAll, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA2Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA3Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA7Y), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeM2Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeM3Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeM5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeM7Y), ShouldBeTrue)

		ft, count, size = db.DGUTAFileTypeTemp|db.DGUTAFileTypeOther, 1, 8
		testAtime, testMtime = mtime3, mtime3

		So(m.has(dir, gid, uid, ft, db.DGUTAgeAll, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeA1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA2Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA3Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeA7Y), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM1M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM2M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM6M, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.has(dir, gid, uid, ft, db.DGUTAgeM1Y, count, size, testAtime, testMtime), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeM2Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeM3Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeM5Y), ShouldBeTrue)
		So(m.hasNot(dir, gid, uid, ft, db.DGUTAgeM7Y), ShouldBeTrue)
	})

	Convey("You can summarise data with different groups and users", t, func() {
		f := statsdata.NewRoot("/a/b/", 0)

		atime1 := int64(100)
		mtime1 := int64(0)
		statsdata.AddFileWithInode(f, "c/3.bam", 2, 2, 1, atime1, mtime1, 11, 1)

		atime2 := int64(250)
		mtime2 := int64(250)
		statsdata.AddFileWithInode(f, "c/7.cram", 10, 2, 2, atime2, mtime2, 12, 1)

		atime3 := int64(201)
		mtime3 := int64(200)
		statsdata.AddFileWithInode(f, "c/d/9.cram", 10, 2, 3, atime3, mtime3, 13, 1)

		atime4 := int64(300)
		mtime4 := int64(301)
		statsdata.AddFileWithInode(f, "c/8.cram", 2, 10, 4, atime4, mtime4, 14, 1)

		dDir := f.AddDirectory("c").AddDirectory("d")
		dDir.UID = 10
		dDir.GID = 2
		dDir.ATime = 50
		dDir.Size = 8192

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{make(map[string]db.GUTAs)}
		op := newDirGroupUserTypeAge(m, refTime)
		s.AddDirectoryOperation(op)

		err := s.Summarise()
		So(err, ShouldBeNil)

		for _, age := range db.DirGUTAges {
			So(m.has("/a/b/c/d/", 2, 10, db.DGUTAFileTypeCram, age, 1, 3, atime3, mtime3), ShouldBeTrue)
		}

		So(m.has("/a/b/c/", 2, 2, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 1, atime1, mtime1), ShouldBeTrue)
		So(m.hasNot("/a/b/c/", 2, 2, db.DGUTAFileTypeCram, db.DGUTAgeAll), ShouldBeTrue)
		So(m.has("/a/b/c/", 2, 10, db.DGUTAFileTypeCram, db.DGUTAgeAll, 2, 5, atime3, mtime2), ShouldBeTrue)
		So(m.has("/a/b/c/", 10, 2, db.DGUTAFileTypeCram, db.DGUTAgeAll, 1, 4, atime4, mtime4), ShouldBeTrue)
		So(m.has("/", 10, 2, db.DGUTAFileTypeCram, db.DGUTAgeAll, 1, 4, atime4, mtime4), ShouldBeTrue)
		So(m.has("/a/", 10, 2, db.DGUTAFileTypeCram, db.DGUTAgeAll, 1, 4, atime4, mtime4), ShouldBeTrue)
		So(m.has("/a/b/", 10, 2, db.DGUTAFileTypeCram, db.DGUTAgeAll, 1, 4, atime4, mtime4), ShouldBeTrue)
	})

	Convey("DirGUTA correctly handles hardlinks and filetypes", t, func() {
		f := statsdata.NewRoot("/", 0)
		f.UID = uid
		f.GID = gid

		refTime := time.Now().Unix()
		atimeRecent := refTime - db.SecondsInAMonth
		mtimeRecent := refTime - db.SecondsInAMonth
		atimeOld := refTime - (db.SecondsInAYear * 3)
		mtimeOld := refTime - (db.SecondsInAYear * 2)

		statsdata.AddFileWithInode(f, "a/b/c/1.bam", uid, gid, 100, atimeRecent, mtimeRecent, 42, 4)
		statsdata.AddFileWithInode(f, "a/b/c/2.bam", uid, gid, 100, atimeRecent, mtimeRecent, 42, 4) // same inode & same file type -> skipped
		statsdata.AddFileWithInode(f, "a/b/c/3.bam", uid, gid, 200, atimeOld, mtimeOld, 43, 1)       // different inode -> counted
		statsdata.AddFileWithInode(f, "a/b/c/4.cram", uid, gid, 100, atimeOld, mtimeOld, 42, 4)      // same inode, different type -> counted
		statsdata.AddFileWithInode(f, "a/x/4.bam", uid, gid, 100, atimeOld, mtimeOld, 42, 4)         // same inode & same file type -> skipped

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{make(map[string]db.GUTAs)}
		op := newDirGroupUserTypeAge(m, refTime)
		s.AddDirectoryOperation(op)

		// io.Copy(os.Stdout, f.AsReader())

		err := s.Summarise()
		So(err, ShouldBeNil)

		dirC := "/a/b/c/"
		ft := db.DGUTAFileTypeBam
		count := uint64(2)
		size := uint64(300)
		So(m.has(dirC, gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeRecent), ShouldBeTrue)

		ft = db.DGUTAFileTypeCram
		count = uint64(1)
		size = uint64(100)
		So(m.has(dirC, gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeOld), ShouldBeTrue)

		dirX := "/a/x/"
		ft = db.DGUTAFileTypeBam
		count = uint64(1)
		size = uint64(100)
		So(m.has(dirX, gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeOld), ShouldBeTrue)

		dirA := "/a/"
		ft = db.DGUTAFileTypeBam | db.DGUTAFileTypeCram
		count = uint64(1)
		size = uint64(100)
		So(m.has(dirA, gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeRecent), ShouldBeTrue)

		// Same inode, different size , atime, mtime
		statsdata.AddFileWithInode(f, "a/b/d/1.bam", uid, gid, 150, atimeRecent, mtimeRecent, 44, 2)
		statsdata.AddFileWithInode(f, "a/b/d/2.bam", uid, gid, 100, atimeOld, mtimeOld, 44, 2)

		s2 := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		op2 := newDirGroupUserTypeAge(m, refTime)
		s2.AddDirectoryOperation(op2)
		err = s2.Summarise()
		So(err, ShouldBeNil)

		ft = db.DGUTAFileTypeBam
		count = 1
		size = 150 // max(150 & 100)
		// So(m.has("/a/b/d/", gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeRecent), ShouldBeTrue)

		// Hardlink inode 50 appears in 3 different directories
		statsdata.AddFileWithInode(f, "a/b/c/5.bam", uid, gid, 100, atimeRecent, mtimeRecent, 50, 3)
		statsdata.AddFileWithInode(f, "a/b/d/5.bam", uid, gid, 200, atimeOld, mtimeOld, 50, 3)
		statsdata.AddFileWithInode(f, "a/x/5.bam", uid, gid, 150, atimeRecent, mtimeOld, 50, 3)

		s3 := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		op3 := newDirGroupUserTypeAge(m, refTime)
		s3.AddDirectoryOperation(op3)
		err = s3.Summarise()
		So(err, ShouldBeNil)

		// Parent /a/ should merge all 3 contributions of inode 50
		ft = db.DGUTAFileTypeBam
		count = 1
		size = 200 // max size
		So(m.has("/a/", gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeRecent), ShouldBeTrue)

	})

}

func isVCF(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeVCF
}

func isVCFGz(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeVCFGz
}

func isBCF(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeBCF
}

func isSam(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeSam
}

func isBam(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeBam
}

func isCram(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeCram
}

func isFasta(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeFasta
}

func isFastq(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeFastq
}

func isFastqGz(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeFastqGz
}

func isPedBed(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypePedBed
}

func isCompressed(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeCompressed
}

func isText(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeText
}

func isLog(name string) bool {
	return FilenameToType(strToBS(name)) == db.DGUTAFileTypeLog
}

func strToBS(str string) []byte {
	return unsafe.Slice(unsafe.StringData(str), len(str))
}

func isTempDir(dir *summary.DirectoryPath) bool {
	for n := dir; n != nil; n = n.Parent {
		if IsTemp(strToBS(n.Name)) {
			return true
		}
	}

	return false
}
