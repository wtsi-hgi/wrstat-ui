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
	"encoding/hex"
	"fmt"
	"strings"
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

func directoryHeavyStats(directories int, refTime int64) string {
	var b strings.Builder

	writeDirectoryStatsRow(&b, "/nfs/t283_imaging/", 0, 0, refTime, refTime, 1, int64(directories+2))

	for n := range directories {
		writeDirectoryStatsRow(
			&b,
			fmt.Sprintf("/nfs/t283_imaging/dir%04d/", n),
			1000,
			1000,
			refTime,
			refTime-db.SecondsInAYear*8,
			int64(n+2),
			2,
		)
	}

	return b.String()
}

func writeDirectoryStatsRow(
	b *strings.Builder,
	path string,
	uid uint32,
	gid uint32,
	atime int64,
	mtime int64,
	inode int64,
	nlink int64,
) {
	fmt.Fprintf(
		b,
		"%q\t4096\t%d\t%d\t%d\t%d\t%d\td\t%d\t%d\t1\t4096\n",
		path,
		uid,
		gid,
		atime,
		mtime,
		mtime,
		inode,
		nlink,
	)
}

func summariseDirectoryHeavyStats(data string, refTime int64) (*countingDB, error) {
	s := summary.NewSummariser(stats.NewStatsParser(strings.NewReader(data)))
	sink := new(countingDB)

	s.AddDirectoryOperation(newDirGroupUserTypeAge(sink, refTime, refTime))

	return sink, s.Summarise()
}

func mustSummariseDirectoryHeavyStats(data string, refTime int64) {
	if _, err := summariseDirectoryHeavyStats(data, refTime); err != nil {
		panic(err)
	}
}

type countingDB struct {
	records uint64
	rows    uint64
}

func (m *countingDB) Add(dguta db.RecordDGUTA) error {
	m.records++
	m.rows += uint64(len(dguta.GUTAs))

	return nil
}

func BenchmarkDirGUTADirectoryHeavy(b *testing.B) {
	const directories = 100000

	refTime := int64(1779120209)
	data := directoryHeavyStats(directories, refTime)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		sink, err := summariseDirectoryHeavyStats(data, refTime)
		if err != nil {
			b.Fatal(err)
		}

		if sink.records != directories+3 {
			b.Fatalf("expected %d records, got %d", directories+3, sink.records)
		}
	}
}

type recordDGUTACatalogRow struct {
	dirID          uint32
	parentID       uint32
	subtreeEnd     uint32
	depth          uint16
	childFileCount uint64
}

type recordDGUTACaptureDB struct {
	records []db.RecordDGUTA
}

func (m *recordDGUTACaptureDB) Add(dguta db.RecordDGUTA) error {
	m.records = append(m.records, dguta)

	return nil
}

func (m *recordDGUTACaptureDB) record(path string) (db.RecordDGUTA, bool) {
	for _, record := range m.records {
		if string(record.Dir.AppendTo(nil)) == path {
			return record, true
		}
	}

	return db.RecordDGUTA{}, false
}

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

	for _, dguta := range dgutas {
		got := *dguta

		got.ATimeRanges = summary.AgeBuckets{}
		got.MTimeRanges = summary.AgeBuckets{}

		if got == expected {
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

	Convey("NewDirGroupUserTypeAgeAt uses the supplied time for directory access times", t, func() {
		referenceTime := time.Date(2026, 6, 12, 10, 30, 15, 0, time.UTC)
		paths := internaltest.NewDirectoryPathCreator()
		m := &mockDB{make(map[string]db.GUTAs)}
		op := NewDirGroupUserTypeAgeAt(m, referenceTime)()
		info := &summary.FileInfo{
			Path:      paths.ToDirectoryPath("/a/"),
			Name:      strToBS("a/"),
			Size:      4096,
			UID:       uid,
			GID:       gid,
			MTime:     referenceTime.Add(-2 * time.Hour).Unix(),
			ATime:     referenceTime.Add(-7 * 24 * time.Hour).Unix(),
			EntryType: stats.DirType,
		}

		So(op.Add(info), ShouldBeNil)
		So(op.Output(), ShouldBeNil)
		So(m.has(
			"/a/",
			gid,
			uid,
			db.DGUTAFileTypeDir,
			db.DGUTAgeAll,
			1,
			4096,
			referenceTime.Unix(),
			info.MTime,
		), ShouldBeTrue)
	})

	Convey("B3 carries catalog ids on each RecordDGUTA", t, func() {
		const b3RefTime = int64(1779120209)

		root := statsdata.NewRoot("/", b3RefTime)
		statsdata.AddFile(root, "catalog/team/branch-a/deeper/file.txt", 10, 20, 1, b3RefTime, b3RefTime)
		statsdata.AddFile(root, "catalog/team/branch-b/leaf/file.txt", 10, 20, 1, b3RefTime, b3RefTime)
		statsdata.AddFile(root, "catalog/team/top-level.txt", 10, 20, 1, b3RefTime, b3RefTime)

		reader := root.AsReader()
		defer reader.Close()

		s := summary.NewSummariser(stats.NewStatsParser(reader))
		sink := new(recordDGUTACaptureDB)
		s.AddDirectoryOperation(newDirGroupUserTypeAge(sink, b3RefTime, b3RefTime))

		err := s.Summarise()
		So(err, ShouldBeNil)

		catalogRows := map[string]recordDGUTACatalogRow{
			"/":                              {dirID: 0, parentID: parentSentinel, subtreeEnd: 7, depth: 0},
			"/catalog/":                      {dirID: 1, parentID: 0, subtreeEnd: 7, depth: 1},
			"/catalog/team/":                 {dirID: 2, parentID: 1, subtreeEnd: 7, depth: 2, childFileCount: 1},
			"/catalog/team/branch-a/":        {dirID: 3, parentID: 2, subtreeEnd: 5, depth: 3},
			"/catalog/team/branch-a/deeper/": {dirID: 4, parentID: 3, subtreeEnd: 5, depth: 4, childFileCount: 1},
			"/catalog/team/branch-b/":        {dirID: 5, parentID: 2, subtreeEnd: 7, depth: 3},
			"/catalog/team/branch-b/leaf/":   {dirID: 6, parentID: 5, subtreeEnd: 7, depth: 4, childFileCount: 1},
		}

		So(len(sink.records), ShouldEqual, len(catalogRows))

		seen := make(map[string]struct{}, len(sink.records))

		for _, record := range sink.records {
			fullPath := string(record.Dir.AppendTo(nil))
			expected, ok := catalogRows[fullPath]
			So(ok, ShouldBeTrue)
			So(recordDGUTACatalogRow{
				dirID:          record.DirID,
				parentID:       record.ParentID,
				subtreeEnd:     record.SubtreeEnd,
				depth:          record.Depth,
				childFileCount: record.ChildFileCount,
			}, ShouldResemble, expected)

			seen[fullPath] = struct{}{}
		}

		So(len(seen), ShouldEqual, len(catalogRows))
	})

	Convey("DirGUTA emits stable bytes for regular, temp, and hardlink files", t, func() {
		const (
			goldenRefTime = int64(1779120209)
			goldenUID     = uint32(101)
			goldenGID     = uint32(202)
		)

		f := statsdata.NewRoot("/", goldenRefTime)
		f.UID = goldenUID
		f.GID = goldenGID

		statsdata.AddFileWithInode(
			f,
			"one/regular.txt",
			goldenUID,
			goldenGID,
			11,
			goldenRefTime-10,
			goldenRefTime-5,
			1001,
			1,
		)
		statsdata.AddFileWithInode(
			f,
			"one/scratch.tmp",
			goldenUID,
			goldenGID,
			13,
			goldenRefTime-20,
			goldenRefTime-15,
			1002,
			1,
		)
		statsdata.AddFileWithInode(
			f,
			"one/hard-a.bam",
			goldenUID,
			goldenGID,
			17,
			goldenRefTime-30,
			goldenRefTime-25,
			1003,
			2,
		)
		statsdata.AddFileWithInode(
			f,
			"one/hard-b.bam",
			goldenUID,
			goldenGID,
			17,
			goldenRefTime-30,
			goldenRefTime-25,
			1003,
			2,
		)

		reader := f.AsReader()
		defer reader.Close()

		s := summary.NewSummariser(stats.NewStatsParser(reader))
		sink := new(recordDGUTACaptureDB)
		s.AddDirectoryOperation(newDirGroupUserTypeAge(sink, goldenRefTime, goldenRefTime))

		err := s.Summarise()
		So(err, ShouldBeNil)

		record, ok := sink.record("/one/")
		So(ok, ShouldBeTrue)
		So(record.ChildFileCount, ShouldEqual, 4)

		_, encodedGUTAs := record.EncodeToBytes()
		So(
			hex.EncodeToString(encodedGUTAs),
			ShouldEqual,
			"04ca00652000000111e6dfd89f0cf0dfd89f0c000000000000000001000000000000000001"+
				"ca0065001000010b8ee0d89f0c98e0d89f0c000000000000000001000000000000000001"+
				"ca006500400001801fa2e0d89f0ca2e0d89f0c000000000000000001000000000000000001"+
				"ca0065018000010dfadfd89f0c84e0d89f0c000000000000000001000000000000000001",
		)
	})

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
		op := newDirGroupUserTypeAge(m, refTime, refTime)
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
		op := newDirGroupUserTypeAge(m, refTime, refTime)
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
		So(m.hasNot("/", 10, 2, db.DGUTAFileTypeCram, db.DGUTAgeAll), ShouldBeTrue)
		So(m.hasNot("/a/", 10, 2, db.DGUTAFileTypeCram, db.DGUTAgeAll), ShouldBeTrue)
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
		statsdata.AddFileWithInode(f, "a/b/c/2.bam", uid, gid, 100, atimeRecent, mtimeRecent, 42, 4)
		statsdata.AddFileWithInode(f, "a/b/c/3.bam", uid, gid, 200, atimeOld, mtimeOld, 43, 1)
		statsdata.AddFileWithInode(f, "a/b/c/4.cram", uid, gid, 100, atimeOld, mtimeOld, 42, 4)
		statsdata.AddFileWithInode(f, "a/x/4.bam", uid, gid, 100, atimeOld, mtimeOld, 42, 4)

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{make(map[string]db.GUTAs)}
		op := newDirGroupUserTypeAge(m, refTime, refTime)
		s.AddDirectoryOperation(op)

		err := s.Summarise()
		So(err, ShouldBeNil)

		ft := db.DGUTAFileTypeBam
		count := uint64(1)
		size := uint64(200)
		So(m.has("/a/b/c/", gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeOld), ShouldBeTrue)

		ft = db.DGUTAFileTypeBam | db.DGUTAFileTypeCram
		count = uint64(1)
		size = uint64(100)
		So(m.has("/a/b/c/", gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeRecent), ShouldBeTrue)

		ft = db.DGUTAFileTypeBam
		count = uint64(1)
		size = uint64(100)
		So(m.has("/a/x/", gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeOld), ShouldBeTrue)

		ft = db.DGUTAFileTypeBam | db.DGUTAFileTypeCram
		count = uint64(1)
		size = uint64(100)
		So(m.has("/a/", gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeRecent), ShouldBeTrue)

		statsdata.AddFileWithInode(f, "a/b/d/1.bam", uid, gid, 150, atimeRecent, mtimeRecent, 44, 2)
		statsdata.AddFileWithInode(f, "a/b/d/2.bam", uid, gid, 100, atimeOld, mtimeOld, 44, 2)

		s2 := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		op2 := newDirGroupUserTypeAge(m, refTime, refTime)
		s2.AddDirectoryOperation(op2)
		err = s2.Summarise()
		So(err, ShouldBeNil)

		ft = db.DGUTAFileTypeBam
		count = 1
		size = 150
		So(m.has("/a/b/d/", gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeRecent), ShouldBeTrue)

		statsdata.AddFileWithInode(f, "a/b/c/5.bam", uid, gid, 100, atimeRecent, mtimeRecent, 50, 3)
		statsdata.AddFileWithInode(f, "a/b/d/5.bam", uid, gid, 200, atimeOld, mtimeOld, 50, 3)
		statsdata.AddFileWithInode(f, "a/x/5.bam", uid, gid, 150, atimeRecent, mtimeOld, 50, 3)

		s3 := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		op3 := newDirGroupUserTypeAge(m, refTime, refTime)
		s3.AddDirectoryOperation(op3)
		err = s3.Summarise()
		So(err, ShouldBeNil)

		ft = db.DGUTAFileTypeBam
		count = 3
		size = 550
		So(m.has("/a/", gid, uid, ft, db.DGUTAgeAll, count, size, atimeOld, mtimeRecent), ShouldBeTrue)

		count = 1
		size = 200
		So(m.has("/a/b/c/", gid, uid, ft, db.DGUTAgeA2Y, count, size, atimeOld, mtimeOld), ShouldBeTrue)

		count = 3
		size = 550
		So(m.has("/a/b/", gid, uid, ft, db.DGUTAgeA2Y, count, size, atimeOld, mtimeRecent), ShouldBeTrue)
	})

	Convey("DirGUTA hardlink merging across nested directories", t, func() {
		uid, gid := uint32(1312), uint32(22762)

		refTime := time.Now().Unix()
		atimeRecent := refTime - db.SecondsInAMonth
		mtimeRecent := refTime - db.SecondsInAMonth
		atimeOld := refTime - (db.SecondsInAYear * 3)
		mtimeOld := refTime - (db.SecondsInAYear * 2)

		f := statsdata.NewRoot("/", 0)
		f.UID = uid
		f.GID = gid

		statsdata.AddFileWithInode(f, "a/b/c/x/50.bam", uid, gid, 100, atimeRecent, mtimeRecent, 50, 3)

		statsdata.AddFileWithInode(f, "a/b/c/y/50.bam", uid, gid, 200, atimeOld, mtimeOld, 50, 3)

		statsdata.AddFileWithInode(f, "a/b/z/50.bam", uid, gid, 150, atimeRecent, mtimeOld, 50, 3)

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{make(map[string]db.GUTAs)}
		op := newDirGroupUserTypeAge(m, refTime, refTime)
		s.AddDirectoryOperation(op)

		err := s.Summarise()
		So(err, ShouldBeNil)

		ft := db.DGUTAFileTypeBam

		Convey("Leaf directories should each have count=1 with their actual size", func() {
			So(m.has("/a/b/c/x/", gid, uid, ft, db.DGUTAgeAll, 1, 100, atimeRecent, mtimeRecent), ShouldBeTrue)
			So(m.has("/a/b/c/y/", gid, uid, ft, db.DGUTAgeAll, 1, 200, atimeOld, mtimeOld), ShouldBeTrue)
			So(m.has("/a/b/z/", gid, uid, ft, db.DGUTAgeAll, 1, 150, atimeRecent, mtimeOld), ShouldBeTrue)
		})

		Convey("Intermediate directory /a/b/c/ merges x/ and y/ correctly", func() {
			So(m.has("/a/b/c/", gid, uid, ft, db.DGUTAgeAll, 1, 200, atimeOld, mtimeRecent), ShouldBeTrue)
		})

		Convey("Directory /a/b/ merges c/ and z/", func() {
			So(m.has("/a/b/", gid, uid, ft, db.DGUTAgeAll, 1, 200, atimeOld, mtimeRecent), ShouldBeTrue)
		})

		Convey("Top-level /a/ merges all children correctly", func() {
			So(m.has("/a/", gid, uid, ft, db.DGUTAgeAll, 1, 200, atimeOld, mtimeRecent), ShouldBeTrue)
		})
	})

	Convey("DirGUTA handles hardlinks and filetypes with different sizes", t, func() {
		uid, gid := uint32(1312), uint32(22762)

		refTime := time.Now().Unix()
		atimeOld := refTime - (db.SecondsInAYear * 3)
		mtimeOld := refTime - (db.SecondsInAYear * 2)

		f := statsdata.NewRoot("/", 0)
		f.UID = uid
		f.GID = gid

		statsdata.AddFileWithInode(f, "a/x/4.bam", uid, gid, 102400, atimeOld, mtimeOld, 162130124446105637, 4)
		statsdata.AddFileWithInode(f, "a/x/5.bam", uid, gid, 153600, atimeOld, mtimeOld, 162130124446105643, 1)
		statsdata.AddFileWithInode(f, "a/b/d/1.bam", uid, gid, 153600, atimeOld, mtimeOld, 162130124446105639, 2)
		statsdata.AddFileWithInode(f, "a/b/d/5.bam", uid, gid, 204800, atimeOld, mtimeOld, 162130124446105642, 3)
		statsdata.AddFileWithInode(f, "a/b/d/2.bam", uid, gid, 102400, atimeOld, mtimeOld, 162130124446105640, 2)
		statsdata.AddFileWithInode(f, "a/b/z/50.bam", uid, gid, 153600, atimeOld, mtimeOld, 162130124446105646, 3)
		statsdata.AddFileWithInode(f, "a/b/c/y/50.bam", uid, gid, 204800, atimeOld, mtimeOld, 162130124446105645, 3)
		statsdata.AddFileWithInode(f, "a/b/c/3.bam", uid, gid, 204800, atimeOld, mtimeOld, 162130124446105638, 1)
		statsdata.AddFileWithInode(f, "a/b/c/1.bam", uid, gid, 102400, atimeOld, mtimeOld, 162130124446105637, 4)
		statsdata.AddFileWithInode(f, "a/b/c/5.bam", uid, gid, 102400, atimeOld, mtimeOld, 162130124446105641, 2)
		statsdata.AddFileWithInode(f, "a/b/c/4.cram", uid, gid, 102400, atimeOld, mtimeOld, 162130124446105637, 4)
		statsdata.AddFileWithInode(f, "a/b/c/x/50.bam", uid, gid, 102400, atimeOld, mtimeOld, 162130124446105644, 1)
		statsdata.AddFileWithInode(f, "a/b/c/2.bam", uid, gid, 102400, atimeOld, mtimeOld, 162130124446105637, 4)

		s := summary.NewSummariser(stats.NewStatsParser(f.AsReader()))
		m := &mockDB{make(map[string]db.GUTAs)}
		op := newDirGroupUserTypeAge(m, refTime, refTime)
		s.AddDirectoryOperation(op)

		err := s.Summarise()
		So(err, ShouldBeNil)

		ft := db.DGUTAFileTypeBam | db.DGUTAFileTypeCram
		So(m.has("/a/b/c/", gid, uid, ft, db.DGUTAgeAll, 1, 100*1024, atimeOld, mtimeOld), ShouldBeTrue)
		So(m.has("/a/b/c/", gid, uid, db.DGUTAFileTypeBam, db.DGUTAgeAll, 4, 614400, atimeOld, mtimeOld), ShouldBeTrue)
		So(m.has("/a/b/d/", gid, uid, db.DGUTAFileTypeBam, db.DGUTAgeAll, 3, 460800, atimeOld, mtimeOld), ShouldBeTrue)
		So(m.has("/a/b/z/", gid, uid, db.DGUTAFileTypeBam, db.DGUTAgeAll, 1, 153600, atimeOld, mtimeOld), ShouldBeTrue)
		So(m.has("/a/x/", gid, uid, db.DGUTAFileTypeBam, db.DGUTAgeAll, 2, 256000, atimeOld, mtimeOld), ShouldBeTrue)

		So(m.has("/a/b/", gid, uid, ft, db.DGUTAgeAll, 1, 102400, atimeOld, mtimeOld), ShouldBeTrue)
		So(m.has("/a/", gid, uid, ft, db.DGUTAgeAll, 1, 102400, atimeOld, mtimeOld), ShouldBeTrue)
	})

	Convey("DirGUTA only tracks non-directory hardlinks", t, func() {
		refTime := time.Now().Unix()
		paths := internaltest.NewDirectoryPathCreator()
		op, ok := newDirGroupUserTypeAge(
			&mockDB{make(map[string]db.GUTAs)},
			refTime,
			refTime,
		)().(*DirGroupUserTypeAge)
		So(ok, ShouldBeTrue)

		dirInfo := &summary.FileInfo{
			Path:      paths.ToDirectoryPath("/a/"),
			Name:      strToBS("a"),
			Size:      4096,
			UID:       uid,
			GID:       gid,
			MTime:     refTime - db.SecondsInAMonth,
			ATime:     refTime - db.SecondsInAYear,
			Inode:     1001,
			Nlink:     3,
			EntryType: stats.DirType,
		}

		handled := op.handleHardlink(dirInfo, db.DGUTAFileTypeOther, refTime)
		So(handled, ShouldBeFalse)
		So(len(op.seenHardlinks), ShouldEqual, 0)

		fileInfo := &summary.FileInfo{
			Path:      paths.ToDirectoryPath("/a/file.txt"),
			Name:      strToBS("file.txt"),
			Size:      100,
			UID:       uid,
			GID:       gid,
			MTime:     refTime - db.SecondsInAMonth,
			ATime:     refTime - db.SecondsInAYear,
			Inode:     2002,
			Nlink:     2,
			EntryType: stats.FileType,
		}

		handled = op.handleHardlink(fileInfo, db.DGUTAFileTypeOther, fileInfo.ATime)
		So(handled, ShouldBeTrue)
		So(len(op.seenHardlinks), ShouldEqual, 1)
		_, exists := op.seenHardlinks[fileInfo.Inode]
		So(exists, ShouldBeTrue)
	})

	Convey("DirGUTA keeps transient summary allocations bounded for directory-heavy stats", t, func() {
		const directories = 1000

		refTime := time.Now().Unix()
		data := directoryHeavyStats(directories, refTime)

		sink, err := summariseDirectoryHeavyStats(data, refTime)
		So(err, ShouldBeNil)
		So(sink.records, ShouldEqual, directories+3)
		So(sink.rows, ShouldBeGreaterThan, directories)

		allocs := testing.AllocsPerRun(3, func() {
			mustSummariseDirectoryHeavyStats(data, refTime)
		})

		So(allocs, ShouldBeLessThan, 30000.0)
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
