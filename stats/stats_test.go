// Copyright © 2024 Genome Research Limited
// Authors:
//  Sendu Bala <sb10@sanger.ac.uk>.
//  Dan Elia <de7@sanger.ac.uk>.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package stats

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParseStats(t *testing.T) {
	Convey("Given a parser and reader", t, func() {
		f, err := os.Open("test.stats.gz")
		So(err, ShouldBeNil)

		defer f.Close()

		gr, err := gzip.NewReader(f)
		So(err, ShouldBeNil)

		defer gr.Close()

		p := NewStatsParser(gr)
		So(p, ShouldNotBeNil)

		Convey("you can extract info for all entries", func() {
			i := 0
			for p.Scan() {
				if i == 0 {
					So(string(p.Path), ShouldEqual, "/lustre/scratch122/tol/teams/blaxter/users/am75/assemblies/dataset/ilXesSexs1.2_genomic.fna") //nolint:lll
					So(p.Size, ShouldEqual, 646315412)
					So(p.GID, ShouldEqual, 15078)
					So(p.MTime, ShouldEqual, 1698792671)
					So(p.CTime, ShouldEqual, 1698917473)
					So(p.EntryType, ShouldEqual, fileType)
				} else if i == 1 {
					So(string(p.Path), ShouldEqual, "/lustre/scratch122/tol/teams/blaxter/users/am75/assemblies/dataset/ilOpeBrum1.1_genomic.fna.fai") //nolint:lll
				}

				i++
			}
			So(i, ShouldEqual, 18890)

			So(p.Err(), ShouldBeNil)
		})
	})

	Convey("Scan generates Err() when", t, func() {
		Convey("there are not enough tab separated columns", func() {
			examplePath := `"/an/example/path"`

			p := NewStatsParser(strings.NewReader(examplePath + "\t1\t1\t1\t1\t1\t1\tf\t1\t1\td\n"))
			So(p.Scan(), ShouldBeTrue)
			So(p.Err(), ShouldBeNil)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\t1\t1\t1\t1\n"))
			So(p.Scan(), ShouldBeFalse)
			So(p.Err(), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\t1\t1\t1\n"))
			So(p.Scan(), ShouldBeFalse)
			So(p.Err(), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\t1\t1\n"))
			So(p.Scan(), ShouldBeFalse)
			So(p.Err(), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\t1\n"))
			So(p.Scan(), ShouldBeFalse)
			So(p.Err(), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\n"))
			So(p.Scan(), ShouldBeFalse)
			So(p.Err(), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\n"))
			So(p.Scan(), ShouldBeFalse)
			So(p.Err(), ShouldEqual, ErrTooFewColumns)

			Convey("but not for blank lines", func() {
				p = NewStatsParser(strings.NewReader("\n"))
				So(p.Scan(), ShouldBeTrue)
				So(p.Err(), ShouldBeNil)

				p := NewStatsParser(strings.NewReader(""))
				So(p.Scan(), ShouldBeFalse)
				So(p.Err(), ShouldBeNil)
			})
		})
	})
}

func BenchmarkScanAndFileInfo(b *testing.B) {
	tempDir := b.TempDir()
	testStatsFile := filepath.Join(tempDir, "test.stats")

	f, gr := openTestFile(b)

	defer f.Close()
	defer gr.Close()

	outFile, err := os.Create(testStatsFile)
	if err != nil {
		b.Fatal(err)
	}

	_, err = io.Copy(outFile, gr)
	if err != nil {
		b.Fatal(err)
	}

	outFile.Close()

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()

		f, err := os.Open(testStatsFile)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()

		p := NewStatsParser(f)

		for p.Scan() {
			if p.Size == 0 {
				continue
			}
		}

		if p.scanner.Err() != nil {
			b.Logf("\nerr: %s\n", p.scanner.Err())

			break
		}

		f.Close()
	}
}

func openTestFile(b *testing.B) (io.ReadCloser, io.ReadCloser) {
	b.Helper()

	f, err := os.Open("test.stats.gz")
	if err != nil {
		b.Fatal(err)
	}

	gr, err := gzip.NewReader(f)
	if err != nil {
		b.Fatal(err)
	}

	return f, gr
}

func BenchmarkRawScanner(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()

		f, gr := openTestFile(b)

		b.StartTimer()

		scanner := bufio.NewScanner(gr)

		for scanner.Scan() {
		}

		gr.Close()
		f.Close()
	}
}

func BenchmarkRawScannerUncompressed(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()

		f, gr := openTestFile(b)

		b.StartTimer()

		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
		}

		gr.Close()
		f.Close()
	}
}
