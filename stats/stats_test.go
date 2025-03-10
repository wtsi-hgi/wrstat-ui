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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
)

func TestParseStats(t *testing.T) {
	Convey("Given a parser and reader", t, func() {
		refTime := time.Now().Unix()

		f := statsdata.TestStats(5, 5, "/opt/", refTime).AsReader()

		var sb strings.Builder

		p := NewStatsParser(io.TeeReader(f, &sb))
		So(p, ShouldNotBeNil)

		Convey("you can extract info for all entries", func() {
			info := new(FileInfo)

			i := 0
			for p.Scan(info) == nil {
				if i == 0 {
					So(string(info.Path), ShouldEqual, "/opt/")
					So(info.Size, ShouldEqual, 4096)
					So(info.GID, ShouldEqual, 0)
					So(info.ATime, ShouldEqual, refTime)
					So(info.MTime, ShouldEqual, refTime)
					So(info.CTime, ShouldEqual, refTime)
					So(info.EntryType, ShouldEqual, DirType)
				} else if i == 1 {
					So(string(info.Path), ShouldEqual, "/opt/dir0/")
				}

				i++
			}

			numLines := strings.Count(sb.String(), "\n")

			So(i, ShouldEqual, numLines)

			So(p.Err(), ShouldBeNil)
		})
	})

	Convey("Scan generates Err() when", t, func() {
		Convey("there are not enough tab separated columns", func() {
			examplePath := `"/an/example/path"`
			info := new(FileInfo)

			p := NewStatsParser(strings.NewReader(examplePath + "\t1\t1\t1\t1\t1\t1\tf\t1\t1\td\t1\n"))
			So(p.Scan(info), ShouldBeNil)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\t1\t1\t1\t1\n"))
			So(p.Scan(info), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\t1\t1\t1\n"))
			So(p.Scan(info), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\t1\t1\n"))
			So(p.Scan(info), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\t1\n"))
			So(p.Scan(info), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\t1\n"))
			So(p.Scan(info), ShouldEqual, ErrTooFewColumns)

			p = NewStatsParser(strings.NewReader(examplePath + "\n"))
			So(p.Scan(info), ShouldEqual, ErrTooFewColumns)

			Convey("but not for blank lines", func() {
				p = NewStatsParser(strings.NewReader("\n"))
				So(p.Scan(info), ShouldBeNil)

				p := NewStatsParser(strings.NewReader(""))
				So(p.Scan(info), ShouldEqual, io.EOF)
			})
		})
	})
}

func TestUnquote(t *testing.T) {
	for n, test := range [...][2][]byte{
		{
			[]byte(`""`),
			[]byte(``),
		},
		{
			[]byte(`"abc"`),
			[]byte(`abc`),
		},
		{
			[]byte(`"\""`),
			[]byte(`"`),
		},
		{
			[]byte(`"\""`),
			[]byte(`"`),
		},
		{
			[]byte(`"\'"`),
			[]byte(`'`),
		},
		{
			[]byte(`"\x20"`),
			[]byte(` `),
		},
		{
			[]byte(`"abc\x20def"`),
			[]byte(`abc def`),
		},
		{
			[]byte(`"abc\u0020def"`),
			[]byte(`abc def`),
		},
		{
			[]byte(`"abc\U00000020def"`),
			[]byte(`abc def`),
		},
	} {
		if out := unquote(test[0]); !bytes.Equal(out, test[1]) {
			t.Errorf("test %d: expecting output %v, got %v", n+1, test[1], out)
		}
	}
}

func BenchmarkScanAndFileInfo(b *testing.B) {
	tempDir := b.TempDir()
	testStatsFile := filepath.Join(tempDir, "test.stats")
	info := new(FileInfo)

	f := statsdata.TestStats(5, 5, "/opt/", 0).AsReader()

	defer f.Close()

	outFile, err := os.Create(testStatsFile)
	if err != nil {
		b.Fatal(err)
	}

	_, err = io.Copy(outFile, f)
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

		for p.Scan(info) == nil {
			if p.size == 0 {
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

func BenchmarkRawScanner(b *testing.B) {
	var buf bytes.Buffer

	io.Copy(&buf, statsdata.TestStats(5, 5, "/opt/", 0).AsReader()) //nolint:errcheck

	data := buf.Bytes()

	for n := 0; n < b.N; n++ {
		b.StopTimer()

		f := bytes.NewReader(data)

		b.StartTimer()

		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
		}
	}
}
