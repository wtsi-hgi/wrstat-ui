/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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
package watch

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestWatch(t *testing.T) {
	exit = func() {}
	runJobs = "0"

	Convey("Given the expected setup", t, func() {
		inputDir := t.TempDir()
		outputDir := t.TempDir()
		testInputA := filepath.Join(inputDir, "12345_abc")
		testInputB := filepath.Join(inputDir, "12345_def")
		wrWrittenCh := make(chan bool)

		pr, pw, err := os.Pipe()
		So(err, ShouldBeNil)

		testOutputFD = int(pw.Fd())

		var wr string

		go func() {
			defer pr.Close()
			defer close(wrWrittenCh)

			var buf [4096]byte

			for {
				n, err := pr.Read(buf[:])
				if err != nil || n == 0 {
					return
				}

				wr += string(buf[:n])
			}
		}()

		So(os.Mkdir(testInputA, 0755), ShouldBeNil)
		So(os.Mkdir(testInputB, 0755), ShouldBeNil)
		So(createFile(filepath.Join(testInputA, inputStatsFile)), ShouldBeNil)

		Convey("Watch will spot a new directory and schedule a summarise", func() {
			err := watch([]string{inputDir}, outputDir, "/path/to/quota", "/path/to/basedirs.config")
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(wr, ShouldEqual, fmt.Sprintf(`{"cmd":"\"%[1]s\" summarise -d \"%[2]s/.12345_abc\" `+
				`-q \"/path/to/quota\" -c \"/path/to/basedirs.config\" \"%[3]s/stats.gz\" && `+
				`touch -r \"%[3]s\" \"%[2]s/.12345_abc\" && mv \"%[2]s/.12345_abc\" \"%[2]s/12345_abc\"",`+
				`"cpus":2,"memory":"8000","req_grp":"wrstat-ui-summarise","rep_grp":"wrstat-ui-summarise-`+
				time.Now().Format("20060102150405")+`"}`, os.Args[0], outputDir, testInputA))
		})

		Convey("Watch will not reschedule a summarise if one has already started", func() {
			So(os.Mkdir(filepath.Join(outputDir, ".12345_abc"), 0755), ShouldBeNil)

			err := watch([]string{inputDir}, outputDir, "/path/to/quota", "/path/to/basedirs.config")
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(wr, ShouldEqual, "")
		})

		Convey("Watch will not reschedule a summarise if one has already completed", func() {
			So(os.Mkdir(filepath.Join(outputDir, "12345_abc"), 0755), ShouldBeNil)

			err := watch([]string{inputDir}, outputDir, "/path/to/quota", "/path/to/basedirs.config")
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(wr, ShouldEqual, "")
		})

		Convey("Watch will recognise existing basedir history in the output path", func() {
			existingOutput := filepath.Join(outputDir, "00001_abc")
			So(os.Mkdir(existingOutput, 0755), ShouldBeNil)
			So(createFile(filepath.Join(existingOutput, basedirBasename)), ShouldBeNil)

			err := watch([]string{inputDir}, outputDir, "/path/to/quota", "/path/to/basedirs.config")
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(wr, ShouldEqual, fmt.Sprintf(`{"cmd":"\"%[1]s\" summarise -d \"%[2]s/.12345_abc\" `+
				`-s \"%[2]s/00001_abc/basedirs.db\" `+
				`-q \"/path/to/quota\" -c \"/path/to/basedirs.config\" \"%[3]s/stats.gz\" && `+
				`touch -r \"%[3]s\" \"%[2]s/.12345_abc\" && mv \"%[2]s/.12345_abc\" \"%[2]s/12345_abc\"",`+
				`"cpus":2,"memory":"8000","req_grp":"wrstat-ui-summarise","rep_grp":"wrstat-ui-summarise-`+
				time.Now().Format("20060102150405")+`"}`, os.Args[0], outputDir, testInputA))
		})

		Convey("Watch can watch multiple directories", func() {
			inputDir2 := t.TempDir()
			testInputC := filepath.Join(inputDir2, "98765_c")
			So(os.Mkdir(testInputC, 0755), ShouldBeNil)
			So(createFile(filepath.Join(testInputC, inputStatsFile)), ShouldBeNil)

			err := watch([]string{inputDir, inputDir2}, outputDir, "/path/to/quota", "/path/to/basedirs.config")
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(wr, ShouldEqual, fmt.Sprintf(`{"cmd":"\"%[1]s\" summarise -d \"%[2]s/.12345_abc\" `+
				`-q \"/path/to/quota\" -c \"/path/to/basedirs.config\" \"%[3]s/stats.gz\" && `+
				`touch -r \"%[3]s\" \"%[2]s/.12345_abc\" && mv \"%[2]s/.12345_abc\" \"%[2]s/12345_abc\"",`+
				`"cpus":2,"memory":"8000","req_grp":"wrstat-ui-summarise","rep_grp":"wrstat-ui-summarise-`+
				time.Now().Format("20060102150405")+`"}`+
				`{"cmd":"\"%[1]s\" summarise -d \"%[2]s/.98765_c\" `+
				`-q \"/path/to/quota\" -c \"/path/to/basedirs.config\" \"%[4]s/stats.gz\" && `+
				`touch -r \"%[4]s\" \"%[2]s/.98765_c\" && mv \"%[2]s/.98765_c\" \"%[2]s/98765_c\"",`+
				`"cpus":2,"memory":"8000","req_grp":"wrstat-ui-summarise","rep_grp":"wrstat-ui-summarise-`+
				time.Now().Format("20060102150405")+`"}`,
				os.Args[0], outputDir, testInputA, testInputC))
		})
	})
}

func createFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	return f.Close()
}
