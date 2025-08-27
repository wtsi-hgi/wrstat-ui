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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
)

func TestWatch(t *testing.T) {
	Convey("Given the expected setup", t, func() {
		inputDir := t.TempDir()
		outputDir := t.TempDir()
		testInputA := filepath.Join(inputDir, "12345_abc")
		testInputB := filepath.Join(inputDir, "12345_def")
		wrWrittenCh := make(chan bool)

		cwd, err := os.Getwd()
		So(err, ShouldBeNil)

		pr, pw, err := os.Pipe()
		So(err, ShouldBeNil)

		client.PretendSubmissions = strconv.FormatUint(uint64(pw.Fd()), 10)

		var jobs []*jobqueue.Job

		go func() {
			defer pr.Close()
			defer close(wrWrittenCh)

			j := json.NewDecoder(pr)

			for {
				var js []*jobqueue.Job

				if errr := j.Decode(&js); errr != nil {
					return
				}

				jobs = append(jobs, js...)
			}
		}()

		So(os.Mkdir(testInputA, 0755), ShouldBeNil)
		So(os.Mkdir(testInputB, 0755), ShouldBeNil)
		So(createFile(filepath.Join(testInputA, inputStatsFile)), ShouldBeNil)

		Convey("Watch will spot a new directory and schedule a summarise", func() {
			err = watch([]string{inputDir}, outputDir, "/path/to/quota", "/path/to/basedirs.config", "/path/to/mounts", nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldResemble, []*jobqueue.Job{
				{
					Cmd: fmt.Sprintf(`%[1]q summarise -d "%[2]s/.12345_abc" `+
						`-m "/path/to/mounts" `+
						`-q "/path/to/quota" -c "/path/to/basedirs.config" `+
						`"%[3]s/stats.gz" && touch -r "%[3]s" "%[2]s/.12345_abc" `+
						`&& mv "%[2]s/.12345_abc" "%[2]s/12345_abc"`,
						os.Args[0], outputDir, testInputA),
					Cwd:        cwd,
					CwdMatters: true,
					RepGroup:   "wrstat-ui-summarise-" + time.Now().Format("20060102150405"),
					ReqGroup:   "wrstat-ui-summarise",
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  10 * time.Second,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
			})
		})

		Convey("Watch will provide absolute paths to summarise given relative paths", func() {
			parentDir := filepath.Dir(inputDir)

			relInput := filepath.Base(inputDir)
			relOutput := filepath.Base(outputDir)

			err = os.Chdir(parentDir)
			So(err, ShouldBeNil)

			err := watch([]string{relInput}, relOutput, "/path/to/quota", "/path/to/basedirs.config", "", nil)

			errr := os.Chdir(cwd)
			So(errr, ShouldBeNil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldResemble, []*jobqueue.Job{
				{
					Cmd: fmt.Sprintf(`%[1]q summarise -d "%[2]s/.12345_abc" `+
						`-q "/path/to/quota" -c "/path/to/basedirs.config" `+
						`"%[3]s/stats.gz" && touch -r "%[3]s" "%[2]s/.12345_abc" `+
						`&& mv "%[2]s/.12345_abc" "%[2]s/12345_abc"`,
						os.Args[0], outputDir, testInputA),
					Cwd:        parentDir,
					CwdMatters: true,
					RepGroup:   "wrstat-ui-summarise-" + time.Now().Format("20060102150405"),
					ReqGroup:   "wrstat-ui-summarise",
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  10 * time.Second,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
			})
		})

		Convey("Watch will not reschedule a summarise if one has already started", func() {
			So(os.Mkdir(filepath.Join(outputDir, ".12345_abc"), 0755), ShouldBeNil)

			err := watch([]string{inputDir}, outputDir, "/path/to/quota", "/path/to/basedirs.config", "", nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(len(jobs), ShouldEqual, 0)
		})

		Convey("Watch will not reschedule a summarise if one has already completed", func() {
			So(os.Mkdir(filepath.Join(outputDir, "12345_abc"), 0755), ShouldBeNil)

			err := watch([]string{inputDir}, outputDir, "/path/to/quota", "/path/to/basedirs.config", "", nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(len(jobs), ShouldEqual, 0)
		})

		Convey("Watch will recognise existing basedir history in the output path", func() {
			existingOutput := filepath.Join(outputDir, "00001_abc")
			So(os.Mkdir(existingOutput, 0755), ShouldBeNil)
			So(createFile(filepath.Join(existingOutput, basedirBasename)), ShouldBeNil)

			err := watch([]string{inputDir}, outputDir, "/path/to/quota", "/path/to/basedirs.config", "", nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldResemble, []*jobqueue.Job{
				{
					Cmd: fmt.Sprintf(`%[1]q summarise -d "%[2]s/.12345_abc" `+
						`-s "%[2]s/00001_abc/basedirs.db" `+
						`-q "/path/to/quota" -c "/path/to/basedirs.config" `+
						`"%[3]s/stats.gz" && touch -r "%[3]s" "%[2]s/.12345_abc" `+
						`&& mv "%[2]s/.12345_abc" "%[2]s/12345_abc"`,
						os.Args[0], outputDir, testInputA),
					Cwd:        cwd,
					CwdMatters: true,
					RepGroup:   "wrstat-ui-summarise-" + time.Now().Format("20060102150405"),
					ReqGroup:   "wrstat-ui-summarise",
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  10 * time.Second,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
			})
		})

		Convey("Watch can watch multiple directories", func() {
			inputDir2 := t.TempDir()
			testInputC := filepath.Join(inputDir2, "98765_c")
			So(os.Mkdir(testInputC, 0755), ShouldBeNil)
			So(createFile(filepath.Join(testInputC, inputStatsFile)), ShouldBeNil)

			err := watch([]string{inputDir, inputDir2}, outputDir, "/path/to/quota", "/path/to/basedirs.config", "", nil)
			So(err, ShouldBeNil)

			pw.Close()

			<-wrWrittenCh

			So(jobs, ShouldResemble, []*jobqueue.Job{
				{
					Cmd: fmt.Sprintf(`%[1]q summarise -d "%[2]s/.12345_abc" `+
						`-q "/path/to/quota" -c "/path/to/basedirs.config" `+
						`"%[3]s/stats.gz" && touch -r "%[3]s" "%[2]s/.12345_abc" `+
						`&& mv "%[2]s/.12345_abc" "%[2]s/12345_abc"`,
						os.Args[0], outputDir, testInputA),
					Cwd:        cwd,
					CwdMatters: true,
					RepGroup:   "wrstat-ui-summarise-" + time.Now().Format("20060102150405"),
					ReqGroup:   "wrstat-ui-summarise",
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  10 * time.Second,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
				{
					Cmd: fmt.Sprintf(`%[1]q summarise -d "%[2]s/.98765_c" `+
						`-q "/path/to/quota" -c "/path/to/basedirs.config" `+
						`"%[3]s/stats.gz" && touch -r "%[3]s" "%[2]s/.98765_c" `+
						`&& mv "%[2]s/.98765_c" "%[2]s/98765_c"`,
						os.Args[0], outputDir, testInputC),
					Cwd:        cwd,
					CwdMatters: true,
					RepGroup:   "wrstat-ui-summarise-" + time.Now().Format("20060102150405"),
					ReqGroup:   "wrstat-ui-summarise",
					Requirements: &scheduler.Requirements{
						RAM:   8192,
						Time:  10 * time.Second,
						Cores: 2,
						Disk:  1,
					},
					Override: 1,
					Retries:  30,
					State:    jobqueue.JobStateDelayed,
				},
			})
		})

		Convey("watch errors if can't connect to manager", func() {
			tempDir := t.TempDir()

			certPath, keyPath, err := gas.CreateTestCert(t)
			So(err, ShouldBeNil)

			tokenPath := filepath.Join(tempDir, "token")
			So(os.WriteFile(tokenPath, []byte("token"), 0600), ShouldBeNil)

			os.Setenv("WR_MANAGERTOKENFILE", tokenPath)
			os.Setenv("WR_MANAGERCAFILE", certPath)
			os.Setenv("WR_MANAGERCERTFILE", certPath)
			os.Setenv("WR_MANAGERKEYFILE", keyPath)

			client.PretendSubmissions = ""
			logger := log15.New()

			errCh := make(chan error, 1)
			errTimedOut := errors.New("timed out") //nolint:err113

			connectTimeout = time.Second

			go func() {
				time.Sleep(3 * connectTimeout)
				errCh <- errTimedOut
			}()

			go func() {
				errCh <- watch([]string{inputDir}, outputDir, "/path/to/quota", "/path/to/basedirs.config", "", logger)
			}()

			err = <-errCh
			So(err, ShouldNotBeNil)
			So(err, ShouldNotEqual, errTimedOut)
			So(err.Error(), ShouldContainSubstring, "could not reach the server")
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
