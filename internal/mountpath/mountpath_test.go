/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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

package mountpath

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const testMountDataPath = "/mnt/data/"

func TestFromOutputDirAcceptsDotPrefixedStagingDir(t *testing.T) {
	t.Parallel()

	Convey("FromOutputDir accepts a watch staging dataset basename", t, func() {
		got, err := FromOutputDir("/tmp/.20260517-200015_／lustre／scratch123")

		So(err, ShouldBeNil)
		So(got, ShouldEqual, "/lustre/scratch123/")
	})
}

func TestFromOutputDir(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		outputDir string
		want      string
		wantErr   error
	}{
		{
			name:      "dataset dir itself",
			outputDir: "/tmp/20250101_／mnt／data／",
			want:      testMountDataPath,
		},
		{
			name:      "subpath inside dataset dir",
			outputDir: "/tmp/20250101_／mnt／data／/dguta.dbs",
			want:      testMountDataPath,
		},
		{
			name:      "adds trailing slash",
			outputDir: "/tmp/20250101_／mnt／data",
			want:      testMountDataPath,
		},
		{
			name:      "empty input",
			outputDir: " ",
			wantErr:   ErrEmptyOutputDir,
		},
		{
			name:      "bad format",
			outputDir: "/tmp/not_a_dataset_dir",
			wantErr:   ErrDatasetDirBadFormat,
		},
		{
			name:      "empty mountkey",
			outputDir: "/tmp/20250101_",
			wantErr:   ErrDatasetDirEmptyMountKey,
		},
		{
			name:      "non-absolute mount path",
			outputDir: "/tmp/20250101_mnt／data",
			wantErr:   ErrDatasetDirBadMountPath,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := FromOutputDir(tc.outputDir)
			if tc.wantErr != nil {
				requireErrorIs(t, err, tc.wantErr)

				return
			}

			requireNoErrorAndEqual(t, got, tc.want, err)
		})
	}
}

func requireErrorIs(t *testing.T, err error, want error) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected error %v, got nil", want)
	}

	if !errors.Is(err, want) {
		t.Fatalf("expected error %v, got %v", want, err)
	}
}

func requireNoErrorAndEqual(t *testing.T, got string, want string, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
