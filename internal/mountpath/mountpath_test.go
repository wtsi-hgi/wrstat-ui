package mountpath

import (
	"errors"
	"testing"
)

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
			want:      "/mnt/data/",
		},
		{
			name:      "subpath inside dataset dir",
			outputDir: "/tmp/20250101_／mnt／data／/dguta.dbs",
			want:      "/mnt/data/",
		},
		{
			name:      "adds trailing slash",
			outputDir: "/tmp/20250101_／mnt／data",
			want:      "/mnt/data/",
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
