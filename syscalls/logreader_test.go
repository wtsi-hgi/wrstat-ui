package syscalls

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestLogReader(t *testing.T) {
Loop:
	for n, test := range [...]struct {
		Input  string
		Output [][][2]string
		Error  error
	}{
		{},
		{
			Input: "a",
			Output: [][][2]string{
				{
					{"", "a"},
				},
			},
		},
		{
			Input: "a=1",
			Output: [][][2]string{
				{
					{"a", "1"},
				},
			},
		},
		{
			Input: "a=1 b=2",
			Output: [][][2]string{
				{
					{"a", "1"},
					{"b", "2"},
				},
			},
		},
		{
			Input: "a=1 b=2\na=2 b=3",
			Output: [][][2]string{
				{
					{"a", "1"},
					{"b", "2"},
				},
				{
					{"a", "2"},
					{"b", "3"},
				},
			},
		},
		{
			Input: "a=\"1\"",
			Output: [][][2]string{
				{
					{"a", "1"},
				},
			},
		},
		{
			Input: "a=\"1",
			Output: [][][2]string{
				{
					{"a", "1"},
				},
			},
			Error: io.ErrUnexpectedEOF,
		},
	} {
		r := newLogReader(strings.NewReader(test.Input))

		for _, expectedLine := range test.Output {
			line, err := r.Read()
			if err != nil { //nolint:gocritic,nestif
				if !errors.Is(err, test.Error) {
					t.Errorf("test %d: expecting error %q, got %q", n+1, test.Error, err)
				}

				continue Loop
			} else if len(line) != len(expectedLine) {
				t.Errorf("test %d: expecting to read %d entries, read %d", n+1, len(expectedLine), len(line))
			} else {
				for m, value := range expectedLine {
					if line[m][0] != value[0] {
						t.Errorf("test %d.%d: expecting key %q, got %q", n+1, m+1, value[0], line[m][0])
					} else if line[m][1] != value[1] {
						t.Errorf("test %d.%d: expecting value %q, got %q", n+1, m+1, value[1], line[m][1])
					}
				}
			}
		}

		_, err := r.Read()
		if !errors.Is(err, io.EOF) {
			t.Errorf("test %d: expecting EOF, got %v", n+1, err)
		}
	}
}
