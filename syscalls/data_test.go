package syscalls

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestData(t *testing.T) {
	for n, test := range [...]struct {
		Input  string
		Events []Event
		Errors []Error
		Error  error
	}{
		{},
		{
			Input: fmt.Sprintf("t=%s ", time.Unix(123456, 0).Format(timeFormat)),
			Events: []Event{
				{
					Time: 123456,
				},
			},
		},
		{
			Input: fmt.Sprintf("t=%s msg=something", time.Unix(123456, 0).Format(timeFormat)),
		},
		{
			Input: fmt.Sprintf("t=%s msg=syscalls", time.Unix(123456, 0).Format(timeFormat)),
			Events: []Event{
				{
					Time: 123456,
				},
			},
		},
		{
			Input: fmt.Sprintf("t=%s msg=\"syscall logging\"", time.Unix(123456, 0).Format(timeFormat)),
			Events: []Event{
				{
					Time: 123456,
				},
			},
		},
		{
			Input: "file=/path/to/something.1",
			Events: []Event{
				{
					File: "something.1",
				},
			},
		},
		{
			Input: "host=abc",
			Events: []Event{
				{
					Host: "abc",
				},
			},
		},
		{
			Input: "opens=123",
			Events: []Event{
				{
					Opens: 123,
				},
			},
		},
		{
			Input: "reads=123",
			Events: []Event{
				{
					Reads: 123,
				},
			},
		},
		{
			Input: "bytes=123",
			Events: []Event{
				{
					Bytes: 123,
				},
			},
		},
		{
			Input: "closes=123",
			Events: []Event{
				{
					Closes: 123,
				},
			},
		},
		{
			Input: "stats=123",
			Events: []Event{
				{
					Stats: 123,
				},
			},
		},
		{
			Input: "lvl=eror",
			Errors: []Error{
				{
					Data: make(map[string]string),
				},
			},
		},
		{
			Input: fmt.Sprintf("t=%s lvl=eror", time.Unix(123456, 0).Format(timeFormat)),
			Errors: []Error{
				{
					Time: 123456,
					Data: make(map[string]string),
				},
			},
		},
		{
			Input: "lvl=eror msg=\"an error\"",
			Errors: []Error{
				{
					Message: "an error",
					Data:    make(map[string]string),
				},
			},
		},
		{
			Input: "lvl=eror arg1=something arg2=\"something else\"",
			Errors: []Error{
				{
					Data: map[string]string{
						"arg1": "something",
						"arg2": "something else",
					},
				},
			},
		},
		{
			Input: fmt.Sprintf("t=%s host=abc\nt=%s opens=1", time.Unix(123456, 0).Format(timeFormat), time.Unix(123457, 0).Format(timeFormat)), //nolint:lll
			Events: []Event{
				{
					Time: 123456,
					Host: "abc",
				},
				{
					Time:  123457,
					Host:  "abc",
					Opens: 1,
				},
			},
		},
		{
			Input: fmt.Sprintf("t=%s host=abc\nt=%s lvl=eror msg=abc", time.Unix(123456, 0).Format(timeFormat), time.Unix(123457, 0).Format(timeFormat)), //nolint:lll
			Events: []Event{
				{
					Time: 123456,
					Host: "abc",
				},
			},
			Errors: []Error{
				{
					Time:    123457,
					Message: "abc",
					Host:    "abc",
					Data:    make(map[string]string),
				},
			},
		},
		{
			Input: fmt.Sprintf("t=%s file=a host=abc\nt=%s file=b host=def\nt=%s lvl=eror msg=abc\nt=%s file=a opens=1", time.Unix(123456, 0).Format(timeFormat), time.Unix(123457, 0).Format(timeFormat), time.Unix(123458, 0).Format(timeFormat), time.Unix(123459, 0).Format(timeFormat)), //nolint:lll
			Events: []Event{
				{
					Time: 123456,
					File: "a",
					Host: "abc",
				},
				{
					Time: 123457,
					File: "b",
					Host: "def",
				},
				{
					Time:  123459,
					File:  "a",
					Host:  "abc",
					Opens: 1,
				},
			},
			Errors: []Error{
				{
					Time:    123458,
					Message: "abc",
					Data:    make(map[string]string),
				},
			},
		},
	} {
		d := &data{hosts: make(map[string]string)}

		if err := d.loadData(strings.NewReader(test.Input)); !errors.Is(err, test.Error) { //nolint:nestif
			t.Errorf("test %d: expecting error %q, got %q", n+1, test.Error, err)
		} else if len(d.Events) != len(test.Events) { //nolint:gocritic
			t.Errorf("test %d: expecting to have %d events, got %d", n+1, len(test.Events), len(d.Events))
		} else if len(d.Errors) != len(test.Errors) {
			t.Errorf("test %d: expecting to have %d errors, got %d", n+1, len(test.Errors), len(d.Errors))
		} else {
			for m, event := range test.Events {
				if !reflect.DeepEqual(event, d.Events[m]) {
					t.Errorf("test %d.1.%d: expecting to read event %#v, got %#v", n+1, m+1, event, d.Events[m])
				}
			}

			for m, error := range test.Errors {
				if !reflect.DeepEqual(error, d.Errors[m]) {
					t.Errorf("test %d.1.%d: expecting to read event %#v, got %#v", n+1, m+1, error, d.Errors[m])
				}
			}
		}
	}
}
