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

package syscalls

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/pgzip"
)

const timeFormat = "2006-01-02T15:04:05-0700"

type data struct {
	Events   []Event `json:"events"`
	Errors   []Error `json:"errors"`
	Complete bool    `json:"complete"`
	hosts    map[string]string
	lastFile string
}

type Event struct {
	Time   int64  `json:"time"`
	File   string `json:"file"`
	Host   string `json:"host"`
	Opens  uint64 `json:"opens,omitempty"`
	Reads  uint64 `json:"reads,omitempty"`
	Bytes  uint64 `json:"bytes,omitempty"`
	Closes uint64 `json:"closes,omitempty"`
	Stats  uint64 `json:"stats,omitempty"`
}

type Error struct {
	Time    int64             `json:"time"`
	Message string            `json:"message"`
	File    string            `json:"file"`
	Host    string            `json:"host"`
	Data    map[string]string `json:"data,omitempty"`
}

func (d *data) loadFile(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}

	defer f.Close()

	var r io.Reader = f

	if strings.HasSuffix(file, ".gz") {
		gr, err := pgzip.NewReader(f)
		if err != nil {
			return err
		}

		defer gr.Close()

		r = gr
	}

	return d.loadData(r)
}

func (d *data) loadData(r io.Reader) error {
	reader := newLogReader(r)

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if err := d.parseLine(line); err != nil {
			return err
		}
	}

	return nil
}

func (d *data) parseLine(line [][2]string) error {
	event := Event{File: d.lastFile}

	var err error

	for _, part := range line {
		switch part[0] {
		case "t":
			var t time.Time

			t, err = time.Parse(timeFormat, part[1])
			event.Time = t.Unix()
		case "lvl":
			if part[1] == "eror" {
				return d.parseErrorLine(line)
			}
		case "msg":
			switch part[1] {
			case `syscall logging`, `syscalls`:
			default:
				return nil
			}
		case "file":
			event.File = filepath.Base(part[1])
		case "host":
			event.Host = part[1]
		case "opens":
			event.Opens, err = strconv.ParseUint(part[1], 10, 64)
		case "reads":
			event.Reads, err = strconv.ParseUint(part[1], 10, 64)
		case "bytes":
			event.Bytes, err = strconv.ParseUint(part[1], 10, 64)
		case "closes":
			event.Closes, err = strconv.ParseUint(part[1], 10, 64)
		case "stats":
			event.Stats, err = strconv.ParseUint(part[1], 10, 64)
		}

		if err != nil {
			return err
		}
	}

	if event.Host != "" {
		d.hosts[event.File] = event.Host
	} else {
		event.Host = d.hosts[event.File]
	}

	pos, _ := slices.BinarySearchFunc(d.Events, event, func(a, b Event) int {
		return int(a.Time) - int(b.Time)
	})
	d.Events = slices.Insert(d.Events, pos, event)

	return nil
}

func (d *data) parseErrorLine(line [][2]string) error {
	event := Error{File: d.lastFile, Host: d.hosts[d.lastFile], Data: make(map[string]string)}

	var err error

	for _, part := range line {
		switch part[0] {
		case "t":
			var t time.Time

			t, err = time.Parse(timeFormat, part[1])
			event.Time = t.Unix()
		case "lvl":
		case "msg":
			event.Message = part[1]
		default:
			event.Data[part[0]] = part[1]
		}

		if err != nil {
			return err
		}
	}

	d.Errors = append(d.Errors, event)

	return nil
}
