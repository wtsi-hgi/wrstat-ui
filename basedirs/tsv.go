/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

package basedirs

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var (
	newLineByte = []byte{'\n'} //nolint:gochecknoglobals
	tabByte     = []byte{'\t'} //nolint:gochecknoglobals
)

const (
	ErrBadTSV = Error("bad TSV")

	numColumns = 3
	noMatch    = -1

	DefaultSplits  = 1
	defaultMinDirs = 2
)

type configAttrs struct {
	Prefix  []string
	Splits  int
	MinDirs int
}

func (c *configAttrs) PathShouldOutput(path *summary.DirectoryPath) bool {
	return path.Depth >= c.MinDirs && path.Depth < c.MinDirs+c.Splits
}

func (c *configAttrs) Match(path *summary.DirectoryPath) bool {
	if len(c.Prefix) == 0 {
		return true
	}

	if path.Depth < len(c.Prefix) {
		return false
	}

	for path.Depth > len(c.Prefix) {
		path = path.Parent
	}

	if !strings.HasPrefix(path.Name, c.Prefix[len(c.Prefix)-1]) {
		return false
	}

	for n := len(c.Prefix) - 2; n >= 0; n-- { //nolint:mnd
		path = path.Parent

		if c.Prefix[n] != path.Name {
			return false
		}
	}

	return true
}

// Config represents the parsed basedirs config.
type Config []configAttrs

// ParseConfig reads basedirs configuration, which is a TSV in the following
// format:
//
// PREFIX	SPLITS	MINDIRS.
func ParseConfig(r io.Reader) (Config, error) {
	b := bufio.NewReader(r)
	end := false

	var result []configAttrs //nolint:prealloc

	for !end {
		line, err := b.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			end = true
		} else if err != nil {
			return nil, err
		}

		line = bytes.TrimSuffix(line, newLineByte)

		if len(line) == 0 || line[0] == '#' {
			continue
		}

		conf, err := parseLine(line)
		if err != nil {
			return nil, err
		}

		result = append(result, conf)
	}

	sort.Slice(result, func(i, j int) bool { return len(result[i].Prefix) > len(result[j].Prefix) })

	return result, nil
}

func parseLine(line []byte) (configAttrs, error) {
	attr := bytes.Split(line, tabByte)
	if len(attr) != numColumns {
		return configAttrs{}, ErrBadTSV
	}

	prefix := string(attr[0])

	splits, err := strconv.ParseUint(string(attr[1]), 10, 0)
	if err != nil {
		return configAttrs{}, err
	}

	minDirs, err := strconv.ParseUint(string(attr[2]), 10, 0)
	if err != nil {
		return configAttrs{}, err
	}

	return configAttrs{
		Prefix:  split.SplitPath(prefix),
		Splits:  int(splits),  //nolint:gosec
		MinDirs: int(minDirs), //nolint:gosec
	}, nil
}

// PathShouldOutput returns true if, according to the parsed config, basedirs
// data should be outputted at the given directory.
func (c Config) PathShouldOutput(path *summary.DirectoryPath) bool {
	for _, ca := range c {
		if ca.Match(path) {
			return path.Depth == ca.MinDirs
		}
	}

	return false
}
