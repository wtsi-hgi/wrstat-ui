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
	"io"
)

// Error is the type of the constant Err* variables.
type Error string

// Error returns a string version of the error.
func (e Error) Error() string { return string(e) }

const (
	fileType                   = byte('f')
	defaultAge                 = 7
	secsPerYear                = 3600 * 24 * 365
	maxLineLength              = 64 * 1024
	maxBase64EncodedPathLength = 1024

	ErrBadPath       = Error("invalid file format: path is not base64 encoded")
	ErrTooFewColumns = Error("invalid file format: too few tab separated columns")
)

// StatsParser is used to parse wrstat stats files.
type StatsParser struct {
	scanner    *bufio.Scanner
	lineBytes  []byte
	lineLength int
	lineIndex  int
	Path       []byte
	Size       int64
	GID        int64
	MTime      int64
	CTime      int64
	EntryType  byte
	error      error
}

// NewStatsParser is used to create a new StatsParser, given uncompressed wrstat
// stats data.
func NewStatsParser(r io.Reader) *StatsParser {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, maxLineLength), maxLineLength)

	return &StatsParser{
		scanner: scanner,
	}
}

// Scan is used to read the next line of stats data, which will then be
// available through the Path, Size, GID, MTime, CTime and EntryType properties.
//
// It returns false when the scan stops, either by reaching the end of the input
// or an error. After Scan returns false, the Err method will return any error
// that occurred during scanning, except that if it was io.EOF, Err will return
// nil.
func (p *StatsParser) Scan() bool {
	keepGoing := p.scanner.Scan()
	if !keepGoing {
		return false
	}

	return p.parseLine()
}

func (p *StatsParser) parseLine() bool {
	p.lineBytes = p.scanner.Bytes()
	p.lineLength = len(p.lineBytes)

	if p.lineLength <= 1 {
		return true
	}

	p.lineIndex = 0

	var ok bool

	p.Path, ok = p.parseNextColumn()
	if !ok {
		return false
	}

	if !p.parseColumns2to7() {
		return false
	}

	entryTypeCol, ok := p.parseNextColumn()
	if !ok {
		return false
	}

	p.EntryType = entryTypeCol[0]

	return true
}

func (p *StatsParser) parseColumns2to7() bool {
	for _, val := range []*int64{&p.Size, nil, &p.GID, nil, &p.MTime, &p.CTime} {
		if !p.parseNumberColumn(val) {
			return false
		}
	}

	return true
}

func (p *StatsParser) parseNextColumn() ([]byte, bool) {
	start := p.lineIndex

	for p.lineBytes[p.lineIndex] != '\t' {
		p.lineIndex++

		if p.lineIndex >= p.lineLength {
			p.error = ErrTooFewColumns

			return nil, false
		}
	}

	end := p.lineIndex
	p.lineIndex++

	return p.lineBytes[start:end], true
}

func (p *StatsParser) parseNumberColumn(v *int64) bool {
	col, ok := p.parseNextColumn()
	if !ok {
		return false
	}

	if v == nil {
		return true
	}

	*v = 0

	for _, c := range col {
		*v = *v*10 + int64(c) - '0'
	}

	return true
}

// Err returns the first non-EOF error that was encountered, available after
// Scan() returns false.
func (p *StatsParser) Err() error {
	return p.error
}
