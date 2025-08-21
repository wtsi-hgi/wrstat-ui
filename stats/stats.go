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
	"slices"
	"unicode/utf8"
)

const (
	FileType    = 'f'
	DirType     = 'd'
	SymlinkType = 'L'
	DeviceType  = 'D'
	PipeType    = 'p'
	SocketType  = 'S'
	CharType    = 'c'
)

var slash = []byte{'/'} //nolint:gochecknoglobals

// Error is the type of the constant Err* variables.
type Error string

// Error returns a string version of the error.
func (e Error) Error() string { return string(e) }

const (
	defaultAge                 = 7
	secsPerYear                = 3600 * 24 * 365
	maxLineLength              = 64 * 1024
	maxBase64EncodedPathLength = 1024

	ErrBadPath       = Error("invalid file format: path is not base64 encoded")
	ErrTooFewColumns = Error("invalid file format: too few tab separated columns")
)

// StatsParser is used to parse wrstat stats files.
type StatsParser struct { //nolint:revive
	scanner      *bufio.Scanner
	lineBytes    []byte
	lineLength   int
	lineIndex    int
	lastPath     []byte
	indexes      []int
	path         []byte
	size         int64
	apparentSize int64
	uid          int64
	gid          int64
	mtime        int64
	atime        int64
	ctime        int64
	entryType    byte
	inode        int64
	error        error
}

// FileInfo represents a parsed line of data from a stats file.
type FileInfo struct {
	Path         []byte
	Size         int64
	ApparentSize int64
	UID          uint32
	GID          uint32
	MTime        int64
	ATime        int64
	CTime        int64
	Inode        int64
	EntryType    byte
}

// IsDir returns true if the FileInfo represents a directory.
func (f *FileInfo) IsDir() bool {
	return f.EntryType == DirType
}

// BaseName returns the name of the file.
func (f *FileInfo) BaseName() []byte {
	return f.Path[bytes.LastIndexByte(f.Path[:len(f.Path)-1], '/')+1:]
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
func (p *StatsParser) Scan(info *FileInfo) error {
	if p.fillMissingOrFullInfo(info) {
		return nil
	}

	keepGoing := p.scanner.Scan()
	if !keepGoing {
		return io.EOF
	}

	if !p.parseLine() {
		return p.error
	}

	return p.fillInfo(info)
}

func (p *StatsParser) fillInfo(info *FileInfo) error {
	if p.lastPath != nil {
		if splitPoint := findMatchingPathPrefix(p.lastPath, p.path); splitPoint != -1 {
			p.setMissingIndexes(splitPoint)

			p.lastPath = append(p.lastPath[:0], p.path...)

			p.fillMissingOrFullInfo(info)

			return nil
		}
	}

	p.lastPath = append(p.lastPath[:0], p.path...)

	p.fillFullInfo(info)

	return nil
}

func findMatchingPathPrefix(last, curr []byte) int {
	lastSlash := 0
	currSlash := 0

	for {
		lnSlash := bytes.IndexByte(last[lastSlash:], '/')
		cnSlash := bytes.IndexByte(curr[currSlash:], '/')

		if cnSlash == -1 {
			return -1
		}

		if lnSlash == -1 {
			lnSlash = len(last)
		} else {
			lnSlash += lastSlash
		}

		cnSlash += currSlash

		if !bytes.Equal(last[lastSlash:lnSlash], curr[currSlash:cnSlash]) {
			return currSlash
		}

		lastSlash = lnSlash + 1
		currSlash = cnSlash + 1
	}
}

func (p *StatsParser) setMissingIndexes(currSlash int) {
	last := len(p.path)
	p.indexes = append(p.indexes, last)

	for {
		idx := bytes.LastIndexByte(p.path[:last-1], '/')
		if idx == -1 || idx <= currSlash {
			break
		}

		last = idx

		p.indexes = append(p.indexes, idx+1)
	}
}

func (p *StatsParser) fillMissingOrFullInfo(info *FileInfo) bool {
	if len(p.indexes) == 0 {
		return false
	}

	info.Path = p.path[:p.indexes[len(p.indexes)-1]]

	info.UID = uint32(p.uid) //nolint:gosec
	info.GID = uint32(p.gid) //nolint:gosec
	info.MTime = p.mtime
	info.ATime = p.atime
	info.CTime = p.ctime

	if p.indexes = p.indexes[:len(p.indexes)-1]; len(p.indexes) == 0 {
		info.Size = p.size
		info.ApparentSize = p.apparentSize
		info.EntryType = p.entryType
		info.Inode = p.inode
	} else {
		info.Size = 0
		info.ApparentSize = 0
		info.EntryType = DirType
		info.Inode = 0
	}

	return true
}

func (p *StatsParser) fillFullInfo(info *FileInfo) {
	info.Path = p.path
	info.Size = p.size
	info.ApparentSize = p.apparentSize
	info.UID = uint32(p.uid) //nolint:gosec
	info.GID = uint32(p.gid) //nolint:gosec
	info.MTime = p.mtime
	info.ATime = p.atime
	info.CTime = p.ctime
	info.EntryType = p.entryType
	info.Inode = p.inode
}

func unquote(path []byte) []byte { //nolint:funlen,gocognit,gocyclo,cyclop
	if path == nil {
		return path
	}

	path = path[1 : len(path)-1]

	for i := 0; i < len(path); i++ {
		if path[i] != '\\' {
			continue
		}

		added := 1
		read := 2

		switch path[i+1] {
		case 'a':
			path[i] = '\a'
		case 'b':
			path[i] = '\b'
		case 'f':
			path[i] = '\f'
		case 'n':
			path[i] = '\n'
		case 'r':
			path[i] = '\r'
		case 't':
			path[i] = '\t'
		case 'v':
			path[i] = '\v'
		case '"':
			path[i] = '"'
		case '\'':
			path[i] = '\''
		case 'x', 'u', 'U':
			n := 0

			switch path[i+1] {
			case 'x':
				n = 2
			case 'u':
				n = 4
			case 'U':
				n = 8
			}

			read = n + 2 //nolint:mnd

			var value rune

			for _, b := range path[i+2 : i+n+2] {
				value <<= 4

				if b >= '0' && b <= '9' { //nolint:gocritic,nestif
					value |= rune(b) - '0'
				} else if b >= 'A' && b <= 'F' {
					value |= rune(b) - 'A'
				} else if b >= 'a' && b <= 'f' {
					value |= rune(b) - 'a'
				}
			}

			a := utf8.AppendRune(path[:i], value)

			added = len(a) - i
		}

		path = slices.Delete(path, i+added, i+read)
	}

	return path
}

func (p *StatsParser) parseLine() bool {
	p.lineBytes = p.scanner.Bytes()
	p.lineLength = len(p.lineBytes)

	if p.lineLength <= 1 {
		return true
	}

	p.lineIndex = 0

	var ok bool

	p.path, ok = p.parseNextColumn()
	if !ok {
		return false
	}

	p.path = unquote(p.path)

	if !p.parseColumns2to7() {
		return false
	}

	entryTypeCol, ok := p.parseNextColumn()
	if !ok {
		return false
	}

	p.entryType = entryTypeCol[0]

	if bytes.HasSuffix(p.path, slash) {
		p.entryType = DirType
	}

	var none int64

	if !p.parseNumberColumn(&p.inode) || !p.parseNumberColumn(&none) || !p.parseNumberColumn(&none) {
		return false
	}

	return p.parseNumberColumn(&p.apparentSize)
}

func (p *StatsParser) parseColumns2to7() bool {
	for _, val := range []*int64{&p.size, &p.uid, &p.gid, &p.atime, &p.mtime, &p.ctime} {
		if !p.parseNumberColumn(val) {
			return false
		}
	}

	return true
}

func (p *StatsParser) parseNextColumn() ([]byte, bool) {
	start := p.lineIndex

	if p.lineIndex >= p.lineLength {
		p.error = ErrTooFewColumns

		return nil, false
	}

	for p.lineIndex < p.lineLength && p.lineBytes[p.lineIndex] != '\t' {
		p.lineIndex++
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
