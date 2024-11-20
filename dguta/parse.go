/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package dguta

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/summary"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrInvalidFormat = Error("the provided data was not in dguta format")
	ErrBlankLine     = Error("the provided line had no information")
)

const (
	gutaDataCols    = 9
	gutaDataIntCols = 8
)

type dgutaParserCallBack func(*DGUTA)

// parseDGUTALines will parse the given dguta file data (as output by
// summary.DirGroupUserTypeAge.Output()) and send *DGUTA structs to your
// callback.
//
// Each *DGUTA will correspond to one of the directories in your dguta file
// data, and contain all the *GUTA information for that directory. Your callback
// will receive exactly 1 *DGUTA per unique directory. (This relies on the dguta
// file data being sorted, as it normally would be.)
//
// Any issues with parsing the dguta file data will result in this method
// returning an error.
func parseDGUTALines(data io.Reader, cb dgutaParserCallBack) error {
	dguta, gutas := &DGUTA{}, []*GUTA{}

	scanner := bufio.NewScanner(data)

	for scanner.Scan() {
		thisDir, g, err := parseDGUTALine(scanner.Text())
		if err != nil {
			if errors.Is(err, ErrBlankLine) {
				continue
			}

			return err
		}

		if thisDir != dguta.Dir {
			populateAndEmitDGUTA(dguta, gutas, cb)
			dguta, gutas = &DGUTA{Dir: thisDir}, []*GUTA{}
		}

		gutas = append(gutas, g)
	}

	if dguta.Dir != "" {
		dguta.GUTAs = gutas
		cb(dguta)
	}

	return scanner.Err()
}

// populateAndEmitDGUTA adds gutas to dgutas and sends dguta to cb, but only if
// the dguta has a Dir.
func populateAndEmitDGUTA(dguta *DGUTA, gutas []*GUTA, cb dgutaParserCallBack) {
	if dguta.Dir != "" {
		dguta.GUTAs = gutas
		cb(dguta)
	}
}

// parseDGUTALine parses a line of summary.DirGroupUserType.Output() into a
// directory string and a *dguta for the other information.
//
// Returns an error if line didn't have the expected format.
func parseDGUTALine(line string) (string, *GUTA, error) {
	parts, err := splitDGUTLine(line)
	if err != nil {
		return "", nil, err
	}

	if parts[0] == "" {
		return "", nil, ErrBlankLine
	}

	path, err := strconv.Unquote(parts[0])
	if err != nil {
		return "", nil, err
	}

	ints, err := gutLinePartsToInts(parts)
	if err != nil {
		return "", nil, err
	}

	return path, &GUTA{
		GID:   uint32(ints[0]),
		UID:   uint32(ints[1]),
		FT:    summary.DirGUTAFileType(ints[2]),
		Age:   summary.DirGUTAge(ints[3]),
		Count: uint64(ints[4]),
		Size:  uint64(ints[5]),
		Atime: ints[6],
		Mtime: ints[7],
	}, nil
}

// splitDGUTLine trims the \n from line and splits it in to 8 columns.
func splitDGUTLine(line string) ([]string, error) {
	line = strings.TrimSuffix(line, "\n")

	parts := strings.Split(line, "\t")
	if len(parts) != gutaDataCols {
		return nil, ErrInvalidFormat
	}

	return parts, nil
}

// gutLinePartsToInts takes the output of splitDGUTLine() and returns the last
// 7 columns as ints.
func gutLinePartsToInts(parts []string) ([]int64, error) {
	ints := make([]int64, gutaDataIntCols)

	var err error

	if ints[0], err = strconv.ParseInt(parts[1], 10, 32); err != nil {
		return nil, ErrInvalidFormat
	}

	if ints[1], err = strconv.ParseInt(parts[2], 10, 32); err != nil {
		return nil, ErrInvalidFormat
	}

	if ints[2], err = strconv.ParseInt(parts[3], 10, 8); err != nil {
		return nil, ErrInvalidFormat
	}

	for i := 3; i < gutaDataIntCols; i++ {
		if ints[i], err = strconv.ParseInt(parts[i+1], 10, 64); err != nil {
			return nil, ErrInvalidFormat
		}
	}

	return ints, nil
}
