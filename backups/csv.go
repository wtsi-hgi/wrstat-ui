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

package backups

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
)

//nolint:gochecknoglobals
var (
	csvHeaders = [...]string{
		"reporting_name",
		"reporting_root",
		"requestor",
		"faculty",
		"directory",
		"instruction ['backup' or 'nobackup' or 'tempbackup']",
		"match",
		"ignore",
	}
	defaultMatch      = slices.Values([]string{"*"})
	ErrHeaderNotFound = errors.New("header not found")
	ErrTooFewColumns  = errors.New("too few columns")
	ErrInvalidAction  = errors.New("invalid action")
)

const (
	colName = iota
	colRoot
	colRequestor
	colFaculty
	colDirectory
	colAction
	colMatch
	colIgnore
)

type headers [len(csvHeaders)]int

// Action represents the action for a parsed ReportLine.
type Action uint8

// Allowed Action states.
const (
	ActionWarn Action = iota
	ActionNoBackup
	ActionTempBackup
	ActionBackup
)

const (
	actionWarnStr       = "warn"
	actionNoBackupStr   = "nobackup"
	actionTempBackupStr = "tempbackup"
	actionBackupStr     = "backup"
)

//nolint:gochecknoglobals
var (
	actionWarnJSON       = []byte("\"" + actionWarnStr + "\"")
	actionNoBackupJSON   = []byte("\"" + actionNoBackupStr + "\"")
	actionTempBackupJSON = []byte("\"" + actionTempBackupStr + "\"")
	actionBackupJSON     = []byte("\"" + actionBackupStr + "\"")
)

// String returns the string representation of the Action.
func (a Action) String() string {
	switch a {
	case ActionWarn:
		return actionWarnStr
	case ActionNoBackup:
		return actionNoBackupStr
	case ActionTempBackup:
		return actionTempBackupStr
	case ActionBackup:
		return actionBackupStr
	}

	return "invalid action"
}

// MarshalJSON implements the json.Marshaler interface.
func (a Action) MarshalJSON() ([]byte, error) {
	switch a {
	case ActionWarn:
		return actionWarnJSON, nil
	case ActionNoBackup:
		return actionNoBackupJSON, nil
	case ActionTempBackup:
		return actionTempBackupJSON, nil
	case ActionBackup:
		return actionBackupJSON, nil
	}

	return nil, ErrInvalidAction
}

// ReportLine is a parsed entry of a Backup plan CSV.
//
// NB: A Backup Action row can be split into multiple ReportLines as described
// in the comment for ParseCSV.
type ReportLine struct {
	Path      []byte
	Action    Action
	Requestor string
	Name      string
	Faculty   string
	Root      string
}

func newLine(line []string, headers headers, action Action, filetype string) *ReportLine {
	return &ReportLine{
		Path:      []byte(filepath.Join(line[headers[colDirectory]], filetype)),
		Action:    action,
		Requestor: line[headers[colRequestor]],
		Name:      line[headers[colName]],
		Faculty:   line[headers[colFaculty]],
		Root:      filepath.Clean(line[headers[colRoot]]),
	}
}

// ParseCSV parses the given reader for a valid backup plan.
//
// The following headers must be in the first row (the order doesn't matter):
//
//	reporting_name
//	reporting_root
//	requestor
//	faculty
//	directory
//	instruction ['backup' or 'nobackup' or 'tempbackup']
//	match
//	ignore
//
// Where `match` and ignore are space-separated lists of * wildcarded values to
// match against, such as *.txt. A blank `match` is treated as `*`. Wildcards
// match `/`, so directory `/a/b` and match `*.txt` will match
// `/a/b/c/file.txt`.
//
// (Temp)Backup rows with multiple wildcards are split into multiple lines, and
// any ignore entries also become their own line.
func ParseCSV(r io.Reader) ([]*ReportLine, error) { //nolint:gocognit
	cr := csv.NewReader(r)

	h, maxHeader, err := parseHeaders(cr)
	if err != nil {
		return nil, err
	}

	lines := make([]*ReportLine, 0)

	for {
		line, err := cr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		if len(line) < maxHeader {
			return nil, ErrTooFewColumns
		}

		if lines, err = processLine(lines, line, h); err != nil {
			return nil, err
		}
	}

	return lines, nil
}

func parseHeaders(cr *csv.Reader) (headers, int, error) {
	var headers [len(csvHeaders)]int

	line, err := cr.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}

		return headers, 0, err
	}

	maxHeader := 0

	for n, header := range csvHeaders {
		pos := slices.Index(line, header)
		if pos == -1 {
			return headers, 0, fmt.Errorf("%s: %w", header, ErrHeaderNotFound)
		}

		headers[n] = pos

		maxHeader = max(maxHeader, pos)
	}

	return headers, maxHeader, nil
}

func processLine(lines []*ReportLine, line []string, headers headers) ([]*ReportLine, error) { //nolint:gocognit,gocyclo
	for n := range line {
		line[n] = strings.TrimSpace(line[n])
	}

	a, err := parseAction(line[headers[colAction]])
	if err != nil {
		return nil, err
	}

	match := defaultMatch

	if a != ActionNoBackup { //nolint:nestif
		if ignore := strings.TrimSpace(line[headers[colIgnore]]); ignore != "" {
			for ft := range strings.SplitSeq(ignore, " ") {
				lines = append(lines, newLine(line, headers, ActionNoBackup, ft))
			}
		}

		if toMatch := strings.TrimSpace(line[headers[colMatch]]); toMatch != "" {
			match = strings.SplitSeq(toMatch, " ")
		}
	}

	for ft := range match {
		lines = append(lines, newLine(line, headers, a, ft))
	}

	return lines, nil
}

func parseAction(actionStr string) (Action, error) {
	var a Action

	switch actionStr {
	case "backup":
		a = ActionBackup
	case "tempbackup":
		a = ActionTempBackup
	case "nobackup":
		a = ActionNoBackup
	default:
		return 0, fmt.Errorf("%s: %w", actionStr, ErrInvalidAction)
	}

	return a, nil
}
