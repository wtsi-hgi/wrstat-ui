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

var (
	csvHeaders = [...]string{
		"reporting_name",
		"reporting_root",
		"requestor",
		"faculty",
		"directory",
		"instruction ['backup' or 'nobackup' or 'tempbackup']",
		"file_types_backup",
		"file_types_ignore",
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
	colFileTypes
	colFileTypesIgnore
)

type headers [len(csvHeaders)]int

type action uint8

const (
	actionWarn action = iota
	actionNoBackup
	actionTempBackup
	actionBackup

	maxActions
)

var (
	actionWarnStr       = []byte("\"warn\"")
	actionNoBackupStr   = []byte("\"nobackup\"")
	actionTempBackupStr = []byte("\"tempbackup\"")
	actionBackupStr     = []byte("\"backup\"")
)

func (a action) MarshalJSON() ([]byte, error) {
	switch a {
	case actionWarn:
		return actionWarnStr, nil
	case actionNoBackup:
		return actionNoBackupStr, nil
	case actionTempBackup:
		return actionTempBackupStr, nil
	case actionBackup:
		return actionBackupStr, nil
	}

	return nil, errors.New("invalid action")
}

type ReportLine struct {
	Path []byte
	action
	requestor string
	name      string
	faculty   string
	root      string
}

func newLine(line []string, headers headers, action action, filetype string) *ReportLine {
	return &ReportLine{
		Path:      []byte(filepath.Join(line[headers[colDirectory]], filetype)),
		action:    action,
		requestor: line[headers[colRequestor]],
		name:      line[headers[colName]],
		faculty:   line[headers[colFaculty]],
		root:      filepath.Clean(line[headers[colRoot]]),
	}
}

func (l *ReportLine) Action() action {
	if l == nil {
		return actionWarn
	}

	return l.action
}

func ParseCSV(r io.Reader) ([]*ReportLine, error) {
	cr := csv.NewReader(r)

	headers, maxHeader, err := parseHeaders(cr)
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

		if lines, err = processLine(lines, line, headers); err != nil {
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

func processLine(lines []*ReportLine, line []string, headers headers) ([]*ReportLine, error) {
	action, err := parseAction(line[headers[colAction]])
	if err != nil {
		return nil, err
	}

	match := defaultMatch

	if action != actionNoBackup {
		if ignore := strings.TrimSpace(line[headers[colFileTypesIgnore]]); ignore != "" {
			for ft := range strings.SplitSeq(ignore, " ") {
				lines = append(lines, newLine(line, headers, actionNoBackup, ft))
			}
		}

		if toMatch := strings.TrimSpace(line[headers[colFileTypes]]); toMatch != "" {
			match = strings.SplitSeq(toMatch, " ")
		}
	}

	for ft := range match {
		lines = append(lines, newLine(line, headers, action, ft))
	}

	return lines, nil
}

func parseAction(actionStr string) (action, error) {
	var action action

	switch actionStr {
	case "backup":
		action = actionBackup
	case "tempbackup":
		action = actionTempBackup
	case "nobackup":
		action = actionNoBackup
	default:
		return 0, fmt.Errorf("%s: %w", actionStr, ErrInvalidAction)
	}

	return action, nil
}
