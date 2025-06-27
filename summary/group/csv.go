package group

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
		"directory",
		"instruction ['backup' or 'nobackup' or 'tempbackup']",
		"file_types_backup",
		"file_types_ignore",
	}
	defaultMatch      = strings.SplitSeq("*", " ")
	ErrHeaderNotFound = errors.New("header not found")
	ErrTooFewColumns  = errors.New("too few columns")
	ErrInvalidAction  = errors.New("invalid action")
)

const (
	colName = iota
	colRoot
	colRequestor
	colDirectory
	colAction
	colFileTypes
	colFileTypesIgnore
)

type headers [len(csvHeaders)]int

type Line struct {
	Path []byte
	action
	requestor string
	name      string
	root      string
}

func newLine(line []string, headers headers, action action, filetype string) *Line {
	return &Line{
		Path:      []byte(filepath.Join(line[headers[colDirectory]], filetype)),
		action:    action,
		requestor: line[headers[colRequestor]],
		name:      line[headers[colName]],
		root:      line[headers[colRoot]],
	}
}

func (l *Line) Action() action {
	if l == nil {
		return actionWarn
	}

	return l.action
}

func ParseCSV(r io.Reader) ([]*Line, error) {
	cr := csv.NewReader(r)

	headers, maxHeader, err := parseHeaders(cr)
	if err != nil {
		return nil, err
	}

	lines := make([]*Line, 0)

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

func processLine(lines []*Line, line []string, headers headers) ([]*Line, error) {
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

		if toMatch := strings.TrimSpace(line[colFileTypes]); toMatch != "" {
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
