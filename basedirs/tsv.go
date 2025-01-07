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

type ConfigAttrs struct {
	Prefix  []string
	Splits  int
	MinDirs int
}

func (c *ConfigAttrs) PathShouldOutput(path *summary.DirectoryPath) bool {
	return path.Depth >= c.MinDirs && path.Depth < c.MinDirs+c.Splits
}

func (c *ConfigAttrs) Match(path *summary.DirectoryPath) bool {
	if path.Depth < len(c.Prefix) {
		return false
	}

	for path.Depth > len(c.Prefix) {
		path = path.Parent
	}

	if !strings.HasPrefix(path.Name, c.Prefix[len(c.Prefix)-1]) {
		return false
	}

	for n := len(c.Prefix) - 2; n >= 0; n-- {
		path = path.Parent

		if c.Prefix[n] != path.Name {
			return false
		}
	}

	return true
}

type Config []ConfigAttrs

var (
	ErrBadTSV = errors.New("bad TSV")

	newLineByte = []byte{'\n'} //nolint:gochecknoglobals
	tabByte     = []byte{'\t'} //nolint:gochecknoglobals
)

const (
	numColumns = 3
	noMatch    = -1

	DefaultSplits  = 1
	defaultMinDirs = 2
)

// ParseConfig reads basedirs configuration, which is a TSV in the following
// format:
//
// PREFIX	SPLITS	MINDIRS.
func ParseConfig(r io.Reader) (Config, error) {
	b := bufio.NewReader(r)

	var ( //nolint:prealloc
		result []ConfigAttrs
		end    bool
	)

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

	sort.Slice(result, func(i, j int) bool {
		return len(result[i].Prefix) > len(result[j].Prefix)
	})

	return result, nil
}

func parseLine(line []byte) (ConfigAttrs, error) {
	attr := bytes.Split(line, tabByte)
	if len(attr) != numColumns {
		return ConfigAttrs{}, ErrBadTSV
	}

	prefix := string(attr[0])

	splits, err := strconv.ParseUint(string(attr[1]), 10, 0)
	if err != nil {
		return ConfigAttrs{}, err
	}

	minDirs, err := strconv.ParseUint(string(attr[2]), 10, 0)
	if err != nil {
		return ConfigAttrs{}, err
	}

	return ConfigAttrs{
		Prefix:  split.SplitPath(prefix),
		Splits:  int(splits),
		MinDirs: int(minDirs),
	}, nil
}

func (c Config) PathShouldOutput(path *summary.DirectoryPath) bool {
	for _, ca := range c {
		if ca.Match(path) {
			return path.Depth >= ca.MinDirs && path.Depth < ca.MinDirs+ca.Splits
		}
	}

	return false
}
