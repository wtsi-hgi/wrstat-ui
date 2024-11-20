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
)

type ConfigAttrs struct {
	Prefix  string
	Score   int
	Splits  int
	MinDirs int
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
		result Config
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
		return result[i].Score > result[j].Score
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
		Prefix:  prefix,
		Score:   strings.Count(prefix, "/"),
		Splits:  int(splits),
		MinDirs: int(minDirs),
	}, nil
}

func (c *Config) splitFn() split.SplitFn {
	return func(path string) int {
		return c.findBestMatch(path).Splits
	}
}

func (c *Config) findBestMatch(path string) ConfigAttrs {
	for _, p := range *c {
		if strings.HasPrefix(path, p.Prefix) {
			return p
		}
	}

	return ConfigAttrs{
		Splits:  DefaultSplits,
		MinDirs: defaultMinDirs,
	}
}
