package summary

import (
	"bytes"
	"io"
	"io/fs"

	"github.com/wtsi-hgi/wrstat-ui/stats"
)

var (
	slash = []byte{'/'}
)

type outputOperator interface {
	Add(path string, info fs.FileInfo) error
	Output(output io.WriteCloser) error
}

// Operation is a callback that once added to a Paths will be called on each
// path encountered. It receives the absolute path to the filesystem entry, and
// the FileInfo returned by Statter.Lstat() on that path.
type Operation interface {
	New() Operation
	Add(info *stats.FileInfo) error
	Output() error
}

type Operations []Operation

func (o Operations) New() Operations {
	newOps := make(Operations, len(o))

	for n, op := range o {
		newOps[n] = op.New()
	}

	return newOps
}

func (o Operations) Add(s *stats.FileInfo) error {
	for _, op := range o {
		if err := op.Add(s); err != nil {
			return err
		}
	}

	return nil
}

func (o Operations) Output() error {
	for _, op := range o {
		if err := op.Output(); err != nil {
			return err
		}
	}

	return nil
}

type Directory struct {
	Parent     *Directory
	Operations Operations
}

func (d Directory) HandleInfo(s *stats.FileInfo) error {
	if err := d.Operations.Add(s); err != nil {
		return err
	}

	if d.Parent == nil {
		return nil
	}

	return d.Parent.HandleInfo(s)
}

func (d Directory) Close() error {
	return d.Operations.Output()
}

type Summariser struct {
	statsParser *stats.StatsParser
	operations  []Operation
}

func NewSummariser(p *stats.StatsParser) *Summariser {
	return &Summariser{
		statsParser: p,
	}
}

func (s *Summariser) AddOperation(op Operation) {
	s.operations = append(s.operations, op)
}

func (s *Summariser) Summarise() error {
	info := new(stats.FileInfo)

	var currentPath []byte

	directory := &Directory{
		Operations: s.operations,
	}

	for s.statsParser.Scan(info) == nil {
		if !bytes.HasPrefix(info.Path, currentPath) {
			for ; !bytes.HasPrefix(info.Path, currentPath); currentPath = parentDir(currentPath) {
				if err := directory.Close(); err != nil {
					return err
				}

				directory = directory.Parent
			}

			currentPath = bytes.Clone(currentPath)
		}

		if bytes.HasSuffix(info.Path, slash) && bytes.HasPrefix(info.Path, currentPath) {
			directory = &Directory{
				Parent:     directory,
				Operations: directory.Operations.New(),
			}

			currentPath = bytes.Clone(info.Path)
		}

		if err := directory.HandleInfo(info); err != nil {
			return err
		}
	}

	for directory != nil {
		if err := directory.Close(); err != nil {
			return err
		}

		directory = directory.Parent
	}

	return s.statsParser.Err()
}

func parentDir(path []byte) []byte {
	path = path[:len(path)-1]
	nextSlash := bytes.LastIndex(path, slash)

	return path[:nextSlash+1]
}
