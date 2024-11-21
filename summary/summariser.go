package summary

import (
	"bytes"

	"github.com/wtsi-hgi/wrstat-ui/stats"
)

var (
	slash = []byte{'/'}
)

const (
	maxPathLen                = 4096
	probableMaxDirectoryDepth = 128
)

// Operation is
type Operation interface {
	// Add is called once for the containing directory and for each of its
	// descendents during a Summariser.Summarise() call.
	Add(info *stats.FileInfo) error

	// Output is called when we return to the parent directory during a
	// Summariser.Summarise() call, having processed all descendent entries.
	//
	// For an Operation working on a directory, this method must also do any
	// necessary reset of the instance of the type before returning.
	Output() error
}

// directory is a collection of the Operation that will be run for a directory,
// with Add being called for itself and every descendent FileInfo during
// Summariser.Summarise().
type directory []Operation

func (d directory) Add(s *stats.FileInfo) error {
	for _, op := range d {
		if err := op.Add(s); err != nil {
			return err
		}
	}

	return nil
}

func (d directory) Output() error {
	for _, op := range d {
		if err := op.Output(); err != nil {
			return err
		}
	}

	return nil
}

type OperationGenerator func() Operation

type operationGenerators []OperationGenerator

func (o operationGenerators) Generate() directory {
	ops := make(directory, len(o))

	for n, op := range o {
		ops[n] = op()
	}

	return ops
}

type directories []directory

func (d directories) Add(info *stats.FileInfo) error {
	for _, o := range d {
		if err := o.Add(info); err != nil {
			return err
		}
	}

	return nil
}

func (d directories) Output() error {
	for _, o := range d {
		if err := o.Output(); err != nil {
			return err
		}
	}

	return nil
}

type Summariser struct {
	statsParser         *stats.StatsParser
	directoryOperations operationGenerators
	globalOperations    operationGenerators
}

func NewSummariser(p *stats.StatsParser) *Summariser {
	return &Summariser{
		statsParser: p,
	}
}

func (s *Summariser) AddDirectoryOperation(op OperationGenerator) {
	s.directoryOperations = append(s.directoryOperations, op)
}

func (s *Summariser) AddGlobalOperation(op OperationGenerator) {
	s.globalOperations = append(s.globalOperations, op)
}

func (s *Summariser) Summarise() error {
	info := new(stats.FileInfo)

	currentDir := make([]byte, 0, maxPathLen)
	directories := make(directories, 1, probableMaxDirectoryDepth)
	directories[0] = s.directoryOperations.Generate()
	global := s.globalOperations.Generate()

	var err error

	for s.statsParser.Scan(info) == nil {
		if err = global.Add(info); err != nil {
			return err
		}

		directories, currentDir, err = s.changeToWorkingDirectoryOfEntry(directories, currentDir, info)
		if err != nil {
			return err
		}

		if err = directories.Add(info); err != nil {
			return err
		}
	}

	if err := s.statsParser.Err(); err != nil {
		return err
	}

	if err := directories.Output(); err != nil {
		return err
	}

	return global.Output()
}

func (s *Summariser) changeToWorkingDirectoryOfEntry(directories directories, currentDir []byte, info *stats.FileInfo) (directories, []byte, error) {
	var err error

	directories, currentDir, err = s.changeToAscendantDirectoryOfEntry(directories, currentDir, info)
	if err != nil {
		return nil, nil, err
	}

	if info.EntryType == stats.DirType {
		directories, currentDir = s.changeToDirectoryOfEntry(directories, currentDir, info)
	}

	return directories, currentDir, nil
}

func (s *Summariser) changeToAscendantDirectoryOfEntry(directories directories, currentDir []byte, info *stats.FileInfo) (directories, []byte, error) {
	for !bytes.HasPrefix(info.Path, currentDir) {
		if err := directories[len(directories)-1].Output(); err != nil {
			return nil, nil, err
		}

		directories = directories[:len(directories)-1]
		currentDir = parentDir(currentDir)
	}

	return directories, currentDir, nil
}

func parentDir(path []byte) []byte {
	path = path[:len(path)-1]
	nextSlash := bytes.LastIndex(path, slash)

	return path[:nextSlash+1]
}

func (s *Summariser) changeToDirectoryOfEntry(directories directories, currentDir []byte,
	info *stats.FileInfo) (directories, []byte) {
	if cap(directories) > len(directories) {
		directories = directories[:len(directories)+1]

		if directories[len(directories)-1] == nil {
			directories[len(directories)-1] = s.directoryOperations.Generate()
		}
	} else {
		directories = append(directories, s.directoryOperations.Generate())
	}

	currentDir = currentDir[:copy(currentDir[:cap(currentDir)], info.Path)]

	return directories, currentDir
}
