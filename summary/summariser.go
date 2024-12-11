package summary

import (
	"bytes"
	"slices"
	"strings"

	"github.com/wtsi-hgi/wrstat-ui/internal/split"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

var slash = []byte{'/'}

const (
	MaxPathLen                = 4096
	probableMaxDirectoryDepth = 128
)

type DirectoryPath struct {
	Name   string
	Depth  int
	Parent *DirectoryPath
}

func (d *DirectoryPath) AppendTo(p []byte) []byte {
	if d == nil {
		return nil
	}

	return append(d.Parent.AppendTo(p), d.Name...)
}

func (d *DirectoryPath) Len() int {
	if d == nil {
		return 0
	}

	return d.Parent.Len() + len(d.Name)
}

func (d *DirectoryPath) Less(e *DirectoryPath) bool {
	if d.Depth < e.Depth {
		return d.compare(e.getDepth(d.Depth)) != 1
	} else if d.Depth > e.Depth {
		return d.getDepth(e.Depth).compare(e) == -1
	}

	return d.compare(e) == -1
}

func (d *DirectoryPath) getDepth(n int) *DirectoryPath {
	for d.Depth != n {
		d = d.Parent
	}

	return d
}

func (d *DirectoryPath) compare(e *DirectoryPath) int {
	if d == e {
		return 0
	}

	cmp := d.Parent.compare(e.Parent)

	if cmp == 0 {
		return strings.Compare(d.Name[:len(d.Name)-1], e.Name[:len(e.Name)-1])
	}

	return cmp
}

type FileInfo struct {
	Path      *DirectoryPath
	Name      []byte
	Size      int64
	UID       uint32
	GID       uint32
	MTime     int64
	ATime     int64
	CTime     int64
	EntryType byte
}

func (f *FileInfo) IsDir() bool {
	return f.EntryType == stats.DirType
}

// Operation is a type that receives file information either for a directory,
// and it's descendants, or for an entire tree.
type Operation interface {
	// Add is called once for the containing directory and for each of its
	// descendents during a Summariser.Summarise() call.
	Add(info *FileInfo) error

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

func (d directory) Add(s *FileInfo) error {
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

// OperationGenerator is used to generate an Operation for a
// Summariser.Summarise() run.
//
// Will be called a single time as a Global Operator and multiple times as a
// Directory Operator.
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

func (d directories) Add(info *FileInfo) error {
	for _, o := range d {
		if err := o.Add(info); err != nil {
			return err
		}
	}

	return nil
}

func (d directories) Output() error {
	for _, o := range slices.Backward(d) {
		if err := o.Output(); err != nil {
			return err
		}
	}

	return nil
}

// Summariser provides methods to register Operators that act on FileInfo
// entries in a tree.
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
	statsInfo := new(stats.FileInfo)

	directories := make(directories, 0, probableMaxDirectoryDepth)
	global := s.globalOperations.Generate()

	var (
		currentDir *DirectoryPath
		err        error
		info       FileInfo
	)

	for s.statsParser.Scan(statsInfo) == nil {
		directories, currentDir, err = s.changeToWorkingDirectoryOfEntry(directories, currentDir, statsInfo)
		if err != nil {
			return err
		}

		info = FileInfo{
			Path:      currentDir,
			Name:      statsInfo.BaseName(),
			Size:      statsInfo.Size,
			UID:       statsInfo.UID,
			GID:       statsInfo.GID,
			MTime:     statsInfo.MTime,
			ATime:     statsInfo.ATime,
			CTime:     statsInfo.CTime,
			EntryType: statsInfo.EntryType,
		}

		if err = global.Add(&info); err != nil {
			return err
		}

		if err = directories.Add(&info); err != nil {
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

func (s *Summariser) changeToWorkingDirectoryOfEntry(directories directories, currentDir *DirectoryPath, info *stats.FileInfo) (directories, *DirectoryPath, error) {
	var err error

	if currentDir != nil {
		depth := bytes.Count(info.Path[:len(info.Path)-1], slash)

		directories, currentDir, err = s.changeToAscendantDirectoryOfEntry(directories, currentDir, depth)
		if err != nil {
			return nil, nil, err
		}
	}

	if info.EntryType == stats.DirType {
		directories, currentDir = s.changeToDirectoryOfEntry(directories, currentDir, info)
	}

	return directories, currentDir, nil
}

func (s *Summariser) changeToAscendantDirectoryOfEntry(directories directories, currentDir *DirectoryPath, depth int) (directories, *DirectoryPath, error) {
	for currentDir.Depth >= depth {
		currentDir = currentDir.Parent

		if err := directories[len(directories)-1].Output(); err != nil {
			return nil, nil, err
		}

		directories = directories[:len(directories)-1]
	}

	return directories, currentDir, nil
}

func parentDir(path []byte) []byte {
	path = path[:len(path)-1]
	nextSlash := bytes.LastIndex(path, slash)

	return path[:nextSlash+1]
}

func (s *Summariser) changeToDirectoryOfEntry(directories directories, currentDir *DirectoryPath,
	info *stats.FileInfo) (directories, *DirectoryPath) {
	if cap(directories) > len(directories) {
		directories = directories[:len(directories)+1]

		if directories[len(directories)-1] == nil {
			directories[len(directories)-1] = s.directoryOperations.Generate()
		}
	} else {
		directories = append(directories, s.directoryOperations.Generate())
	}

	if currentDir == nil {
		currentDir = &DirectoryPath{
			Name:  "/",
			Depth: 0,
		}

		for _, part := range split.SplitPath(string(info.Path)) {
			currentDir = &DirectoryPath{
				Name:   part,
				Depth:  currentDir.Depth + 1,
				Parent: currentDir,
			}
		}
	} else {
		currentDir = &DirectoryPath{
			Name:   string(info.BaseName()),
			Depth:  currentDir.Depth + 1,
			Parent: currentDir,
		}
	}

	return directories, currentDir
}
