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

package dirguta

import (
	"encoding/binary"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
)

// typeCheckers take a path and return true if the path is of their file type.
type typeChecker func(path string) bool

var typeCheckers = map[db.DirGUTAFileType]typeChecker{
	db.DGUTAFileTypeVCF:        isVCF,
	db.DGUTAFileTypeVCFGz:      isVCFGz,
	db.DGUTAFileTypeBCF:        isBCF,
	db.DGUTAFileTypeSam:        isSam,
	db.DGUTAFileTypeBam:        isBam,
	db.DGUTAFileTypeCram:       isCram,
	db.DGUTAFileTypeFasta:      isFasta,
	db.DGUTAFileTypeFastq:      isFastq,
	db.DGUTAFileTypeFastqGz:    isFastqGz,
	db.DGUTAFileTypePedBed:     isPedBed,
	db.DGUTAFileTypeCompressed: isCompressed,
	db.DGUTAFileTypeText:       isText,
	db.DGUTAFileTypeLog:        isLog,
}

type Error string

func (e Error) Error() string { return string(e) }

var (
	tmpSuffixes        = [...]string{".tmp", ".temp"}                                          //nolint:gochecknoglobals
	tmpPaths           = [...]string{"tmp", "temp"}                                            //nolint:gochecknoglobals
	tmpPrefixes        = [...]string{".tmp.", "tmp.", ".temp.", "temp."}                       //nolint:gochecknoglobals
	fastASuffixes      = [...]string{".fasta", ".fa"}                                          //nolint:gochecknoglobals
	fastQSuffixes      = [...]string{".fastq", ".fq"}                                          //nolint:gochecknoglobals
	fastQQZSuffixes    = [...]string{".fastq.gz", ".fq.gz"}                                    //nolint:gochecknoglobals
	pedBedSuffixes     = [...]string{".ped", ".map", ".bed", ".bim", ".fam"}                   //nolint:gochecknoglobals
	compressedSuffixes = [...]string{".bzip2", ".gz", ".tgz", ".zip", ".xz", ".bgz"}           //nolint:gochecknoglobals
	textSuffixes       = [...]string{".csv", ".tsv", ".txt", ".text", ".md", ".dat", "readme"} //nolint:gochecknoglobals
	logSuffixes        = [...]string{".log", ".out", ".o", ".err", ".e", ".oe"}                //nolint:gochecknoglobals
)

const (
	maxNumOfGUTAKeys = 34
	lengthOfGUTAKey  = 12
)

var gutaKey = sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		return new([maxNumOfGUTAKeys]GUTAKey)
	},
}

// gutaStore is a sortable map with gid,uid,filetype,age as keys and
// summaryWithAtime as values.
type gutaStore struct {
	sumMap  map[GUTAKey]*summary.SummaryWithTimes
	refTime int64
}

// add will auto-vivify a summary for the given key (which should have been
// generated with statToGUTAKey()) and call add(size, atime, mtime) on it.
func (store gutaStore) add(gkey GUTAKey, size int64, atime int64, mtime int64) {
	if !gkey.Age.FitsAgeInterval(atime, mtime, store.refTime) {
		return
	}

	s, ok := store.sumMap[gkey]
	if !ok {
		s = new(summary.SummaryWithTimes)
		store.sumMap[gkey] = s
	}

	s.Add(size, atime, mtime)
}

// sort returns a slice of our summaryWithAtime values, sorted by our dguta keys
// which are also returned.
func (store gutaStore) sort() GUTAKeys {
	keys := GUTAKeys(slices.Collect(maps.Keys(store.sumMap)))

	sort.Sort(keys)

	return keys
}

// dirToGUTAStore is a sortable map of directory to gutaStore.
type dirToGUTAStore struct {
	gsMap   map[string]gutaStore
	refTime int64
}

// getGUTAStore auto-vivifies a gutaStore for the given dir and returns it.
func (store dirToGUTAStore) getGUTAStore(dir string) gutaStore {
	gStore, ok := store.gsMap[dir]
	if !ok {
		gStore = gutaStore{make(map[GUTAKey]*summary.SummaryWithTimes), store.refTime}
		store.gsMap[dir] = gStore
	}

	return gStore
}

// sort returns a slice of our gutaStore values, sorted by our directory keys
// which are also returned.
func (store dirToGUTAStore) sort() ([]string, []gutaStore) {
	keys := make([]string, len(store.gsMap))
	i := 0

	for k := range store.gsMap {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]gutaStore, len(keys))

	for i, k := range keys {
		s[i] = store.gsMap[k]
	}

	return keys, s
}

// isTemp tells you if path is named like a temporary file.
func isTempFile(name string) bool {
	if hasOneOfSuffixes(name, tmpSuffixes[:]) {
		return true
	}

	for _, prefix := range tmpPrefixes {
		if len(name) < len(prefix) {
			break
		}

		if caseInsensitiveCompare(name[:len(prefix)], prefix) {
			return true
		}
	}

	return false
}

func isTempDir(path *summary.DirectoryPath) bool {
	for path != nil {
		name := path.Name

		if name[len(name)-1] == '/' {
			name = name[:len(name)-1]
		}

		if hasOneOfSuffixes(name, tmpSuffixes[:]) {
			return true
		}

		for _, containing := range tmpPaths {
			if len(name) != len(containing) {
				continue
			}

			if caseInsensitiveCompare(name, containing) {
				return true
			}
		}

		for _, prefix := range tmpPrefixes {
			if len(name) < len(prefix) {
				break
			}

			if caseInsensitiveCompare(name[:len(prefix)], prefix) {
				return true
			}
		}

		path = path.Parent
	}

	return false
}

// hasOneOfSuffixes tells you if path has one of the given suffixes.
func hasOneOfSuffixes(path string, suffixes []string) bool {
	for _, suffix := range suffixes {
		if hasSuffix(path, suffix) {
			return true
		}
	}

	return false
}

// isVCF tells you if path is named like a vcf file.
func isVCF(path string) bool {
	return hasSuffix(path, ".vcf")
}

// caseInsensitiveCompare compares to equal length string for a case insensitive
// match.
func caseInsensitiveCompare(a, b string) bool {
	for n := len(a) - 1; n >= 0; n-- {
		if charToLower(a[n]) != charToLower(b[n]) {
			return false
		}
	}

	return true
}

// charToLower returns the lowercase form of an ascii letter passed to it,
// returning any other character unmodified.
func charToLower(char byte) byte {
	if char >= 'A' && char <= 'Z' {
		char += 'a' - 'A'
	}

	return char
}

// hasSuffix tells you if path has the given suffix.
func hasSuffix(path, suffix string) bool {
	if len(path) < len(suffix) {
		return false
	}

	return caseInsensitiveCompare(path[len(path)-len(suffix):], suffix)
}

// isVCFGz tells you if path is named like a vcf.gz file.
func isVCFGz(path string) bool {
	return hasSuffix(path, ".vcf.gz")
}

// isBCF tells you if path is named like a bcf file.
func isBCF(path string) bool {
	return hasSuffix(path, ".bcf")
}

// isSam tells you if path is named like a sam file.
func isSam(path string) bool {
	return hasSuffix(path, ".sam")
}

// isBam tells you if path is named like a bam file.
func isBam(path string) bool {
	return hasSuffix(path, ".bam")
}

// isCram tells you if path is named like a cram file.
func isCram(path string) bool {
	return hasSuffix(path, ".cram")
}

// isFasta tells you if path is named like a fasta file.
func isFasta(path string) bool {
	return hasOneOfSuffixes(path, fastASuffixes[:])
}

// isFastq tells you if path is named like a fastq file.
func isFastq(path string) bool {
	return hasOneOfSuffixes(path, fastQSuffixes[:])
}

// isFastqGz tells you if path is named like a fastq.gz file.
func isFastqGz(path string) bool {
	return hasOneOfSuffixes(path, fastQQZSuffixes[:])
}

// isPedBed tells you if path is named like a ped/bed file.
func isPedBed(path string) bool {
	return hasOneOfSuffixes(path, pedBedSuffixes[:])
}

// isCompressed tells you if path is named like a compressed file.
func isCompressed(path string) bool {
	if isFastqGz(path) || isVCFGz(path) {
		return false
	}

	return hasOneOfSuffixes(path, compressedSuffixes[:])
}

// isText tells you if path is named like some standard text file.
func isText(path string) bool {
	return hasOneOfSuffixes(path, textSuffixes[:])
}

// isLog tells you if path is named like some standard log file.
func isLog(path string) bool {
	return hasOneOfSuffixes(path, logSuffixes[:])
}

type DB interface {
	Add(db.RecordDGUTA) error
}

// DirGroupUserTypeAge is used to summarise file stats by directory, group,
// user, file type and age.
type DirGroupUserTypeAge struct {
	db       DB
	store    gutaStore
	thisDir  *summary.DirectoryPath
	children []string
}

// NewDirGroupUserTypeAge returns a DirGroupUserTypeAge.
func NewDirGroupUserTypeAge(db DB) summary.OperationGenerator {
	return newDirGroupUserTypeAge(db, time.Now().Unix())
}

func newDirGroupUserTypeAge(db DB, refTime int64) summary.OperationGenerator {
	return func() summary.Operation {
		return &DirGroupUserTypeAge{
			db:    db,
			store: gutaStore{make(map[GUTAKey]*summary.SummaryWithTimes), refTime},
		}
	}
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will break path in to
// its directories and add the file size, increment the file count to each,
// summed for the info's group, user, filetype and age. It will also record the
// oldest file access time for each directory, plus the newest modification
// time.
//
// If path is a directory, its access time is treated as now, so that when
// interested in files that haven't been accessed in a long time, directories
// that haven't been manually visted in a longer time don't hide the "real"
// results.
//
// "Access" times are actually considered to be the greatest of atime, mtime and
// unix epoch.
//
// NB: the "temp" filetype is an extra filetype on top of the other normal
// filetypes, so if you sum all the filetypes to get information about a given
// directory+group+user combination, you should ignore "temp". Only count "temp"
// when it's the only type you're considering, or you'll count some files twice.
func (d *DirGroupUserTypeAge) Add(info *summary.FileInfo) error {
	if d.thisDir == nil {
		d.thisDir = info.Path
	}

	if info.IsDir() && info.Path != nil && info.Path.Parent == d.thisDir {
		d.children = append(d.children, string(info.Name))
	}

	atime := info.ATime

	if info.IsDir() {
		atime = time.Now().Unix()
	}

	gutaKeysA := gutaKey.Get().(*[maxNumOfGUTAKeys]GUTAKey) //nolint:errcheck,forcetypeassert
	gutaKeys := GUTAKeys(gutaKeysA[:0])

	filetype, isTmp := InfoToType(info)

	gutaKeys.append(info.GID, info.UID, filetype)

	if isTmp {
		gutaKeys.append(info.GID, info.UID, db.DGUTAFileTypeTemp)
	}

	d.addForEach(gutaKeys, info.Size, atime, maxInt(0, info.MTime))

	gutaKey.Put(gutaKeysA)

	return nil
}

func InfoToType(info *summary.FileInfo) (db.DirGUTAFileType, bool) {
	var (
		isTmp    bool
		filetype db.DirGUTAFileType
	)

	if info.IsDir() {
		filetype = db.DGUTAFileTypeDir
	} else {
		filetype, isTmp = filenameToType(string(info.Name))
	}

	if !isTmp {
		isTmp = isTempDir(info.Path)
	}

	return filetype, isTmp
}

type GUTAKey struct {
	GID, UID uint32
	FileType db.DirGUTAFileType
	Age      db.DirGUTAge
}

type GUTAKeys []GUTAKey

func (g GUTAKeys) Len() int {
	return len(g)
}

func (g GUTAKeys) Less(i, j int) bool {
	if g[i].GID < g[j].GID {
		return true
	}

	if g[i].GID > g[j].GID {
		return false
	}

	if g[i].UID < g[j].UID {
		return true
	}

	if g[i].UID > g[j].UID {
		return false
	}

	if g[i].FileType < g[j].FileType {
		return true
	}

	if g[i].FileType > g[j].FileType {
		return false
	}

	return g[i].Age < g[j].Age
}

func (g GUTAKeys) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

func gutaKeyFromString(key string) GUTAKey {
	dgutaBytes := unsafe.Slice(unsafe.StringData(key), len(key))

	return GUTAKey{
		GID:      binary.BigEndian.Uint32(dgutaBytes[:4]),
		UID:      binary.BigEndian.Uint32(dgutaBytes[4:8]),
		FileType: db.DirGUTAFileType(dgutaBytes[8]),
		Age:      db.DirGUTAge(dgutaBytes[9]),
	}
}

func (g GUTAKey) String() string {
	var a [lengthOfGUTAKey]byte

	binary.BigEndian.PutUint32(a[:4], g.GID)
	binary.BigEndian.PutUint32(a[4:8], g.UID)
	a[8] = uint8(g.FileType)
	a[9] = uint8(g.Age)

	return unsafe.String(&a[0], len(a))
}

// appendGUTAKeys appends gutaKeys with keys including the given gid, uid, file
// type and age.
func (g *GUTAKeys) append(gid, uid uint32, fileType db.DirGUTAFileType) {
	for _, age := range db.DirGUTAges {
		*g = append(*g, GUTAKey{gid, uid, fileType, age})
	}
}

// maxInt returns the greatest of the inputs.
func maxInt(ints ...int64) int64 {
	var max int64

	for _, i := range ints {
		if i > max {
			max = i
		}
	}

	return max
}

// pathToTypes determines the filetype of the given path based on its basename,
// and returns a slice of our DirGUTAFileType. More than one is possible,
// because a path can be both a temporary file, and another type.
func filenameToType(name string) (db.DirGUTAFileType, bool) {
	isTmp := isTempFile(name)

	for ftype, isThisType := range typeCheckers {
		if isThisType(name) {
			return ftype, isTmp
		}
	}

	return db.DGUTAFileTypeOther, isTmp
}

// addForEach breaks path into each directory, gets a gutaStore for each and
// adds a file of the given size to them under the given gutaKeys.
func (d *DirGroupUserTypeAge) addForEach(gutaKeys []GUTAKey, size int64, atime int64, mtime int64) {
	for _, gutaKey := range gutaKeys {
		d.store.add(gutaKey, size, atime, mtime)
	}
}

type DirGUTA struct {
	Path *summary.DirectoryPath
}

// Output will write summary information for all the paths previously added. The
// format is (tab separated):
//
// directory gid uid filetype age filecount filesize atime mtime
//
// Where atime is oldest access time in seconds since Unix epoch of any file
// nested within directory. mtime is similar, but the newest modification time.
//
// age is one of our age ints:
//
//		 0 = all ages
//		 1 = older than one month according to atime
//		 2 = older than two months according to atime
//		 3 = older than six months according to atime
//		 4 = older than one year according to atime
//		 5 = older than two years according to atime
//		 6 = older than three years according to atime
//		 7 = older than five years according to atime
//		 8 = older than seven years according to atime
//		 9 = older than one month according to mtime
//		10 = older than two months according to mtime
//		11 = older than six months according to mtime
//		12 = older than one year according to mtime
//		13 = older than two years according to mtime
//		14 = older than three years according to mtime
//	 15 = older than five years according to mtime
//		16 = older than seven years according to mtime
//
// directory, gid, uid, filetype and age are sorted. The sort on the columns is
// not numeric, but alphabetical. So gid 10 will come before gid 2.
//
// filetype is one of our filetype ints:
//
//	 0 = other (not any of the others below)
//	 1 = temp (.tmp | temp suffix, or .tmp. | .temp. | tmp. | temp. prefix, or
//	           a directory in its path is named "tmp" or "temp")
//	 2 = vcf
//	 3 = vcf.gz
//	 4 = bcf
//	 5 = sam
//	 6 = bam
//	 7 = cram
//	 8 = fasta (.fa | .fasta suffix)
//	 9 = fastq (.fq | .fastq suffix)
//	10 = fastq.gz (.fq.gz | .fastq.gz suffix)
//	11 = ped/bed (.ped | .map | .bed | .bim | .fam suffix)
//	12 = compresed (.bzip2 | .gz | .tgz | .zip | .xz | .bgz suffix)
//	13 = text (.csv | .tsv | .txt | .text | .md | .dat | readme suffix)
//	14 = log (.log | .out | .o | .err | .e | .err | .oe suffix)
//
// Returns an error on failure to write.
func (d *DirGroupUserTypeAge) Output() error {
	dgutas := d.store.sort()

	dguta := db.RecordDGUTA{
		Dir:      d.thisDir,
		Children: d.children,
	}

	for _, guta := range dgutas {
		s := d.store.sumMap[guta]

		dguta.GUTAs = append(dguta.GUTAs, &db.GUTA{
			GID:   guta.GID,
			UID:   guta.UID,
			FT:    guta.FileType,
			Age:   guta.Age,
			Count: uint64(s.Count),
			Size:  uint64(s.Size),
			Atime: s.Atime,
			Mtime: s.Mtime,
		})
	}

	if err := d.db.Add(dguta); err != nil {
		return err
	}

	for k := range d.store.sumMap {
		delete(d.store.sumMap, k)
	}

	d.thisDir = nil
	d.children = nil

	return nil
}
