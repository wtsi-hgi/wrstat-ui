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

	"github.com/wtsi-hgi/wrstat-ui/summary"
)

const (
	SecondsInAMonth = 2628000
	SecondsInAYear  = SecondsInAMonth * 12
)

var ageThresholds = [8]int64{ //nolint:gochecknoglobals
	SecondsInAMonth, SecondsInAMonth * 2, SecondsInAMonth * 6, SecondsInAYear,
	SecondsInAYear * 2, SecondsInAYear * 3, SecondsInAYear * 5, SecondsInAYear * 7,
}

// DirGUTAge is one of the age types that the
// directory,group,user,filetype,age summaries group on. All is for files of
// all ages. The AgeA* consider age according to access time. The AgeM* consider
// age according to modify time. The *\dM ones are age in the number of months,
// and the *\dY ones are in number of years.
type DirGUTAge uint8

const (
	DGUTAgeAll DirGUTAge = 0
	DGUTAgeA1M DirGUTAge = 1
	DGUTAgeA2M DirGUTAge = 2
	DGUTAgeA6M DirGUTAge = 3
	DGUTAgeA1Y DirGUTAge = 4
	DGUTAgeA2Y DirGUTAge = 5
	DGUTAgeA3Y DirGUTAge = 6
	DGUTAgeA5Y DirGUTAge = 7
	DGUTAgeA7Y DirGUTAge = 8
	DGUTAgeM1M DirGUTAge = 9
	DGUTAgeM2M DirGUTAge = 10
	DGUTAgeM6M DirGUTAge = 11
	DGUTAgeM1Y DirGUTAge = 12
	DGUTAgeM2Y DirGUTAge = 13
	DGUTAgeM3Y DirGUTAge = 14
	DGUTAgeM5Y DirGUTAge = 15
	DGUTAgeM7Y DirGUTAge = 16
)

var DirGUTAges = [17]DirGUTAge{ //nolint:gochecknoglobals
	DGUTAgeAll, DGUTAgeA1M, DGUTAgeA2M, DGUTAgeA6M, DGUTAgeA1Y,
	DGUTAgeA2Y, DGUTAgeA3Y, DGUTAgeA5Y, DGUTAgeA7Y, DGUTAgeM1M,
	DGUTAgeM2M, DGUTAgeM6M, DGUTAgeM1Y, DGUTAgeM2Y, DGUTAgeM3Y,
	DGUTAgeM5Y, DGUTAgeM7Y,
}

// DirGUTAFileType is one of the special file types that the
// directory,group,user,filetype,age summaries group on.
type DirGUTAFileType uint8

const (
	DGUTAFileTypeOther      DirGUTAFileType = 0
	DGUTAFileTypeTemp       DirGUTAFileType = 1
	DGUTAFileTypeVCF        DirGUTAFileType = 2
	DGUTAFileTypeVCFGz      DirGUTAFileType = 3
	DGUTAFileTypeBCF        DirGUTAFileType = 4
	DGUTAFileTypeSam        DirGUTAFileType = 5
	DGUTAFileTypeBam        DirGUTAFileType = 6
	DGUTAFileTypeCram       DirGUTAFileType = 7
	DGUTAFileTypeFasta      DirGUTAFileType = 8
	DGUTAFileTypeFastq      DirGUTAFileType = 9
	DGUTAFileTypeFastqGz    DirGUTAFileType = 10
	DGUTAFileTypePedBed     DirGUTAFileType = 11
	DGUTAFileTypeCompressed DirGUTAFileType = 12
	DGUTAFileTypeText       DirGUTAFileType = 13
	DGUTAFileTypeLog        DirGUTAFileType = 14
	DGUTAFileTypeDir        DirGUTAFileType = 15
)

var AllTypesExceptDirectories = []DirGUTAFileType{ //nolint:gochecknoglobals
	DGUTAFileTypeOther,
	DGUTAFileTypeTemp,
	DGUTAFileTypeVCF,
	DGUTAFileTypeVCFGz,
	DGUTAFileTypeBCF,
	DGUTAFileTypeSam,
	DGUTAFileTypeBam,
	DGUTAFileTypeCram,
	DGUTAFileTypeFasta,
	DGUTAFileTypeFastq,
	DGUTAFileTypeFastqGz,
	DGUTAFileTypePedBed,
	DGUTAFileTypeCompressed,
	DGUTAFileTypeText,
	DGUTAFileTypeLog,
}

// typeCheckers take a path and return true if the path is of their file type.
type typeChecker func(path string) bool

var typeCheckers = map[DirGUTAFileType]typeChecker{
	DGUTAFileTypeVCF:        isVCF,
	DGUTAFileTypeVCFGz:      isVCFGz,
	DGUTAFileTypeBCF:        isBCF,
	DGUTAFileTypeSam:        isSam,
	DGUTAFileTypeBam:        isBam,
	DGUTAFileTypeCram:       isCram,
	DGUTAFileTypeFasta:      isFasta,
	DGUTAFileTypeFastq:      isFastq,
	DGUTAFileTypeFastqGz:    isFastqGz,
	DGUTAFileTypePedBed:     isPedBed,
	DGUTAFileTypeCompressed: isCompressed,
	DGUTAFileTypeText:       isText,
	DGUTAFileTypeLog:        isLog,
}

type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrInvalidType = Error("not a valid file type")
	ErrInvalidAge  = Error("not a valid age")
)

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

// String lets you convert a DirGUTAFileType to a meaningful string.
func (d DirGUTAFileType) String() string {
	return [...]string{
		"other", "temp", "vcf", "vcf.gz", "bcf", "sam", "bam",
		"cram", "fasta", "fastq", "fastq.gz", "ped/bed", "compressed", "text",
		"log", "dir",
	}[d]
}

// FileTypeStringToDirGUTAFileType converts the String() representation of a
// DirGUTAFileType back in to a DirGUTAFileType. Errors if an invalid string
// supplied.
func FileTypeStringToDirGUTAFileType(ft string) (DirGUTAFileType, error) {
	convert := map[string]DirGUTAFileType{
		"other":      DGUTAFileTypeOther,
		"temp":       DGUTAFileTypeTemp,
		"vcf":        DGUTAFileTypeVCF,
		"vcf.gz":     DGUTAFileTypeVCFGz,
		"bcf":        DGUTAFileTypeBCF,
		"sam":        DGUTAFileTypeSam,
		"bam":        DGUTAFileTypeBam,
		"cram":       DGUTAFileTypeCram,
		"fasta":      DGUTAFileTypeFasta,
		"fastq":      DGUTAFileTypeFastq,
		"fastq.gz":   DGUTAFileTypeFastqGz,
		"ped/bed":    DGUTAFileTypePedBed,
		"compressed": DGUTAFileTypeCompressed,
		"text":       DGUTAFileTypeText,
		"log":        DGUTAFileTypeLog,
		"dir":        DGUTAFileTypeDir,
	}

	dgft, ok := convert[ft]

	if !ok {
		return DGUTAFileTypeOther, ErrInvalidType
	}

	return dgft, nil
}

// AgeStringToDirGUTAge converts the String() representation of a DirGUTAge
// back in to a DirGUTAge. Errors if an invalid string supplied.
func AgeStringToDirGUTAge(age string) (DirGUTAge, error) {
	convert := map[string]DirGUTAge{
		"0":  DGUTAgeAll,
		"1":  DGUTAgeA1M,
		"2":  DGUTAgeA2M,
		"3":  DGUTAgeA6M,
		"4":  DGUTAgeA1Y,
		"5":  DGUTAgeA2Y,
		"6":  DGUTAgeA3Y,
		"7":  DGUTAgeA5Y,
		"8":  DGUTAgeA7Y,
		"9":  DGUTAgeM1M,
		"10": DGUTAgeM2M,
		"11": DGUTAgeM6M,
		"12": DGUTAgeM1Y,
		"13": DGUTAgeM2Y,
		"14": DGUTAgeM3Y,
		"15": DGUTAgeM5Y,
		"16": DGUTAgeM7Y,
	}

	dgage, ok := convert[age]

	if !ok {
		return DGUTAgeAll, ErrInvalidAge
	}

	return dgage, nil
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
	if !fitsAgeInterval(gkey, atime, mtime, store.refTime) {
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

type db interface {
	Add(recordDGUTA) error
}

// DirGroupUserTypeAge is used to summarise file stats by directory, group,
// user, file type and age.
type DirGroupUserTypeAge struct {
	db      db
	store   gutaStore
	thisDir *summary.DirectoryPath
}

// NewDirGroupUserTypeAge returns a DirGroupUserTypeAge.
func NewDirGroupUserTypeAge(db db) summary.OperationGenerator {
	return newDirGroupUserTypeAge(db, time.Now().Unix())
}

func newDirGroupUserTypeAge(db db, refTime int64) summary.OperationGenerator {
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

	atime := info.ATime

	if info.IsDir() {
		atime = time.Now().Unix()
	}

	gutaKeysA := gutaKey.Get().(*[maxNumOfGUTAKeys]GUTAKey) //nolint:errcheck,forcetypeassert
	gutaKeys := GUTAKeys(gutaKeysA[:0])

	filetype, isTmp := infoToType(info)

	gutaKeys.append(info.GID, info.UID, filetype)

	if isTmp {
		gutaKeys.append(info.GID, info.UID, DGUTAFileTypeTemp)
	}

	d.addForEach(gutaKeys, info.Size, atime, maxInt(0, info.MTime))

	gutaKey.Put(gutaKeysA)

	return nil
}

func infoToType(info *summary.FileInfo) (DirGUTAFileType, bool) {
	var (
		isTmp    bool
		filetype DirGUTAFileType
	)

	if info.IsDir() {
		filetype = DGUTAFileTypeDir
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
	FileType DirGUTAFileType
	Age      DirGUTAge
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
		FileType: DirGUTAFileType(dgutaBytes[8]),
		Age:      DirGUTAge(dgutaBytes[9]),
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
func (g *GUTAKeys) append(gid, uid uint32, fileType DirGUTAFileType) {
	for _, age := range DirGUTAges {
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
func filenameToType(name string) (DirGUTAFileType, bool) {
	isTmp := isTempFile(name)

	for ftype, isThisType := range typeCheckers {
		if isThisType(name) {
			return ftype, isTmp
		}
	}

	return DGUTAFileTypeOther, isTmp
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

	dguta := recordDGUTA{
		Dir: d.thisDir,
	}

	for _, guta := range dgutas {
		s := d.store.sumMap[guta]

		dguta.GUTAs = append(dguta.GUTAs, &GUTA{
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

	return nil
}

// fitsAgeInterval takes a dguta and the mtime and atime and reference time. It
// checks the value of age inside the dguta, and then returns true if the mtime
// or atime respectively fits inside the age interval. E.g. if age = 3, this
// corresponds to DGUTAgeA6M, so atime is checked to see if it is older than 6
// months.
func fitsAgeInterval(dguta GUTAKey, atime, mtime, refTime int64) bool {
	age := int(dguta.Age)

	if age > len(ageThresholds) {
		return checkTimeIsInInterval(mtime, refTime, age-(len(ageThresholds)+1))
	} else if age > 0 {
		return checkTimeIsInInterval(atime, refTime, age-1)
	}

	return true
}

func checkTimeIsInInterval(amtime, refTime int64, thresholdIndex int) bool {
	return amtime <= refTime-ageThresholds[thresholdIndex]
}
