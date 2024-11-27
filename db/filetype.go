package db

const ErrInvalidType = Error("not a valid file type")

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
