/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

var AllTypesExceptDirectories = [...]DirGUTAFileType{ //nolint:gochecknoglobals
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
