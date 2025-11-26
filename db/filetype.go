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

import "strings"

const ErrInvalidType = Error("not a valid file type")

// DirGUTAFileType is one of the special file types that the
// directory,group,user,filetype,age summaries group on.
type DirGUTAFileType uint16

const (
	DGUTAFileTypeTemp       DirGUTAFileType = 1
	DGUTAFileTypeVCF        DirGUTAFileType = 2
	DGUTAFileTypeVCFGz      DirGUTAFileType = 4
	DGUTAFileTypeBCF        DirGUTAFileType = 8
	DGUTAFileTypeSam        DirGUTAFileType = 16
	DGUTAFileTypeBam        DirGUTAFileType = 32
	DGUTAFileTypeCram       DirGUTAFileType = 64
	DGUTAFileTypeFasta      DirGUTAFileType = 128
	DGUTAFileTypeFastq      DirGUTAFileType = 256
	DGUTAFileTypeFastqGz    DirGUTAFileType = 512
	DGUTAFileTypePedBed     DirGUTAFileType = 1024
	DGUTAFileTypeCompressed DirGUTAFileType = 2048
	DGUTAFileTypeText       DirGUTAFileType = 4096
	DGUTAFileTypeLog        DirGUTAFileType = 8192
	DGUTAFileTypeDir        DirGUTAFileType = 16384
	DGUTAFileTypeOther      DirGUTAFileType = 32768
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
	var out []string

	if d&DGUTAFileTypeTemp != 0 {
		out = append(out, "temp")
	}

	if d&DGUTAFileTypeVCF != 0 {
		out = append(out, "vcf")
	}

	if d&DGUTAFileTypeVCFGz != 0 {
		out = append(out, "vcf.gz")
	}

	if d&DGUTAFileTypeBCF != 0 {
		out = append(out, "bcf")
	}

	if d&DGUTAFileTypeSam != 0 {
		out = append(out, "sam")
	}

	if d&DGUTAFileTypeBam != 0 {
		out = append(out, "bam")
	}

	if d&DGUTAFileTypeCram != 0 {
		out = append(out, "cram")
	}

	if d&DGUTAFileTypeFasta != 0 {
		out = append(out, "fasta")
	}

	if d&DGUTAFileTypeFastq != 0 {
		out = append(out, "fastq")
	}

	if d&DGUTAFileTypeFastqGz != 0 {
		out = append(out, "fastq.gz")
	}

	if d&DGUTAFileTypePedBed != 0 {
		out = append(out, "ped/bed")
	}

	if d&DGUTAFileTypeCompressed != 0 {
		out = append(out, "compressed")
	}

	if d&DGUTAFileTypeText != 0 {
		out = append(out, "text")
	}

	if d&DGUTAFileTypeLog != 0 {
		out = append(out, "log")
	}

	if d&DGUTAFileTypeDir != 0 {
		out = append(out, "dir")
	}

	if d&DGUTAFileTypeOther != 0 {
		out = append(out, "other")
	}

	return strings.Join(out, "|")
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
