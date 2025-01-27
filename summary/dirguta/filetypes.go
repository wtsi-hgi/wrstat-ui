/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

//nolint:mnd
package dirguta

import (
	"github.com/wtsi-hgi/wrstat-ui/db"
)

type chars [256]uint8

type state struct {
	chars chars
	typ   db.DirGUTAFileType
}

var filenameSuffixes = [...]state{ //nolint:gochecknoglobals
	{ // 0
		chars: chars{
			'/': 1,  // Directory
			'2': 2,  // Compressed
			'A': 8,  // FastA
			'a': 8,  // FastA
			'D': 14, // PedBed, Text
			'd': 14, // PedBed, Text
			'E': 20, // Log, Text
			'e': 20, // Log, Text
			'F': 27, // BCF, VCF
			'f': 27, // BCF, VCF
			'G': 33, // Log
			'g': 33, // Log
			'M': 35, // Bam, Cram, Sam, PedBed
			'm': 35, // Bam, Cram, Sam, PedBed
			'O': 47, // Log
			'o': 47, // Log
			'P': 48, // Compressed, PedBed
			'p': 48, // Compressed, PedBed
			'Q': 51, // FastQ
			'q': 51, // FastQ
			'R': 57, // Log
			'r': 57, // Log
			'T': 59, // Log, Text
			't': 59, // Log, Text
			'V': 64, // Text
			'v': 64, // Text
			'Z': 66, // Compressed, FastQGZ, VCFGz
			'z': 66, // Compressed, FastQGZ, VCFGz
		},
	},
	{ // 1: "/"
		typ: db.DGUTAFileTypeDir,
	},
	{ // 2: "2"
		chars: chars{
			'P': 3, // Compressed
			'p': 3, // Compressed
		},
	},
	{ // 3: "p2"
		chars: chars{
			'I': 4, // Compressed
			'i': 4, // Compressed
		},
	},
	{ // 4: "ip2"
		chars: chars{
			'Z': 5, // Compressed
			'z': 5, // Compressed
		},
	},
	{ // 5: "zip2"
		chars: chars{
			'B': 6, // Compressed
			'b': 6, // Compressed
		},
	},
	{ // 6: "bgz", "bzip2", "tgz", "xz", "zip"
		chars: chars{
			'.': 7, // Compressed
		},
	},
	{ // 7: ".bgz", ".bzip2", ".tgz", ".xz", ".zip"
		typ: db.DGUTAFileTypeCompressed,
	},
	{ // 8: "a"
		chars: chars{
			'F': 9,  // FastA
			'f': 9,  // FastA
			'T': 11, // FastA
			't': 11, // FastA
		},
	},
	{ // 9: "fa", "fasta"
		chars: chars{
			'.': 10, // FastA
		},
	},
	{ // 10: ".fa", ".fasta"
		typ: db.DGUTAFileTypeFasta,
	},
	{ // 11: "ta"
		chars: chars{
			'S': 12, // FastA
			's': 12, // FastA
		},
	},
	{ // 12: "sta"
		chars: chars{
			'A': 13, // FastA
			'a': 13, // FastA
		},
	},
	{ // 13: "asta"
		chars: chars{
			'F': 9, // FastA
			'f': 9, // FastA
		},
	},
	{ // 14: "d"
		chars: chars{
			'E': 15, // PedBed
			'e': 15, // PedBed
			'M': 18, // Text
			'm': 18, // Text
		},
	},
	{ // 15: "ed"
		chars: chars{
			'B': 16, // PedBed
			'b': 16, // PedBed
			'P': 16, // PedBed
			'p': 16, // PedBed
		},
	},
	{ // 16: "bed", "ped"
		chars: chars{
			'.': 17, // PedBed
		},
	},
	{ // 17: ".bed", ".ped"
		typ: db.DGUTAFileTypePedBed,
	},
	{ // 18: "csv", "dat", "md", "text", "txt", "tsv"
		chars: chars{
			'.': 19, // Text
		},
	},
	{ // 19: ".csv", ".dat", ".md", ".readme", ".text", ".txt", ".tsv"
		typ: db.DGUTAFileTypeText,
	},
	{ // 20: "e"
		chars: chars{
			'.': 21, // Log
			'M': 22, // Text
			'm': 22, // Text
			'O': 26, // Log
			'o': 26, // Log
		},
	},
	{ // 21: ".e", ".err", ".log", ".oe"
		typ: db.DGUTAFileTypeLog,
	},
	{ // 22: "me"
		chars: chars{
			'D': 23, // Text
			'd': 23, // Text
		},
	},
	{ // 23: "dme"
		chars: chars{
			'A': 24, // Text
			'a': 24, // Text
		},
	},
	{ // 24: "adme"
		chars: chars{
			'E': 25, // Text
			'e': 25, // Text
		},
	},
	{ // 25: "eadme"
		chars: chars{
			'R': 19, // Text
			'r': 19, // Text
		},
	},
	{ // 26: "oe", "log"
		chars: chars{
			'.': 21, // Log
		},
	},
	{ // 27: "f"
		chars: chars{
			'C': 28, // BCF, VCF
			'c': 28, // BCF, VCF
		},
	},
	{ // 28: "cf"
		chars: chars{
			'B': 29, // BCF
			'b': 29, // BCF
			'V': 31, // VCF
			'v': 31, // VCF
		},
	},
	{ // 29: "bcf"
		chars: chars{
			'.': 30, // BCF
		},
	},
	{ // 30: ".bcf"
		typ: db.DGUTAFileTypeBCF,
	},
	{ // 31: "vcf"
		chars: chars{
			'.': 32, // VCF
		},
	},
	{ // 32: ".vcf"
		typ: db.DGUTAFileTypeVCF,
	},
	{ // 33: "g"
		chars: chars{
			'O': 34, // Log
			'o': 34, // Log
		},
	},
	{ // 34: "og"
		chars: chars{
			'L': 26, // Log
			'l': 26, // Log
		},
	},
	{ // 35: "m"
		chars: chars{
			'A': 36, // Bam, Cram, PedBed, Sam
			'a': 36, // Bam, Cram, PedBed, Sam
			'I': 46, // PedBed
			'i': 46, // PedBed
		},
	},
	{ // 36: "am"
		chars: chars{
			'B': 37, // Bam
			'b': 37, // Bam
			'F': 39, // PedBed
			'f': 39, // PedBed
			'R': 41, // Cram
			'r': 41, // Cram
			'S': 44, // Sam
			's': 44, // Sam
		},
	},
	{ // 37: "bam"
		chars: chars{
			'.': 38, // Bam
		},
	},
	{ // 38: ".bam"
		typ: db.DGUTAFileTypeBam,
	},
	{ // 39: "bim", "fam", "map"
		chars: chars{
			'.': 40, // PedBed
		},
	},
	{ // 40: ".bim", ".fam", ".map"
		typ: db.DGUTAFileTypePedBed,
	},
	{ // 41: "ram"
		chars: chars{
			'C': 42, // Cram
			'c': 42, // Cram
		},
	},
	{ // 42: "cram"
		chars: chars{
			'.': 43, // Cram
		},
	},
	{ // 43: ".cram"
		typ: db.DGUTAFileTypeCram,
	},
	{ // 44: "sam"
		chars: chars{
			'.': 45, // Sam
		},
	},
	{ // 45: ".sam"
		typ: db.DGUTAFileTypeSam,
	},
	{ // 46: "im"
		chars: chars{
			'B': 39, // PedBed
			'b': 39, // PedBed
		},
	},
	{ // 47: "err", "o"
		chars: chars{
			'.': 21, // Log
		},
	},
	{ // 48: "p"
		chars: chars{
			'A': 49, // PedBed
			'a': 49, // PedBed
			'I': 50, // Compressed
			'i': 50, // Compressed
		},
	},
	{ // 49: "ap"
		chars: chars{
			'M': 39, // PedBed
			'm': 39, // PedBed
		},
	},
	{ // 50: "ip"
		chars: chars{
			'Z': 6, // Compressed
			'z': 6, // Compressed
		},
	},
	{ // 51: "q"
		chars: chars{
			'F': 52, // FastQ
			'f': 52, // FastQ
			'T': 54, // FastQ
			't': 54, // FastQ
		},
	},
	{ // 52: "fq"
		chars: chars{
			'.': 53, // FastQ
		},
	},
	{ // 53: ".fastq", ".fq"
		typ: db.DGUTAFileTypeFastq,
	},
	{ // 54: "tq"
		chars: chars{
			'S': 55, // FastQ
			's': 55, // FastQ
		},
	},
	{ // 55: "stq"
		chars: chars{
			'A': 56, // FastQ
			'a': 56, // FastQ
		},
	},
	{ // 56: "astq"
		chars: chars{
			'F': 52, // FastQ
			'f': 52, // FastQ
		},
	},
	{ // 57: "r"
		chars: chars{
			'R': 58, // Log
			'r': 58, // Log
		},
	},
	{ // 58: "rr"
		chars: chars{
			'E': 47, // Log
			'e': 47, // Log
		},
	},
	{ // 59: "t"
		chars: chars{
			'A': 60, // Text
			'a': 60, // Text
			'U': 61, // Log
			'u': 61, // Log
			'X': 62, // Text
			'x': 62, // Text
		},
	},
	{ // 60: "at"
		chars: chars{
			'D': 18, // Text
			'd': 18, // Text
		},
	},
	{ // 61: "ut"
		chars: chars{
			'O': 47, // Log
			'o': 47, // Log
		},
	},
	{ // 62: "xt"
		chars: chars{
			'E': 63, // Text
			'e': 63, // Text
			'T': 18, // Text
			't': 18, // Text
		},
	},
	{ // 63: "ext"
		chars: chars{
			'T': 18, // Text
			't': 18, // Text
		},
	},
	{ // 64: "v"
		chars: chars{
			'S': 65, // Text
			's': 65, // Text
		},
	},
	{ // 65: "sv"
		chars: chars{
			'C': 18, // Text
			'c': 18, // Text
			'T': 18, // Text
			't': 18, // Text
		},
	},
	{ // 66: "z"
		chars: chars{
			'G': 67, // Compressed, FastQGz, VCFGz
			'g': 67, // Compressed, FastQGz, VCFGz
			'X': 6,  // Compressed
			'x': 6,  // Compressed
		},
	},
	{ // 67: "gz"
		chars: chars{
			'.': 68, // Compressed, FastQGz, VCFGz
			'B': 6,  // Compressed
			'b': 6,  // Compressed
			'T': 6,  // Compressed
			't': 6,  // Compressed
		},
	},
	{ // 68: ".gz"
		chars: chars{
			'F': 69, // Compressed, VCFGz
			'f': 69, // Compressed, VCFGz
			'Q': 73, // Compressed, FastQGz
			'q': 73, // Compressed, FastQGz
		},
		typ: db.DGUTAFileTypeCompressed,
	},
	{ // 69: "f.gz"
		chars: chars{
			'C': 70, // Compressed, VCFGz
			'c': 70, // Compressed, VCFGz
		},
		typ: db.DGUTAFileTypeCompressed,
	},
	{ // 70: "cf.gz"
		chars: chars{
			'V': 71, // Compressed, VCFGz
			'v': 71, // Compressed, VCFGz
		},
		typ: db.DGUTAFileTypeCompressed,
	},
	{ // 71: "vcf.gz"
		chars: chars{
			'.': 72, // VCFGz
		},
		typ: db.DGUTAFileTypeCompressed,
	},
	{ // 72: ".vcf.gz"
		typ: db.DGUTAFileTypeVCFGz,
	},
	{ // 73: "q.gz"
		chars: chars{
			'F': 74, // Compressed, FastQGz
			'f': 74, // Compressed, FastQGz
			'T': 76, // Compressed, FastQGz
			't': 76, // Compressed, FastQGz
		},
		typ: db.DGUTAFileTypeCompressed,
	},
	{ // 74: "fastq.gz". "fq.gz"
		chars: chars{
			'.': 75, // VCFGz
		},
		typ: db.DGUTAFileTypeCompressed,
	},
	{ // 75: ".fastq.gz", ".fq.gz"
		typ: db.DGUTAFileTypeFastqGz,
	},
	{ // 76: "tq.gz"
		chars: chars{
			'S': 77, // Compressed, FastQGz
			's': 77, // Compressed, FastQGz
		},
		typ: db.DGUTAFileTypeCompressed,
	},
	{ // 77: "stq.gz"
		chars: chars{
			'A': 78, // Compressed, FastQGz
			'a': 78, // Compressed, FastQGz
		},
		typ: db.DGUTAFileTypeCompressed,
	},
	{ // 78: "astq.gz"
		chars: chars{
			'F': 74, // Compressed, FastQGz
			'f': 74, // Compressed, FastQGz
		},
		typ: db.DGUTAFileTypeCompressed,
	},
}

var tmpPrefixes = [...]state{ //nolint:gochecknoglobals
	{ // 0
		chars: chars{
			'.': 1,
			'T': 7,
			't': 7,
		},
	},
	{ // 1: "."
		chars: chars{
			'T': 2,
			't': 2,
		},
	},
	{ // 2: ".t"
		chars: chars{
			'E': 3,
			'e': 3,
			'M': 4,
			'm': 4,
		},
	},
	{ // 3: ".te"
		chars: chars{
			'M': 4,
			'm': 4,
		},
	},
	{ // 4: ".tem", ".tm"
		chars: chars{
			'P': 5,
			'p': 5,
		},
	},
	{ // 5: ".temp", ".tmp"
		chars: chars{
			'.': 6,
		},
	},
	{ // 6: ".temp.", ".tmp.", "temp.", "tmp.", "temp/", "tmp/"
		typ: db.DGUTAFileTypeTemp,
	},
	{ // 7: "t"
		chars: chars{
			'E': 8,
			'e': 8,
			'M': 9,
			'm': 9,
		},
	},
	{ // 8: "te"
		chars: chars{
			'M': 9,
			'm': 9,
		},
	},
	{ // 9: "tem", "tm"
		chars: chars{
			'P': 10,
			'p': 10,
		},
	},
	{ // 10: "temp", "tmp"
		chars: chars{
			'.': 6,
			'/': 6,
		},
	},
}

var tmpSuffixes = [...]state{ //nolint:gochecknoglobals
	{ // 0
		chars: chars{
			'P': 1,
			'p': 1,
		},
	},
	{ // 1: "p"
		chars: chars{
			'M': 2,
			'm': 2,
		},
	},
	{ // 2: "mp"
		chars: chars{
			'E': 3,
			'e': 3,
			'T': 4,
			't': 4,
		},
	},
	{ // 3: "emp"
		chars: chars{
			'T': 4,
			't': 4,
		},
	},
	{ // 4: "temp", "tmp"
		chars: chars{
			'.': 5,
		},
	},
	{ // 5: ".temp", ".tmp"
		typ: db.DGUTAFileTypeTemp,
	},
}

// FilenameToType determines the filetype of the given path based on its
// basename, and returns a a DirGUTAFileType.
func FilenameToType(name []byte) db.DirGUTAFileType {
	var place uint8

	for len(name) > 0 {
		next := filenameSuffixes[place].chars[name[len(name)-1]]
		if next == 0 {
			break
		}

		name = name[:len(name)-1]
		place = next
	}

	return filenameSuffixes[place].typ
}

// IsTemp tells you if path is named like a temporary file.
func IsTemp(name []byte) bool {
	return hasTempPrefix(name) || hasTempSuffix(name)
}

func hasTempPrefix(name []byte) bool {
	var place uint8

	for _, b := range name {
		next := tmpPrefixes[place].chars[b]
		if next == 0 {
			break
		}

		place = next
	}

	return tmpPrefixes[place].typ == db.DGUTAFileTypeTemp
}

func hasTempSuffix(name []byte) bool {
	var place uint8

	for len(name) > 0 {
		next := tmpSuffixes[place].chars[name[len(name)-1]]
		if next == 0 {
			break
		}

		name = name[:len(name)-1]
		place = next
	}

	return tmpSuffixes[place].typ == db.DGUTAFileTypeTemp
}
