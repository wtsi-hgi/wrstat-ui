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

const (
	SecondsInAMonth = 2628000
	SecondsInAYear  = SecondsInAMonth * 12
	ErrInvalidAge   = Error("not a valid age")
)

var AgeThresholds = [8]int64{ //nolint:gochecknoglobals
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

// FitsAgeInterval takes a dguta and the mtime and atime and reference time. It
// checks the value of age inside the dguta, and then returns true if the mtime
// or atime respectively fits inside the age interval. E.g. if age = 3, this
// corresponds to DGUTAgeA6M, so atime is checked to see if it is older than 6
// months.
func (d DirGUTAge) FitsAgeInterval(atime, mtime, refTime int64) bool {
	age := int(d)

	if age > len(AgeThresholds) {
		return checkTimeIsInInterval(mtime, refTime, age-(len(AgeThresholds)+1))
	} else if age > 0 {
		return checkTimeIsInInterval(atime, refTime, age-1)
	}

	return true
}

func checkTimeIsInInterval(amtime, refTime int64, thresholdIndex int) bool {
	return amtime <= refTime-AgeThresholds[thresholdIndex]
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
