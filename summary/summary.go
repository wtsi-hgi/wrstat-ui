/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
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

// package summary lets you summarise file stats.

package summary

const (
	SecondsInAMonth = 2628000
	SecondsInAYear  = SecondsInAMonth * 12
)

var ageThresholds = [8]int64{ //nolint:gochecknoglobals
	SecondsInAMonth, SecondsInAMonth * 2, SecondsInAMonth * 6, SecondsInAYear,
	SecondsInAYear * 2, SecondsInAYear * 3, SecondsInAYear * 5, SecondsInAYear * 7,
}

// summary holds count and size and lets you accumulate count and size as you
// add more things with a size.
type summary struct {
	count int64
	size  int64
}

// add will increment our count and add the given size to our size.
func (s *summary) add(size int64) {
	s.count++
	s.size += size
}

// summaryWithTimes is like summary, but also holds the reference time, oldest
// atime, newest mtime add()ed.
type summaryWithTimes struct {
	summary
	refTime int64 // seconds since Unix epoch
	atime   int64 // seconds since Unix epoch
	mtime   int64 // seconds since Unix epoch
}

// add will increment our count and add the given size to our size. It also
// stores the given atime if it is older than our current one, and the given
// mtime if it is newer than our current one.
func (s *summaryWithTimes) add(size int64, atime int64, mtime int64) {
	s.summary.add(size)

	if atime > 0 && (s.atime == 0 || atime < s.atime) {
		s.atime = atime
	}

	if mtime > 0 && (s.mtime == 0 || mtime > s.mtime) {
		s.mtime = mtime
	}
}

// FitsAgeInterval takes a dguta and the mtime and atime and reference time. It
// checks the value of age inside the dguta, and then returns true if the mtime
// or atime respectively fits inside the age interval. E.g. if age = 3, this
// corresponds to DGUTAgeA6M, so atime is checked to see if it is older than 6
// months.
func FitsAgeInterval(dguta GUTAKey, atime, mtime, refTime int64) bool {
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
