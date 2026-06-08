/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

// Info holds summary information about a database.
type Info struct {
	NumDirs     int
	NumDGUTAs   int
	NumParents  int
	NumChildren int
}

// CountValues returns Info counts as non-negative unsigned report values.
func (i *Info) CountValues() []uint64 {
	if i == nil {
		return nil
	}

	return []uint64{
		infoCountValue(i.NumDirs),
		infoCountValue(i.NumDGUTAs),
		infoCountValue(i.NumParents),
		infoCountValue(i.NumChildren),
	}
}

func infoCountValue(value int) uint64 {
	if value <= 0 {
		return 0
	}

	return uint64(value)
}

// InfoCountFieldNames returns Info count names in CountValues order.
func InfoCountFieldNames() []string {
	return []string{
		"num_dirs",
		"num_dgutas",
		"num_parents",
		"num_children",
	}
}
