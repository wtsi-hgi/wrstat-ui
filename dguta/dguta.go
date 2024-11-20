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

// package dguta lets you create and query a database made from dguta files.

package dguta

import (
	"github.com/ugorji/go/codec"
)

// DGUTA handles all the *GUTA information for a directory.
type DGUTA struct {
	Dir   string
	GUTAs GUTAs
}

// encodeToBytes returns our Dir as a []byte and our GUTAs encoded in another
// []byte suitable for storing on disk.
func (d *DGUTA) encodeToBytes(ch codec.Handle) ([]byte, []byte) {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, ch)
	enc.MustEncode(d.GUTAs)

	return []byte(d.Dir), encoded
}

// decodeDGUTAbytes converts the byte slices returned by DGUTA.Encode() back in to
// a *DGUTA.
func decodeDGUTAbytes(ch codec.Handle, dir, encoded []byte) *DGUTA {
	dec := codec.NewDecoderBytes(encoded, ch)

	var g GUTAs

	dec.MustDecode(&g)

	return &DGUTA{
		Dir:   string(dir),
		GUTAs: g,
	}
}

// Summary sums the count and size of all our GUTAs and returns the results,
// along with the oldest atime and newset mtime (seconds since Unix epoch) and
// unique set of UIDs, GIDs and FTs in all our GUTAs.
//
// See GUTAs.Summary for an explanation of the filter.
func (d *DGUTA) Summary(filter *Filter) *DirSummary {
	return d.GUTAs.Summary(filter)
}

// Append appends the GUTAs in the given DGUTA to our own. Useful when you have
// 2 DGUTAs for the same Dir that were calculated on different subdirectories
// independently, and now you're dealing with DGUTAs for their common parent
// directories.
func (d *DGUTA) Append(other *DGUTA) {
	d.GUTAs = append(d.GUTAs, other.GUTAs...)
}
