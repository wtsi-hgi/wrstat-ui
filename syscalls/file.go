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

package syscalls

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"sync"
	"time"

	"vimagination.zapto.org/httpencoding"
)

var isGzip = httpencoding.HandlerFunc(func(enc httpencoding.Encoding) bool { return enc == "gzip" })

type file struct {
	name string

	mu                       sync.RWMutex
	compressed, uncompressed []byte
	modTime                  time.Time
}

func (f *file) ReadFrom(r io.Reader) (int64, error) {
	var compressed, uncompressed bytes.Buffer

	g := gzip.NewWriter(&compressed)

	n, err := uncompressed.ReadFrom(io.TeeReader(r, g))
	if err != nil {
		return n, err
	}

	g.Flush()

	f.mu.Lock()
	defer f.mu.Unlock()

	f.modTime = time.Now()
	f.compressed = compressed.Bytes()
	f.uncompressed = uncompressed.Bytes()

	return n, nil
}

func (f *file) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var br *bytes.Reader

	f.mu.RLock()

	if httpencoding.HandleEncoding(r, isGzip) {
		br = bytes.NewReader(f.compressed)

		w.Header().Add("Content-Encoding", "gzip")
	} else {
		br = bytes.NewReader(f.uncompressed)
	}

	f.mu.RUnlock()

	http.ServeContent(w, r, f.name, f.modTime, br)
}
