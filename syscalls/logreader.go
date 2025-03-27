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
	"bufio"
	"io"
	"strconv"

	"vimagination.zapto.org/parser"
)

type logReader struct {
	tk    parser.Tokeniser
	isEOF bool
}

func newLogReader(r io.Reader) *logReader {
	return &logReader{tk: parser.NewReaderTokeniser(bufio.NewReader(r))}
}

func (l *logReader) Read() ([][2]string, error) { //nolint:gocognit,gocyclo,cyclop,funlen
	var (
		fields   [][2]string
		key, val string
		err      error
	)

	for len(fields) == 0 {
		if l.isEOF {
			return nil, io.EOF
		}

	Loop:
		for {
			l.tk.AcceptRun(" ")
			l.tk.Get()

			switch l.tk.Peek() {
			case -1:
				l.isEOF = true

				break Loop
			case '\n':
				l.tk.Next()

				break Loop
			}

			switch l.tk.ExceptRun(" =\n") {
			case -1:
				fields = append(fields, [2]string{"", l.tk.Get()})
			case '\n':
				break Loop
			case ' ':
				l.tk.Get()
			case '=':
				key = l.tk.Get()

				l.tk.Accept("=")
				l.tk.Get()

				if l.tk.Accept("\"") { //nolint:nestif
				StrLoop:
					for {
						switch l.tk.ExceptRun("\\\"") {
						case -1:
							return nil, io.ErrUnexpectedEOF
						case '\\':
							l.tk.Next()
							l.tk.Next()
						case '"':
							l.tk.Next()

							break StrLoop
						}
					}

					if val, err = strconv.Unquote(l.tk.Get()); err != nil {
						return nil, err
					}
				} else {
					l.tk.ExceptRun(" \n")

					val = l.tk.Get()
				}

				fields = append(fields, [2]string{key, val})
			}
		}
	}

	return fields, nil
}
