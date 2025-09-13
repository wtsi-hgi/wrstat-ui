/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wtsi-hgi/wrstat-ui/stats"
)

func TestMapEntryType(t *testing.T) {
	tests := []struct {
		name         string
		entryTypeByte byte
		expected     FileType
	}{
		{"File", stats.FileType, FileTypeFile},
		{"Directory", stats.DirType, FileTypeDir},
		{"Symlink", stats.SymlinkType, FileTypeSymlink},
		{"Device", stats.DeviceType, FileTypeDevice},
		{"Pipe", stats.PipeType, FileTypePipe},
		{"Socket", stats.SocketType, FileTypeSocket},
		{"Char", stats.CharType, FileTypeChar},
		{"Unknown", 255, FileTypeUnknown}, // assuming 255 is not a valid type
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MapEntryType(tt.entryTypeByte)
			assert.Equal(t, tt.expected, result)
		})
	}
}