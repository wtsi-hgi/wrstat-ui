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
	"fmt"
	"os"
)

// Debugging utilities for verbose instrumentation. Controlled via env vars:
//  - WRSTAT_DEBUG_CLICKHOUSE=1 enables debug logging
//  - WRSTAT_DEBUG_CLICKHOUSE_VERBOSE=1 enables per-row/per-append logs

func debugEnabled() bool {
	return os.Getenv("WRSTAT_DEBUG_CLICKHOUSE") == "1"
}

func debugVerbose() bool {
	return os.Getenv("WRSTAT_DEBUG_CLICKHOUSE_VERBOSE") == "1"
}

func debugf(format string, a ...any) {
	if !debugEnabled() {
		return
	}

	// Print to stderr to avoid interfering with test JSON output
	_, _ = fmt.Fprintf(os.Stderr, "[CH-DEBUG] "+format+"\n", a...)
}
