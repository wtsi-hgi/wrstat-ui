/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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

package dirbuild

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const linuxProcessWriteBytesSource = "/proc/self/io write_bytes"

var errLinuxProcessWriteBytesUnavailable = errors.New("linux process write bytes unavailable")

type processWriteBytesMeasurement struct {
	bytes     uint64
	available bool
}

func startProcessWriteBytesMeasurement(metrics *DiskMetrics) processWriteBytesMeasurement {
	bytes, err := readDiskProcessWriteBytes(metrics)

	return processWriteBytesMeasurement{bytes: bytes, available: err == nil}
}

func finishProcessWriteBytesMeasurement(
	metrics *DiskMetrics,
	before processWriteBytesMeasurement,
) (uint64, bool) {
	after, err := readDiskProcessWriteBytes(metrics)
	if err != nil || !before.available || after < before.bytes {
		return 0, false
	}

	return after - before.bytes, true
}

func readDiskProcessWriteBytes(metrics *DiskMetrics) (uint64, error) {
	if metrics.readProcessWriteBytes != nil {
		return metrics.readProcessWriteBytes()
	}

	return readLinuxProcessWriteBytes()
}

func readLinuxProcessWriteBytes() (uint64, error) {
	contents, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, err
	}

	for line := range strings.SplitSeq(string(contents), "\n") {
		name, value, found := strings.Cut(line, ":")
		if !found || name != "write_bytes" {
			continue
		}

		bytes, parseErr := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("parse Linux process write bytes: %w", parseErr)
		}

		return bytes, nil
	}

	return 0, errLinuxProcessWriteBytesUnavailable
}
