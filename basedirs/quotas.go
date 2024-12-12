/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Partially based on github.com/MichaelTJones/walk
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

package basedirs

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	quotaCSVCols       = 4
	errBadQuotaCSVFile = Error("invalid number of columns in quota csv file")
)

// diskQuota stores the quota in bytes for a particular disk location.
type diskQuota struct {
	disk       string
	quotaSize  uint64
	quotaInode uint64
}

// Quotas stores information about group disk quotas.
type Quotas struct {
	gids map[uint32][]*diskQuota
}

// ParseQuotas parses the given quotas csv file (gid,disk,quota) and returns a
// Quotas struct.
func ParseQuotas(path string) (*Quotas, error) {
	q := &Quotas{
		gids: make(map[uint32][]*diskQuota),
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)

	for {
		row, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}

			return q, err
		}

		if err = parseRowAndStore(row, q); err != nil {
			return nil, err
		}
	}
}

// parseRowAndStore parses a row from a quotas csv file and stores in the given
// Quotas.
func parseRowAndStore(row []string, q *Quotas) error {
	if len(row) != quotaCSVCols {
		return errBadQuotaCSVFile
	}

	gid, err := strconv.ParseUint(row[0], 10, 32)
	if err != nil {
		return err
	}

	quotaSize, err := strconv.ParseUint(row[2], 10, 64)
	if err != nil {
		return err
	}

	quotaInode, err := strconv.ParseUint(row[3], 10, 64)
	if err != nil {
		return err
	}

	q.store(uint32(gid), row[1], quotaSize, quotaInode)

	return nil
}

// store stores the given quota information.
func (q *Quotas) store(gid uint32, disk string, quotaSize, quotaInode uint64) {
	q.gids[gid] = append(q.gids[gid], &diskQuota{
		disk:       disk,
		quotaSize:  quotaSize,
		quotaInode: quotaInode,
	})
}

// Get returns the quota (in bytes) for the given gid for the given disk
// location. If path isn't a sub-directory of a disk in the csv file used to
// create this Quotas, or gid doesn't have a quota on that disk, returns 0.
func (q *Quotas) Get(gid uint32, path string) (uint64, uint64) {
	dqs, found := q.gids[gid]
	if !found {
		return 0, 0
	}

	for _, dq := range dqs {
		if !strings.HasSuffix(dq.disk, "/") {
			dq.disk += "/"
		}

		if strings.HasPrefix(path, dq.disk) {
			return dq.quotaSize, dq.quotaInode
		}
	}

	return 0, 0
}
