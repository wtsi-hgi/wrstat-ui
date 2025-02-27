/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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

package analytics

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"unsafe"

	_ "github.com/mattn/go-sqlite3" //
)

// StartServer will start an anlytics viewing server on the given server address
// and using the given sqlite database.
func StartServer(addr, dbPath, host string) error {
	db, err := newDB(dbPath, host)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()

	mux.Handle("/", index)
	mux.Handle("/analytics", handle[summaryInput](db.summary))
	mux.Handle("/host", handle[byte](db.host))

	return http.ListenAndServe(addr, mux) //nolint:gosec
}

type httpError struct {
	code int
	msg  string
}

func (h httpError) Error() string {
	return fmt.Sprintf("%d: %s", h.code, h.msg)
}

type handle[T any] func(T) (any, error)

func (h handle[T]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var val T

	if err := readBody(r.Body, &val); err != nil { //nolint:nestif
		handleError(w, err)
	} else if data, err := h(val); err != nil {
		handleError(w, err)
	} else if err = json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("error writing response", "err", err)
	}
}

func readBody(r io.ReadCloser, data any) error {
	defer r.Close()

	if err := json.NewDecoder(r).Decode(data); err != nil {
		return httpError{
			code: http.StatusBadRequest,
			msg:  err.Error(),
		}
	}

	return nil
}

func handleError(w http.ResponseWriter, err error) {
	var herr httpError

	if errors.As(err, &herr) {
		http.Error(w, herr.msg, herr.code)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type database struct {
	db          *sql.DB
	summaryStmt *sql.Stmt
	hostname    string
}

func newDB(dbPath, host string) (*database, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	rdb := &database{db: db, hostname: host}

	if rdb.summaryStmt, err = db.Prepare("SELECT [user], [session], [state], " +
		"[time] FROM [events] WHERE [time] BETWEEN ? AND ?;"); err != nil {
		return nil, err
	}

	return rdb, nil
}

type summaryInput struct {
	StartTime int64 `json:"startTime"`
	EndTime   int64 `json:"endTime"`
}

type event struct {
	Timestamp uint64
	State     json.RawMessage
}

type Response map[string]map[string][]event

func (r Response) add(username, session, state string, timestamp uint64) {
	u, ok := r[username]
	if !ok {
		u = make(map[string][]event)

		r[username] = u
	}

	s := u[session]
	ne := event{Timestamp: timestamp, State: json.RawMessage(unsafe.Slice(unsafe.StringData(state), len(state)))}
	pos, _ := slices.BinarySearchFunc(s, ne, func(a, b event) int {
		return int(b.Timestamp) - int(a.Timestamp) //nolint:gosec
	})
	u[session] = slices.Insert(s, pos, ne)
}

func (d *database) host(_ byte) (any, error) {
	return d.hostname, nil
}

func (d *database) summary(i summaryInput) (any, error) {
	if i.StartTime > i.EndTime {
		return nil, errInvalidRange
	}

	rows, err := d.summaryStmt.Query(i.StartTime, i.EndTime)
	if err != nil {
		return nil, err
	}

	r := make(Response)

	for rows.Next() {
		var (
			username  string
			session   string
			state     string
			timestamp uint64
		)

		if err := rows.Scan(&username, &session, &state, &timestamp); err != nil {
			return nil, err
		}

		r.add(username, session, state, timestamp)
	}

	return r, nil
}

var errInvalidRange = httpError{http.StatusBadRequest, "invalid date range"}
