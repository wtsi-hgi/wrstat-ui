package analytics

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	_ "github.com/mattn/go-sqlite3" //
)

func StartServer(addr, dbPath string) error {
	db, err := newDB(dbPath)
	if err != nil {
		return err
	}

	http.Handle("/", index)
	http.Handle("/summary", handle[summaryInput](db.summary))
	http.Handle("/user", handle[userInput](db.user))
	http.Handle("/session", handle[sessionInput](db.session))

	return http.ListenAndServe(addr, nil)
}

type HTTPError struct {
	code int
	msg  string
}

func (h HTTPError) Error() string {
	return fmt.Sprintf("%d: %s", h.code, h.msg)
}

type handle[T any] func(T) (any, error)

func (h handle[T]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var val T

	if err := readBody(r.Body, &val); err != nil {
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
		return HTTPError{
			code: http.StatusBadRequest,
			msg:  err.Error(),
		}
	}

	return nil
}

func handleError(w http.ResponseWriter, err error) {
	var herr HTTPError

	if errors.As(err, &herr) {
		http.Error(w, herr.msg, herr.code)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type DB struct {
	db *sql.DB
}

func newDB(dbPath string) (*DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

type summaryInput struct {
	StartTime uint64 `json:"startTime"`
	EndTime   uint64 `json:"endTime"`
}

func (d *DB) summary(i summaryInput) (any, error) {
	return nil, nil
}

type userInput struct {
	Username  string `json:"username"`
	StartTime uint64 `json:"startTime"`
	EndTime   uint64 `json:"endTime"`
}

func (d *DB) user(i userInput) (any, error) {
	return nil, nil
}

type sessionInput struct {
	Username string `json:"username"`
	Session  string `json:"string"`
}

func (d *DB) session(i sessionInput) (any, error) {
	return nil, nil
}
