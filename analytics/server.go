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

	summaryStmt *sql.Stmt
	userStmt    *sql.Stmt
	sessionStmt *sql.Stmt
}

func newDB(dbPath string) (*DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	rdb := &DB{db: db}

	for stmt, sql := range map[**sql.Stmt]string{
		&rdb.summaryStmt: "SELECT [user], [session], [state], [time] FROM [events] WHERE [time] BETWEEN ? AND ?;",
		&rdb.userStmt:    "SELECT [session], [state], [time] FROM [events] WHERE [user] = ? AND [time] BETWEEN ? AND ?;",
		&rdb.sessionStmt: "SELECT [state], [time] FROM [events] WHERE [username] = ? AND [session] = ?;",
	} {
		if *stmt, err = db.Prepare(sql); err != nil {
			return nil, err
		}
	}

	return rdb, nil
}

type summaryInput struct {
	StartTime uint64 `json:"startTime"`
	EndTime   uint64 `json:"endTime"`
}

type Summary struct {
	Users    map[string]uint `json:"users"`
	Sessions map[string]uint `json:"sessions"`
}

func newSummary() *Summary {
	return &Summary{
		Users:    make(map[string]uint),
		Sessions: make(map[string]uint),
	}
}

func (s *Summary) addToSummary(user, session string, state json.RawMessage, timestamp uint64) {
	s.Users[user] = s.Users[user] + 1
	s.Sessions[session] = s.Sessions[session] + 1
}

func (d *DB) summary(i summaryInput) (any, error) {
	if i.StartTime > i.EndTime {
		return nil, ErrInvalidRange
	}

	rows, err := d.summaryStmt.Query(i.StartTime, i.EndTime)
	if err != nil {
		return nil, err
	}

	s := newSummary()

	for rows.Next() {
		var (
			username  string
			session   string
			state     json.RawMessage
			timestamp uint64
		)

		if err := rows.Scan(&username, &session, &state, &timestamp); err != nil {
			return nil, err
		}

		s.addToSummary(username, session, state, timestamp)
	}

	return s, nil
}

type userInput struct {
	Username  string `json:"username"`
	StartTime uint64 `json:"startTime"`
	EndTime   uint64 `json:"endTime"`
}

func (d *DB) user(i userInput) (any, error) {
	if i.StartTime > i.EndTime {
		return nil, ErrInvalidRange
	}

	rows, err := d.userStmt.Query(i.Username, i.StartTime, i.EndTime)
	if err != nil {
		return nil, err
	}

	s := newSummary()

	for rows.Next() {
		var (
			session   string
			state     json.RawMessage
			timestamp uint64
		)

		if err := rows.Scan(&session, &state, &timestamp); err != nil {
			return nil, err
		}

		s.addToSummary(i.Username, session, state, timestamp)
	}

	return s, nil
}

type sessionInput struct {
	Username string `json:"username"`
	Session  string `json:"string"`
}

type Event struct {
	Data      json.RawMessage `json:"data"`
	Timestamp uint64          `json:"timestamp"`
}

func (d *DB) session(i sessionInput) (any, error) {
	rows, err := d.sessionStmt.Query(i.Username, i.Session)
	if err != nil {
		return nil, err
	}

	var events []Event

	for rows.Next() {
		var e Event

		if err := rows.Scan(&e.Data, &e.Timestamp); err != nil {
			return nil, err
		}

		events = append(events, e)
	}

	return events, nil
}

var ErrInvalidRange = HTTPError{http.StatusBadRequest, "invalid date range"}
