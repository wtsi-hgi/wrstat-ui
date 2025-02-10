package analytics

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"unsafe"

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
		&rdb.sessionStmt: "SELECT [state], [time] FROM [events] WHERE [user] = ? AND [session] = ?;",
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

type Session struct {
	Start, End uint64
	Events     uint64
	DiskTree   uint64
	GroupBase  uint64
	UserBase   uint64
	Age        uint64
	Owners     uint64
	Groups     uint64
	Users      uint64
}

type Summary struct {
	Users    map[string]uint     `json:"users"`
	Sessions map[string]*Session `json:"sessions"`
}

func newSummary() *Summary {
	return &Summary{
		Users:    make(map[string]uint),
		Sessions: make(map[string]*Session),
	}
}

func (s *Summary) addToSummary(user, session, state string, timestamp uint64) {
	s.Users[user]++

	sess, ok := s.Sessions[session]
	if !ok {
		sess = &Session{
			Start: math.MaxUint64,
		}

		s.Sessions[session] = sess
	}

	if sess.Start > timestamp {
		sess.Start = timestamp
	}

	if sess.End < timestamp {
		sess.End = timestamp
	}

	processState(sess, state)
}

func processState(sess *Session, state string) {
	sess.Events++

	stateMap := make(map[string]json.RawMessage)

	if json.Unmarshal(unsafe.Slice(unsafe.StringData(state), len(state)), &stateMap) != nil {
		return
	}

	if countState(stateMap, "just", "", nil, &sess.DiskTree) {
		countState(stateMap, "byUser", "", &sess.GroupBase, &sess.UserBase)
	}

	countState(stateMap, "age", "", nil, &sess.Age)
	countState(stateMap, "owners", "", nil, &sess.Owners)
	countState(stateMap, "groups", "", nil, &sess.Groups)
	countState(stateMap, "users", "", nil, &sess.Users)
}

func countState(stateMap map[string]json.RawMessage, key, value string, t, f *uint64) bool {
	if string(stateMap[key]) == value {
		if t != nil {
			*t++
		}

		return true
	}

	if f != nil {
		*f++
	}

	return false
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
			state     string
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
			state     string
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
