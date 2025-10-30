package syscalls

import (
	"compress/gzip"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestGetPaths(t *testing.T) {
	tmp := t.TempDir()

	for _, path := range [...]string{
		"a/123_abc/walk.log",
		"a/124_abc/logs.gz",
		"a/125abc/logs.gz",
		"a/125_abc/other.gz",
		"b/126_def/logs.gz",
	} {
		if err := os.MkdirAll(filepath.Join(tmp, filepath.Dir(path)), 0700); err != nil {
			t.Fatalf("unexpected error creating directory: %s", err)
		}

		if err := os.WriteFile(filepath.Join(tmp, path), nil, 0400); err != nil {
			t.Fatalf("unexpected error creating log file: %s", err)
		}
	}

	paths, err := getDBPaths([]string{filepath.Join(tmp, "a"), filepath.Join(tmp, "b")})
	if err != nil {
		t.Errorf("unexpected error finding db paths: %s", err)
	}

	expectedPaths := []string{
		filepath.Join(tmp, "a", "123_abc"),
		filepath.Join(tmp, "a", "124_abc"),
		filepath.Join(tmp, "b", "126_def"),
	}

	if !slices.Equal(paths, expectedPaths) {
		t.Errorf("expecting to find db paths %v, got %v", expectedPaths, paths)
	}
}

func TestLogAnalyzer(t *testing.T) {
	slog.SetDefault(slog.New(slog.DiscardHandler))

	tmp := t.TempDir()

	for path, contents := range map[string]string{
		"123_abc/logs.gz":    "t=2025-03-12T17:00:02+0000 lvl=info msg=\"syscall logging\" host=host1\nt=2025-03-12T17:10:02+0000 lvl=info msg=syscalls opens=259918 reads=585308 bytes=436687248 closes=259902 stats=0\nt=2025-03-18T03:01:55+0000 lvl=info msg=syscalls opens=0 reads=238 bytes=936128 closes=1 stats=0\nt=2025-03-18T22:39:21+0000 lvl=info msg=\"syscall logging\" host=host2 file=walk.1\nt=2025-03-13T03:01:55+0000 lvl=info msg=syscalls opens=0 reads=238 bytes=936128 closes=1 stats=0\nt=2025-03-18T22:39:22+0000 lvl=info msg=\"syscall logging\" host=host3 file=walk.2\nt=2025-03-18T22:49:21+0000 lvl=info msg=syscalls file=walk.1 stats=236102\nt=2025-03-18T22:49:21+0000 lvl=info msg=syscalls file=walk.2 stats=236081\n", //nolint:lll
		"124_def/walk.log":   "t=2025-03-17T17:04:16+0000 lvl=info msg=\"syscall logging\" host=host2\nt=2025-03-17T17:14:16+0000 lvl=info msg=syscalls opens=1508655 reads=3119571 bytes=821857992 closes=1508654 stats=0\nt=2025-03-18T22:31:06+0000 lvl=info msg=syscalls opens=956665 reads=1994960 bytes=699646056 closes=956666 stats=0\n",                                                                                                                                                                                                                                                                                                                                                                                                             //nolint:lll
		"124_def/walk.1.log": "t=2025-03-13T03:01:55+0000 lvl=info msg=\"syscall logging\" host=host2 file=walk.1\nt=2025-03-13T03:11:55+0000 lvl=info msg=syscalls file=walk.1 stats=5625\n",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                //nolint:lll
		"124_def/walk.2.log": "t=2025-03-13T03:01:55+0000 lvl=info msg=\"syscall logging\" host=host1 file=walk.2\nt=2025-03-13T03:11:55+0000 lvl=info msg=syscalls file=walk.2 stats=5624\n",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                //nolint:lll
		"124_def/jobs":       `[{"ReqGroup": "wrstat-walk","StartTime": "2025-06-20T12:05:23.738808994+01:00","EndTime": "2025-06-20T18:19:14.286877588+01:00"},{"ReqGroup": "wrstat-combine","StartTime": "2025-06-20T12:05:23.738808994+01:00","EndTime": "0001-01-01T00:00:00Z"}]`,                                                                                                                                                                                                                                                                                                                                                                                                                                                                        //nolint:lll
	} {
		if err := createLog(filepath.Join(tmp, path), contents); err != nil {
			t.Fatalf("error creating log: %s", err)
		}
	}

	l := newLogAnalyzer()

	l.loadDirs([]string{filepath.Join(tmp, "123_abc"), filepath.Join(tmp, "124_def")})

	var sb strings.Builder

	l.File.WriteTo(&sb) //nolint:errcheck

	expected := `{"123_abc":{"complete":true},"124_def":{"complete":false}}` + "\n"

	if sb.String() != expected {
		t.Errorf("expecting output JSON:\n%s\ngot:\n%s", expected, sb.String())
	}

	if _, ok := l.stats["123_abc"]; !ok {
		t.Errorf("expected 123_abc in stats map")
	}

	if _, ok := l.stats["124_def"]; !ok {
		t.Errorf("expected 124_def in stats map")
	}

	req := httptest.NewRequest(http.MethodGet, "/logs/123_abc", nil)
	w := httptest.NewRecorder()
	l.handleRunRequest(w, req)

	res := w.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /logs/123_abc, got %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("failed to read response body for /logs/123_abc: %v", err)
	}

	if string(body) == `"123_abc":{"events":[{"time":1741798802,"file":"walk","host":"host1"},{"time":1741799402,"file":"walk","host":"host1","opens":259918,"reads":585308,"bytes":436687248,"closes":259902},{"time":1741834915,"file":"walk","host":"host1","reads":238,"bytes":936128,"closes":1},{"time":1742266915,"file":"walk","host":"host1","reads":238,"bytes":936128,"closes":1},{"time":1742337561,"file":"walk.1","host":"host2"},{"time":1742337562,"file":"walk.2","host":"host3"},{"time":1742338161,"file":"walk.2","host":"host3","stats":236081},{"time":1742338161,"file":"walk.1","host":"host2","stats":236102}],"errors":null,"complete":true}` { //nolint:lll
		t.Errorf("expected 'complete' key in /logs/123_abc response, got: %s", body)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/logs/124_def", nil)
	w2 := httptest.NewRecorder()
	l.handleRunRequest(w2, req2)
	res2 := w2.Result()

	defer res2.Body.Close()

	if res2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /logs/124_def, got %d", res2.StatusCode)
	}

	body2, err := io.ReadAll(res2.Body)
	if err != nil {
		t.Fatalf("failed to read response body for /logs/124_def: %v", err)
	}

	if string(body2) == `"124_def":{"events":[{"time":1741834915,"file":"walk.2","host":"host1"},{"time":1741834915,"file":"walk.1","host":"host2"},{"time":1741835515,"file":"walk.2","host":"host1","stats":5624},{"time":1741835515,"file":"walk.1","host":"host2","stats":5625},{"time":1742231056,"file":"walk","host":"host2"},{"time":1742231656,"file":"walk","host":"host2","opens":1508655,"reads":3119571,"bytes":821857992,"closes":1508654},{"time":1742337066,"file":"walk","host":"host2","opens":956665,"reads":1994960,"bytes":699646056,"closes":956666}],"errors":[{"time":1750417523,"message":"Timeout: wrstat-combine","file":"jobs","host":""}],"complete":false}` { //nolint:lll
		t.Errorf("expected complete:false in /logs/124_def response, got: %s", body2)
	}

	req404 := httptest.NewRequest(http.MethodGet, "/logs/missing_run", nil)
	w404 := httptest.NewRecorder()
	l.handleRunRequest(w404, req404)

	if w404.Code != http.StatusNotFound {
		t.Errorf("expected 404 for missing run, got %d", w404.Code)
	}

	if err := createLog(filepath.Join(tmp, "125_def", "logs.gz"), `t=2025-03-26T17:00:02+0000 lvl=info msg="syscall logging" host=host4`); err != nil { //nolint:lll
		t.Fatalf("error creating log: %s", err)
	}

	l.loadDirs([]string{filepath.Join(tmp, "124_def"), filepath.Join(tmp, "125_def")})

	expected = expected[:len(expected)-2] + `,"125_def":{"complete":true}}` + "\n"

	sb.Reset()

	l.File.WriteTo(&sb) //nolint:errcheck

	if sb.String() != expected {
		t.Errorf("expecting output JSON:\n%s\ngot:\n%s", expected, sb.String())
	}
}

func createLog(path, contents string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}

	if err := writeFile(path, contents); err != nil {
		return err
	}

	return nil
}

func writeFile(path, contents string) (err error) {
	if strings.HasSuffix(path, ".gz") { //nolint:nestif
		f, err := os.Create(path)
		if err != nil {
			return err
		}

		defer func() {
			if errr := f.Close(); err == nil {
				err = errr
			}
		}()

		w := gzip.NewWriter(f)

		defer func() {
			if errr := w.Close(); err == nil {
				err = errr
			}
		}()

		_, err = io.WriteString(w, contents)

		return err
	}

	return os.WriteFile(path, []byte(contents), 0400)
}
