//go:build !dev

package analytics

import (
	_ "embed"
	"net/http"
	"strings"
	"time"
)

var (
	//go:embed index.html
	indexHTML string

	modtime = time.Now()                                                      //nolint:gochecknoglobals
	index   = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { //nolint:gochecknoglobals
		http.ServeContent(w, r, "index.html", modtime, strings.NewReader((indexHTML)))
	})
)
