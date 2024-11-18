package analytics

import (
	"net/http"
	"os"

	"vimagination.zapto.org/tsserver"
)

var index = http.FileServer(http.FS(tsserver.WrapFS(os.DirFS("./src"))))
