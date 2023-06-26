/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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

// package server provides a web server for a REST API and website.

package server

import (
	"embed"
	"io"
	"sync"
	"time"

	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-ssg/wrstat/v4/basedirs"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	"github.com/wtsi-ssg/wrstat/v4/watch"
)

//go:embed static
var staticFS embed.FS

const (
	wherePath = "/where"

	// EndPointWhere is the endpoint for making where queries if authorization
	// isn't implemented.
	EndPointWhere = gas.EndPointREST + wherePath

	// EndPointAuthWhere is the endpoint for making where queries if
	// authorization is implemented.
	EndPointAuthWhere = gas.EndPointAuth + wherePath

	groupAreasPaths = "/group-areas"

	// EndPointAuthGroupAreas is the endpoint for making queries on what the
	// group areas are, which is available if authorization is implemented.
	EndPointAuthGroupAreas = gas.EndPointAuth + groupAreasPaths

	basedirsPath            = "/basedirs"
	basedirsUsagePath       = basedirsPath + "/usage"
	basedirsGroupUsagePath  = basedirsUsagePath + "/groups"
	basedirsUserUsagePath   = basedirsUsagePath + "/users"
	basedirsSubdirPath      = basedirsPath + "/subdirs"
	basedirsGroupSubdirPath = basedirsSubdirPath + "/group"
	basedirsUserSubdirPath  = basedirsSubdirPath + "/user"
	basedirsHistoryPath     = basedirsPath + "/history"

	// EndPointBasedir* are the endpoints for making base directory related
	// queries if authorization isn't implemented.
	EndPointBasedirUsageGroup  = gas.EndPointREST + basedirsGroupUsagePath
	EndPointBasedirUsageUser   = gas.EndPointREST + basedirsUserUsagePath
	EndPointBasedirSubdirGroup = gas.EndPointREST + basedirsGroupSubdirPath
	EndPointBasedirSubdirUser  = gas.EndPointREST + basedirsUserSubdirPath
	EndPointBasedirHistory     = gas.EndPointREST + basedirsHistoryPath

	// EndPointAuthBasedir* are the endpoints for making base directory related
	// queries if authorization is implemented.
	EndPointAuthBasedirUsageGroup  = gas.EndPointAuth + basedirsGroupUsagePath
	EndPointAuthBasedirUsageUser   = gas.EndPointAuth + basedirsUserUsagePath
	EndPointAuthBasedirSubdirGroup = gas.EndPointAuth + basedirsGroupSubdirPath
	EndPointAuthBasedirSubdirUser  = gas.EndPointAuth + basedirsUserSubdirPath
	EndPointAuthBasedirHistory     = gas.EndPointAuth + basedirsHistoryPath

	// TreePath is the path to the static tree website.
	TreePath = "/tree"

	// EndPointAuthTree is the endpoint for making treemap queries when
	// authorization is implemented.
	EndPointAuthTree = gas.EndPointAuth + TreePath

	defaultDir    = "/"
	defaultSplits = "2"
	unknown       = "#unknown"
)

// Server is used to start a web server that provides a REST API to the dgut
// package's database, and a website that displays the information nicely.
type Server struct {
	gas.Server
	tree           *dgut.Tree
	treeMutex      sync.RWMutex
	whiteCB        WhiteListCallback
	uidToNameCache map[uint32]string
	gidToNameCache map[uint32]string
	userToGIDs     map[string][]string
	dgutPaths      []string
	dgutWatcher    *watch.Watcher
	dataTimeStamp  time.Time
	areas          map[string][]string

	basedirsMutex sync.RWMutex
	basedirs      *basedirs.BaseDirReader
	basedirsPath  string
}

// New creates a Server which can serve a REST API and website.
//
// It logs to the given io.Writer, which could for example be syslog using the
// log/syslog pkg with syslog.new(syslog.LOG_INFO, "tag").
func New(logWriter io.Writer) *Server {
	s := &Server{
		Server:         *gas.New(logWriter),
		uidToNameCache: make(map[uint32]string),
		gidToNameCache: make(map[uint32]string),
		userToGIDs:     make(map[string][]string),
	}

	s.SetStopCallBack(s.stop)

	return s
}

// stop is called when the server is Stop()ped, cleaning up our additional
// properties.
func (s *Server) stop() {
	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	if s.dgutWatcher != nil {
		s.dgutWatcher.Stop()
		s.dgutWatcher = nil
	}

	if s.tree != nil {
		s.tree.Close()
		s.tree = nil
	}
}
