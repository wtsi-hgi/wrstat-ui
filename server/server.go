/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
 *	- Michael Grace <mg38@sanger.ac.uk>
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
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-contrib/secure"
	"github.com/gin-gonic/gin"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/watch"
	"gopkg.in/tylerb/graceful.v1"
)

//go:embed static
var staticFS embed.FS

const (
	// EndPointREST is the base location for all REST endpoints.
	EndPointREST = "/rest/v1"

	// EndPointJWT is the endpoint for creating or refreshing a JWT.
	EndPointJWT = EndPointREST + "/jwt"

	// EndPointAuth is the name of the router group that endpoints requiring JWT
	// authorisation should belong to.
	EndPointAuth = EndPointREST + "/auth"

	wherePath = "/where"

	// EndPointWhere is the endpoint for making where queries if authorization
	// isn't implemented.
	EndPointWhere = EndPointREST + wherePath

	// EndPointAuthWhere is the endpoint for making where queries if
	// authorization is implemented.
	EndPointAuthWhere = EndPointAuth + wherePath

	groupAreasPaths = "/group-areas"

	// EndPointAuthGroupAreas is the endpoint for making queries on what the
	// group areas are, which is available if authorization is implemented.
	EndPointAuthGroupAreas = EndPointAuth + groupAreasPaths

	// TreePath is the path to the static tree website.
	TreePath = "/tree"

	// EndpointOIDCLogin will be handled by redirecting the user to Okta.
	EndpointOIDCLogin = "/login"

	// EndpointOIDCCLILogin will be handled by redirecting the user to Okta,
	// to get an auth code back to copy paste.
	EndpointOIDCCLILogin = "/login-cli"

	// EndpointAuthCallback is the endpoint where the OIDC provider will
	// send the user back to after login.
	EndpointAuthCallback    = "/callback"
	EndpointAuthCLICallback = "/callback-cli"

	// EndpointCLIAuthCode is the endpoint the user can get an auth code from
	// to copy paste into the terminal for a CLI session.
	EndpointCLIAuthCode = "/auth-code"

	// EndPointAuthTree is the endpoint for making treemap queries when
	// authorization is implemented.
	EndPointAuthTree = EndPointAuth + TreePath

	ErrNeedsAuth = Error("authentication must be enabled")

	defaultDir        = "/"
	defaultSplits     = "2"
	stopTimeout       = 10 * time.Second
	devEnvKey         = "WRSTAT_SERVER_DEV"
	devEnvVal         = "1"
	unknown           = "#unknown"
	readHeaderTimeout = 20 * time.Second
)

// AuthCallback is a function that returns true if the given password is valid
// for the given username. It also returns the user's UID.
type AuthCallback func(username, password string) (bool, string)

// Server is used to start a web server that provides a REST API to the dgut
// package's database, and a website that displays the information nicely.
type Server struct {
	router         *gin.Engine
	tree           *dgut.Tree
	treeMutex      sync.RWMutex
	srv            *graceful.Server
	srvMutex       sync.Mutex
	authGroup      *gin.RouterGroup
	authCB         AuthCallback
	whiteCB        WhiteListCallback
	uidToNameCache map[uint32]string
	gidToNameCache map[uint32]string
	userToGIDs     map[string][]string
	dgutPaths      []string
	dgutWatcher    *watch.Watcher
	dataTimeStamp  time.Time
	logger         *log.Logger
	webOAuth       *oauthEnv
	cliOAuth       *oauthEnv
	areas          map[string][]string
}

// New creates a Server which can serve a REST API and website.
//
// It logs to the given io.Writer, which could for example be syslog using the
// log/syslog pkg with syslog.new(syslog.LOG_INFO, "tag").
func New(logWriter io.Writer) *Server {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()

	logger := log.New(logWriter, "", 0)

	gin.DisableConsoleColor()
	gin.DefaultWriter = logger.Writer()

	r.Use(ginLogger())

	r.Use(gin.RecoveryWithWriter(logWriter))

	return &Server{
		router:         r,
		uidToNameCache: make(map[uint32]string),
		gidToNameCache: make(map[uint32]string),
		userToGIDs:     make(map[string][]string),
		logger:         logger,
	}
}

// ginLogger returns a handler that will format logs in a way that is searchable
// and nice in syslog output.
func ginLogger() gin.HandlerFunc {
	return gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf("%s - [%s %s %s \"%s\"] STATUS=%d %s %s\n",
			param.ClientIP,
			param.Method,
			param.Path,
			param.Request.Proto,
			param.Request.UserAgent(),
			param.StatusCode,
			param.Latency,
			param.ErrorMessage,
		)
	})
}

// Start will start listening to the given address (eg. "localhost:8080"), and
// serve the REST API and website over https; you must provide paths to your
// certficate and key file.
//
// It blocks, but will gracefully shut down on SIGINT and SIGTERM. If you
// Start() in a go-routine, you can call Stop() manually.
func (s *Server) Start(addr, certFile, keyFile string) error {
	s.router.Use(secure.New(secure.DefaultConfig()))

	srv := &graceful.Server{
		Timeout: stopTimeout,

		Server: &http.Server{
			Addr:              addr,
			Handler:           s.router,
			ReadHeaderTimeout: readHeaderTimeout,
		},
	}

	s.srvMutex.Lock()
	s.srv = srv
	s.srvMutex.Unlock()

	return srv.ListenAndServeTLS(certFile, keyFile)
}

// Stop() gracefully stops the server after Start(), and waits for active
// connections to close and the port to be available again. It also closes the
// database if you LoadDGUTDBs().
func (s *Server) Stop() {
	s.srvMutex.Lock()

	if s.srv == nil {
		s.srvMutex.Unlock()

		return
	}

	srv := s.srv
	s.srv = nil
	s.srvMutex.Unlock()

	ch := srv.StopChan()
	srv.Stop(stopTimeout)
	<-ch

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
