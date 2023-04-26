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

package cmd

import (
	"encoding/csv"
	"errors"
	"io"
	"log/syslog"
	"os"
	"path/filepath"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v4/server"
)

const sentinelPollFrequencty = 1 * time.Minute

// options for this cmd.
var serverLogPath string
var serverBind string
var serverCert string
var serverKey string
var oktaURL string
var oktaOAuthIssuer string
var oktaOAuthClientID string
var oktaOAuthClientSecret string
var areasPath string

// serverCmd represents the server command.
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the web server",
	Long: `Start the web server.

Starting the web server brings up a web interface and REST API that will use the
latest *.dgut.dbs directory inside the given 'wrstat multi' output directory to
answer questions about where data is on the disks. (Provide your
'wrstat multi -f' argument as an unamed argument to this command.)

Your --bind address should include the port, and for it to work with your
--cert, you probably need to specify it as fqdn:port.

The server authenticates users using Okta. You must specify all of
--okta_issuer, --okta_id and --okta_secret or env vars OKTA_OAUTH2_ISSUER,
OKTA_OAUTH2_CLIENT_ID and OKTA_OAUTH2_CLIENT_SECRET. You must also specify
--okta_url if that is different to --bind (eg. the service is bound to localhost
and will be behind a proxy accessed at a different domain).

The server will log all messages (of any severity) to syslog at the INFO level,
except for non-graceful stops of the server, which are sent at the CRIT level or
include 'panic' in the message. The messages are tagged 'wrstat-server', and you
might want to filter away 'STATUS=200' to find problems.
If --logfile is supplied, logs to that file instaed of syslog.

If --areas is supplied, the group,area csv file pointed to will be used to add
"areas" to the server, allowing clients to specify an area to filter on all
groups with that area.

The server must be running for 'wrstat where' calls to succeed.

This command will block forever in the foreground; you can background it with
ctrl-z; bg. Or better yet, use the daemonize program to daemonize this.

It will monitor a file called ".dgut.dbs.updated" in the given directory and
attempt to reload the databases when the file is updated by another run of
'wrstat multi' with the same output directory. After reloading, will delete the
previous run's database files. It will use the mtime of the file as the data
creation time in reports.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("you must supply the path to your 'wrstat multi -f' output directory")
		}

		if serverBind == "" {
			die("you must supply --bind")
		}

		if serverCert == "" {
			die("you must supply --cert")
		}

		if serverKey == "" {
			die("you must supply --key")
		}

		checkOAuthArgs()

		logWriter := setServerLogger(serverLogPath)

		s := server.New(logWriter)

		err := s.EnableAuth(serverCert, serverKey, authenticateDeny)
		if err != nil {
			die("failed to enable authentication: %s", err)
		}

		if oktaURL == "" {
			oktaURL = serverBind
		}

		s.AddOIDCRoutes(oktaURL, oktaOAuthIssuer, oktaOAuthClientID, oktaOAuthClientSecret)

		s.WhiteListGroups(whiteLister)

		if areasPath != "" {
			s.AddGroupAreas(areasCSVToMap(areasPath))
		}

		info("opening databases, please wait...")
		dbPaths, err := server.FindLatestDgutDirs(args[0], dgutDBsSuffix)
		if err != nil {
			die("failed to find database paths: %s", err)
		}

		err = s.LoadDGUTDBs(dbPaths...)
		if err != nil {
			die("failed to load database: %s", err)
		}

		sentinel := filepath.Join(args[0], dgutDBsSentinelBasename)

		err = s.EnableDGUTDBReloading(sentinel, args[0], dgutDBsSuffix, sentinelPollFrequencty)
		if err != nil {
			die("failed to set up database reloading: %s", err)
		}

		err = s.AddTreePage()
		if err != nil {
			die("failed to add tree page: %s", err)
		}

		defer s.Stop()

		sayStarted()

		err = s.Start(serverBind, serverCert, serverKey)
		if err != nil {
			die("non-graceful stop: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(serverCmd)

	// flags specific to this sub-command
	serverCmd.Flags().StringVarP(&serverBind, "bind", "b", ":80",
		"address to bind to, eg host:port")
	serverCmd.Flags().StringVarP(&serverCert, "cert", "c", "",
		"path to certificate file")
	serverCmd.Flags().StringVarP(&serverKey, "key", "k", "",
		"path to key file")
	serverCmd.Flags().StringVar(&oktaURL, "okta_url", "",
		"Okta application URL, eg host:port (defaults to --bind)")
	serverCmd.Flags().StringVar(&oktaOAuthIssuer, "okta_issuer", os.Getenv("OKTA_OAUTH2_ISSUER"),
		"URL for Okta Oauth")
	serverCmd.Flags().StringVar(&oktaOAuthClientID, "okta_id", os.Getenv("OKTA_OAUTH2_CLIENT_ID"),
		"Okta Client ID")
	serverCmd.Flags().StringVar(&oktaOAuthClientSecret, "okta_secret", "",
		"Okta Client Secret (default $OKTA_OAUTH2_CLIENT_SECRET)")
	serverCmd.Flags().StringVar(&areasPath, "areas", "", "path to group,area csv file")
	serverCmd.Flags().StringVar(&serverLogPath, "logfile", "",
		"log to this file instead of syslog")

	serverCmd.SetHelpFunc(func(command *cobra.Command, strings []string) {
		hideGlobalFlags(serverCmd, command, strings)
	})
}

// checkOAuthArgs ensures we have the necessary args/ env vars for Okta auth.
func checkOAuthArgs() {
	if oktaOAuthClientSecret == "" {
		oktaOAuthClientSecret = os.Getenv("OKTA_OAUTH2_CLIENT_SECRET")
	}

	if oktaOAuthIssuer == "" || oktaOAuthClientID == "" || oktaOAuthClientSecret == "" {
		die("you must specify all info needed for Okta logins; see --help")
	}
}

// setServerLogger makes our appLogger log to the given path if non-blank,
// otherwise to syslog. Returns an io.Writer version of our appLogger for the
// server to log to.
func setServerLogger(path string) io.Writer {
	if path == "" {
		logToSyslog()
	} else {
		logToFile(path)
	}

	return &log15Writer{logger: appLogger}
}

// logToSyslog sets our applogger to log to syslog, dies if it can't.
func logToSyslog() {
	fh, err := log15.SyslogHandler(syslog.LOG_INFO|syslog.LOG_DAEMON, "wrstat-server", log15.LogfmtFormat())
	if err != nil {
		die("failed to log to syslog: %s", err)
	}

	appLogger.SetHandler(fh)
}

// log15Writer wraps a log15.Logger to make it conform to io.Writer interface.
type log15Writer struct {
	logger log15.Logger
}

// Write conforms to the io.Writer interface.
func (w *log15Writer) Write(p []byte) (n int, err error) {
	w.logger.Info(string(p))

	return len(p), nil
}

// authenticateDeny always returns false, since we don't do basic auth, but Okta
// instead.
func authenticateDeny(_, _ string) (bool, string) {
	return false, ""
}

var whiteListGIDs = map[string]struct{}{
	"0":     {},
	"1105":  {},
	"1313":  {},
	"1818":  {},
	"15306": {},
	"1662":  {},
	"15394": {},
}

// whiteLister is currently hard-coded to say that membership of certain gids
// means users should be treated like root.
func whiteLister(gid string) bool {
	_, ok := whiteListGIDs[gid]

	return ok
}

// areasCSVToMap takes a group,area csv file and converts it in to a map of
// area -> groups slice.
func areasCSVToMap(path string) map[string][]string {
	r, f := makeCSVReader(path)
	defer f.Close()

	areas := make(map[string][]string)

	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			die("could not read areas csv: %s", err)
		}

		groups, present := areas[rec[1]]
		if !present {
			groups = []string{}
		}

		areas[rec[1]] = append(groups, rec[0])
	}

	return areas
}

// makeCSVReader opens the given path and returns a CSV reader configured for
// 2 column CSV files. Also returns an *os.File that should you Close() after
// reading.
func makeCSVReader(path string) (*csv.Reader, *os.File) {
	f, err := os.Open(path)
	if err != nil {
		die("could not open areas csv: %s", err)
	}

	r := csv.NewReader(f)
	r.FieldsPerRecord = 2
	r.ReuseRecord = true

	return r, f
}

// sayStarted logs to console that the server stated. It does this a second
// after being calling in a goroutine, when we can assume the server has
// actually started; if it failed, we expect it to do so in less than a second
// and exit.
func sayStarted() {
	<-time.After(1 * time.Second)

	info("server started")
}
