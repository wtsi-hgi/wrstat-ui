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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/server"
)

const (
	defaultSplits             = 2
	defaultSize               = "50M"
	hoursePerDay              = 24
	jwtBasename               = ".wrstat.jwt"
	privatePerms  os.FileMode = 0600
)

// options for this cmd.
var whereQueryDir string
var whereSplits int
var whereGroups string
var whereUsers string
var whereTypes string
var whereSize string
var whereAge int
var whereCert string
var whereJSON bool
var whereOrder string
var whereShowUG bool

// whereCmd represents the where command.
var whereCmd = &cobra.Command{
	Use:   "where",
	Short: "Find out where data is on disks",
	Long: `Find out where data is on disks.

Query the wrstat server by providing its URL in the form domain:port (using the
WRSTAT_SERVER environment variable, or overriding that with a command line
argument), and the --dir you wish to know about (defaults to the root
directory).

This tool will show where data really lies: the deepest directory that has all
filter-passing files nested under it. It reports the count of files nested each
directory, the total size of all those files, and the oldest age of those files
(in days since last access).

With a --splits value of 0, returns just a single directory.
With a --splits value of 1, it also returns the results that --splits 0 on each
of the first result directory's children would give. And so on recursively for
larger numbers.

Think of the splits value as something like "depth" in to the filesystem tree,
but not quite, because it will give you the deepest directory past the split.
The default of 2 should give you useful results.

You can filter what files should be considered and reported on:

--groups: only consider files that have group ownership of one of these
          comma-separated groups.
--users:  only consider files that have user ownership of one of these
          comma-separated users.
--types:  only consider files that are one of these comma-separated file types,
          from this set of allowed values: cram,bam,index,compressed,
		  uncompressed,checkpoint,other,temporary

To avoid producing too much output, the --size option (specify your own units,
eg. 50M for 50 megabytes) can be used to not display directories that have less
than that size of data nested inside. Defaults to 50M. Likewise, you can use
--age (in days) to only show directories where a file nested inside hasn't been
accessed for at least that long.

You can change the sort --order from the default of by 'size', to by 'count',
'age' or 'dir'.

--size, --age and --sort are ignored, however, if you choose --json output,
which will just give you all the filtered results. In the JSON output, the Size
is in bytes and instead of "age" you get "Atime".

--show_ug adds columns for the users and groups that own files nested under each
directory.

If the wrstat server is using an untrusted certificate, the path to its
certificate can be provided with --cert, or the WRSTAT_SERVER_CERT environment
variable, to force trust in it.

On first usage, you will be asked to login via Okta to authenticate with the
server. A JWT with your verified username will be stored in your home directory
at ~/.wrstat.jwt.

When you run this, you will effectively have a hardcoded --groups filter
corresponding to your permissions, though you can restrict it further to a
subset of the groups you are allowed to see. (You will by default see
information about files created by all users that are group owned by the groups
you belong to, but can also filter on --users as well if desired.)

With the JWT in place, you won't have to login again, until it expires. Expiry
time is 5 days, but the JWT is automatically refreshed every time you use this,
with refreshes possible up to 5 days after expiry.
`,
	Run: func(cmd *cobra.Command, args []string) {
		setCLIFormat()

		url := getServerURL(args)

		if whereCert == "" {
			whereCert = os.Getenv("WRSTAT_SERVER_CERT")
		}

		if whereQueryDir == "" {
			die("you must supply a --dir you wish to query")
		}

		minSizeBytes, err := bytefmt.ToBytes(whereSize)
		if err != nil {
			die("bad --size: %s", err)
		}

		minAtime := time.Now().Add(-(time.Duration(whereAge*hoursePerDay) * time.Hour))

		err = where(url, whereCert, whereQueryDir, whereGroups, whereUsers, whereTypes,
			fmt.Sprintf("%d", whereSplits), whereOrder, minSizeBytes, minAtime, whereJSON)
		if err != nil {
			die(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(whereCmd)

	// flags specific to these sub-commands
	whereCmd.Flags().StringVarP(&whereQueryDir, "dir", "d", "/",
		"directory path you wish to query")
	whereCmd.Flags().IntVarP(&whereSplits, "splits", "s", defaultSplits,
		"number of splits (see help text)")
	whereCmd.Flags().StringVarP(&whereGroups, "groups", "g", "",
		"comma separated list of unix groups to filter on")
	whereCmd.Flags().StringVarP(&whereUsers, "users", "u", "",
		"comma separated list of usernames to filter on")
	whereCmd.Flags().StringVarP(&whereTypes, "types", "t", "",
		"comma separated list of types (amongst cram,bam,index,compressed,uncompressed,"+
			"checkpoint,other,temporary) to filter on")
	whereCmd.Flags().StringVar(&whereSize, "size", defaultSize,
		"minimum size (specify the unit) of files nested under a directory for it to be reported on")
	whereCmd.Flags().IntVar(&whereAge, "age", 0,
		"minimum age (in days) of the oldest file nested under a directory for it to be reported on")
	whereCmd.Flags().StringVarP(&whereCert, "cert", "c", "",
		"path to the server's certificate to force trust in it")
	whereCmd.Flags().StringVarP(&whereOrder, "order", "o", "size",
		"sort order of results; size, count, age or dir")
	whereCmd.Flags().BoolVar(&whereShowUG, "show_ug", false,
		"output USERS and GROUPS columns")
	whereCmd.Flags().BoolVarP(&whereJSON, "json", "j", false,
		"output JSON (ignores --minimum and --order)")

	whereCmd.SetHelpFunc(func(command *cobra.Command, strings []string) {
		hideGlobalFlags(whereCmd, command, strings)
	})
}

// getServerURL gets the wrstat server URL from the commandline arg or
// WRSTAT_SERVER env var.
func getServerURL(args []string) string {
	var url string

	if len(args) == 1 {
		url = args[0]
	} else {
		url = os.Getenv("WRSTAT_SERVER")
	}

	if url == "" {
		die("you must supply the server url")
	}

	return url
}

// where does the main job of querying the server to answer where the data is on
// disk.
func where(url, cert, dir, groups, users, types, splits, order string,
	minSizeBytes uint64, minAtime time.Time, json bool) error {
	token, err := getJWT(url, cert)
	if err != nil {
		return err
	}

	body, dss, err := server.GetWhereDataIs(url, cert, token, dir, groups, users, types, splits)
	if err != nil {
		return err
	}

	if json {
		cliPrint(string(body))

		return nil
	}

	orderDSs(dss, order)

	printWhereDataIs(dss, minSizeBytes, minAtime)

	return nil
}

// JWTPermissionsError is used to distinguish this type of error - where the
// already stored JWT token doesn't have private permissions.
type JWTPermissionsError struct {
	tokenFile string
}

// Error is the print out string for JWTPermissionsError, so the user can
// see and rectify the permissions issue.
func (e JWTPermissionsError) Error() string {
	return fmt.Sprintf("Token %s does not have %v permissions "+
		"- won't use it", e.tokenFile, privatePerms)
}

// getJWT checks if we have stored a jwt in a file in user's home directory.
// If so, the JWT is refreshed and returned.
//
// Otherwise, we ask the user for the password and login, storing and returning
// the new JWT.
func getJWT(url, cert string) (string, error) {
	token, err := getStoredJWT(url, cert)
	if err == nil {
		return token, nil
	}

	if errors.As(err, &JWTPermissionsError{}) {
		return "", err
	}

	token, err = login(url, cert)
	if err == nil {
		err = storeJWT(token)
	}

	return token, err
}

// getStoredJWT sees if we've previously called storeJWT(), gets the token
// from the file it made, then tries to refresh it on the Server.
//
// We also check if the token has private permissions, otherwise we won't use
// it. This is as an attempt to reduce the likelihood of the token being
// leaked with its long expiry time (used so the user doesn't have to continuously
// log in, as we're not working with specific refresh tokens to get new access
// tokens).
func getStoredJWT(url, cert string) (string, error) {
	path, err := jwtStoragePath()
	if err != nil {
		return "", err
	}

	stat, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	if stat.Mode() != privatePerms {
		return "", JWTPermissionsError{tokenFile: path}
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	token := strings.TrimSpace(string(content))

	return server.RefreshJWT(url, cert, token)
}

// jwtStoragePath returns the path where we store our JWT.
func jwtStoragePath() (string, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, jwtBasename), nil
}

// login gives the user a URL to visit to log in using Okta,
// which will give them back a code to paste here to authenticate.
func login(url, cert string) (string, error) {
	cliPrint("Login at this URL, and then copy and paste the given code back here: https://%s%s\n",
		url, server.EndpointOIDCCLILogin)
	cliPrint("Auth Code:")

	var authCode string

	fmt.Scanln(&authCode)

	return server.LoginWithOKTA(url, cert, authCode)
}

// storeJWT writes the given token string to a private file in user's home dir.
func storeJWT(token string) error {
	path, err := jwtStoragePath()
	if err != nil {
		return err
	}

	return os.WriteFile(path, []byte(token), privatePerms)
}

// orderDSs reorders the given DirSummarys by count or dir, does nothing if
// order is size (the default) or invalid.
func orderDSs(dss []*server.DirSummary, order string) {
	switch order {
	case "count":
		sort.Slice(dss, func(i, j int) bool {
			return dss[i].Count > dss[j].Count
		})
	case "age":
		sort.Slice(dss, func(i, j int) bool {
			return dss[i].Atime.Before(dss[j].Atime)
		})
	case "dir":
		sort.Slice(dss, func(i, j int) bool {
			return dss[i].Dir < dss[j].Dir
		})
	}
}

// printWhereDataIs formats query results and prints it to STDOUT.
func printWhereDataIs(dss []*server.DirSummary, minSizeBytes uint64, minAtime time.Time) {
	if len(dss) == 0 || (len(dss) == 1 && dss[0].Count == 0) {
		warn("no results")

		return
	}

	table := prepareWhereTable()
	skipped := 0

	for _, ds := range dss {
		if skipDS(ds, minSizeBytes, minAtime) {
			skipped++

			continue
		}

		table.Append(columns(ds))
	}

	table.Render()

	printSkipped(skipped)
}

// prepareWhereTable creates a table with a header that outputs to STDOUT.
func prepareWhereTable() *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)

	if whereShowUG {
		table.SetHeader([]string{"Directory", "Users", "Groups", "Count", "Size", "Age"})
	} else {
		table.SetHeader([]string{"Directory", "Count", "Size", "Age"})
	}

	return table
}

// skipDS returns true if the ds has a size smaller than the given minSizeByes,
// or an Atime after the given minAtime.
func skipDS(ds *server.DirSummary, minSizeBytes uint64, minAtime time.Time) bool {
	return ds.Size < minSizeBytes || ds.Atime.After(minAtime)
}

// columns returns the column data to display in the table for a given row.
func columns(ds *server.DirSummary) []string {
	cols := []string{ds.Dir}

	if whereShowUG {
		cols = append(cols, strings.Join(ds.Users, ","), strings.Join(ds.Groups, ","))
	}

	return append(cols,
		fmt.Sprintf("%d", ds.Count),
		humanize.IBytes(ds.Size),
		fmt.Sprintf("%d", timeToDaysAgo(ds.Atime)))
}

// timeToDaysAgo returns the given time converted to number of days ago.
func timeToDaysAgo(t time.Time) int {
	return int(time.Since(t).Hours() / hoursePerDay)
}

// printSkipped prints the given number of results were skipped.
func printSkipped(n int) {
	if n == 0 {
		return
	}

	warn(fmt.Sprintf("(%d results not displayed as smaller than --size or younger than --age)", n))
}
