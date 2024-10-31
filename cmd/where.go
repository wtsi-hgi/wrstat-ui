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
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/server"
	"github.com/wtsi-ssg/wrstat/v5/summary"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	defaultWhereSplits             = 2
	defaultSize                    = "50M"
	hoursPerDay                    = 24
	jwtBasename                    = ".wrstat.jwt"
	errBadGroupArea                = Error("unknown group area")
	privatePerms       os.FileMode = 0600
)

// options for this cmd.
var (
	whereQueryDir        string
	whereSplits          int
	whereGroups          string
	whereUsers           string
	whereTypes           string
	whereSize            string
	whereAccess          int
	whereShowSupergroups bool
	whereSupergroup      string
	whereCert            string
	whereJSON            bool
	whereOrder           string
	whereShowUG          bool
	whereUnused          string
	whereUnchanged       string
)

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

--groups: 	 only consider files that have group ownership of one of these
          	 comma-separated groups.
--users: 	 only consider files that have user ownership of one of these
         	 comma-separated users.
--types: 	 only consider files that are one of these comma-separated file
			 types, from this set of allowed values: 
			 	vcf,vcf.gz,bcf,sam,bam,cram,fasta,fastq,fastq.gz,ped/bed,
				compressed,text,log,temp,other
--unused:	 only consider files that have been unused (based on access time)
			 for certain amounts of time, from this set of allowed values 
			 (where M is months and Y is years):
			 	1M,2M,6M,1Y,2Y,3Y,5Y,7Y
--unchanged: only consider files that have been unchanged (based on modify time)
			 for certain amounts of time, using the same values as unused.  

If --unused and --unchanged are not specified then files of any age are 
considered. These options are mutually exclusive.

To avoid producing too much output, the --size option (specify your own units,
eg. 50M for 50 megabytes) can be used to not display directories that have less
than that size of data nested inside. Defaults to 50M. Likewise, you can use
--access (number of days) to only show directories where a file nested inside 
hasn't been accessed for at least that long.

You can change the sort --order from the default of by 'size', to by 'count',
'age' or 'dir'.

--size, --access and --sort are ignored, however, if you choose --json output,
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

--show_areas will output a JSON map of group area names, with values being the
list of unix groups belonging to that area. You can then supply one of the
group area names to --area to also add the corresponding groups to the --groups
filter.

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

		c, err := gas.NewClientCLI(jwtBasename, serverTokenBasename, url, whereCert, true)
		if err != nil {
			die(err.Error())
		}

		if whereShowSupergroups {
			if errs := showSupergroups(c); err != nil {
				die(errs.Error())
			}

			return
		}

		if whereQueryDir == "" {
			die("you must supply a --dir you wish to query")
		}

		minSizeBytes, err := bytefmt.ToBytes(whereSize)
		if err != nil {
			die("bad --size: %s", err)
		}

		if whereUnused != "" && whereUnchanged != "" {
			die("--unused and --unchanged are mutually exclusive")
		}

		age := summary.DGUTAgeAll
		if whereUnused != "" {
			age = stringToAge("A" + whereUnused)
		} else if whereUnchanged != "" {
			age = stringToAge("M" + whereUnchanged)
		}

		minAtime := time.Now().Add(-(time.Duration(whereAccess*hoursPerDay) * time.Hour))

		err = where(c, whereQueryDir, whereGroups, whereSupergroup, whereUsers, whereTypes, age,
			fmt.Sprintf("%d", whereSplits), whereOrder, minSizeBytes, minAtime, whereJSON)
		if err != nil {
			die(err.Error())
		}
	},
}

func init() { //nolint:funlen
	RootCmd.AddCommand(whereCmd)

	// flags specific to these sub-commands
	whereCmd.Flags().StringVarP(&whereQueryDir, "dir", "d", "/",
		"directory path you wish to query")
	whereCmd.Flags().IntVarP(&whereSplits, "splits", "s", defaultWhereSplits,
		"number of splits (see help text)")
	whereCmd.Flags().StringVarP(&whereGroups, "groups", "g", "",
		"comma separated list of unix groups to filter on")
	whereCmd.Flags().StringVar(&whereSupergroup, "area", "",
		"filter on the groups in this --show_areas area")
	whereCmd.Flags().BoolVar(&whereShowSupergroups, "show_areas", false,
		"just show group area details")
	whereCmd.Flags().StringVarP(&whereUsers, "users", "u", "",
		"comma separated list of usernames to filter on")
	whereCmd.Flags().StringVarP(&whereTypes, "types", "t", "",
		"comma separated list of types (amongst vcf,vcf.gz,bcf,sam,bam,cram,fasta,fastq,fastq.gz,"+
			"ped/bed,compressed,text,log,temp,other) to filter on")
	whereCmd.Flags().StringVar(&whereSize, "size", defaultSize,
		"minimum size (specify the unit) of files nested under a directory for it to be reported on")
	whereCmd.Flags().IntVar(&whereAccess, "access", 0,
		"do not report on directories that contain a file whose access time falls within the last x days")
	whereCmd.Flags().StringVarP(&whereCert, "cert", "c", "",
		"path to the server's certificate to force trust in it")
	whereCmd.Flags().StringVarP(&whereOrder, "order", "o", "size",
		"sort order of results; size, count, age or dir")
	whereCmd.Flags().BoolVar(&whereShowUG, "show_ug", false,
		"output USERS and GROUPS columns")
	whereCmd.Flags().BoolVarP(&whereJSON, "json", "j", false,
		"output JSON (ignores --minimum and --order)")
	whereCmd.Flags().StringVar(&whereUnused, "unused", "",
		"unused age value to filter on (amongst 1M,2M,6M,1Y,2Y,3Y,5Y,7Y)")
	whereCmd.Flags().StringVar(&whereUnchanged, "unchanged", "",
		"unchanged age value to filter on (amongst 1M,2M,6M,1Y,2Y,3Y,5Y,7Y)")
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

// showSupergroups does a query just to get details about the group areas.
func showSupergroups(c *gas.ClientCLI) error {
	areas, err := getSupergroups(c)
	if err != nil {
		return err
	}

	m, err := json.MarshalIndent(areas, "", "  ")
	if err != nil {
		return err
	}

	cliPrint(string(m))

	return nil
}

// getSupergroups returns the map of the server's configured group areas.
func getSupergroups(c *gas.ClientCLI) (map[string][]string, error) {
	areas, err := server.GetGroupAreas(c)
	if err != nil {
		return nil, err
	}

	return areas, nil
}

func stringToAge(ageStr string) summary.DirGUTAge { //nolint:funlen,gocyclo,cyclop
	switch ageStr {
	case "A1M":
		return summary.DGUTAgeA1M
	case "A2M":
		return summary.DGUTAgeA2M
	case "A6M":
		return summary.DGUTAgeA6M
	case "A1Y":
		return summary.DGUTAgeA1Y
	case "A2Y":
		return summary.DGUTAgeA2Y
	case "A3Y":
		return summary.DGUTAgeA3Y
	case "A5Y":
		return summary.DGUTAgeA5Y
	case "A7Y":
		return summary.DGUTAgeA7Y
	case "M1M":
		return summary.DGUTAgeM1M
	case "M2M":
		return summary.DGUTAgeM2M
	case "M6M":
		return summary.DGUTAgeM6M
	case "M1Y":
		return summary.DGUTAgeM1Y
	case "M2Y":
		return summary.DGUTAgeM2Y
	case "M3Y":
		return summary.DGUTAgeM3Y
	case "M5Y":
		return summary.DGUTAgeM5Y
	case "M7Y":
		return summary.DGUTAgeM7Y
	}

	die("invalid age")

	return summary.DGUTAgeAll
}

// where does the main job of querying the server to answer where the data is on
// disk.
func where(c *gas.ClientCLI, dir, groups, supergroup, users, types string, age summary.DirGUTAge,
	splits, order string, minSizeBytes uint64, minAtime time.Time, json bool,
) error {
	var err error

	if groups, err = mergeGroupsWithAreaGroups(c, groups, supergroup); err != nil {
		return err
	}

	body, dss, err := server.GetWhereDataIs(c, dir, groups, users, types, age, splits)
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

// mergeGroupsWithAreaGroups will get the groups belonging to the given
// supergroup "group area", and merge them with the given groups, removing dups.
func mergeGroupsWithAreaGroups(c *gas.ClientCLI, groups, supergroup string) (string, error) {
	if supergroup == "" {
		return groups, nil
	}

	areas, err := getSupergroups(c)
	if err != nil {
		return "", err
	}

	superGroups, found := areas[supergroup]
	if !found {
		return "", errBadGroupArea
	}

	if groups == "" {
		return strings.Join(superGroups, ","), nil
	}

	noDups := deDup(append(strings.Split(groups, ","), superGroups...))

	return strings.Join(noDups, ","), nil
}

// deDup returns the given slice with duplicates removed.
func deDup(withDups []string) []string {
	allGroups := make(map[string]bool)

	var noDups []string

	for _, group := range withDups {
		if _, exists := allGroups[group]; !exists {
			allGroups[group] = true

			noDups = append(noDups, group)
		}
	}

	return noDups
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
		table.SetHeader([]string{"Directory", "Users", "Groups", "Count", "Size", "Age", "Modified"})
	} else {
		table.SetHeader([]string{"Directory", "Count", "Size", "Age", "Modified"})
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
		fmt.Sprintf("%d", timeToDaysAgo(ds.Atime)),
		fmt.Sprintf("%d", timeToDaysAgo(ds.Mtime)))
}

// timeToDaysAgo returns the given time converted to number of days ago.
func timeToDaysAgo(t time.Time) int {
	return int(time.Since(t).Hours() / hoursPerDay)
}

// printSkipped prints the given number of results were skipped.
func printSkipped(n int) {
	if n == 0 {
		return
	}

	warn("(%d results not displayed as smaller than --size or younger than --access)", n)
}
