/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package server

import (
	"net/http"
	"os/user"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/summary"
)

// getWhere responds with a list of directory stats describing where data is on
// disks. LoadDGUTDB() must already have been called. This is called when there
// is a GET on /rest/v1/where or /rest/v1/auth/where.
func (s *Server) getWhere(c *gin.Context) {
	dir := c.DefaultQuery("dir", defaultDir)
	splits := c.DefaultQuery("splits", defaultSplits)

	filter, err := s.getFilter(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	dcss, err := s.tree.Where(dir, filter, convertSplitsValue(splits))
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.IndentedJSON(http.StatusOK, s.dcssToSummaries(dcss))
}

// getFilter extracts the user's filter requests, as restricted by their jwt,
// and returns a tree filter.
func (s *Server) getFilter(c *gin.Context) (*dgut.Filter, error) {
	groups := c.Query("groups")
	users := c.Query("users")
	types := c.Query("types")

	filterGIDs, err := s.restrictedGroups(c, groups)
	if err != nil {
		return nil, err
	}

	filterUIDs, err := s.userIDsFromNames(users)
	if err != nil {
		return nil, err
	}

	return makeTreeFilter(filterGIDs, filterUIDs, types)
}

// restrictedGroups checks our JWT if present, and will return the GIDs that
// user is allowed to query. If groups arg is not blank, but a comma separated
// list of group names, further limits the GIDs returned to be amongst those. If
// the user is not restricted on GIDs, returns all the given group names as
// GIDs.
func (s *Server) restrictedGroups(c *gin.Context, groups string) ([]string, error) {
	ids, wanted, err := getWantedIDs(groups, groupNameToGID)
	if err != nil {
		return nil, err
	}

	var allowedIDs []string

	if u := s.getUserFromContext(c); u != nil {
		allowedIDs, err = s.userGIDs(u)
		if err != nil {
			return nil, err
		}
	}

	if allowedIDs == nil {
		return ids, nil
	}

	return restrictIDsToWanted(allowedIDs, wanted)
}

// groupNameToGID converts group name to GID.
func groupNameToGID(name string) (string, error) {
	g, err := user.LookupGroup(name)
	if err != nil {
		return "", err
	}

	return g.Gid, nil
}

// getWantedIDs splits the given comma separated names in to a slice and then
// passes each name to the given callback to convert it to an id, then returns
// a slice of the ids, along with a map where the slice elements are the keys.
// Both will be nil if names is blank.
func getWantedIDs(names string, cb func(name string) (string, error)) ([]string, map[string]bool, error) {
	splitNames := splitCommaSeparatedString(names)

	ids := make([]string, len(splitNames))
	wanted := make(map[string]bool, len(splitNames))

	for i, name := range splitNames {
		id, err := cb(name)
		if err != nil {
			return nil, nil, err
		}

		ids[i] = id
		wanted[id] = true
	}

	return ids, wanted, nil
}

// splitCommaSeparatedString splits the given comma separated string in to a
// slice of string. Returns nil if value is blank.
func splitCommaSeparatedString(value string) []string {
	var parts []string
	if value != "" {
		parts = strings.Split(value, ",")
	}

	return parts
}

// getUserFromContext extracts the User information from our JWT. Returns nil if
// we're not doing auth.
func (s *Server) getUserFromContext(c *gin.Context) *gas.User {
	if s.AuthRouter() == nil {
		return nil
	}

	return s.GetUser(c)
}

// restrictIDsToWanted returns the elements of ids that are in wanted. Will
// return ids if wanted is empty. Returns an error if you don't want any of the
// given ids.
func restrictIDsToWanted(ids []string, wanted map[string]bool) ([]string, error) {
	if len(wanted) == 0 {
		return ids, nil
	}

	var final []string //nolint:prealloc

	for _, id := range ids {
		if !wanted[id] {
			continue
		}

		final = append(final, id)
	}

	if final == nil {
		return nil, ErrBadQuery
	}

	return final, nil
}

// userIDsFromNames returns the user IDs that correspond to the given comma
// separated list of user names. This does not check the usernames stored in the
// JWT, because users are allowed to know about files owned by other users in
// the groups they belong to; security restrictions are purely based on the
// enforced restrictedGroups().
func (s *Server) userIDsFromNames(users string) ([]string, error) {
	ids, _, err := getWantedIDs(users, gas.UserNameToUID)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// makeTreeFilter creates a filter from string args.
func makeTreeFilter(gids, uids []string, types string) (*dgut.Filter, error) {
	filter := makeTreeGroupFilter(gids)

	addUsersToFilter(filter, uids)

	err := addTypesToFilter(filter, types)

	return filter, err
}

// makeTreeGroupFilter creates a filter for groups.
func makeTreeGroupFilter(gids []string) *dgut.Filter {
	if len(gids) == 0 {
		return &dgut.Filter{}
	}

	return &dgut.Filter{GIDs: idStringsToInts(gids)}
}

// idStringsToInts converts a slice of id strings into uint32s.
func idStringsToInts(idStrings []string) []uint32 {
	ids := make([]uint32, len(idStrings))

	for i, idStr := range idStrings {
		// no error is possible here, with the number string coming from an OS
		// lookup.
		//nolint:errcheck
		id, _ := strconv.ParseUint(idStr, 10, 32)

		ids[i] = uint32(id)
	}

	return ids
}

// addUsersToFilter adds a filter for users to the given filter.
func addUsersToFilter(filter *dgut.Filter, uids []string) {
	if len(uids) == 0 {
		return
	}

	filter.UIDs = idStringsToInts(uids)
}

// addTypesToFilter adds a filter for types to the given filter.
func addTypesToFilter(filter *dgut.Filter, types string) error {
	if types == "" {
		return nil
	}

	tnames := splitCommaSeparatedString(types)
	fts := make([]summary.DirGUTFileType, len(tnames))

	for i, name := range tnames {
		ft, err := summary.FileTypeStringToDirGUTFileType(name)
		if err != nil {
			return err
		}

		fts[i] = ft
	}

	filter.FTs = fts

	return nil
}

// convertSplitsValue converts the given number string in to an int. On failure,
// returns our default value for splits of 2.
func convertSplitsValue(splits string) int {
	splitsN, err := strconv.ParseUint(splits, 10, 8)
	if err != nil {
		return convertSplitsValue(defaultSplits)
	}

	return int(splitsN)
}
