/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
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
	"os/user"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	"github.com/wtsi-ssg/wrstat/v4/summary"
)

// makeFilterFromContext extracts the user's filter requests, and returns a tree
// filter.
func makeFilterFromContext(c *gin.Context) (*dgut.Filter, error) {
	groups, users, types := getFilterArgsFromContext(c)

	filterGIDs, err := getWantedIDs(groups, groupNameToGID)
	if err != nil {
		return nil, err
	}

	return makeFilterGivenGIDs(filterGIDs, users, types)
}

func getFilterArgsFromContext(c *gin.Context) (groups string, users string, types string) {
	groups = c.Query("groups")
	users = c.Query("users")
	types = c.Query("types")

	return
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
// a slice of the ids. Returns nil if names is blank.
func getWantedIDs(names string, cb func(name string) (string, error)) ([]uint32, error) {
	splitNames := splitCommaSeparatedString(names)

	ids := make([]uint32, len(splitNames))

	for i, name := range splitNames {
		id, err := cb(name)
		if err != nil {
			return nil, err
		}

		ids[i] = idStringsToInts(id)
	}

	return ids, nil
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

// idStringToInt converts a an id string to a uint32.
func idStringsToInts(idString string) uint32 {
	// no error is possible here, with the number string coming from an OS
	// lookup.
	//nolint:errcheck
	id, _ := strconv.ParseUint(idString, 10, 32)

	return uint32(id)
}

func makeFilterGivenGIDs(filterGIDs []uint32, users, types string) (*dgut.Filter, error) {
	filterUIDs, err := userIDsFromNames(users)
	if err != nil {
		return nil, err
	}

	return makeTreeFilter(filterGIDs, filterUIDs, types)
}

// userIDsFromNames returns the user IDs that correspond to the given comma
// separated list of user names. This does not check the usernames stored in the
// JWT, because users are allowed to know about files owned by other users in
// the groups they belong to; security restrictions are purely based on the
// enforced restrictedGroups().
func userIDsFromNames(users string) ([]uint32, error) {
	ids, err := getWantedIDs(users, gas.UserNameToUID)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// makeTreeFilter creates a filter from string args.
func makeTreeFilter(gids, uids []uint32, types string) (*dgut.Filter, error) {
	filter := makeTreeGroupFilter(gids)

	addUsersToFilter(filter, uids)

	err := addTypesToFilter(filter, types)

	return filter, err
}

// makeTreeGroupFilter creates a filter for groups.
func makeTreeGroupFilter(gids []uint32) *dgut.Filter {
	if len(gids) == 0 {
		return &dgut.Filter{}
	}

	return &dgut.Filter{GIDs: gids}
}

// addUsersToFilter adds a filter for users to the given filter.
func addUsersToFilter(filter *dgut.Filter, uids []uint32) {
	if len(uids) == 0 {
		return
	}

	filter.UIDs = uids
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

// allowedGIDs checks our JWT if present, and will return the GIDs that
// user is allowed to query. If the user is not restricted on GIDs, returns nil.
func (s *Server) allowedGIDs(c *gin.Context) (map[uint32]bool, error) {
	var allowedIDs []string

	var err error

	if u := s.getUserFromContext(c); u != nil {
		allowedIDs, err = s.userGIDs(u)
		if err != nil {
			return nil, err
		}
	}

	if allowedIDs == nil {
		return nil, nil //nolint:nilnil
	}

	allowed := make(map[uint32]bool, len(allowedIDs))

	for _, id := range allowedIDs {
		converted, erra := strconv.Atoi(id)
		if erra != nil {
			return nil, erra
		}

		allowed[uint32(converted)] = true
	}

	return allowed, nil
}

// getUserFromContext extracts the User information from our JWT. Returns nil if
// we're not doing auth.
func (s *Server) getUserFromContext(c *gin.Context) *gas.User {
	if s.AuthRouter() == nil {
		return nil
	}

	return s.GetUser(c)
}

// makeRestrictedFilterFromContext extracts the user's filter requests, as
// restricted by their jwt, and returns a tree filter.
func (s *Server) makeRestrictedFilterFromContext(c *gin.Context) (*dgut.Filter, error) {
	groups, users, types := getFilterArgsFromContext(c)

	restrictedGIDs, err := s.getRestrictedGIDs(c, groups)
	if err != nil {
		return nil, err
	}

	return makeFilterGivenGIDs(restrictedGIDs, users, types)
}

func (s *Server) getRestrictedGIDs(c *gin.Context, groups string) ([]uint32, error) {
	filterGIDs, err := getWantedIDs(groups, groupNameToGID)
	if err != nil {
		return nil, err
	}

	allowedGIDs, err := s.allowedGIDs(c)
	if err != nil {
		return nil, err
	}

	return restrictGIDs(allowedGIDs, filterGIDs)
}

// restrictGIDs returns the keys of allowedIDs that are in wantedIDs. Will
// return allowedIDs if wanted is empty; will return wantedIDs if allowedIDs is
// nil. Returns an error if you don't want any of the allowedIDs.
func restrictGIDs(allowedIDs map[uint32]bool, wantedIDs []uint32) ([]uint32, error) {
	if allowedIDs == nil {
		return wantedIDs, nil
	}

	ids := make([]uint32, 0, len(allowedIDs))

	for id := range allowedIDs {
		ids = append(ids, id)
	}

	if len(wantedIDs) == 0 {
		return ids, nil
	}

	var final []uint32 //nolint:prealloc

	for _, id := range wantedIDs {
		if !allowedIDs[id] {
			continue
		}

		final = append(final, id)
	}

	if final == nil {
		return nil, ErrBadQuery
	}

	return final, nil
}
