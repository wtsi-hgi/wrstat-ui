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

package server

import (
	gas "github.com/wtsi-hgi/go-authserver"
)

// userGIDs returns the unix group IDs for the given User's UIDs. This calls
// *User.GIDs(), but caches the result against username, and returns cached
// results if possible.
//
// As a special case, if one of the groups is white-listed per
// WhiteListGroups(), returns a nil slice.
func (s *Server) userGIDs(u *gas.User) ([]string, error) {
	if gids, found := s.userToGIDs[u.Username]; found {
		return gids, nil
	}

	gids, err := u.GIDs()
	if err != nil {
		return nil, err
	}

	if s.whiteListed(gids) {
		gids = nil
	}

	s.userToGIDs[u.Username] = gids

	return gids, nil
}

// WhiteListCallback is passed to WhiteListGroups() and is used by the server
// to determine if a given unix group ID is special, indicating that users
// belonging to it have permission to view information about all other unix
// groups. If it's a special group, return true; otherwise false.
type WhiteListCallback func(gid string) bool

// WhiteListGroups sets the given callback on the server, which will now be used
// to check if any of the groups that a user belongs to have been whitelisted,
// giving that user unrestricted access to know about all groups.
//
// Do NOT call this more than once or after the server has started responding to
// client queries.
func (s *Server) WhiteListGroups(wcb WhiteListCallback) {
	s.whiteCB = wcb
}

// whiteListed returns true if one of the gids has been white-listed.
func (s *Server) whiteListed(gids []string) bool {
	if s.whiteCB == nil {
		return false
	}

	for _, gid := range gids {
		if s.whiteCB(gid) {
			return true
		}
	}

	return false
}
