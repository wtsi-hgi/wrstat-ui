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
	"os/user"
)

// User is what we store in our JWTs.
type User struct {
	Username string
	UID      string
}

// GIDs returns the unix group IDs that our UID belongs to (unsorted, with no
// duplicates).
func (u *User) GIDs() ([]string, error) {
	if u.UID == "" {
		return nil, nil
	}

	return getGIDsForUID(u.UID)
}

// getGIDsForUID returns the group IDs that the given UID belongs to.
func getGIDsForUID(uid string) ([]string, error) {
	u, err := user.LookupId(uid)
	if err != nil {
		return nil, err
	}

	return u.GroupIds()
}

// userNameToUID converts user name to UID.
func userNameToUID(name string) (string, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return "", err
	}

	return u.Uid, nil
}
