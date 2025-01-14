/*******************************************************************************
 * Copyright (c) 2024, 2025 Genome Research Ltd.
 *
 * Authors:
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

package user

import (
	"os/user"
	"strconv"
)

// RealGIDAndUID returns the currently logged in user's gid and uid, and the
// corresponding group and user names.
func RealGIDAndUID() (uint32, uint32, string, string, error) {
	u, err := user.Current()
	if err != nil {
		return 0, 0, "", "", err
	}

	uid64, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		return 0, 0, "", "", err
	}

	groups, err := u.GroupIds()
	if err != nil || len(groups) == 0 {
		return 0, 0, "", "", err
	}

	gid64, err := strconv.ParseUint(groups[0], 10, 32)
	if err != nil {
		return 0, 0, "", "", err
	}

	group, err := user.LookupGroupId(groups[0])
	if err != nil {
		return 0, 0, "", "", err
	}

	return uint32(gid64), uint32(uid64), group.Name, u.Username, nil
}
