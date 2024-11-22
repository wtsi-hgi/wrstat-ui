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
