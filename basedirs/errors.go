package basedirs

import "errors"

// ErrNoSuchUserOrGroup is returned when requested subdir data is missing.
var ErrNoSuchUserOrGroup = errors.New("no such user or group")
