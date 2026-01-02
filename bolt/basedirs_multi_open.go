package bolt

import (
	"errors"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
)

// OpenMultiBaseDirsReader opens multiple basedirs.db files and returns a
// basedirs.Reader that preserves current multi-mount aggregation behaviour.
//
// ownersPath is the owners CSV path used to populate Usage.Owner.
func OpenMultiBaseDirsReader(ownersPath string, dbPaths ...string) (basedirs.Reader, error) {
	if len(dbPaths) == 0 {
		return nil, errors.New("no basedirs db paths provided") //nolint:err113
	}

	readers := make([]basedirs.Reader, 0, len(dbPaths))
	for _, p := range dbPaths {
		r, err := OpenBaseDirsReader(p, ownersPath)
		if err != nil {
			for _, rr := range readers {
				_ = rr.Close()
			}

			return nil, err
		}

		readers = append(readers, r)
	}

	return multiBaseDirsReader(readers), nil
}
