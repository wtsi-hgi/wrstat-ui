package summariseutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	sbasedirs "github.com/wtsi-hgi/wrstat-ui/summary/basedirs"
	dirguta "github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

const dbBatchSize = 10000

func ParseBasedirConfig(quotaPath, basedirsConfig string) (*basedirs.Quotas, basedirs.Config, error) {
	quotas, err := basedirs.ParseQuotas(quotaPath)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing quotas file: %w", err)
	}

	cf, err := os.Open(basedirsConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening basedirs config: %w", err)
	}
	defer cf.Close()

	config, err := basedirs.ParseConfig(cf)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing basedirs config: %w", err)
	}

	return quotas, config, nil
}

func CopyHistory(bd *basedirs.BaseDirs, basedirsHistoryDB string) error {
	db, err := basedirs.OpenDBRO(basedirsHistoryDB)
	if err != nil {
		return err
	}
	defer db.Close()

	return bd.CopyHistoryFrom(db)
}

func ParseMountpointsFromFile(mountpoints string) ([]string, error) {
	if mountpoints == "" {
		return nil, nil
	}

	data, err := os.ReadFile(mountpoints)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	mounts := make([]string, 0, len(lines))

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		mountpoint, err := strconv.Unquote(line)
		if err != nil {
			return nil, err
		}

		mounts = append(mounts, mountpoint)
	}

	return mounts, nil
}

func AddBasedirsSummariser(
	s *summary.Summariser,
	basedirsDB, basedirsHistoryDB, quotaPath, basedirsConfig, mountpoints string,
	modtime time.Time,
) error {
	bd, config, err := newBasedirsCreator(
		basedirsDB,
		quotaPath,
		basedirsConfig,
		mountpoints,
		modtime,
	)
	if err != nil {
		return err
	}

	if basedirsHistoryDB != "" {
		if err := CopyHistory(bd, basedirsHistoryDB); err != nil {
			return err
		}
	}

	s.AddDirectoryOperation(sbasedirs.NewBaseDirs(config.PathShouldOutput, bd))

	return nil
}

func newBasedirsCreator(
	basedirsDB, quotaPath, basedirsConfig, mountpoints string,
	modtime time.Time,
) (*basedirs.BaseDirs, basedirs.Config, error) {
	quotas, config, err := ParseBasedirConfig(quotaPath, basedirsConfig)
	if err != nil {
		return nil, nil, err
	}

	if err = os.Remove(basedirsDB); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, nil, err
	}

	bd, err := basedirs.NewCreator(basedirsDB, quotas)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new basedirs creator: %w", err)
	}

	mps, err := ParseMountpointsFromFile(mountpoints)
	if err != nil {
		return nil, nil, err
	}

	if len(mps) > 0 {
		bd.SetMountPoints(mps)
	}

	bd.SetModTime(modtime)

	return bd, config, nil
}

func AddDirgutaSummariser(s *summary.Summariser, dirgutaDB string) (func() error, error) {
	if err := os.RemoveAll(dirgutaDB); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	if err := os.MkdirAll(dirgutaDB, 0o755); err != nil {
		return nil, err
	}

	dg := db.NewDB(dirgutaDB)

	if err := dg.CreateDB(); err != nil {
		return nil, err
	}

	dg.SetBatchSize(dbBatchSize)

	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(dg))

	return dg.Close, nil
}
