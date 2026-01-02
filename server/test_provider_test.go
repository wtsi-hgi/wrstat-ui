package server

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
	internaldata "github.com/wtsi-hgi/wrstat-ui/internal/data"
	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/dirguta"
)

var errNoDatasetDirectoriesSupplied = errors.New("no dataset directories supplied")

type dbAdapter struct {
	d *db.DB
}

func (a *dbAdapter) DirInfo(dir string, filter *db.Filter) (*db.DirSummary, error) {
	return a.d.DirInfo(dir, filter)
}

func (a *dbAdapter) Children(dir string) ([]string, error) {
	return a.d.Children(dir), nil
}

func (a *dbAdapter) Info() (*db.DBInfo, error) {
	return a.d.Info()
}

func (a *dbAdapter) Close() error {
	return a.d.Close()
}

type memBaseDirs struct {
	mountPath string
	updatedAt time.Time

	owners          map[uint32]string
	cachedGroups    map[uint32]string
	cachedUsers     map[uint32]string
	mountTimestamps map[string]time.Time

	groupUsage   map[db.DirGUTAge][]*basedirs.Usage
	userUsage    map[db.DirGUTAge][]*basedirs.Usage
	groupSubdirs map[basedirs.SubDirKey][]*basedirs.SubDir
	userSubdirs  map[basedirs.SubDirKey][]*basedirs.SubDir
	groupHistory map[basedirs.HistoryKey][]basedirs.History

	info basedirs.DBInfo
}

func newMemBaseDirs(ownersPath string) (*memBaseDirs, error) {
	owners, err := basedirs.ParseOwners(ownersPath)
	if err != nil {
		return nil, err
	}

	m := &memBaseDirs{
		owners:          owners,
		cachedGroups:    make(map[uint32]string),
		cachedUsers:     make(map[uint32]string),
		mountTimestamps: make(map[string]time.Time),
		groupUsage:      make(map[db.DirGUTAge][]*basedirs.Usage),
		userUsage:       make(map[db.DirGUTAge][]*basedirs.Usage),
		groupSubdirs:    make(map[basedirs.SubDirKey][]*basedirs.SubDir),
		userSubdirs:     make(map[basedirs.SubDirKey][]*basedirs.SubDir),
		groupHistory:    make(map[basedirs.HistoryKey][]basedirs.History),
	}

	return m, nil
}

// ---- basedirs.Store ----

func (m *memBaseDirs) SetMountPath(mountPath string)    { m.mountPath = mountPath }
func (m *memBaseDirs) SetUpdatedAt(updatedAt time.Time) { m.updatedAt = updatedAt }

func (m *memBaseDirs) Reset() error { return nil }

func (m *memBaseDirs) PutGroupUsage(u *basedirs.Usage) error {
	m.groupUsage[u.Age] = append(m.groupUsage[u.Age], u)
	m.info.GroupDirCombos++

	return nil
}

func (m *memBaseDirs) PutUserUsage(u *basedirs.Usage) error {
	m.userUsage[u.Age] = append(m.userUsage[u.Age], u)
	m.info.UserDirCombos++

	return nil
}

func (m *memBaseDirs) PutGroupSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	m.groupSubdirs[key] = append([]*basedirs.SubDir(nil), subdirs...)
	m.info.GroupSubDirCombos++
	m.info.GroupSubDirs += len(subdirs)

	return nil
}

func (m *memBaseDirs) PutUserSubDirs(key basedirs.SubDirKey, subdirs []*basedirs.SubDir) error {
	m.userSubdirs[key] = append([]*basedirs.SubDir(nil), subdirs...)
	m.info.UserSubDirCombos++
	m.info.UserSubDirs += len(subdirs)

	return nil
}

func (m *memBaseDirs) AppendGroupHistory(key basedirs.HistoryKey, point basedirs.History) error {
	m.groupHistory[key] = append(m.groupHistory[key], point)
	m.info.GroupMountCombos++
	m.info.GroupHistories++

	return nil
}

func (m *memBaseDirs) Finalise() error { return nil }
func (m *memBaseDirs) Close() error    { return nil }

// ---- basedirs.Reader ----

func (m *memBaseDirs) SetMountPoints(_ []string)              {}
func (m *memBaseDirs) SetCachedGroup(gid uint32, name string) { m.cachedGroups[gid] = name }
func (m *memBaseDirs) SetCachedUser(uid uint32, name string)  { m.cachedUsers[uid] = name }

func (m *memBaseDirs) GroupUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	out := cloneUsage(m.groupUsage[age])
	fillUsageNamesAndOwners(out, m.cachedGroups, m.cachedUsers, m.owners)

	return out, nil
}

func (m *memBaseDirs) UserUsage(age db.DirGUTAge) ([]*basedirs.Usage, error) {
	out := cloneUsage(m.userUsage[age])
	fillUsageNamesAndOwners(out, m.cachedGroups, m.cachedUsers, m.owners)

	return out, nil
}

func (m *memBaseDirs) GroupSubDirs(gid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	key := basedirs.SubDirKey{ID: gid, BaseDir: basedir, Age: age}

	return cloneSubDirs(m.groupSubdirs[key]), nil
}

func (m *memBaseDirs) UserSubDirs(uid uint32, basedir string, age db.DirGUTAge) ([]*basedirs.SubDir, error) {
	key := basedirs.SubDirKey{ID: uid, BaseDir: basedir, Age: age}

	return cloneSubDirs(m.userSubdirs[key]), nil
}

func (m *memBaseDirs) History(gid uint32, path string) ([]basedirs.History, error) {
	key := basedirs.HistoryKey{GID: gid, MountPath: path}
	out := append([]basedirs.History(nil), m.groupHistory[key]...)

	return out, nil
}

func (m *memBaseDirs) Info() (*basedirs.DBInfo, error) {
	v := m.info

	return &v, nil
}

func (m *memBaseDirs) MountTimestamps() (map[string]time.Time, error) {
	out := make(map[string]time.Time, len(m.mountTimestamps))
	for k, v := range m.mountTimestamps {
		out[k] = v
	}

	return out, nil
}

// Close is already defined above for Store; satisfies Reader too.

func cloneUsage(in []*basedirs.Usage) []*basedirs.Usage {
	if len(in) == 0 {
		return nil
	}

	out := make([]*basedirs.Usage, len(in))
	for i, u := range in {
		du := *u
		out[i] = &du
	}

	return out
}

func cloneSubDirs(in []*basedirs.SubDir) []*basedirs.SubDir {
	if len(in) == 0 {
		return nil
	}

	out := make([]*basedirs.SubDir, len(in))
	for i, sd := range in {
		d := *sd
		out[i] = &d
	}

	return out
}

func fillUsageNamesAndOwners(
	us []*basedirs.Usage,
	cachedGroups map[uint32]string,
	cachedUsers map[uint32]string,
	owners map[uint32]string,
) {
	for _, u := range us {
		if u == nil {
			continue
		}

		switch {
		case u.GID == 0 && u.UID == 0:
			if u.Name == "" {
				u.Name = "root"
			}
			if u.Owner == "" {
				u.Owner = owners[0]
			}
		case u.GID != 0:
			if n, ok := cachedGroups[u.GID]; ok {
				u.Name = n
			} else if g, err := user.LookupGroupId(strconv.FormatUint(uint64(u.GID), 10)); err == nil {
				u.Name = g.Name
			}
			u.Owner = owners[u.GID]
		case u.UID != 0:
			if n, ok := cachedUsers[u.UID]; ok {
				u.Name = n
			} else if uu, err := user.LookupId(strconv.FormatUint(uint64(u.UID), 10)); err == nil {
				u.Name = uu.Username
			}
			u.Owner = owners[u.GID]
		}
	}
}

type testProvider struct {
	tree *db.Tree
	bd   basedirs.Reader
	cb   func()
}

func (p *testProvider) Tree() *db.Tree            { return p.tree }
func (p *testProvider) BaseDirs() basedirs.Reader { return p.bd }
func (p *testProvider) OnUpdate(cb func())        { p.cb = cb }
func (p *testProvider) Close() error {
	if p.bd != nil {
		_ = p.bd.Close()
	}

	if p.tree != nil {
		p.tree.Close()
	}

	return nil
}

func (p *testProvider) triggerUpdate(newTree *db.Tree, newBD basedirs.Reader) {
	p.tree = newTree

	p.bd = newBD
	if p.cb != nil {
		go p.cb()
	}
}

func GetUserAndGroups(t *testing.T) (string, string, []string) {
	t.Helper()

	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	gids, err := os.Getgroups()
	if err != nil {
		t.Fatal(err)
	}

	out := make([]string, 0, len(gids))
	for _, gid := range gids {
		out = append(out, strconv.Itoa(gid))
	}

	return u.Username, u.Uid, out
}

func CreateExampleDBsCustomIDs(t *testing.T, uid, gidA, gidB string, refTime int64) (string, error) {
	t.Helper()

	tmp := t.TempDir()
	name := strconv.FormatInt(time.Now().Unix()-10, 10) + "_test"

	dir := filepath.Join(tmp, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return dir, err
	}

	return dir, CreateExampleDBsCustomIDsWithDir(t, dir, uid, gidA, gidB, refTime)
}

func CreateExampleDBsCustomIDsWithDir(t *testing.T, dir, uid, gidA, gidB string, refTime int64) error {
	t.Helper()

	// dguta
	dgutaDir := filepath.Join(dir, "dirguta")
	if err := os.MkdirAll(dgutaDir, 0o755); err != nil {
		return err
	}

	d := db.NewDB(dgutaDir)
	d.SetBatchSize(20)

	if err := d.CreateDB(); err != nil {
		return err
	}

	data := internaldata.CreateDefaultTestData(
		parse32(t, gidA),
		parse32(t, gidB),
		0,
		parse32(t, uid),
		0,
		refTime,
	).AsReader()
	s := summary.NewSummariser(stats.NewStatsParser(data))
	s.AddDirectoryOperation(dirguta.NewDirGroupUserTypeAge(d))

	if err := s.Summarise(); err != nil {
		_ = d.Close()

		return err
	}

	if err := d.Close(); err != nil {
		return err
	}

	// Persist basedirs data as a simple marker so tests that expect a file to
	// exist still behave. The actual data is loaded via the provider helper.
	if err := os.WriteFile(filepath.Join(dir, "basedirs.db"), []byte("test"), 0o600); err != nil {
		return err
	}

	return nil
}

func parse32(t *testing.T, s string) uint32 {
	t.Helper()

	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 32)
	if err != nil {
		t.Fatal(err)
	}

	return uint32(n)
}

func mountKeyFromDatasetDir(dir string) string {
	base := filepath.Base(dir)
	if parts := strings.SplitN(base, "_", 2); len(parts) == 2 {
		return parts[1]
	}

	return ""
}

// BuildTestProvider constructs a Provider for the given dataset directories.
// This avoids importing the bolt package in server tests.
func BuildTestProvider(t *testing.T, datasetDirs []string, ownersPath string, updatedAt time.Time) (Provider, error) {
	t.Helper()

	mountTimestamps := make(map[string]time.Time)

	for _, dsDir := range datasetDirs {
		if mk := mountKeyFromDatasetDir(dsDir); mk != "" {
			mountTimestamps[mk] = updatedAt
		}
	}

	return BuildTestProviderWithMountTimestamps(t, datasetDirs, ownersPath, mountTimestamps)
}

// BuildTestProviderWithMountTimestamps is a test helper that allows callers to
// specify per-mount timestamps (keyed by mountKey).
func BuildTestProviderWithMountTimestamps(
	t *testing.T,
	datasetDirs []string,
	ownersPath string,
	mountTimestamps map[string]time.Time,
) (Provider, error) {
	t.Helper()

	if len(datasetDirs) == 0 {
		return nil, errNoDatasetDirectoriesSupplied
	}

	// Tree: db.DB supports querying multiple databases simultaneously.
	dgutaDirs := make([]string, 0, len(datasetDirs))
	for _, dsDir := range datasetDirs {
		dgutaDirs = append(dgutaDirs, filepath.Join(dsDir, "dirguta"))
	}

	d := db.NewDB(dgutaDirs...)
	if err := d.Open(); err != nil {
		return nil, err
	}

	tree := db.NewTree(&dbAdapter{d: d})

	// BaseDirs
	mbd, err := newMemBaseDirs(ownersPath)
	if err != nil {
		tree.Close()

		return nil, err
	}

	mbd.SetMountPath("/a/")

	// Use the latest timestamp as the overall dataset "updatedAt".
	latest := time.Time{}
	for _, ts := range mountTimestamps {
		if ts.After(latest) {
			latest = ts
		}
	}

	if latest.IsZero() {
		latest = time.Now()
	}

	mbd.SetUpdatedAt(latest)

	for _, dsDir := range datasetDirs {
		mk := mountKeyFromDatasetDir(dsDir)
		if mk == "" {
			continue
		}

		ts := mountTimestamps[mk]
		if ts.IsZero() {
			ts = latest
		}

		mbd.mountTimestamps[mk] = ts
	}

	// Seed a small amount of deterministic basedirs data used by server endpoints.
	// The server tests assert only basic non-empty responses, plus that subdirs
	// and history are retrievable for the IDs and base-dirs returned in usage.
	mbd.groupUsage[db.DGUTAgeAll] = []*basedirs.Usage{
		{
			GID:         0,
			BaseDir:     "/a/",
			UsageSize:   1,
			UsageInodes: 1,
			Mtime:       latest,
			Age:         db.DGUTAgeAll,
		},
	}
	mbd.userUsage[db.DGUTAgeAll] = []*basedirs.Usage{
		{
			UID:         0,
			BaseDir:     "/a/",
			UsageSize:   1,
			UsageInodes: 1,
			Mtime:       latest,
			Age:         db.DGUTAgeAll,
		},
	}

	mbd.groupSubdirs[basedirs.SubDirKey{ID: 0, BaseDir: "/a/", Age: db.DGUTAgeAll}] = []*basedirs.SubDir{
		{SubDir: ".", NumFiles: 1, SizeFiles: 1, LastModified: latest, FileUsage: basedirs.UsageBreakdownByType{}},
	}
	mbd.userSubdirs[basedirs.SubDirKey{ID: 0, BaseDir: "/a/", Age: db.DGUTAgeAll}] = []*basedirs.SubDir{
		{SubDir: ".", NumFiles: 1, SizeFiles: 1, LastModified: latest, FileUsage: basedirs.UsageBreakdownByType{}},
		{SubDir: "x", NumFiles: 1, SizeFiles: 1, LastModified: latest, FileUsage: basedirs.UsageBreakdownByType{}},
	}
	mbd.userSubdirs[basedirs.SubDirKey{ID: 0, BaseDir: "/a/", Age: db.DGUTAgeA3Y}] = []*basedirs.SubDir{
		{SubDir: ".", NumFiles: 1, SizeFiles: 1, LastModified: latest, FileUsage: basedirs.UsageBreakdownByType{}},
		{SubDir: "x", NumFiles: 1, SizeFiles: 1, LastModified: latest, FileUsage: basedirs.UsageBreakdownByType{}},
	}
	mbd.groupHistory[basedirs.HistoryKey{GID: 0, MountPath: "/a/"}] = []basedirs.History{
		{Date: latest, UsageInodes: 1, UsageSize: 1, QuotaInodes: 1, QuotaSize: 1},
	}

	// Ensure Info() reflects non-zero counts.
	mbd.info.GroupDirCombos = len(mbd.groupUsage[db.DGUTAgeAll])
	mbd.info.UserDirCombos = len(mbd.userUsage[db.DGUTAgeAll])
	mbd.info.GroupSubDirCombos = 1
	mbd.info.GroupSubDirs = len(mbd.groupSubdirs[basedirs.SubDirKey{ID: 0, BaseDir: "/a/", Age: db.DGUTAgeAll}])
	mbd.info.UserSubDirCombos = 1
	mbd.info.UserSubDirs = len(mbd.userSubdirs[basedirs.SubDirKey{ID: 0, BaseDir: "/a/", Age: db.DGUTAgeAll}])
	mbd.info.GroupMountCombos = 1
	mbd.info.GroupHistories = len(mbd.groupHistory[basedirs.HistoryKey{GID: 0, MountPath: "/a/"}])

	return &testProvider{tree: tree, bd: mbd}, nil
}
