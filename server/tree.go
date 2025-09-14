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
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/wrstat-ui/clickhouse"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// javascriptToJSONFormat is the date format emitted by javascript's Date's
// toJSON method. It conforms to ISO 8601 and is like RFC3339 and in UTC.
const javascriptToJSONFormat = "2006-01-02T15:04:05.999Z"

func (s *Server) addBaseDGUTARoutes() {
	authGroup := s.AuthRouter()

	if authGroup == nil {
		if useClickHouseFeatureFlag() {
			s.Router().GET(EndPointWhere, notImplementedHandler)
		} else {
			s.Router().GET(EndPointWhere, s.getWhere)
		}
	} else {
		if useClickHouseFeatureFlag() {
			authGroup.GET(wherePath, notImplementedHandler)
		} else {
			authGroup.GET(wherePath, s.getWhere)
		}
	}
}

// AddTreePage adds the /tree static web page to the server, along with the
// /rest/v1/auth/tree endpoint. It only works if EnableAuth() has been called
// first.
func (s *Server) AddTreePage() error {
	authGroup := s.AuthRouter()
	if authGroup == nil {
		return gas.ErrNeedsAuth
	}

	staticServer := http.FileServer(http.FS(getStaticFS()))

	s.Router().NoRoute(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/tree/") {
			c.Redirect(http.StatusMovedPermanently, "/")

			return
		}

		c.Writer.Header().Del("Content-Security-Policy")
		staticServer.ServeHTTP(c.Writer, c.Request)
	})

	if useClickHouseFeatureFlag() {
		authGroup.GET(TreePath, s.getTreeCH)
	} else {
		authGroup.GET(TreePath, s.getTree)
	}
	authGroup.GET(DBsUpdated, s.dbUpdateTimestamps)

	return nil
}

// getStaticFS returns an FS for the static files needed for the tree webpage.
// Returns embedded files by default, or a live view of the git repo files if
// env var WRSTAT_SERVER_DEV is set to 1.
func getStaticFS() fs.FS {
	var fsys fs.FS

	treeDir := "static/wrstat/build"

	if os.Getenv(gas.DevEnvKey) == gas.DevEnvVal {
		fsys = os.DirFS(treeDir)
	} else {
		fsys, _ = fs.Sub(staticFS, treeDir) //nolint:errcheck
	}

	return fsys
}

// AddGroupAreas takes a map of area keys and group slice values. Clients will
// then receive this map on TreeElements in the "areas" field.
//
// If EnableAuth() has been called, also creates the /auth/group-areas endpoint
// that returns the given value.
func (s *Server) AddGroupAreas(areas map[string][]string) {
	s.areas = areas

	authGroup := s.AuthRouter()
	if authGroup != nil {
		authGroup.GET(groupAreasPaths, s.getGroupAreas)
	}
}

// getGroupAreas serves up our areas hash as JSON.
func (s *Server) getGroupAreas(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, s.areas)
}

// TreeElement holds tree.DirInfo type information in a form suited to passing
// to the treemap web interface. It also includes the server's dataTimeStamp so
// interfaces can report on how long ago the data forming the tree was
// captured.
type TreeElement struct {
	Name        string              `json:"name"`
	Path        string              `json:"path"`
	Count       uint64              `json:"count"`
	Size        uint64              `json:"size"`
	Atime       string              `json:"atime"`
	Mtime       string              `json:"mtime"`
	Age         db.DirGUTAge        `json:"age"`
	Users       []string            `json:"users"`
	Groups      []string            `json:"groups"`
	FileTypes   []string            `json:"filetypes"`
	HasChildren bool                `json:"has_children"`
	Children    []*TreeElement      `json:"children,omitempty"`
	Areas       map[string][]string `json:"areas"`
	NoAuth      bool                `json:"noauth"`
}

// getTree responds with the data needed by the tree web interface.
// LoadDGUTADB() must already have been called. This is called when there is a
// GET on /rest/v1/auth/tree.
func (s *Server) getTree(c *gin.Context) {
	path := c.DefaultQuery("path", "/")

	filter, err := makeFilterFromContext(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	di, err := s.tree.DirInfo(path, filter)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	allowedGIDs, err := s.allowedGIDs(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.JSON(http.StatusOK, s.diToTreeElement(di, filter, allowedGIDs, path))
}

// getTreeCH is a ClickHouse-backed implementation of getTree.
// It produces the same TreeElement payload using ClickHouse summaries.
func (s *Server) getTreeCH(c *gin.Context) { //nolint:funlen
	path := c.DefaultQuery("path", "/")

	filter, err := makeFilterFromContext(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	ch, err := s.getClickHouse()
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	// Map db.Filter to clickhouse.Filters
	cf := filtersToCH(filter)

	current, err := s.chDirSummary(c, ch, path, cf)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	allowedGIDs, err := s.getAllowedGIDsSafe(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	childrenTEs, err := s.chChildrenTreeElements(c, ch, path, cf, allowedGIDs)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	te := s.ddsToTreeElement(current, allowedGIDs)
	te.Areas = s.areas
	te.HasChildren = len(childrenTEs) > 0
	te.Children = childrenTEs

	c.JSON(http.StatusOK, te)
}

// getAllowedGIDsSafe wraps allowedGIDs with the server read lock.
func (s *Server) getAllowedGIDsSafe(c *gin.Context) (map[uint32]bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.allowedGIDs(c)
}

// chDirSummary returns a DirSummary for the given path using ClickHouse.
func (s *Server) chDirSummary(c *gin.Context, ch *clickhouse.Clickhouse, path string, cf clickhouse.Filters) (*db.DirSummary, error) { //nolint:lll
	sum, err := ch.SubtreeSummary(c, path, cf)
	if err != nil {
		return nil, err
	}

	// Build current TreeElement-compatible summary
	current := &db.DirSummary{
		Dir:   path,
		Count: sum.FileCount,
		Size:  sum.TotalSize,
		Atime: sum.OldestATime, // matches existing semantics (oldest atime)
		Mtime: sum.MostRecentMTime,
		UIDs:  sum.UIDs,
		GIDs:  sum.GIDs,
		FTs:   chExtsToDGUTA(sum.Exts),
	}

	return current, nil
}

// chChildrenTreeElements builds TreeElements for immediate child directories.
func (s *Server) chChildrenTreeElements(c *gin.Context, ch *clickhouse.Clickhouse, path string, cf clickhouse.Filters, allowedGIDs map[uint32]bool) ([]*TreeElement, error) { //nolint:lll
	entries, err := ch.ListImmediateChildren(c, path)
	if err != nil {
		return nil, err
	}

	// Use a set to collect unique child dirs
	seen := make(map[string]struct{})

	childrenTEs := make([]*TreeElement, 0)

	for _, e := range entries {
		if e.FType != uint8(clickhouse.FileTypeDir) {
			continue
		}

		child := e.Path
		if _, ok := seen[child]; ok {
			continue
		}

		seen[child] = struct{}{}

		childTE, err := s.chChildTreeElement(c, ch, child, cf, allowedGIDs)
		if err != nil {
			return nil, err
		}

		childrenTEs = append(childrenTEs, childTE)
	}

	return childrenTEs, nil
}

// chChildTreeElement summarises a child path and returns its TreeElement.
func (s *Server) chChildTreeElement(c *gin.Context, ch *clickhouse.Clickhouse, child string, cf clickhouse.Filters, allowedGIDs map[uint32]bool) (*TreeElement, error) { //nolint:lll
	csum, err := ch.SubtreeSummary(c, child, cf)
	if err != nil {
		return nil, err
	}

	cds := &db.DirSummary{
		Dir:   child,
		Count: csum.FileCount,
		Size:  csum.TotalSize,
		Atime: csum.OldestATime,
		Mtime: csum.MostRecentMTime,
		UIDs:  csum.UIDs,
		GIDs:  csum.GIDs,
		FTs:   chExtsToDGUTA(csum.Exts),
	}

	childTE := s.ddsToTreeElement(cds, allowedGIDs)

	// Determine if child has children with files (heuristic: list its children)
	gkids, err := ch.ListImmediateChildren(c, child)
	if err == nil {
		childTE.HasChildren = childrenContainDirs(gkids)
	}

	return childTE, nil
}

// childrenContainDirs returns true if any entry is a directory.
func childrenContainDirs(ents []clickhouse.FileEntry) bool {
	for _, g := range ents {
		if g.FType == uint8(clickhouse.FileTypeDir) {
			return true
		}
	}

	return false
}

// chExtsToDGUTA maps ClickHouse ext_low values to our DGUTA file type categories.
// The mapping is heuristic and mirrors the extsFromFileTypes reverse mapping.
var extToDGUTA = map[string]db.DirGUTAFileType{ //nolint:gochecknoglobals
	"vcf":      db.DGUTAFileTypeVCF,
	"vcf.gz":   db.DGUTAFileTypeVCFGz,
	"bcf":      db.DGUTAFileTypeBCF,
	"sam":      db.DGUTAFileTypeSam,
	"bam":      db.DGUTAFileTypeBam,
	"cram":     db.DGUTAFileTypeCram,
	"fa":       db.DGUTAFileTypeFasta,
	"fasta":    db.DGUTAFileTypeFasta,
	"fastq":    db.DGUTAFileTypeFastq,
	"fq":       db.DGUTAFileTypeFastq,
	"fastq.gz": db.DGUTAFileTypeFastqGz,
	"fq.gz":    db.DGUTAFileTypeFastqGz,
	"ped":      db.DGUTAFileTypePedBed,
	"bed":      db.DGUTAFileTypePedBed,
	"bim":      db.DGUTAFileTypePedBed,
	"fam":      db.DGUTAFileTypePedBed,
	"map":      db.DGUTAFileTypePedBed,
	"csv":      db.DGUTAFileTypeText,
	"dat":      db.DGUTAFileTypeText,
	"md":       db.DGUTAFileTypeText,
	"readme":   db.DGUTAFileTypeText,
	"text":     db.DGUTAFileTypeText,
	"txt":      db.DGUTAFileTypeText,
	"tsv":      db.DGUTAFileTypeText,
	"log":      db.DGUTAFileTypeLog,
	"err":      db.DGUTAFileTypeLog,
	"e":        db.DGUTAFileTypeLog,
	"oe":       db.DGUTAFileTypeLog,
	"gz":       db.DGUTAFileTypeCompressed,
	"bz2":      db.DGUTAFileTypeCompressed,
	"xz":       db.DGUTAFileTypeCompressed,
	"zip":      db.DGUTAFileTypeCompressed,
	"tgz":      db.DGUTAFileTypeCompressed,
	"bzip2":    db.DGUTAFileTypeCompressed,
	"bgz":      db.DGUTAFileTypeCompressed,
	"zst":      db.DGUTAFileTypeCompressed,
	"lz4":      db.DGUTAFileTypeCompressed,
	"lz":       db.DGUTAFileTypeCompressed,
	"br":       db.DGUTAFileTypeCompressed,
}

func chExtsToDGUTA(exts []string) []db.DirGUTAFileType {
	if len(exts) == 0 {
		return nil
	}

	set := make(map[db.DirGUTAFileType]struct{})

	for _, e := range exts {
		if t, ok := extToDGUTA[e]; ok {
			set[t] = struct{}{}
		}
	}

	if len(set) == 0 {
		return nil
	}

	out := make([]db.DirGUTAFileType, 0, len(set))
	for k := range set {
		out = append(out, k)
	}

	return out
}

// diToTreeElement converts the given dguta.DirInfo to our own TreeElement. It
// has to do additional database queries to find out if di's children have
// children. If results don't belong to at least one of the allowedGIDs, they
// will be marked as NoAuth and won't include child info.
func (s *Server) diToTreeElement(di *db.DirInfo, filter *db.Filter,
	allowedGIDs map[uint32]bool, path string) *TreeElement {
	if di == nil {
		return &TreeElement{Path: path}
	}
	te := s.ddsToTreeElement(di.Current, allowedGIDs)
	te.Areas = s.areas
	te.HasChildren = len(di.Children) > 0

	if te.NoAuth {
		return te
	}

	childElements := make([]*TreeElement, len(di.Children))

	for i, dds := range di.Children {
		childTE := s.ddsToTreeElement(dds, allowedGIDs)
		childTE.HasChildren = s.tree.DirHasChildren(dds.Dir, filter)
		childElements[i] = childTE
	}

	te.Children = childElements

	return te
}

// ddsToTreeElement converts a dguta.DirSummary to a TreeElement, but with no
// child info. It uses the allowedGIDs to mark the returned element NoAuth if
// none of the GIDs for the dds are in the allowedGIDs. If allowedGIDs is nil,
// NoAuth will always be false.
func (s *Server) ddsToTreeElement(dds *db.DirSummary, allowedGIDs map[uint32]bool) *TreeElement {
	return &TreeElement{
		Name:      filepath.Base(dds.Dir),
		Path:      dds.Dir,
		Count:     dds.Count,
		Size:      dds.Size,
		Atime:     timeToJavascriptDate(dds.Atime),
		Mtime:     timeToJavascriptDate(dds.Mtime),
		Age:       dds.Age,
		Users:     s.uidsToUsernames(dds.UIDs),
		Groups:    s.gidsToNames(dds.GIDs),
		FileTypes: s.ftsToNames(dds.FTs),
		NoAuth:    areDisjoint(allowedGIDs, dds.GIDs),
	}
}

// timeToJavascriptDate returns the given time in javascript Date's toJSON
// format.
func timeToJavascriptDate(t time.Time) string {
	return t.UTC().Format(javascriptToJSONFormat)
}

// areDisjoint returns true if none of the keys of `a` are the same as any
// element of `b`. As a special case, returns false if `a` is nil.
func areDisjoint(a map[uint32]bool, b []uint32) bool {
	if a == nil {
		return false
	}

	for _, id := range b {
		if a[id] {
			return false
		}
	}

	return true
}
