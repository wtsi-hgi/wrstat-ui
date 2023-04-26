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
	"time"

	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
)

// javascriptToJSONFormat is the date format emitted by javascript's Date's
// toJSON method. It conforms to ISO 8601 and is like RFC3339 and in UTC.
const javascriptToJSONFormat = "2006-01-02T15:04:05.999Z"

// AddTreePage adds the /tree static web page to the server, along with the
// /rest/v1/auth/tree endpoint. It only works if EnableAuth() has been called
// first.
func (s *Server) AddTreePage() error {
	authGroup := s.AuthRouter()
	if authGroup == nil {
		return gas.ErrNeedsAuth
	}

	fsys := getStaticFS()

	s.Router().StaticFS(TreePath, http.FS(fsys))

	s.Router().NoRoute(func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/tree/tree.html")
	})

	authGroup.GET(TreePath, s.getTree)

	return nil
}

// getStaticFS returns an FS for the static files needed for the tree webpage.
// Returns embedded files by default, or a live view of the git repo files if
// env var WRSTAT_SERVER_DEV is set to 1.
func getStaticFS() fs.FS {
	var fsys fs.FS

	treeDir := "static/tree"

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
	Users       []string            `json:"users"`
	Groups      []string            `json:"groups"`
	FileTypes   []string            `json:"filetypes"`
	HasChildren bool                `json:"has_children"`
	Children    []*TreeElement      `json:"children,omitempty"`
	TimeStamp   string              `json:"timestamp"`
	Areas       map[string][]string `json:"areas"`
}

// getTree responds with the data needed by the tree web interface. LoadDGUTDB()
// must already have been called. This is called when there is a GET on
// /rest/v1/auth/tree.
func (s *Server) getTree(c *gin.Context) {
	path := c.DefaultQuery("path", "/")

	filter, err := s.getFilter(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	s.treeMutex.RLock()
	defer s.treeMutex.RUnlock()

	di, err := s.tree.DirInfo(path, filter)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.JSON(http.StatusOK, s.diToTreeElement(di, filter))
}

// diToTreeElement converts the given dgut.DirInfo to our own TreeElement. It
// has to do additional database queries to find out if di's children have
// children.
func (s *Server) diToTreeElement(di *dgut.DirInfo, filter *dgut.Filter) *TreeElement {
	te := s.ddsToTreeElement(di.Current)
	te.HasChildren = len(di.Children) > 0
	childElements := make([]*TreeElement, len(di.Children))

	for i, dds := range di.Children {
		childTE := s.ddsToTreeElement(dds)
		childTE.HasChildren = s.tree.DirHasChildren(dds.Dir, filter)
		childElements[i] = childTE
	}

	te.Children = childElements
	te.Areas = s.areas

	return te
}

// ddsToTreeElement converts a dgut.DirSummary to a TreeElement, but with no
// child info.
func (s *Server) ddsToTreeElement(dds *dgut.DirSummary) *TreeElement {
	return &TreeElement{
		Name:      filepath.Base(dds.Dir),
		Path:      dds.Dir,
		Count:     dds.Count,
		Size:      dds.Size,
		Atime:     timeToJavascriptDate(dds.Atime),
		Users:     s.uidsToUsernames(dds.UIDs),
		Groups:    s.gidsToNames(dds.GIDs),
		FileTypes: s.ftsToNames(dds.FTs),
		TimeStamp: timeToJavascriptDate(s.dataTimeStamp),
	}
}

// timeToJavascriptDate returns the given time in javascript Date's toJSON
// format.
func timeToJavascriptDate(t time.Time) string {
	return t.UTC().Format(javascriptToJSONFormat)
}
