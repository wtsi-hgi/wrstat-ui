/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// Note: Any filesystem path scanning/loading is backend-specific and must live
// outside the server package. The server only accepts pre-constructed assets.

// LoadDBs loads dirguta and basedir databases from the given
// directory/directories, and adds the relevant endpoints to the REST API.
//
// For the dirguta databases (as produced by one or more invocations of
// dguta.DB.Store()) it adds the /rest/v1/where GET endpoint to the REST API. If
// you call EnableAuth() first, then this endpoint will be secured and be
// available at /rest/v1/auth/where.
//
// The where endpoint can take the dir, splits, groups, users and types
// parameters, which correspond to arguments that dguta.Tree.Where() takes.
//
// For the basedirs database (as produced by basedirs.CreateDatabase()), in
// combination with an owners file (a gid,owner csv), it adds the following GET
// endpoints to the REST API:
//
// /rest/v1/basedirs/usage/groups /rest/v1/basedirs/usage/users
// /rest/v1/basedirs/subdirs/group /rest/v1/basedirs/subdirs/user
// /rest/v1/basedirs/history
//
// If you call EnableAuth() first, then these endpoints will be secured and be
// available at /rest/v1/auth/basedirs/*.
//
// The subdir endpoints require id (gid or uid) and basedir parameters. The
// history endpoint requires a gid and basedir (can be basedir, actually a
// mountpoint) parameter.
// LoadDBs loads pre-constructed assets into the server without using paths.
// Callers provide dguta sources, a basedirs.MultiReader, and timestamps map.
// If mounts are provided, they will be set on the basedirs reader.
func (s *Server) LoadDBs(srcs []db.Source, bd basedirs.MultiReader, timestamps map[string]int64, mounts ...string) error {
	// Basic validation: require non-empty sources, a basedirs reader, and timestamps
	if len(srcs) == 0 || bd == nil || timestamps == nil {
		return basedirs.Error("invalid assets to load")
	}
	tree, err := db.NewTree(srcs...)
	if err != nil {
		return err
	}

	if len(mounts) > 0 {
		bd.SetMountPoints(mounts)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.prewarmCaches(bd); err != nil {
		return err
	}

	loaded := s.basedirs != nil
	s.basedirs = bd
	s.tree = tree
	s.dataTimeStamp = timestamps

	if !loaded {
		s.addBaseDGUTARoutes()
		s.addBaseDirRoutes()
	}

	return nil
}

// LoadDBAssets loads pre-constructed assets into the server without using paths.
// Callers provide dguta sources, a basedirs.MultiReader, and timestamps map.
// If mounts are provided, they will be set on the basedirs reader.
// LoadDBAssets removed in favor of LoadDBs.

// Removed: path-based timestamp helper; handled by backend-specific reloader.

func (s *Server) dbUpdateTimestamps(c *gin.Context) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c.JSON(http.StatusOK, s.dataTimeStamp)
}
