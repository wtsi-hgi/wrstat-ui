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

// getMountTimestamps extracts mount points and timestamps from a slice of sources
// by merging the results of GetMountTimestamps() calls.
func getMountTimestamps(sources []db.Source) (map[string]int64, []string) {
	timestamps := make(map[string]int64)
	var mountPoints []string

	// Collect mount points and timestamps from all sources
	for _, src := range sources {
		// Get mount timestamps from the source
		mountTimesMap := src.GetMountTimestamps()

		// Add each mount point and its timestamp to our collections
		for mount, modTime := range mountTimesMap {
			if mount != "" {
				timestamps[mount] = modTime.Unix()
				mountPoints = append(mountPoints, mount)
			}
		}
	}

	return timestamps, mountPoints
}

// LoadDBs loads pre-constructed assets into the server without using paths.
// Callers provide dguta sources and a basedirs.MultiReader.
// This method extracts mount points and timestamps directly from the sources,
// eliminating the need for external timestamp tracking.
func (s *Server) LoadDBs(srcs []db.Source, bd basedirs.MultiReader) error {
	// Basic validation: require non-empty sources and a basedirs reader
	if len(srcs) == 0 || bd == nil {
		return basedirs.Error("invalid assets to load")
	}
	tree, err := db.NewTree(srcs...)
	if err != nil {
		return err
	}

	// Get mount points and timestamps using our helper function
	timestamps, mountPoints := getMountTimestamps(srcs)

	// Set mount points on the basedirs reader if any were found
	if len(mountPoints) > 0 {
		bd.SetMountPoints(mountPoints)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.prewarmCaches(bd); err != nil {
		return err
	}

	loaded := s.basedirs != nil
	s.basedirs = bd
	s.tree = tree
	s.sources = srcs // Store sources for later use in dbUpdateTimestamps
	s.dataTimeStamp = timestamps

	if !loaded {
		s.addBaseDGUTARoutes()
		s.addBaseDirRoutes()
	}

	return nil
}

func (s *Server) dbUpdateTimestamps(c *gin.Context) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Update timestamps using the sources stored on the server
	if s.tree != nil && len(s.sources) > 0 {
		// Use the same helper function we use in LoadDBs
		timestamps, _ := getMountTimestamps(s.sources)
		// Update the server's cached timestamps
		s.dataTimeStamp = timestamps
	}

	c.JSON(http.StatusOK, s.dataTimeStamp)
}
