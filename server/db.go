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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/provider"
)

func validateProvider(p provider.Provider) (basedirs.Reader, error) {
	if p == nil {
		return nil, basedirs.Error("provider is nil")
	}

	bd := p.BaseDirs()
	if bd == nil {
		return nil, basedirs.Error("provider returned nil basedirs")
	}

	return bd, nil
}

func mountTimestampsToUnixSeconds(mt map[string]time.Time) map[string]int64 {
	out := make(map[string]int64, len(mt))

	for mountKey, t := range mt {
		out[mountKey] = t.Unix()
	}

	return out
}

func (s *Server) prepareProvider(p provider.Provider) (basedirs.Reader, map[string]int64, error) {
	bd, err := validateProvider(p)
	if err != nil {
		return nil, nil, err
	}

	mt, err := bd.MountTimestamps()
	if err != nil {
		return nil, nil, err
	}

	dataTimeStamp := mountTimestampsToUnixSeconds(mt)

	if err := s.prewarmCaches(bd); err != nil {
		return nil, nil, err
	}

	return bd, dataTimeStamp, nil
}

// SetProvider wires a backend bundle into the server.
//
// This replaces the legacy LoadDBs/EnableDBReloading flow; reloading is an
// implementation detail of the provider.
func (s *Server) SetProvider(p provider.Provider) error {
	bd, dataTimeStamp, err := s.prepareProvider(p)
	if err != nil {
		return err
	}

	old := func() provider.Provider {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return s.provider
	}()

	loaded := old != nil

	s.assignProviderFields(p, bd, dataTimeStamp, loaded)

	if old != nil {
		_ = old.Close()
	}

	p.OnUpdate(s.handleProviderUpdate)

	return nil
}

func (s *Server) assignProviderFields(p provider.Provider, bd basedirs.Reader,
	dataTimeStamp map[string]int64, loaded bool) {
	s.mu.Lock()
	s.provider = p
	s.tree = p.Tree()
	s.basedirs = bd
	s.dataTimeStamp = dataTimeStamp

	if !loaded {
		s.addBaseDGUTARoutes()
		s.addBaseDirRoutes()
	}

	s.mu.Unlock()
}

func (s *Server) handleProviderUpdate() {
	s.mu.RLock()
	p := s.provider
	s.mu.RUnlock()

	if p == nil {
		return
	}

	if err := s.refreshProviderFrom(p); err != nil {
		s.Logger.Printf("provider update failed: %s", err)
	} else {
		s.Logger.Printf("server ready again after provider update")
	}
}

func (s *Server) refreshProviderFrom(p provider.Provider) error {
	bd := p.BaseDirs()
	if bd == nil {
		return basedirs.Error("provider returned nil basedirs")
	}

	mt, err := bd.MountTimestamps()
	if err != nil {
		return err
	}

	dataTimeStamp := mountTimestampsToUnixSeconds(mt)

	if err := s.prewarmCaches(bd); err != nil {
		return err
	}

	s.mu.Lock()
	s.tree = p.Tree()
	s.basedirs = bd
	s.dataTimeStamp = dataTimeStamp
	s.mu.Unlock()

	return nil
}

func (s *Server) dbUpdateTimestamps(c *gin.Context) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c.JSON(http.StatusOK, s.dataTimeStamp)
}
