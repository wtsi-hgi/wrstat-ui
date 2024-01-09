/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
 *	- Michael Grace <mg38@sanger.ac.uk>
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

	gas "github.com/wtsi-hgi/go-authserver"
)

const ErrBadQuery = gas.Error("bad query; check dir, group, user and type")

// GetGroupAreas is a client call to a Server that queries its configured group
// area information. The returned map has area keys and group slices.
func GetGroupAreas(c *gas.ClientCLI) (map[string][]string, error) {
	r, err := c.AuthenticatedRequest()
	if err != nil {
		return nil, err
	}

	resp, err := r.SetResult(map[string][]string{}).
		ForceContentType("application/json").
		Get(EndPointAuthGroupAreas)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode() {
	case http.StatusUnauthorized, http.StatusNotFound:
		return nil, gas.ErrNoAuth
	}

	return *resp.Result().(*map[string][]string), nil //nolint:forcetypeassert
}

// GetWhereDataIs is a client call to a Server listening at the given
// domain:port url that queries where data is and returns the raw response body
// (JSON string), and that body converted in to a slice of *DirSummary.
//
// Provide a non-blank path to a certificate to force us to trust that
// certificate, eg. if the server was started with a self-signed certificate.
//
// You must first Login() to get a JWT that you must supply here.
//
// The other parameters correspond to arguments that dgut.Tree.Where() takes.
func GetWhereDataIs(c *gas.ClientCLI, dir, groups, users, types, splits string) ([]byte, []*DirSummary, error) {
	r, err := c.AuthenticatedRequest()
	if err != nil {
		return nil, nil, err
	}

	resp, err := r.SetResult([]*DirSummary{}).
		ForceContentType("application/json").
		SetQueryParams(map[string]string{
			"dir":    dir,
			"groups": groups,
			"users":  users,
			"types":  types,
			"splits": splits,
		}).
		Get(EndPointAuthWhere)
	if err != nil {
		return nil, nil, err
	}

	switch resp.StatusCode() {
	case http.StatusUnauthorized, http.StatusNotFound:
		return nil, nil, gas.ErrNoAuth
	case http.StatusOK:
		return resp.Body(), *resp.Result().(*[]*DirSummary), nil //nolint:forcetypeassert
	}

	return nil, nil, ErrBadQuery
}
