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
	"strings"

	"github.com/go-resty/resty/v2"
)

type Error string

func (e Error) Error() string { return string(e) }

const ErrNoAuth = Error("authentication failed")
const ErrBadQuery = Error("bad query; check dir, group, user and type")

const ClientProtocol = "https://"

// Login is a client call to a Server listening at the given domain:port url
// that checks the given password is valid for the given username, and returns a
// JWT if so.
//
// Provide a non-blank path to a certificate to force us to trust that
// certificate, eg. if the server was started with a self-signed certificate.
func Login(url, cert, username, password string) (string, error) {
	r := newClientRequest(url, cert)

	resp, err := r.SetFormData(map[string]string{
		"username": username,
		"password": password,
	}).
		Post(EndPointJWT)
	if err != nil {
		return "", err
	}

	if resp.StatusCode() != http.StatusOK {
		return "", ErrNoAuth
	}

	return jsonStringBodyToString(resp.Body()), nil
}

// LoginWithOKTA sends a request to the server containing the token as a cookie,
// so it will be able to return the JWT for the user. Provide an addr that is
// just the domain:port that was used to Start() the server.
func LoginWithOKTA(addr, cert, token string) (string, error) {
	r := newClientRequest(addr, cert)

	resp, err := r.SetCookie(&http.Cookie{
		Name:  oktaCookieName,
		Value: token,
	}).Post(EndPointJWT)

	if err != nil {
		return "", err
	}

	if resp.StatusCode() != http.StatusOK {
		return "", ErrNoAuth
	}

	return jsonStringBodyToString(resp.Body()), nil
}

// newClientRequest creates a resty Request that will trust the certificate at
// the given path. cert can be blank to only trust the normal installed cert
// chain.
func newClientRequest(url, cert string) *resty.Request {
	client := newRestyClient(url, cert)

	return client.R()
}

// newRestyClient creates a Resty client that will trust the certificate at the
// given path. cert can be blank to only trust the normal installed cert chain.
func newRestyClient(url, cert string) *resty.Client {
	client := resty.New()

	if cert != "" {
		client.SetRootCertificate(cert)
	}

	client.SetBaseURL(ClientProtocol + url)

	return client
}

// jsonStringBodyToString takes the response body of a JSON string, and returns
// it as a string.
func jsonStringBodyToString(body []byte) string {
	str := string(body)
	str = strings.TrimPrefix(str, `"`)
	str = strings.TrimSuffix(str, `"`)

	return str
}

// RefreshJWT is like Login(), but refreshes a JWT previously returned by
// Login() if it's still valid.
func RefreshJWT(url, cert, token string) (string, error) {
	r := newAuthenticatedClientRequest(url, cert, token)

	resp, err := r.Get(EndPointJWT)
	if err != nil {
		return "", err
	}

	if resp.StatusCode() != http.StatusOK {
		return "", ErrNoAuth
	}

	return jsonStringBodyToString(resp.Body()), nil
}

// newAuthenticatedClientRequest is like newClientRequest, but sets the given
// JWT in the authorization header.
func newAuthenticatedClientRequest(url, cert, jwt string) *resty.Request {
	client := newRestyClient(url, cert)

	client.SetAuthToken(jwt)

	return client.R()
}

// GetGroupAreas is a client call to a Server listening at the given
// domain:port url that queries its configured group area information. The
// returned map has area keys and group slices.
//
// Provide a non-blank path to a certificate to force us to trust that
// certificate, eg. if the server was started with a self-signed certificate.
//
// You must first Login() to get a JWT that you must supply here.
func GetGroupAreas(url, cert, jwt string) (map[string][]string, error) {
	r := newAuthenticatedClientRequest(url, cert, jwt)

	resp, err := r.SetResult(map[string][]string{}).
		ForceContentType("application/json").
		Get(EndPointAuthGroupAreas)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode() {
	case http.StatusUnauthorized, http.StatusNotFound:
		return nil, ErrNoAuth
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
func GetWhereDataIs(url, cert, jwt, dir, groups, users, types, splits string) ([]byte, []*DirSummary, error) {
	r := newAuthenticatedClientRequest(url, cert, jwt)

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
		return nil, nil, ErrNoAuth
	case http.StatusOK:
		return resp.Body(), *resp.Result().(*[]*DirSummary), nil //nolint:forcetypeassert
	}

	return nil, nil, ErrBadQuery
}
