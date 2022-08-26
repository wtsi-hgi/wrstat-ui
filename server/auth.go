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
	"errors"
	"net/http"
	"strings"
	"time"

	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"
)

type login struct {
	Username string `form:"username" json:"username" binding:"required"`
	Password string `form:"password" json:"password" binding:"required"`
}

const (
	tokenDuration      = time.Hour * 24 * 5
	userKey            = "user"
	claimKeyUsername   = "Username"
	claimKeyUID        = "UID"
	ErrBadJWTClaim     = Error("JWT had bad claims")
	ErrEmailNotPresent = Error("field `email` not present")
)

// EnableAuth adds the /rest/v1/jwt POST and GET endpoints to the REST API.
//
// The /rest/v1/jwt POST endpoint requires the username and password parameters
// in a form or as JSON. It passes these to the given auth callback, and if it
// returns true, a JWT is returned (as a JSON string) in the response that
// contains Username and UIDs (comma separated strings).
//
// Alternatively, you can POST with an oktaCookieName cookie with a value of the
// okta auth code from the auth-code endpoint. If the code is valid, likewise
// returns a JWT. You'll also need to call AddOIDCRoutes() for this scheme to
// work.
//
// Queries to endpoints that need authorisation should include the JWT in the
// authorization header as a bearer token. Those endpoints can be implemented by
// extracting the *User information out of the JWT using getUser().
//
// JWTs are signed and verified using the given cert and key files.
//
// GET on the endpoint will refresh the JWT. JWTs expire after 5 days, but can
// be refreshed up until day 10 from issue.
func (s *Server) EnableAuth(certFile, keyFile string, acb AuthCallback) error {
	s.authCB = acb

	authMiddleware, err := s.createAuthMiddleware(certFile, keyFile)
	if err != nil {
		return err
	}

	s.router.POST(EndPointJWT, authMiddleware.LoginHandler)
	s.router.GET(EndPointJWT, authMiddleware.RefreshHandler)

	auth := s.router.Group(EndPointAuth)
	auth.Use(authMiddleware.MiddlewareFunc())
	s.authGroup = auth

	return nil
}

// createAuthMiddleware creates jin-compatible middleware that enables logins
// and authorisation with JWTs.
func (s *Server) createAuthMiddleware(certFile, keyFile string) (*jwt.GinJWTMiddleware, error) {
	return jwt.New(&jwt.GinJWTMiddleware{
		Realm:            "wrstat",
		SigningAlgorithm: "RS512",
		PubKeyFile:       certFile,
		PrivKeyFile:      keyFile,
		Timeout:          tokenDuration,
		MaxRefresh:       tokenDuration,
		IdentityKey:      userKey,
		PayloadFunc:      authPayLoad,
		IdentityHandler:  authIdentityHandler,
		Authenticator:    s.authenticator,
		Authorizator: func(data interface{}, c *gin.Context) bool {
			return data != nil
		},
		LoginResponse:   tokenResponder,
		RefreshResponse: tokenResponder,
		TokenLookup:     "header: Authorization",
		TokenHeadName:   "Bearer",
		TimeFunc:        time.Now,
	})
}

// authPayLoad is a function property for jwt.GinJWTMiddleware. It adds extra
// claims to the JWT we send to the user.
func authPayLoad(data interface{}) jwt.MapClaims {
	if v, ok := data.(*User); ok {
		return jwt.MapClaims{
			claimKeyUsername: v.Username,
			claimKeyUID:      v.UID,
		}
	}

	return jwt.MapClaims{}
}

// authIdentityHandler is a function property for jwt.GinJWTMiddleware. It
// extracts their user-related claims we stored in the JWT and turns them into
// a *User.
func authIdentityHandler(c *gin.Context) interface{} {
	claims := jwt.ExtractClaims(c)

	username, err1 := retrieveClaimString(claims, claimKeyUsername)
	uid, err2 := retrieveClaimString(claims, claimKeyUID)

	if username == "" || hasError(err1, err2) {
		return nil
	}

	return &User{
		Username: username,
		UID:      uid,
	}
}

// retrieveClaimString finds and converts to a string the given claim in amongst
// the given claims. If it doesn't exist or convert to a string, returns an
// error.
func retrieveClaimString(claims jwt.MapClaims, claim string) (string, error) {
	value, existed := claims[claim]
	if !existed {
		return "", ErrBadJWTClaim
	}

	str, ok := value.(string)
	if !ok {
		return "", ErrBadJWTClaim
	}

	return str, nil
}

// hasError tells you if any of the given errors is not nil.
func hasError(errs ...error) bool {
	for _, err := range errs {
		if err != nil {
			return true
		}
	}

	return false
}

// authenticator is a function property for jwt.GinJWTMiddleware. It creates a
// *User based on the auth method used (oauth through cookie, or plain username
// and password). That in turn gets passed to authPayload().
func (s *Server) authenticator(c *gin.Context) (interface{}, error) {
	_, err := c.Request.Cookie(oktaCookieName)
	if errors.Is(err, http.ErrNoCookie) {
		return s.basicAuth(c)
	}

	return s.oidcAuth(c)
}

// basicAuth takes a web request and extracts the username and password from it
// and then passes it to the server's auth callback so it can validate the login
// and return a *User.
func (s *Server) basicAuth(c *gin.Context) (*User, error) {
	var loginVals login
	if err := c.ShouldBind(&loginVals); err != nil {
		return nil, jwt.ErrMissingLoginValues
	}

	username := loginVals.Username
	password := loginVals.Password

	ok, uid := s.authCB(username, password)

	if !ok {
		return nil, jwt.ErrFailedAuthentication
	}

	return &User{
		Username: username,
		UID:      uid,
	}, nil
}

// getUsernameFromEmail returns the part before '@' in an email address.
func getUsernameFromEmail(email string) string {
	return strings.Split(email, "@")[0]
}

// tokenResponder returns token as a simple JSON string.
func tokenResponder(c *gin.Context, code int, token string, t time.Time) {
	c.JSON(http.StatusOK, token)
}

// getUser retrieves the *User information extracted from the JWT in the auth
// header. This will only be present after calling EnableAuth(), on a route in
// the authGroup.
func (s *Server) getUser(c *gin.Context) *User {
	userI, ok := c.Get(userKey)
	if !ok {
		return nil
	}

	user, ok := userI.(*User)
	if !ok {
		return nil
	}

	return user
}

// userGIDs returns the unix group IDs for the given User's UIDs. This calls
// *User.GIDs(), but caches the result against username, and returns cached
// results if possible.
//
// As a special case, if one of the groups is white-listed per
// WhiteListGroups(), returns a nil slice.
func (s *Server) userGIDs(u *User) ([]string, error) {
	if gids, found := s.userToGIDs[u.Username]; found {
		return gids, nil
	}

	gids, err := u.GIDs()
	if err != nil {
		return nil, err
	}

	if s.whiteListed(gids) {
		gids = nil
	}

	s.userToGIDs[u.Username] = gids

	return gids, nil
}

// WhiteListCallback is passed to WhiteListGroups() and is used by the server
// to determine if a given unix group ID is special, indicating that users
// belonging to it have permission to view information about all other unix
// groups. If it's a special group, return true; otherwise false.
type WhiteListCallback func(gid string) bool

// WhiteListGroups sets the given callback on the server, which will now be used
// to check if any of the groups that a user belongs to have been whitelisted,
// giving that user unrestricted access to know about all groups.
//
// Do NOT call this more than once or after the server has started responding to
// client queries.
func (s *Server) WhiteListGroups(wcb WhiteListCallback) {
	s.whiteCB = wcb
}

// whiteListed returns true if one of the gids has been white-listed.
func (s *Server) whiteListed(gids []string) bool {
	if s.whiteCB == nil {
		return false
	}

	for _, gid := range gids {
		if s.whiteCB(gid) {
			return true
		}
	}

	return false
}
