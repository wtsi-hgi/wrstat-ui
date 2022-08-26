/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Michael Grace <mg38@sanger.ac.uk>
 *	- Sendu Bala <sb10@sanger.ac.uk>
 *
 * Based on code from:
 * https://developer.okta.com/docs/guides/sign-into-web-app-redirect/go/main/
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

// oidc implements the authorization-code-flow-with-pkce described at
// https://developer.okta.com/docs/concepts/oauth-openid/ , using the redirect
// model.

package server

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/sessions"
	verifier "github.com/okta/okta-jwt-verifier-golang"
	oauthUtils "github.com/okta/okta-jwt-verifier-golang/utils"
	"github.com/thanhpk/randstr"
	"golang.org/x/oauth2"
)

const (
	oktaCookieName               = "okta-hosted-login-session-store"
	oauth2IDTokenKey             = "id_token"
	oauth2AccessTokenKey         = "access_token"
	oauth2AuthCodeKey            = "code"
	oauth2AuthURLKey             = "code_verifier"
	oauth2AuthVerifierKey        = "oauth_code_verifier"
	oauth2StateKeyQuery          = "state"
	oauth2StateKeyCookie         = "oauth_state"
	oauth2ErrorKey               = "error"
	oauth2AuthChallengeKey       = "code_challenge"
	oauth2AuthChallengeMethodKey = "code_challenge_method"
	oauth2AuthChallengeMethod    = "S256"
	oauth2JWTClaimKey            = "aud"
	oidcWellKnownURL             = "/.well-known/openid-configuration"
	oidcAuthKey                  = "authorization_endpoint"
	oidcTokenKey                 = "token_endpoint"
	oidcUserKey                  = "userinfo_endpoint"
	csrfStateLength              = 16
	pkceCodeVerifierLength       = 50
	metadataFetchTimeout         = 20 * time.Second

	ErrCouldNotVerifyToken = Error("token could not be verified")
	ErrOIDCUnexpectedState = Error("the state was not as expected")
	ErrOIDCUnavailableCode = Error("the code was not returned or is not accessible")
	ErrOIDCMissingToken    = Error("id token missing from OAuth2 token")
	ErrJSONValueNotString  = Error("non-string value in JSON field")
	ErrOIDCBadMeta         = Error("issuer meta information not found")
)

// oidcAuth takes the HTTP request, gets the user from it and returns a `*User`
// object.
func (s *Server) oidcAuth(c *gin.Context) (*User, error) {
	email, err := s.extractEmailFromOktaSession(c.Request)
	if err != nil {
		return nil, err
	}

	username := getUsernameFromEmail(email)

	uid, err := userNameToUID(username)
	if err != nil {
		return nil, err
	}

	return &User{
		Username: username,
		UID:      uid,
	}, nil
}

// oAuthParameters are used during AddOIDCRoutes() to create oauthEnv.
type oAuthParameters struct {
	issuer           string
	userinfoEndpoint string
	clientID         string
	clientSecret     string
	logger           *log.Logger
}

// toOauthEnv returns an oauthEnv with the appropriate values based on defaults,
// the paramenters in ourselves and the given callack and redirect URLs.
func (p oAuthParameters) toOauthEnv(callbackURL, clientRedirect string) *oauthEnv {
	authURL, tokenURL, userURL := p.determineURLs()

	p.userinfoEndpoint = userURL

	return &oauthEnv{
		Config: oauth2.Config{
			RedirectURL:  callbackURL,
			ClientID:     p.clientID,
			ClientSecret: p.clientSecret,
			Scopes:       []string{"openid", "email"},
			Endpoint: oauth2.Endpoint{
				AuthURL:   authURL,
				TokenURL:  tokenURL,
				AuthStyle: oauth2.AuthStyleInParams,
			},
		},
		params:         p,
		sessionStore:   sessions.NewCookieStore([]byte(oktaCookieName)),
		clientRedirect: clientRedirect,
		logger:         p.logger,
	}
}

// determineURLs returns the authorize, token and userinfo urls based on our
// issuer. If issuer/.well-known/openid-configuration doesn't exist or can't be
// parsed, assumes some standard urls.
func (p oAuthParameters) determineURLs() (string, string, string) {
	authURL, tokenURL, userURL, err := getURLsFromWellKnown(p.issuer)
	if err != nil {
		return p.issuer + "/v1/authorize", p.issuer + "/v1/token", p.issuer + "/v1/userinfo"
	}

	return authURL, tokenURL, userURL
}

// getURLsFromWellKnown gets the auth, token and userinfo URLs from the well
// known URL at the given issuer url.
func getURLsFromWellKnown(issuer string) (string, string, string, error) {
	meta, err := fetchOIDCMetaData(issuer + oidcWellKnownURL)
	if err != nil {
		return "", "", "", err
	}

	authURL, err := extractStringFromOIDCMeta(meta, oidcAuthKey)
	if err != nil {
		return "", "", "", err
	}

	tokenURL, err := extractStringFromOIDCMeta(meta, oidcTokenKey)
	if err != nil {
		return "", "", "", err
	}

	userURL, err := extractStringFromOIDCMeta(meta, oidcUserKey)
	if err != nil {
		return "", "", "", err
	}

	return authURL, tokenURL, userURL, nil
}

// fetchOIDCMetaData tries to fetch the given url and decode it as a json map.
func fetchOIDCMetaData(url string) (map[string]interface{}, error) {
	ctx, ctxCancel := context.WithTimeout(context.Background(), metadataFetchTimeout)
	defer ctxCancel()

	r, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	metadata := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&metadata)

	return metadata, err
}

// extractStringFromOIDCMeta extracts the given key from the given map.
func extractStringFromOIDCMeta(meta map[string]interface{}, key string) (string, error) {
	val, found := meta[key]
	if !found {
		return "", ErrOIDCBadMeta
	}

	str, ok := val.(string)
	if !ok {
		return "", ErrOIDCBadMeta
	}

	return str, nil
}

// AddOIDCRoutes creates the OAuth environments for both the web app and the CLI
// and adds the login and callback endpoints, along with an endpoint to get an
// auth code for the CLI. Addr should be the same domain:port as later supplied
// to Start().
func (s *Server) AddOIDCRoutes(addr, issuer, clientID, clientSecret string) {
	params := oAuthParameters{
		issuer:       issuer,
		clientID:     clientID,
		clientSecret: clientSecret,
		logger:       s.logger,
	}

	s.webOAuth = params.toOauthEnv(ClientProtocol+addr+EndpointAuthCallback, "/")
	s.cliOAuth = params.toOauthEnv(ClientProtocol+addr+EndpointAuthCLICallback, EndpointCLIAuthCode)

	s.router.GET(EndpointAuthCallback, s.webOAuth.HandleOIDCCallback)
	s.router.GET(EndpointOIDCLogin, s.webOAuth.HandleOIDCLogin)

	s.router.GET(EndpointAuthCLICallback, s.cliOAuth.HandleOIDCCallback)
	s.router.GET(EndpointOIDCCLILogin, s.cliOAuth.HandleOIDCLogin)

	s.router.GET(EndpointCLIAuthCode, oktaCLIAuthCode)
}

// oktaCLIAuthCode extracts the auth code for the CLI from the okta cookie.
func oktaCLIAuthCode(ctx *gin.Context) {
	cookie, err := ctx.Request.Cookie(oktaCookieName)
	if err != nil {
		ctx.Writer.Write([]byte(err.Error())) //nolint:errcheck

		return
	}

	ctx.Writer.Write([]byte(cookie.Value)) //nolint:errcheck
}

// oauthEnv contains all the information needed to do oauth2.
type oauthEnv struct {
	oauth2.Config
	params         oAuthParameters
	sessionStore   *sessions.CookieStore
	clientRedirect string
	ccs256         string // for testing purposes
	logger         *log.Logger
}

// HandleOIDCCallback is the handler function for any callback in OAuth. It will
// eventually redirect the user to the clientRedirect in the oauthEnv.
func (o *oauthEnv) HandleOIDCCallback(c *gin.Context) {
	session := o.getSession(c)
	if session == nil {
		return
	}

	if !o.queryIsValid(c, session) {
		return
	}

	accessToken := o.getAccessToken(c, session)
	if accessToken == "" {
		return
	}

	session.Values[oauth2AccessTokenKey] = accessToken

	if !o.saveSession(c, session) {
		return
	}

	c.Redirect(http.StatusFound, o.clientRedirect)
}

// getSession returns a session from our cookie store. Aborts and returns nil on
// error.
func (o *oauthEnv) getSession(c *gin.Context) *sessions.Session {
	session, err := o.sessionStore.Get(c.Request, oktaCookieName)
	if err != nil {
		c.AbortWithError(http.StatusForbidden, err) //nolint:errcheck

		return nil
	}

	return session
}

// queryIsValid checks that the query and session cookie state are valid. Aborts
// and returns false if not.
func (o *oauthEnv) queryIsValid(c *gin.Context, session *sessions.Session) bool {
	if c.Query(oauth2StateKeyQuery) == "" || c.Query(oauth2StateKeyQuery) != session.Values[oauth2StateKeyCookie] {
		c.AbortWithError(http.StatusForbidden, ErrOIDCUnexpectedState) //nolint:errcheck

		return false
	}

	if errStr := c.Query(oauth2ErrorKey); errStr != "" {
		c.AbortWithError(http.StatusForbidden, Error(errStr)) //nolint:errcheck

		return false
	}

	if c.Query(oauth2AuthCodeKey) == "" {
		c.AbortWithError(http.StatusForbidden, ErrOIDCUnavailableCode) //nolint:errcheck

		return false
	}

	return true
}

// getAccessToken extracts the token from the query, verifies it and returns
// it. Aborts and returns blank on error.
func (o *oauthEnv) getAccessToken(c *gin.Context, session *sessions.Session) string {
	token, err := o.Exchange(
		context.Background(),
		c.Query(oauth2AuthCodeKey),
		oauth2.SetAuthURLParam(oauth2AuthURLKey, session.Values[oauth2AuthVerifierKey].(string)),
	)
	if err != nil {
		c.AbortWithError(http.StatusUnauthorized, err) //nolint:errcheck

		return ""
	}

	rawIDToken, ok := token.Extra(oauth2IDTokenKey).(string)
	if !ok {
		c.AbortWithError(http.StatusUnauthorized, ErrOIDCMissingToken) //nolint:errcheck

		return ""
	}

	_, err = o.params.verifyToken(rawIDToken)
	if err != nil {
		o.logger.Printf("raw id token: %s", rawIDToken)
		c.AbortWithError(http.StatusForbidden, err) //nolint:errcheck

		return ""
	}

	return token.AccessToken
}

// saveSession saves the session to our cookie store. Aborts and returns false
// on error.
func (o *oauthEnv) saveSession(c *gin.Context, session *sessions.Session) bool {
	if err := session.Save(c.Request, c.Writer); err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return false
	}

	return true
}

// HandleOIDCLogin handles redirecting the user to the Okta login, as well as
// providing it challenge codes.
func (o *oauthEnv) HandleOIDCLogin(c *gin.Context) {
	c.Header("Cache-Control", "no-cache")

	session := o.getSession(c)
	if session == nil {
		return
	}

	oauthState := randomOuathState(session)
	codeChallenge, worked := createOauthCodeChallenge(c, session)
	o.ccs256 = codeChallenge

	if !worked || !o.saveSession(c, session) {
		return
	}

	redirectURI := o.AuthCodeURL(
		oauthState,
		oauth2.SetAuthURLParam(oauth2AuthChallengeKey, codeChallenge),
		oauth2.SetAuthURLParam(oauth2AuthChallengeMethodKey, oauth2AuthChallengeMethod),
	)

	c.Redirect(http.StatusFound, redirectURI)
}

// randomOuathState returns a random string for CSRF security. It also stores it
// in the given session as a cookie.
func randomOuathState(session *sessions.Session) string {
	state := randstr.Hex(csrfStateLength)
	session.Values[oauth2StateKeyCookie] = state

	return state
}

// createOauthCodeChallenge creates a PKCE code verifer and stores stores it in
// the given session as a cookie. Returns a sha256 code challenge from the
// verifier. On failure, aborts and returns false.
func createOauthCodeChallenge(c *gin.Context, session *sessions.Session) (string, bool) {
	verifier, err := oauthUtils.GenerateCodeVerifierWithLength(pkceCodeVerifierLength)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return "", false
	}

	session.Values[oauth2AuthVerifierKey] = verifier.String()

	return verifier.CodeChallengeS256(), true
}

// verifyToken passes the token and the oAuthParameters through a JWT verifier.
func (p oAuthParameters) verifyToken(t string) (*verifier.Jwt, error) {
	jv := verifier.JwtVerifier{
		Issuer:           p.issuer,
		ClaimsToValidate: map[string]string{oauth2JWTClaimKey: p.clientID},
	}

	result, err := jv.New().VerifyIdToken(t)
	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, ErrCouldNotVerifyToken
	}

	return result, nil
}

// extractEmailFromOktaSession takes a HTTP request (containing things like the
// cookie) and will get user's email from Okta.
func (s *Server) extractEmailFromOktaSession(r *http.Request) (string, error) {
	session, err := s.webOAuth.sessionStore.Get(r, oktaCookieName)
	if err != nil || session.Values[oauth2AccessTokenKey] == nil || session.Values[oauth2AccessTokenKey] == "" {
		return "", err
	}

	ctx, cnlFunc := context.WithTimeout(context.Background(), time.Minute)
	defer cnlFunc()

	req, err := http.NewRequestWithContext(ctx, "GET", s.webOAuth.params.userinfoEndpoint, nil)
	if err != nil {
		return "", err
	}

	token, ok := session.Values[oauth2AccessTokenKey].(string)
	if !ok {
		return "", nil
	}

	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("Accept", "application/json")

	return s.extractEmailFromOktaUserClaims(req)
}

// OktaUser is used to json.Unmarshal Okta user claims.
type OktaUser struct {
	Email string `json:"email"`
}

// extractEmailFromOktaUserClaims does the given request, interpreting the body
// as JSON, and extracts the email claim.
func (s *Server) extractEmailFromOktaUserClaims(r *http.Request) (string, error) {
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	ou := &OktaUser{}
	err = json.Unmarshal(body, ou)

	return ou.Email, err
}
