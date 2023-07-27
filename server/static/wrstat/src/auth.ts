/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *   Michael Woolnough <mw31@sanger.ac.uk>
 *   Sendu Bala <sb10@sanger.ac.uk>
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

const usernameFromJWT = (jwt: string): string => JSON.parse(atob(jwt.split('.')[1])).Username,
	getJWT = () => new Promise<string>((successFn, errorFn) => {
		const xh = new XMLHttpRequest();

		xh.addEventListener("readystatechange", () => {
			if (xh.readyState === 4) {
				if (xh.status === 200) {
					const token = JSON.parse(xh.response);

					document.cookie = `jwt=${token};samesite=strict;path=/tree`

					successFn(usernameFromJWT(token));
				} else {
					errorFn(new Error(xh.responseText));
				}
			}
		});
		xh.open("POST", "/rest/v1/jwt");
		xh.send(null);
	});

export const getCookie = (toFind: string) => {
	for (const cookie of document.cookie.split("; ")) {
		const [name, value] = cookie.split("=");

		if (name === toFind) {
			return value;
		}
	}

	return "";
},
	logout = () => {
		document.cookie = "jwt=;expires=Thu, 1 Jan 1970 00:00:00 GMT;path=/tree";
		document.cookie = "okta-hosted-login-session-store=;expires=Thu, 1 Jan 1970 00:00:00 GMT;path=/";

		window.location.reload();
	};

const AuthComponent = () => {
	const jwt = getCookie("jwt"),
		oidc = getCookie("okta-hosted-login-session-store");

	if (jwt) {
		return Promise.resolve(usernameFromJWT(jwt));
	}

	if (!oidc) {
		return Promise.reject();
	}

	return getJWT();
};

export default AuthComponent;