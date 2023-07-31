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

import { getCookie, logout } from "./auth";

type ChildFilter = {
	path: string;
	groups?: string;
	users?: string;
	types?: string;
}

export type Child = {
	name: string;
	path: string;
	count: number;
	size: number;
	atime: string;
	mtime: string;
	users: string[];
	groups: string[];
	filetypes: string[];
	has_children: boolean;
	children: Child[];
	timestamp: string;
	areas: Record<string, string[]>;
	noauth: boolean;
}

export type Usage = {
	GID: number;
	UID: number;
	GIDs: number[];
	UIDs: number[];
	Name: string;
	Owner: string;
	BaseDir: string;
	UsageSize: number;
	QuotaSize: number;
	UsageInodes: number;
	QuotaInodes: number;
	Mtime: string;
	DateNoSpace: string;
	DateNoFiles: string;
	status?: string;
	percentSize?: number;
	percentInodes?: number;
}

export type SubDir = {
	SubDir: string;
	NumFiles: number;
	SizeFiles: number;
	LastModified: string;
	FileUsage: Record<number, number>;
}

export type History = {
	Date: string;
	UsageSize: number;
	QuotaSize: number;
	UsageInodes: number;
	QuotaInodes: number;
}

const endpointREST = "/rest/v1/auth",
	treeURL = endpointREST + "/tree",
	groupUsageURL = endpointREST + "/basedirs/usage/groups",
	userUsageURL = endpointREST + "/basedirs/usage/users",
	groupSubDirPathURL = endpointREST + "/basedirs/subdirs/group",
	userSubDirPathURL = endpointREST + "/basedirs/subdirs/user",
	baseDirHistoryURL = endpointREST + "/basedirs/history";

const cache = new Map<string, string>(),
	getURL = <T>(url: string, params: Record<string, unknown> = {}) => {
		return new Promise<T>((successFn, errorFn) => {
			const urlParams: string[] = [];

			for (const [key, value] of Object.entries(params)) {
				urlParams.push(`${key}=${encodeURIComponent(value + "")}`);
			}

			if (urlParams.length) {
				url += "?" + urlParams.join("&");
			}

			const res = cache.get(url);
			if (res) {
				successFn(JSON.parse(res));

				return;
			}

			const xh = new XMLHttpRequest();

			xh.open("GET", url);
			xh.setRequestHeader("Authorization", `Bearer ${getCookie("jwt")}`);
			xh.addEventListener("readystatechange", () => {
				if (xh.readyState === 4) {
					if (xh.status === 200) {
						cache.set(url, xh.responseText);
						successFn(JSON.parse(xh.responseText));
					} else if (xh.status === 401) {
						logout();
					} else {
						errorFn(new Error(xh.responseText));
					}
				}
			});
			xh.send();
		});
	}

const RPC = {
	"getChildren": (filter: ChildFilter) => getURL<Child>(treeURL, filter),
	"getGroupUsageData": () => getURL<Usage[]>(groupUsageURL),
	"getUserUsageData": () => getURL<Usage[]>(userUsageURL),
	"getBasedirsGroupSubdirs": (id: number, basedir: string) => getURL<SubDir[]>(groupSubDirPathURL, { id, basedir }),
	"getBasedirsUserSubdirs": (id: number, basedir: string) => getURL<SubDir[]>(userSubDirPathURL, { id, basedir }),
	"getBasedirsHistory": (id: number, basedir: string) => getURL<History[]>(baseDirHistoryURL, { id, basedir })
};

export default RPC;