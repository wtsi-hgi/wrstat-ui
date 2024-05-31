/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Authors:
 *   Daniel Elia <de7@sanger.ac.uk>
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

import type { Child } from "./rpc";
import Table from "./Table";
import { asTimeAgo, formatBytes, formatNumber, asBasename } from './format';


const TreeTableComponent = ({ details, setTreePath, setChildDetails }: { details: Child | null, setTreePath: (s: string) => void, setChildDetails: (c: Child) => void }) => {
	if (!details) {
		return <></>;
	}

	if (!details.children) {
		// TODO: no subdirs message?
		return <></>;
	}

	const stringSort = new Intl.Collator().compare,
		sortPath = (a: Child, b: Child) => stringSort(a.path, b.path),
		sortSize = (a: Child, b: Child) => a.size - b.size,
		sortCount = (a: Child, b: Child) => a.count - b.count,
		sortAtime = (a: Child, b: Child) => new Date(b.atime).valueOf() - new Date(a.atime).valueOf(),
		sortMtime = (a: Child, b: Child) => new Date(b.mtime).valueOf() - new Date(a.mtime).valueOf();

	return (
		<Table
			table={details.children}
			onRowClick={(child => child.has_children && !child.noauth && setTreePath(child.path))}
			onRowHover={(child => setChildDetails(child))}
			onMouseOut={(() => setChildDetails(details))}
			id="treeTable"
			cols={[
				{ title: "Sub-Directories", key: "path", formatter: asBasename, sortFn: sortPath },
				{ title: "File Size", key: "size", formatter: formatBytes, sortFn: sortSize },
				{ title: "File Count", key: "count", formatter: formatNumber, sortFn: sortCount },
				{ title: "Earliest access", key: "atime", formatter: asTimeAgo, sortFn: sortAtime },
				{ title: "Last modified", key: "mtime", formatter: asTimeAgo, sortFn: sortMtime },
			]}
			className={"prettyTable"}
			rowExtra={(child => {
				if (!child.has_children || child.noauth) {
					return {
						"className": "noHover"
					};
				}

				return {};
			})}
		/>
	);
};

export default TreeTableComponent;