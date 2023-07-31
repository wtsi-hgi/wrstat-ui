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

import type { GroupUserFilterParams } from './GroupUserFilter';
import type { Child } from './rpc';
import type { Entry } from './Treemap';
import { useEffect, useState } from "react"
import GroupUserFilter from './GroupUserFilter';
import MultiSelect from './MultiSelect';
import TreeDetails from "./TreeDetails";
import Treemap from "./Treemap";
import RPC from "./rpc";
import { useSavedState } from './state';

type DiskTreeParams = {
	treePath: string;
	userMap: Map<number, string>;
	groupMap: Map<number, string>;
	setTreePath: (v: string) => void;
	guf: GroupUserFilterParams;
}

const colours = [
	"#d73027",
	"#f46c43",
	"#fdaf61",
	"#fedf8b",
	"#ffffbf",
	"#d9ef8b",
	"#a6d96a",
	"#66bd63",
	"#1a9850",
	"#fff"
] as const,
	day = 24 * 60 * 60 * 1000,
	now = +Date.now(),
	colourFromAge = (lm: number) => {
		const diff = now - lm;

		if (diff > 2 * 365 * day) {
			return colours[0];
		} else if (diff > 365 * day) {
			return colours[1];
		} else if (diff > 10 * 30 * day) {
			return colours[2];
		} else if (diff > 8 * 30 * day) {
			return colours[3];
		} else if (diff > 6 * 30 * day) {
			return colours[4];
		} else if (diff > 3 * 30 * day) {
			return colours[5];
		} else if (diff > 2 * 30 * day) {
			return colours[6];
		} else if (diff > 30 * day) {
			return colours[7];
		}
		return colours[8];
	},
	Breadcrumb = ({ path, part, setPath }: { path: string; part: string; setPath: (path: string) => void }) => <li>
		<button title={`Jump To: ${part}`} onClick={e => {
			if (e.button !== 0) {
				return;
			}

			setPath(path);
		}}>{part}</button>
	</li>,
	makeBreadcrumbs = (path: string, setPath: (path: string) => void) => {
		let last = 0;

		const breadcrumbs = [
			<Breadcrumb key={`breadcrumb_root`} path="/" part="Root" setPath={setPath} />
		];

		while (true) {
			const pos = path.indexOf("/", last + 1);

			if (pos === -1) {
				break;
			}

			breadcrumbs.push(<Breadcrumb key={`breadcrumb_${breadcrumbs.length}`} path={path.slice(0, pos)} part={path.slice(last + 1, pos)} setPath={setPath} />);

			last = pos;
		}

		breadcrumbs.push(<li key={`breadcrumb_${breadcrumbs.length}`} tabIndex={0} aria-current="location">{path.slice(last + 1) || "/"}</li>);

		return breadcrumbs;
	},
	determineTreeWidth = () => Math.max(window.innerWidth - 420, 400),
	makeFilter = (path: string, uids: number[], gids: number[], filetypes: string[], users: Map<number, string>, groups: Map<number, string>) => ({
		path,
		"users": uids.map(uid => users.get(uid) ?? "").filter(u => u).join(",") ?? "",
		"groups": gids.map(gid => groups.get(gid) ?? "").filter(g => g).join(",") ?? "",
		"types": filetypes.join(",")
	}),
	fileTypes = [
		"other", "temp", "vcf", "vcf.gz", "bcf", "sam", "bam",
		"cram", "fasta", "fastq", "fastq.gz", "ped/bed", "compressed", "text",
		"log", "dir"
	] as const,
	timesSinceAccess = [
		["> 0 days", 0],
		["> 1 month", 30],
		["> 2 months", 60],
		["> 3 months", 120],
		["> 6 months", 180],
		["> 8 months", 240],
		["> 10 months", 300],
		["> 1 year", 365],
		["> 2 years", 730]
	] as const,
	entrySort = (a: Entry, b: Entry) => b.value - a.value,
	DiskTreeComponent = ({ treePath, userMap, groupMap, setTreePath, guf }: DiskTreeParams) => {
		const [treeMapData, setTreeMapData] = useState<Entry[] | null>(null),
			[breadcrumbs, setBreadcrumbs] = useState<JSX.Element[]>([]),
			[childDetails, setChildDetails] = useState<Child | null>(null),
			[dirDetails, setDirDetails] = useState<Child | null>(childDetails),
			[useMTime, setUseMTime] = useSavedState("useMTime", false),
			[useCount, setUseCount] = useSavedState("useCount", false),
			[treeWidth, setTreeWidth] = useState(determineTreeWidth()),
			[filterFileTypes, setFilterFileTypes] = useSavedState<string[]>("treeTypes", []),
			[sinceLastAccess, setSinceLastAccess] = useSavedState("sinceLastAccess", 0),
			[hasAuth, setHasAuth] = useState(true);

		useEffect(() => window.addEventListener("resize", () => setTreeWidth(determineTreeWidth())), []);

		useEffect(() => {
			RPC.getChildren(makeFilter(treePath, guf.users, guf.groups, filterFileTypes, userMap, groupMap))
				.then(children => {
					const entries: Entry[] = [],
						since = Date.now() - sinceLastAccess * 86_400_000;

					for (const child of children.children ?? []) {
						if (new Date(child.atime).valueOf() > since) {
							continue;
						}

						entries.push({
							key: btoa(child.path),
							name: child.name,
							value: useCount ? child.count : child.size,
							backgroundColour: colourFromAge(+(new Date(useMTime ? child.mtime : child.atime))),
							onclick: child.has_children && !child.noauth ? () => setTreePath(child.path) : undefined,
							onmouseover: () => setChildDetails(child),
							noauth: child.noauth
						});
					}

					entries.sort(entrySort);

					setHasAuth(!children.noauth);
					setTreeMapData(entries);
					setChildDetails(children);
					setDirDetails(children);
				});

			setBreadcrumbs(makeBreadcrumbs(treePath, setTreePath));
		}, [treePath, useMTime, useCount, filterFileTypes, sinceLastAccess, guf.groups, guf.users]);

		return <>
			<div id="disktree">
				<div>
					<div className="treeFilter">
						<div className="title">Colour By</div>
						<label aria-label="Colour by Oldest Access Time" title="Oldest Access Time" htmlFor="aTime">Access Time</label><input type="radio" id="aTime" checked={!useMTime} onChange={() => setUseMTime(false)} />
						<label aria-label="Colour by Latest Modified Time" title="Latest Modified Time" htmlFor="mTime">Modified Time</label><input type="radio" id="mTime" checked={useMTime} onChange={() => setUseMTime(true)} />
						<div className="title">Area Represents</div>
						<label aria-label="Area represents File Size" htmlFor="useSize">File Size</label><input type="radio" id="useSize" checked={!useCount} onChange={() => setUseCount(false)} />
						<label aria-label="Area represents File Count" htmlFor="useCount">File Count</label><input type="radio" id="useCount" checked={useCount} onChange={() => setUseCount(true)} />
						<div className="title">Filter</div>
						<label htmlFor="filetypes">File Types</label><MultiSelect id="filetypes" list={fileTypes} selected={filterFileTypes} onchange={setFilterFileTypes} />
						<label htmlFor="sinceAccess">Time Since Access</label>
						<select value={sinceLastAccess} id="sinceAccess" onChange={e => setSinceLastAccess(parseInt(e.target.value) ?? 0)}>
							{timesSinceAccess.map(([l, t]) => <option key={`tsa_${t}`} value={t}>{l}</option>)}
						</select>
						<br />
						<GroupUserFilter {...guf} num={1} />
						<button onClick={e => {
							if (e.button !== 0) {
								return;
							}

							setFilterFileTypes([]);
							setSinceLastAccess(0);
							guf.setGroups([]);
							guf.setUsers([]);
						}}>Reset Filter</button>
					</div>
					<div id="treeKey">
						<div>
							<span>Colour Key</span>
							{useMTime ? "Least" : "Greatest"} time since a file nested within the directory was {useMTime ? "modified" : "accessed"}:
						</div>
						<ol>
							<li className="age_2years">&gt; 2 years</li>
							<li className="age_1year">&gt; 1 year</li>
							<li className="age_10months">&gt; 10 months</li>
							<li className="age_8months">&gt; 8 months</li>
							<li className="age_6months">&gt; 6 months</li>
							<li className="age_3months">&gt; 3 months</li>
							<li className="age_2months">&gt; 2 months</li>
							<li className="age_1month">&gt; 1 month</li>
							<li className="age_1week">&lt; 1 month</li>
						</ol>
					</div>
				</div>
				<ul id="treeBreadcrumbs">{breadcrumbs}</ul>
				<div>
					<Treemap table={treeMapData} width={treeWidth} height={500} noAuth={!hasAuth} onmouseout={() => setChildDetails(dirDetails)} />
					<TreeDetails details={childDetails} style={{ width: treeWidth + "px" }} />
				</div>
			</div>
		</>
	};

export default DiskTreeComponent;