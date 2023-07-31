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

import type { Usage } from './rpc';
import type { Filter, } from './Table';
import Table, { fitlerTableRows } from './Table';
import { downloadGroups, downloadUsers } from './download';
import { asDaysAgoStr, formatBytes, formatNumber } from './format';
import { useSavedState } from './state';


type FilterTableParams = {
	byUser: boolean;
	selectedID: number;
	setSelectedID: (id: number) => void;
	selectedDir: string;
	setSelectedDir: (dir: string) => void;
	setTreePath: (v: string) => void;
	usage: Usage[];
	userMap: Map<number, string>;
	groupMap: Map<number, string>;
	filter: Filter<Usage>;
	justDisktree: boolean;
}

const stringSort = new Intl.Collator().compare,
	sortBaseDirs = (a: Usage, b: Usage) => stringSort(a.BaseDir, b.BaseDir) || stringSort(a.Name, b.Name),
	sortNames = (a: Usage, b: Usage) => stringSort(a.Name, b.Name) || stringSort(a.BaseDir, b.BaseDir),
	sortUsageSize = (a: Usage, b: Usage) => a.UsageSize - b.UsageSize || stringSort(a.BaseDir, b.BaseDir),
	sortQuotaSize = (a: Usage, b: Usage) => a.QuotaSize - b.QuotaSize || stringSort(a.BaseDir, b.BaseDir),
	sortPercentSize = (a: Usage, b: Usage) => (b.QuotaSize * a.UsageSize - a.QuotaSize * b.UsageSize) || stringSort(a.BaseDir, b.BaseDir),
	sortUsageInodes = (a: Usage, b: Usage) => a.UsageInodes - b.UsageInodes || stringSort(a.BaseDir, b.BaseDir),
	sortQuotaInodes = (a: Usage, b: Usage) => a.QuotaInodes - b.QuotaInodes || stringSort(a.BaseDir, b.BaseDir),
	sortPercentInodes = (a: Usage, b: Usage) => (b.QuotaInodes * a.UsageInodes - a.QuotaInodes * b.UsageInodes) || stringSort(a.BaseDir, b.BaseDir),
	sortMTime = (a: Usage, b: Usage) => new Date(b.Mtime).valueOf() - new Date(a.Mtime).valueOf() || stringSort(a.BaseDir, b.BaseDir),
	sortStatus = (a: Usage, b: Usage) => stringSort(a.status ?? "", b.status ?? "") || stringSort(a.BaseDir, b.BaseDir),
	reverseSortPercentSize = (a: Usage, b: Usage) => !a.QuotaSize ? 1 : !b.QuotaSize ? -1 : sortPercentSize(b, a),
	reverseSortPercentInodes = (a: Usage, b: Usage) => !a.QuotaInodes ? 1 : !b.QuotaInodes ? -1 : sortPercentInodes(b, a),
	UsageTableComponent = ({ usage, byUser, groupMap, userMap, selectedID, selectedDir, setSelectedID, setSelectedDir, setTreePath, filter, justDisktree }: FilterTableParams) => {
		const [perPage, setPerPage] = useSavedState("perPage", 10),
			hasQuota = usage.some(u => u.QuotaSize || u.QuotaInodes);

		return <>
			<details open id="usage" style={justDisktree ? { display: "none" } : undefined}>
				<summary><h1>{byUser ? "User" : "Group"} Usage Table</h1></summary>
				<span id="perPage">Show
					<select value={perPage} onChange={e => { setPerPage(parseInt(e.target.value) ?? 10) }}>
						<option>10</option>
						<option>20</option>
						<option>50</option>
						<option>100</option>
						<option value={Number.MAX_SAFE_INTEGER}>All</option>
					</select>
					Entries</span>
				<Table
					caption={`${byUser ? "User" : " Group"} Usage Table`}
					rowExtra={row => {
						if ((byUser ? row.UID : row.GID) === selectedID && row.BaseDir === selectedDir) {
							return { "className": "selected" };
						}

						return {};
					}}
					perPage={perPage} filter={filter} onRowClick={(data: Usage) => {
						const id = byUser ? data.UID : data.GID;

						setSelectedDir(data.BaseDir);
						setSelectedID(id);
						setTreePath(data.BaseDir);
					}}
					cols={[
						{
							title: "Base Directory",
							key: "BaseDir",
							extra: (path, row) => ({
								title: path +
									(row.Owner ? "\nPI: " + row.Owner : "") +
									"\n" + (byUser ? "User: " : "Group: ") + row.Name +
									"\n" + (byUser ? "Groups: " : "Users: ") +
									(byUser ? row.GIDs : row.UIDs)
										.map(id => (byUser ? groupMap : userMap).get(id) ?? "")
										.filter(n => n)
										.join(", ")
							}),
							sortFn: sortBaseDirs
						},
						{
							title: byUser ? "User" : "Group",
							key: "Name",
							extra: title => ({ title }),
							sortFn: sortNames
						},
						{
							title: "Space Used",
							key: "UsageSize",
							extra: used => ({ title: formatNumber(used) + " Bytes" }),
							sortFn: sortUsageSize,
							startReverse: true,
							formatter: formatBytes,
							sum: values => formatBytes(values.reduce((a, b) => a + b, 0))
						},
						{
							title: "Space Quota",
							key: "QuotaSize",
							extra: quota => ({ title: formatNumber(quota) + " Bytes" }),
							sortFn: sortQuotaSize,
							startReverse: true,
							formatter: formatBytes
						},
						{
							title: "Space Usage (%)",
							key: "percentSize",
							sortFn: sortPercentSize,
							reverseFn: reverseSortPercentSize,
							startReverse: true,
							formatter: (p: number | undefined) => p ? formatNumber(p) : "0"
						},
						{
							title: "Num. Files",
							key: "UsageInodes",
							sortFn: sortUsageInodes,
							startReverse: true,
							formatter: formatNumber,
							sum: values => formatNumber(values.reduce((a, b) => a + b, 0))
						},
						{
							title: "Max Files",
							key: "QuotaInodes",
							sortFn: sortQuotaInodes,
							startReverse: true,
							formatter: formatNumber
						},
						{
							title: "File Usage (%)",
							key: "percentInodes",
							sortFn: sortPercentInodes,
							reverseFn: reverseSortPercentInodes,
							startReverse: true,
							formatter: (p: number | undefined) => p ? formatNumber(p) : "0"
						},
						{
							title: "Last Modified (days)",
							key: "Mtime",
							extra: title => ({ title }),
							sortFn: sortMTime,
							formatter: asDaysAgoStr
						},
						{
							title: "Status",
							key: "status",
							extra: title => ({ title }),
							formatter: status => <svg xmlns="http://www.w3.org/2000/svg" style={{ width: "1em", height: "1em" }}>
								<title>{status}</title>
								<use href={status === "OK" ? "#ok" : "#notok"} />
							</svg>,
							sortFn: sortStatus
						}
					]}
					table={usage}
					id="usageTable"
					className={"prettyTable " + (hasQuota ? undefined : "noQuota")}
				/>
				<button className="download" onClick={e => {
					if (e.button !== 0) {
						return;
					}

					(byUser ? downloadUsers : downloadGroups)(usage);
				}}>Download Unfiltered Table</button>
				<button className="download" onClick={e => {
					if (e.button !== 0) {
						return;
					}

					(byUser ? downloadUsers : downloadGroups)(fitlerTableRows(usage, filter));
				}}>Download Filtered Table</button>
			</details>
		</>
	};

export default UsageTableComponent;