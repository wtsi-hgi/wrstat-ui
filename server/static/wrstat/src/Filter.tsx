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

import type { Usage } from "./rpc";
import Minmax from "./MinMax";
import GroupUserFilter, { type GroupUserFilterParams } from "./GroupUserFilter";
import MultiSelect from "./MultiSelect";
import { asDaysAgo, formatBytes, formatNumber } from "./format";
import { clearState } from "./state";

type FilterParams = {
	usage: Usage[];
	groupUsage: Usage[];
	byUser: boolean;
	axisMinSize: number;
	setAxisMinSize: (v: number) => void;
	axisMaxSize: number;
	setAxisMaxSize: (v: number) => void;
	axisMinDaysAgo: number;
	setAxisMinDaysAgo: (v: number) => void;
	axisMaxDaysAgo: number;
	setAxisMaxDaysAgo: (v: number) => void;
	scaleSize: boolean;
	setScaleSize: (v: boolean) => void;
	scaleDays: boolean;
	setScaleDays: (v: boolean) => void;
	owners: string[];
	setOwners: (v: string[]) => void;
	guf: GroupUserFilterParams;
	setByUser: (v: boolean) => void;
}

const stringSort = new Intl.Collator().compare,
	FilterComponent = ({
		usage,
		byUser,
		axisMinSize, setAxisMinSize,
		axisMaxSize, setAxisMaxSize,
		axisMinDaysAgo, setAxisMinDaysAgo,
		axisMaxDaysAgo, setAxisMaxDaysAgo,
		scaleSize, setScaleSize,
		scaleDays, setScaleDays,
		owners,
		setOwners,
		groupUsage,
		guf,
		setByUser
	}: FilterParams) => {
		return <>
			<div className="treeFilter">
				<label htmlFor={`owners`}>Owners</label>
				<MultiSelect
					id={`owners`}
					list={Array.from(new Set(groupUsage.map(e => e.Owner).filter(o => o)).values()).sort(stringSort)}
					selected={owners}
					onchange={setOwners}
					disabled={byUser} />
				<GroupUserFilter {...guf} num={0} />
				<label>Size</label>
				<Minmax
					max={usage.reduce((max, curr) => Math.max(max, curr.UsageSize), 0)}
					width={300}
					minValue={axisMinSize}
					maxValue={axisMaxSize}
					onchange={(min: number, max: number) => {
						setAxisMinSize(min);
						setAxisMaxSize(max);
					}}
					formatter={formatBytes}
					label="Size Filter" />
				<label>Last Modified</label>
				<Minmax
					max={usage.reduce((curr, next) => Math.max(curr, asDaysAgo(next.Mtime)), 0)}
					minValue={axisMinDaysAgo}
					maxValue={axisMaxDaysAgo}
					width={300}
					onchange={(min: number, max: number) => {
						setAxisMinDaysAgo(min);
						setAxisMaxDaysAgo(max);
					}}
					formatter={formatNumber}
					label="Days since last modified" />
				<label htmlFor="scaleSize">Log Size Axis</label>
				<input type="checkbox" id="scaleSize" checked={scaleSize} onChange={e => setScaleSize(e.target.checked)} />
				<label htmlFor="scaleDays">Log Days Axis</label>
				<input type="checkbox" id="scaleDays" checked={scaleDays} onChange={e => setScaleDays(e.target.checked)} />
				<button onClick={e => {
					if (e.button !== 0) {
						return;
					}

					clearState();
					setByUser(byUser);
				}}>Reset</button>
			</div>
		</>
	};

export default FilterComponent;