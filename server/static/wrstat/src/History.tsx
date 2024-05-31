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

import type { History } from "./rpc";
import { useEffect, useState } from "react";
import HistoryGraph from "./HistoryGraph";
import Tabs from "./Tabs";
import { formatBytes, formatLargeNumber, formatNumber } from "./format";
import RPC from "./rpc";
import { useSavedState } from "./state";
import { exceedDates } from "./trend";

type HistoryParams = {
	id: number;
	path: string;
	name: string;
	owner: string;
	isUser: boolean;
	justDisktree: boolean;
}

const determineGraphWidth = () => Math.max(500, window.innerWidth - 60),
	maxWarningDay = 25,
	daysToday = Math.round(Date.now() / 86_400_000),
	formatSmallBytes = (num: number) => formatNumber(num) + " Bytes",
	formatLog10 = (maxAmount: number) => {
		const order = Math.pow(10, Math.max(Math.floor(Math.log10(maxAmount)), 1));

		return order * Math.ceil(maxAmount / order);
	},
	formatLog2 = (maxAmount: number) => 100 * Math.pow(2, Math.ceil(Math.log2(maxAmount / 100))),
	HistoryWarning = ({ isInodes, history }: { isInodes: boolean, history: History[] }) => {
		const [exceedSize, exceedInode] = exceedDates(history);

		if (isInodes) {
			if (exceedInode === 0) {
				return <div className="exceeded">Inode Quota has been reached</div>;
			}
			if (exceedInode !== Infinity) {
				const prox = 100 * Math.min(maxWarningDay, (exceedInode - daysToday)) / maxWarningDay;
				return <div
					style={{
						[`--warningProx` as any]: prox + "%"
					}}
					className="exceed"
				>Expected to exceed inode quota in {formatNumber(exceedInode - daysToday)} days</div>;
			}

			return <></>;
		}

		if (exceedSize === 0) {
			return <div className="exceeded">Size Quota has been reached</div>;
		}

		if (exceedSize !== Infinity) {
			const daysLeft = exceedSize - daysToday,
				prox = 100 * Math.min(maxWarningDay, daysLeft) / maxWarningDay;

			return <div
				style={{ [`--warningProx` as any]: prox + "%" }}
				className="exceed"
			>Expected to exceed size quota in {formatNumber(daysLeft)} day{daysLeft === 1 ? "" : "s"}</div>;
		}


		return <></>;
	},
	HistoryComponent = ({ id, path, name, owner, isUser, justDisktree }: HistoryParams) => {
		const [inodeHistory, setInodeHistory] = useSavedState("inodeHistory", false),
			[history, setHistory] = useState<History[]>([]),
			[historyWidth, setHistoryWidth] = useState(determineGraphWidth()),
			graphData = history.map(h => ({
				Date: h.Date,
				Usage: inodeHistory ? h.UsageInodes : h.UsageSize,
				Quota: inodeHistory ? h.QuotaInodes : h.QuotaSize
			}));

		useEffect(() => {
			if (id === -1 || path === "") {
				setHistory([]);

				return;
			}

			RPC.getBasedirsHistory(id, path).then(setHistory);
		}, [id, path]);

		useEffect(() => window.addEventListener("resize", () => setHistoryWidth(determineGraphWidth())), []);

		if (history.length === 0 || isUser) {
			return <></>;
		}

		return <>
			<details open style={justDisktree ? { display: "none" } : undefined}>
				<summary><h1>Usage History</h1></summary>
				<Tabs id="historyTabs" tabs={[
					{
						title: "Size",
						onClick: () => setInodeHistory(false),
						selected: !inodeHistory,
					},
					{
						title: "Count",
						onClick: () => setInodeHistory(true),
						selected: inodeHistory,
					}
				]} />
				<div>
					{name} {owner && `(${owner})`} | {path.split("/")[2]}
					<HistoryWarning isInodes={inodeHistory} history={history} />
				</div>
				<HistoryGraph
					history={graphData}
					width={historyWidth}
					height={500}
					yFormatter={inodeHistory ? formatLargeNumber : formatBytes}
					secondaryFormatter={inodeHistory ? formatNumber : formatSmallBytes}
					yRounder={inodeHistory ? formatLog10 : formatLog2}
				/>
			</details>
		</>;
	};

export default HistoryComponent;