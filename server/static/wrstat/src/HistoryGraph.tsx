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

import { useEffect, useState } from 'react';
import { formatDate } from './format';

type HistoryGraphParams = {
	history: UsageHistory[];
	width: number;
	height: number;
	yFormatter: (num: number) => string;
	secondaryFormatter: (num: number) => string;
	yRounder: (num: number) => number;
}

type UsageHistory = {
	Quota: number;
	Usage: number;
	Date: string;
}

const paddingXL = 80,
	paddingXR = 80,
	paddingYT = 10,
	paddingYB = 80,
	HistoryGraph = ({ history, width, height, yFormatter, secondaryFormatter, yRounder }: HistoryGraphParams) => {
		const [infoBox, setInfoBox] = useState(-1),
			amountToY = (amount: number) => paddingYT + maxY - amount * yScale,
			dateToX = (date: number) => paddingXL + (date - minDate) * xScale,
			quotaPoints: JSX.Element[] = [],
			usagePoints: JSX.Element[] = [],
			infoBoxes: JSX.Element[] = [];

		let minDate = Infinity,
			maxDate = -Infinity,
			maxAmount = 0,
			quotaPath = "M",
			usagePath = "M",
			n = 0;

		useEffect(() => setInfoBox(-1), [history, width, height]);

		if (history.length === 0) {
			return <></>;
		}

		for (const h of history) {
			if (h.Quota > maxAmount) {
				maxAmount = h.Quota;
			}

			if (h.Usage > maxAmount) {
				maxAmount = h.Usage;
			}

			const d = new Date(h.Date).valueOf();

			if (d < minDate) {
				minDate = d;
			}

			if (d > maxDate) {
				maxDate = d;
			}
		}

		const projectDate = maxDate = maxDate += (maxDate - minDate) / 5;

		minDate -= (maxDate - minDate) / 10;
		maxDate += (maxDate - minDate) / 10;

		maxAmount = Math.max(yRounder(maxAmount), 5);

		const dateDiff = maxDate - minDate,
			xScale = (width - paddingXL - paddingXR) / dateDiff,
			yScale = (height - paddingYT - paddingYB) / maxAmount,
			maxY = maxAmount * yScale;

		for (const h of history) {
			const d = new Date(h.Date).valueOf(),
				x = dateToX(d),
				quotaY = amountToY(h.Quota),
				sizeY = amountToY(h.Usage),
				qFormatted = yFormatter(h.Quota),
				qSFormatted = secondaryFormatter(h.Quota),
				quotaBox = infoBoxes.push(<div key={`quota_${n}`} style={{ left: x + "px", top: quotaY + "px", display: infoBoxes.length === infoBox ? "inline-block" : "" }}>
					{qFormatted}
					{qFormatted !== qSFormatted ?
						<>
							<br />
							{qSFormatted}
						</> :
						<></>
					}
					<br />
					{formatDate(h.Date)}
				</div>) - 1,
				uFormatted = yFormatter(h.Usage),
				uSFormatted = secondaryFormatter(h.Usage),
				usageBox = infoBoxes.push(<div key={`amount_${n}`} style={{ left: x + "px", top: sizeY + "px", display: infoBoxes.length === infoBox ? "inline-block" : "" }}>
					{uFormatted}
					{uFormatted !== uSFormatted ?
						<>
							<br />
							{uSFormatted}
						</> :
						<></>
					}
					<br />
					{formatDate(h.Date)}
				</div>) - 1;

			if (quotaPath.length > 1) {
				quotaPath += " L";
				usagePath += " L";
			}

			quotaPath += `${x},${quotaY}`;
			usagePath += `${x},${sizeY}`;

			quotaPoints.push(<use key={`point_quota_${n}`} href="#point" style={{ fill: "var(--graphQuota)" }} x={x} y={quotaY} onMouseOver={() => setInfoBox(quotaBox)} onMouseOut={() => setInfoBox(-1)} />);
			usagePoints.push(<use key={`point_usage_${n++}`} href="#point" style={{ fill: "var(--graphUsage)" }} x={x} y={sizeY} onMouseOver={() => setInfoBox(usageBox)} onMouseOut={() => setInfoBox(-1)} />);
		}

		const previousHistory = history.at(-3) ?? history.at(0)!,
			latestHistory = history.at(-1)!,
			previousDate = new Date(previousHistory.Date).valueOf(),
			latestDate = new Date(latestHistory.Date).valueOf(),
			dt = latestDate - previousDate,
			dy = latestHistory.Usage - previousHistory.Usage,
			m = dy / (dt || 1),
			c = latestHistory.Usage - latestDate * m;

		let x = projectDate,
			y = m * projectDate + c;

		if (latestHistory.Usage > latestHistory.Quota) {
			y = -Infinity;
		} else if (y < 0) {
			x = -c / m;
			y = 0;
		} else if (y > latestHistory.Quota) {
			x = (latestHistory.Quota - c) / m;
			y = latestHistory.Quota;

			infoBoxes.push(<div style={{ left: dateToX(x) + "px", top: amountToY(y) + "px", display: infoBoxes.length === infoBox ? "inline-block" : "" }}>
				Fill Quota Data: {formatDate(x)}
			</div>);
		}

		return <>
			<div id="historyInfo">
				{infoBoxes}
			</div>
			<svg id="history" xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
				<defs>
					<path id="point" d="M0,5 L5,0 0,-5 -5,0 Z" />
				</defs>
				<rect rx={4} x={paddingXL} y={paddingYT} width={width - paddingXL - paddingXR} height={height - paddingYT - paddingYB} style={{ "fill": "var(--graphBack, #ddd)" }} stroke="currentColor" />
				{
					Array.from({ length: 4 }, (_, n) => <line key={`graph_lh_${n}`} x1={paddingXL} x2={width - paddingXR} y1={amountToY((n + 1) * maxAmount / 5)} y2={amountToY((n + 1) * maxAmount / 5)} className="graphLines" />)
				}
				{
					Array.from({ length: 6 }, (_, n) => <text key={`graph_th_${n}`} x={paddingXL - 3} y={amountToY(maxAmount - (n * maxAmount) / 5) + 5} fill="currentColor" textAnchor="end"><title>{secondaryFormatter(maxAmount * (5 - n) / 5)}</title>{yFormatter(maxAmount * (5 - n) / 5)}</text>)
				}
				{
					Array.from({ length: 4 }, (_, n) => <line key={`graph_lv_${n}`} x1={dateToX(minDate + (n + 1) * dateDiff / 5)} x2={dateToX(minDate + (n + 1) * dateDiff / 5)} y1={paddingYT} y2={height - paddingYB} className="graphLines" />)
				}
				{
					Array.from({ length: 6 }, (_, n) => <text key={`graph_tv_${n}`} transform={`translate(${dateToX(minDate + n * dateDiff / 5)} ${height - paddingYB + 15}) rotate(-45)`} fill="currentColor" textAnchor="end">{formatDate(minDate + dateDiff * n / 5)}</text>)
				}
				<path d={quotaPath} style={{ stroke: "var(--graphQuota)" }} fill="none" />
				<path d={`M${dateToX(latestDate)},${amountToY(latestHistory.Quota)} L${dateToX(projectDate)},${amountToY(latestHistory.Quota)}`} style={{ stroke: "var(--graphQuota)" }} fill="none" strokeWidth="3" strokeDasharray="3" />
				<path d={usagePath} style={{ stroke: "var(--graphUsage)" }} fill="none" />
				{y !== -Infinity &&
					<path d={`M${dateToX(latestDate)},${amountToY(latestHistory.Usage)} L${dateToX(x)},${amountToY(y)}`} style={{ stroke: "var(--graphUsage)" }} fill="none" strokeWidth="3" strokeDasharray="3" />
				}
				{quotaPoints}
				{usagePoints}
				{
					y === latestHistory.Quota ? <path d="M5,5 L-5,-5 M-5,5 L5,-5" stroke="#f00" strokeWidth={2} transform={`translate(${paddingXL + (x - minDate) * xScale} ${amountToY(y)})`} onMouseOver={() => setInfoBox(infoBoxes.length - 1)} onMouseOut={() => setInfoBox(-1)} /> : []
				}
				<rect rx={4} x={width - 70} y={paddingYT} width={69} height={43} stroke="currentColor" fill="none" />
				<rect rx={2} x={width - 65} y={paddingYT + 5} width={10} height={10} stroke="currentColor" style={{ fill: "var(--graphQuota)" }} />
				<text x={width - 48} y={paddingYT + 15} fill="currentColor">Quota</text>
				<rect rx={2} x={width - 65} y={paddingYT + 25} width={10} height={10} stroke="currentColor" style={{ fill: "var(--graphUsage)" }} />
				<text x={width - 48} y={paddingYT + 35} fill="currentColor">Usage</text>
			</svg>
		</>;
	};

export default HistoryGraph;