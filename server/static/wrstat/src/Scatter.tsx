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

import { useEffect, useState } from "react";
import { asDaysAgo, formatBytes, formatNumber } from "./format";
import { firstRender, restoring } from "./state";

type ScatterParams = {
	data: Data[];
	width: number;
	height: number;
	logX?: boolean;
	logY?: boolean;
	minX: number;
	maxX: number;
	minY: number;
	maxY: number;
	setLimits: (minSize: number, maxSize: number, minDate: number, maxDate: number) => void;
	previewLimits: (minSize: number, maxSize: number, minDate: number, maxDate: number) => void;
	isSelected: (u: Data) => boolean;
}

type Data = {
	UsageSize: number;
	Mtime: string;
}

const minDaysAgo = (date: string) => {
	const daysAgo = asDaysAgo(date);
	if (daysAgo < 0) {
		return 0;
	}

	return daysAgo;
},
	paddingXL = 80,
	paddingXR = 10,
	paddingYT = 1,
	paddingYB = 65,
	innerPadding = 10,
	logRatio = (value: number, min: number, max: number) => min ? Math.log(value / min) / Math.log(max / min) : Math.log(value + 1) / Math.log(max + 1),
	logRatioToValue = (r: number, min: number, max: number) => Math.pow(Math.E, (min ? Math.log(max / min) : Math.log(max + 1)) * r) * (min || 1) - (min || 1),
	ScatterComponent = ({
		data,
		width,
		height,
		logX = false,
		logY = false,
		setLimits,
		previewLimits,
		minX,
		maxX,
		minY,
		maxY,
		isSelected
	}: ScatterParams) => {
		const graphWidth = width - paddingXL - paddingXR - 2 * innerPadding,
			graphHeight = height - paddingYT - paddingYB - 2 * innerPadding,
			sizeToY = (size: number, log = logY) => paddingYT + innerPadding + (log ? logSizeToY : nonLogSizeToY)(size),
			dateToX = (days: number, log = logX) => paddingXL + innerPadding + (log ? logDateToX : nonLogDateToX)(days),
			nonLogSizeToY = (size: number) => yScale * (maxSize - size),
			nonLogDateToX = (days: number) => xScale * (days - minDate),
			logSizeToY = (size: number) => graphHeight - graphHeight * logRatio(size, minSize, maxSize),
			logDateToX = (days: number) => graphWidth * logRatio(days, minDate, maxDate),
			fractionToSize = (f: number) => Math.round((logY ? logFractionToSize : nonLogFractionToSize)(f)),
			fractionToDate = (f: number) => Math.round((logX ? logFractionToDate : nonLogFractionToDate)(f)),
			nonLogFractionToSize = (f: number) => minSize + f * (maxSize - minSize),
			nonLogFractionToDate = (f: number) => minDate + f * (maxDate - minDate),
			logFractionToSize = (f: number) => minSize + logRatioToValue(f, minSize, maxSize),
			logFractionToDate = (f: number) => minDate + logRatioToValue(f, minDate, maxDate),
			[highlightCoords, setHighlightCoords] = useState<null | [number, number, number, number]>(null),
			onDrag = (e: React.MouseEvent<SVGSVGElement, MouseEvent>) => {
				if (e.button !== 0) {
					return;
				}

				const coords = e.currentTarget.getBoundingClientRect(),
					graphLeft = coords.left + paddingXL,
					graphTop = coords.top + paddingYT,
					startX = e.clientX - graphLeft,
					startY = e.clientY - graphTop,
					mousemove = (e: MouseEvent, cb = previewLimits) => {
						const x = e.clientX - graphLeft,
							y = e.clientY - graphTop,
							minX = Math.max(Math.min(x, startX), 0),
							maxX = Math.min(Math.max(x, startX), graphWidth + 2 * innerPadding),
							minY = Math.max(Math.min(y, startY), 0),
							maxY = Math.min(Math.max(y, startY), graphHeight + 2 * innerPadding);

						if (minX === maxX || minY === maxY) {
							setHighlightCoords(null);
							cb(-Infinity, Infinity, -Infinity, Infinity);

							return;
						}

						setHighlightCoords([minX, maxX - minX, minY, maxY - minY]);

						const fMinX = Math.max(0, minX - innerPadding) / graphWidth,
							fMaxX = Math.min(graphWidth, maxX - innerPadding) / graphWidth,
							fMinY = Math.max(0, graphHeight - maxY + innerPadding) / graphHeight,
							fMaxY = Math.min(graphHeight, graphHeight - minY + innerPadding) / graphHeight,
							minDaysAgo = fractionToDate(fMinX),
							maxDaysAgo = fractionToDate(fMaxX),
							minFileSize = fractionToSize(fMinY),
							maxFileSize = fractionToSize(fMaxY);

						cb(minFileSize, maxFileSize, minDaysAgo, maxDaysAgo);
					},
					mouseup = (e: MouseEvent) => {
						if (e.button !== 0) {
							return;
						}

						mousemove(e, setLimits);
						window.removeEventListener("mousemove", mousemove);
						window.removeEventListener("mouseup", mouseup);
						window.removeEventListener("keydown", keydown);
					},
					keydown = (e: KeyboardEvent) => {
						if (e.key === "Escape") {
							const x = dateToX(minX),
								y = sizeToY(maxY),
								width = dateToX(maxX) - x,
								height = sizeToY(minY) - y;

							setHighlightCoords([x - paddingXL, width, y - paddingYT, height]);
							setLimits(minY, maxY, minX, maxX);

							window.removeEventListener("mousemove", mousemove);
							window.removeEventListener("mouseup", mouseup);
							window.removeEventListener("keydown", keydown);
						}
					};

				if (startX < 0 || startX > graphWidth + 2 * innerPadding || startY < 0 || startY > graphHeight + 2 * innerPadding) {
					return;
				}

				window.addEventListener("mousemove", mousemove);
				window.addEventListener("mouseup", mouseup);
				window.addEventListener("keydown", keydown);
			},
			dataStr = JSON.stringify(data);

		useEffect(() => {
			const x = dateToX(minX),
				y = sizeToY(maxY),
				width = dateToX(maxX) - x,
				height = sizeToY(minY) - y;

			setHighlightCoords([x - paddingXL, width, y - paddingYT, height]);
		}, [minX, minY, maxX, maxY, width, height]);

		useEffect(() => {
			if (firstRender || restoring) {
				return;
			}

			setHighlightCoords(null);
			setLimits(-Infinity, Infinity, -Infinity, Infinity);
		}, [dataStr]);

		if (data.length === 0) {
			return <svg id="scatter" xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
				<rect width={width} height={height} stroke="currentColor" fill="none" />
				<text fill="currentColor" textAnchor="middle" x={width / 2} y={height / 2}>—No Data—</text>
			</svg>
		}

		let minSize = Infinity,
			maxSize = -Infinity,
			minDate = Infinity,
			maxDate = -Infinity;

		for (const d of data) {
			if (d.UsageSize < minSize) {
				minSize = d.UsageSize
			}

			if (d.UsageSize > maxSize) {
				maxSize = d.UsageSize;
			}

			const daysAgo = minDaysAgo(d.Mtime);

			if (daysAgo < minDate) {
				minDate = daysAgo;
			}

			if (daysAgo > maxDate) {
				maxDate = daysAgo;
			}
		}

		const orderMax = Math.pow(10, Math.max(Math.floor(Math.log10(maxDate)), 1)),
			orderMin = Math.pow(10, Math.max(Math.floor(Math.log10(minDate)), 1));

		minDate = orderMin * Math.floor(minDate / orderMin);
		maxDate = orderMax * Math.ceil(maxDate / orderMax);
		minSize = 100 * Math.pow(2, Math.floor(Math.log2(minSize / 100)));
		maxSize = 100 * Math.pow(2, Math.ceil(Math.log2(maxSize / 100)));

		const xScale = graphWidth / (maxDate - minDate),
			yScale = graphHeight / (maxSize - minSize);

		return <svg id="scatter" xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`} onMouseDown={onDrag}>
			<defs>
				<circle id="marker" r="2.5" />
			</defs>
			<rect rx={4} className="back" x={paddingXL} y={paddingYT} width={graphWidth + 2 * innerPadding} height={graphHeight + 2 * innerPadding} style={{ "fill": "var(--graphBack, #ddd)" }} stroke="currentColor" />
			{
				Array.from({ length: 6 }, (_, n) => <line key={`scatter_hl_${n}`} x1={dateToX(nonLogFractionToDate(0), false) - innerPadding} x2={dateToX(nonLogFractionToDate(1), false) + innerPadding} y1={sizeToY(nonLogFractionToSize(n / 5), false)} y2={sizeToY(nonLogFractionToSize(n / 5), false)} className="graphLines" />)
			}
			{
				Array.from({ length: 6 }, (_, n) => <text key={`scatter_ht_${n}`} x={dateToX(nonLogFractionToDate(0), false) - innerPadding - 5} y={Math.max(sizeToY(nonLogFractionToSize(n / 5), false), paddingYT) + 5} fill="currentColor" textAnchor="end">{formatBytes(fractionToSize(n / 5))}</text>)
			}
			{
				Array.from({ length: 6 }, (_, n) => <line key={`scatter_vl_${n}`} x1={dateToX(nonLogFractionToDate(n / 5), false)} x2={dateToX(nonLogFractionToDate(n / 5), false)} y1={sizeToY(nonLogFractionToSize(1), false) - innerPadding} y2={sizeToY(nonLogFractionToSize(0), false) + innerPadding} className="graphLines" />)
			}
			{
				Array.from({ length: 6 }, (_, n) => <text key={`scatter_vt_${n}`} x={-10} y={20} transform={`translate(${dateToX(nonLogFractionToDate(n / 5), false)} ${sizeToY(nonLogFractionToSize(0), false)}) rotate(-45)`} fill="currentColor" textAnchor="end">{formatNumber(fractionToDate(n / 5))}</text>)
			}
			{
				highlightCoords && highlightCoords.every(v => v !== -Infinity && v !== Infinity) ? <rect className="back" x={highlightCoords[0] + paddingXL} width={highlightCoords[1]} y={highlightCoords[2] + paddingYT} height={highlightCoords[3]} fill="#9cf" fillOpacity={0.25} stroke="#036" strokeOpacity={0.25} /> : []
			}
			{
				data.map((d, n) => <use key={`scatter_${d.UsageSize}_${d.Mtime}_${n}`} className={isSelected(d) ? "selected" : ""} href="#marker" x={dateToX(minDaysAgo(d.Mtime))} y={sizeToY(d.UsageSize)} onClick={e => {
					if (e.button !== 0) {
						return;
					}

					setLimits(d.UsageSize, d.UsageSize, asDaysAgo(d.Mtime), asDaysAgo(d.Mtime));
					setHighlightCoords(null);
				}} />)
			}
			<text x={paddingXL + (width - paddingXL - paddingXR) / 2} y={height - 5} textAnchor="middle" fill="currentColor">Last Modified (Days)</text>
		</svg>
	};

export default ScatterComponent;