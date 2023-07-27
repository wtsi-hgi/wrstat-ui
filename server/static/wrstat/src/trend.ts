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

import type { History } from './rpc';

const calculateTrend = (max: number, latestTime: number, oldestTime: number, latestValue: number, oldestValue: number) => {
	if (latestValue >= max) {
		return 0;
	}

	if (latestTime === oldestTime || latestValue <= oldestValue) {
		return Infinity;
	}

	const dt = latestTime - oldestTime,
		dy = latestValue - oldestValue,
		c = latestValue - latestTime * dy / dt,
		secs = (max - c) * dt / dy;

	return secs;
},
	day = 86_400;

export const exceedDates = (history: History[]): [number, number] => {
	if (history.length === 0) {
		return [0, 0];
	}

	const latest = history[history.length - 1],
		oldest = history[Math.max(history.length - 3, 0)],
		latestDate = new Date(latest.Date).valueOf() / 1000,
		oldestDate = new Date(oldest.Date).valueOf() / 1000,
		untilSize = calculateTrend(latest.QuotaSize, latestDate, oldestDate, latest.UsageSize, oldest.UsageSize),
		untilInodes = calculateTrend(latest.QuotaInodes, latestDate, oldestDate, latest.UsageInodes, oldest.UsageInodes);

	return [Math.round(untilSize / day), Math.round(untilInodes / day)];
};