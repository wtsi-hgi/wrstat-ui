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

import 'jest-canvas-mock';
import { render } from '@testing-library/react';
import TreeMap, { type Table } from './Treemap';

const testData: Table = [
	{
		name: "A",
		value: 60,
		noauth: false
	},
	{
		name: "B",
		value: 15,
		noauth: false
	},
	{
		name: "C",
		value: 15,
		noauth: false
	},
	{
		name: "D",
		value: 10,
		noauth: false
	},
	{
		name: "E",
		value: 0,
		noauth: false
	},
];

type Box = {
	top: number;
	left: number;
	bottom: number;
	right: number;
}

it("TreeMap creates boxes with the correct area", () => {
	const { asFragment } = render(<TreeMap width={10} height={10} table={testData} />),
		expectedAreas = new Map<string, number>(testData.map(({ name, value }) => [name, value])),
		boxesGot: Box[] = [],
		overlapsExisting = (box: Box) => {
			let overlap = false;

			for (const existing of boxesGot) {
				if (box.left < existing.right && box.right > existing.left &&
					box.top < existing.bottom && box.bottom > existing.top
				) {
					overlap = true;

					break;
				}
			}

			boxesGot.push(box);

			return overlap;
		};

	for (const svg of asFragment().childNodes) {
		if (svg.nodeName === "svg") {
			for (const child of svg.childNodes) {
				if (child.nodeName === "rect") {
					const left = parseFloat((child as SVGRectElement).getAttribute("x") || "0"),
						top = parseFloat((child as SVGRectElement).getAttribute("y") || "0"),
						width = parseFloat((child as SVGRectElement).getAttribute("width") || "0"),
						height = parseFloat((child as SVGRectElement).getAttribute("height") || "0"),
						bottom = top + height,
						right = left + width,
						area = width * height,
						expectedArea = expectedAreas.get(child.textContent ?? "");

					expect(area).toEqual(expectedArea);
					expect(left).toBeGreaterThanOrEqual(0);
					expect(top).toBeGreaterThanOrEqual(0);
					expect(right).toBeLessThanOrEqual(10);
					expect(bottom).toBeLessThanOrEqual(10);

					expect(overlapsExisting({
						top,
						left,
						bottom,
						right
					})).toEqual(false);
				}
			}
		}
	}

	expect(boxesGot.length).toEqual(4);
});