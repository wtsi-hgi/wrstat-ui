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

import type { CSSProperties } from "react";
import Pagination from "./Pagination";
import { useSavedState } from "./state";

type Column<T> = {
	[K in Extract<keyof T, string>]-?: {
		title: string;
		key: K;
		extra?: (data: T[K], row: T) => Record<string, any>;
		formatter?: (data: T[K], row: T) => JSX.Element | string;
		sortFn?: (a: T, b: T) => number;
		reverseFn?: (a: T, b: T) => number;
		startReverse?: boolean;
		sum?: (vals: T[K][]) => string;
	}
}[Extract<keyof T, string>];

export type Filter<T> = {
	[K in keyof T]?: ((col: T[K]) => boolean) | T[K][] | (T[K] extends number ? { min: number, max: number } : never);
}

type Params<T> = {
	table: T[];
	cols: Column<T>[];
	perPage?: number;
	filter?: Filter<T>;
	rowExtra?: (row: T) => Record<string, any>;
	onRowClick?: (row: T) => void;
	onRowHover?: (row: T) => void;
	onMouseOut?: (row: T) => void;
	className?: string;
	id: string;
	style?: CSSProperties;
	caption?: string;
}

export const fitlerTableRows = <T extends Record<string, any>>(table: T[], filter: Filter<T>) => {
	const toRet: T[] = [],
		filterKeys = Object.keys(filter) as (keyof Filter<T>)[];

	FilterLoop:
	for (const row of table) {
		for (const f of filterKeys) {
			const toFilter = filter[f];
			if (toFilter instanceof Array) {
				if (toFilter.length && !toFilter.includes(row[f])) {
					continue FilterLoop;
				}
			} else if (toFilter instanceof Function) {
				if (!toFilter(row[f])) {
					continue FilterLoop;
				}
			} else if (toFilter) {
				if (row[f] < toFilter.min || row[f] > toFilter.max) {
					continue FilterLoop;
				}
			}
		}

		toRet.push(row);
	}

	return toRet;
};

const noopFormatter = (a: { toString(): string }) => a + "",
	noopSort = () => 0,
	reverseSort = <T extends Record<string, any>,>(fn: (a: T, b: T) => number) => (a: T, b: T) => fn(b, a),
	getSorter = <T extends Record<string, any>>(col: Column<T>, reverse = false) => {
		if (!reverse) {
			return col["sortFn"] ?? noopSort;
		}

		return col["reverseFn"] ?? reverseSort(col["sortFn"] ?? noopSort);
	},
	makeRows = <T extends Record<string, any>>(rows: T[], cols: Column<T>[], onRowClick?: (row: T) => void, onRowHover?: (row: T) => void, onMouseOut?: (row: T) => void, rowExtra?: (row: T) => Record<string, any>) => {
		return rows.map((row, n) => <tr
			key={`table_row_${n}`}
			role="button"
			onKeyDown={e => {
				if (e.key === "Enter") {
					onRowClick?.(row);
				}
			}}
			onClick={e => {
				if (e.button !== 0) {
					return;
				}

				onRowClick?.(row);
			}}
			onMouseOver={() => {
				onRowHover?.(row);
			}}
			onMouseOut={() => {
				onMouseOut?.(row);
			} }
			{...(rowExtra?.(row) ?? {})}
		>
			{cols.map((col, m) => <td
				key={`table_cell_${n}_${m}`}
				aria-label={col.title}
				{...(col.extra?.(row[col.key], row) ?? {})}
			> {(col.formatter ?? noopFormatter)(row[col.key], row)}</td>)
			}
		</tr>);
	},
	TableComponent = <T extends Record<string, any>>({
		table,
		cols,
		onRowClick,
		onRowHover,
		onMouseOut,
		perPage = Infinity,
		filter = {},
		id,
		rowExtra,
		caption,
		...additional
	}: Params<T>) => {
		const [page, setPage] = useSavedState(id + "Page", 0),
			[sortBy, setSortBy] = useSavedState(id + "Sort", -1),
			[sortReverse, setSortReverse] = useSavedState(id + "Reverse", false),
			rows = fitlerTableRows(table, filter),
			maxPages = Math.ceil(rows.length / perPage),
			currPage = perPage === Infinity ? 0 : Math.min(page, maxPages - 1);

		if (sortBy >= 0) {
			rows.sort(getSorter(cols[sortBy], sortReverse));
		}

		const pagedRows = rows.slice(currPage * perPage, (currPage + 1) * perPage);

		return <>
			<Pagination currentPage={currPage} onClick={setPage} totalPages={maxPages} />
			<table aria-label={caption} tabIndex={0} id={id} {...additional}>
				<colgroup>
					{cols.map((_, n) => <col key={`table_${id}_col_${n}`} />)}
				</colgroup>
				<thead>
					<tr>
						{cols.map((c, n) => <th
							key={`table_${id}_th_${n}`}
							scope="col"
							role="button"
							tabIndex={0}
							aria-sort={sortBy === n ? sortReverse ? "descending" : "ascending" : undefined}
							className={c.sortFn ? "sortable" + (sortBy === n ? " sort" + (sortReverse ? " reverse" : "") : "") : ""}
							onKeyPress={e => {
								if (e.key === "Enter") {
									(e.target as HTMLElement)?.click();
								}
							}}
							onClick={c.sortFn ? e => {
								if (e.button !== 0) {
									return;
								}

								if (sortBy === n) {
									setSortReverse(!sortReverse);
								} else {
									setSortBy(n);
									setSortReverse(cols[n].startReverse ?? false);
								}
							} : undefined}
						>{c.title}</th>)}
					</tr>
				</thead>
				<tbody>
					{makeRows(pagedRows, cols, onRowClick, onRowHover, onMouseOut, rowExtra)}
				</tbody>
				{
					cols.some(c => c.sum) ?
						<tfoot>
							<tr>
								{cols.map((c, n) => <td key={`total_${n}`} aria-label={c.sum && `Total ${c.title}`}>{
									c.sum ?
										c.sum(pagedRows.map(row => row[c.key])) :
										<></>
								}</td>)}
							</tr>
						</tfoot>
						:
						<></>
				}
			</table>
		</>;
	};

export default TableComponent;