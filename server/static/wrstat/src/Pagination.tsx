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

type PaginationParams = {
	totalPages: number;
	currentPage: number;
	onClick: (page: number) => void;
}

const paginationEnd = 3,
	paginationSurround = 3,
	processPaginationSection = (ret: JSX.Element[], currPage: number, from: number, to: number, onClick: (page: number) => void) => {
		if (ret.length !== 0) {
			ret.push(<li key={`pagination_gap_${from}`}>…</li>);
		}

		for (let p = from; p <= to; p++) {
			const page = p;

			ret.push(<li
				key={`pagination_page_${p}`}
				tabIndex={0}
				role={currPage === p ? undefined : "button"}
				aria-label={currPage === p ? "Current Table Page" : `Go to Table Page ${p + 1}`}
				aria-current={currPage === p ? "page" : undefined}
				className={currPage === p ? "pagination_selected" : "pagination_link"}
				onKeyPress={currPage === p ? undefined : e => {
					if (e.key === "Enter") {
						onClick(page);
					}
				}}
				onClick={currPage === p ? undefined : e => {
					if (e.button !== 0) {
						return;
					}

					onClick(page);
				}}
			>{p + 1}</li>);
		}
	},
	PaginationComponent = ({ totalPages, currentPage, onClick }: PaginationParams) => {
		const ret: JSX.Element[] = [],
			lastPage = totalPages - 1;

		if (lastPage < 1) {
			return <></>
		}

		if (currentPage > lastPage) {
			currentPage = lastPage;
		}

		let start = 0;

		for (let page = 0; page <= lastPage; page++) {
			if (!(page < paginationEnd || page > lastPage - paginationEnd ||
				((paginationSurround > currentPage ||
					page >= currentPage - paginationSurround) && page <= currentPage + paginationSurround) ||
				(paginationEnd > 0 && ((currentPage - paginationSurround - 1 === paginationEnd && page === paginationEnd) ||
					(currentPage + paginationSurround + 1 === lastPage - paginationEnd && page === lastPage - paginationEnd))))) {
				if (page !== start) {
					processPaginationSection(ret, currentPage, start, page - 1, onClick);
				}
				start = page + 1
			}
		}

		if (start < lastPage) {
			processPaginationSection(ret, currentPage, start, lastPage, onClick);
		}

		return <ul className="pagination">
			<li
				key={`pagination_prev`}
				tabIndex={0}
				aria-label={`Go to Previous Table Page`}
				className={"pagination_prev" + (currentPage === 0 ? "" : " pagination_link")}
				onKeyPress={currentPage === 0 ? undefined : e => {
					if (e.key === "Enter") {
						onClick(currentPage - 1);
					}
				}}
				onClick={currentPage === 0 ? undefined : e => {
					if (e.button !== 0) {
						return;
					}

					onClick(currentPage - 1);
				}}
			>Previous</li>
			{ret}
			<li
				key={`pagination_next`}
				tabIndex={0}
				aria-label={`Go to Next Table Page`}
				className={"pagination_next" + (currentPage === lastPage ? "" : " pagination_link")}
				onKeyPress={currentPage === lastPage ? undefined : e => {
					if (e.key === "Enter") {
						onClick(currentPage + 1);
					}
				}}
				onClick={currentPage === lastPage ? undefined : e => {
					if (e.button !== 0) {
						return;
					}

					onClick(currentPage + 1);
				}}
			>Next</li>
		</ul >
	};

export default PaginationComponent;