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

type TabData = {
	title: string;
	onClick: (title: string) => void;
	selected?: boolean;
}

const Tabs = ({ id, tabs }: { id: string, tabs: TabData[] }) => <ul id={id} className="tabs">
	{tabs.map((tab, n) => <li
		tabIndex={0}
		key={`tab_${id}_${n}`}
		role="tab"
		aria-selected={tab.selected ? "true" : "false"}
		className={tab.selected ? "selected" : ""}
		onClick={tab.selected ? undefined : e => {
			if (e.button !== 0) {
				return;
			}

			tab.onClick(tab.title);
		}}
		onKeyDown={tab.selected ? undefined : e => {
			if (e.key === "Enter") {
				tab.onClick(tab.title);
			}
		}}
	>{tab.title}</li>)}
</ul>

export default Tabs;