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

import type { KeyboardEvent } from "react";
import { useRef, useState } from "react";

type MultiSelectParams = {
	id: string;
	disabled?: boolean;
	list: readonly string[];
	selected: readonly string[];
	onchange: (list: string[], deleted: string | null) => void;
}

type ItemParams = {
	item: string;
	selectedSet: Set<string>;
	onchange: (list: string[], deleted: string | null) => (void | boolean);
}

type SelectItemParams = ItemParams & {
	keypress: (e: KeyboardEvent) => void;
	disabled: boolean;
}

const SelectItem = ({ item, selectedSet, disabled, onchange, keypress }: SelectItemParams) => <li>
	<label>
		<input tabIndex={-1} type="checkbox" disabled={disabled} checked={selectedSet.has(item)} onChange={() => {
			let deleted: null | string = null;

			if (selectedSet.has(item)) {
				selectedSet.delete(item);
				deleted = item;
			} else {
				selectedSet.add(item);
			}

			const selected = Array.from(selectedSet);

			onchange(selected, deleted);
		}} onKeyDown={keypress} />{item}</label>
</li>,
	RemoveItem = ({ item, selectedSet, onchange }: ItemParams) => <li>
		<button tabIndex={-1} title={`Deselect ${item}`} onClick={e => {
			if (e.button !== 0) {
				return;
			}

			selectedSet.delete(item);

			const selected = Array.from(selectedSet);

			onchange(selected, item);
		}}>{item}</button>
	</li>,
	MultiSelectComponent = ({
		id,
		list,
		selected,
		disabled = false,
		onchange
	}: MultiSelectParams) => {

		const filterRef = useRef<HTMLInputElement>(null),
			[filter, setFilter] = useState(""),
			selectedSet = new Set(selected),
			filteredList = list.filter(e => e.toLowerCase().includes(filter.toLowerCase())),
			keypress = (e: KeyboardEvent) => {
				if (e.key === "Tab") {
					const elmPP = (e.target as HTMLInputElement)?.parentElement?.parentElement,
						elm = elmPP?.[e.shiftKey ? "previousElementSibling" : "nextElementSibling"]?.firstElementChild?.firstElementChild ??
							elmPP?.parentElement?.parentElement?.firstElementChild;

					if (elm instanceof HTMLInputElement) {
						elm.focus();

						e.preventDefault();
					}
				}
			};

		return <div className="multiSelect" id={`multi_${id}`}>
			<ul>
				<li>
					<button
						id={id}
						aria-haspopup="listbox"
						aria-label="Select/Deselect"
						disabled={disabled}
						onClick={e => {
							if (e.button !== 0) {
								return;
							}

							filterRef.current?.focus();
						}}
					>+</button>
				</li>
				{disabled ? <></> : Array.from(selected).map(item => <RemoveItem
					key={`ms_${id}_s_${item}`}
					item={item}
					selectedSet={selectedSet}
					onchange={onchange}
				/>)}
			</ul>
			<div>
				<div>
					<input
						ref={filterRef}
						placeholder="Filter"
						disabled={disabled}
						aria-label="Filter List"
						tabIndex={-1}
						value={filter}
						onKeyDown={e => {
							if (e.key === "Escape") {
								(e.target as HTMLInputElement).blur();
							} else if (e.key === "Enter") {
								if (filteredList.length === 1) {
									selectedSet.add(filteredList[0]);

									onchange(Array.from(selectedSet), null);
								} else if (filteredList.includes(filter)) {
									selectedSet.add(filter);

									onchange(Array.from(selectedSet), null);
								}
							} else if (e.key === "Tab") {
								e.preventDefault();

								const input = (e.target as HTMLElement)?.nextElementSibling?.[e.shiftKey ? "lastElementChild" : "firstElementChild"]?.firstElementChild?.firstElementChild;

								if (input instanceof HTMLInputElement) {
									input.focus();
								}
							}
						}}
						onChange={e => setFilter(e.target.value)}
					/>
					<ul tabIndex={-1} onKeyDown={e => {
						if (e.key === "Escape") {
							(e.target as HTMLInputElement).blur();
						}
					}}>
						{
							filteredList.map(item => <SelectItem
								key={`ms_${id}_${item}`}
								item={item}
								disabled={disabled}
								selectedSet={selectedSet}
								onchange={onchange}
								keypress={keypress}
							/>)
						}
					</ul>
				</div>
			</div>
		</div>;
	};

export default MultiSelectComponent;