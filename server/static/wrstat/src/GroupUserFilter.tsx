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

import MultiSelect from "./MultiSelect";

export type GroupUserFilterParams = {
	areas: Record<string, string[]>;
	users: number[];
	setUsers: (v: number[]) => void;
	groups: number[];
	setGroups: (v: number[]) => void;
	groupNameToIDMap: Map<string, number>;
	groupIDToNameMap: Map<number, string>;
	userNameToIDMap: Map<string, number>;
	userIDToNameMap: Map<number, string>;
}

const stringSort = new Intl.Collator().compare,
	GroupUserFilter = ({
		areas,
		groupNameToIDMap,
		groupIDToNameMap,
		userIDToNameMap,
		userNameToIDMap,
		users,
		setUsers,
		groups,
		setGroups,
		num
	}: GroupUserFilterParams & { num: number }) => {
		const selectedGroups = groups.map(gid => groupIDToNameMap.get(gid) ?? "").sort(stringSort).filter(g => g),
			selectedUsers = users.map(uid => userIDToNameMap.get(uid) ?? "").sort(stringSort).filter(u => u),
			selectedBOMs = Object.entries(areas).map(([bom, groups]) => groups.every(g => groupNameToIDMap.get(g) === undefined || selectedGroups.includes(g)) ? bom : "").filter(b => b).sort(stringSort);

		return <>
			<label htmlFor={`bom_${num}`}>Group Areas</label>
			<MultiSelect
				id={`bom_${num}`}
				list={Object.keys(areas).sort(stringSort)}
				selected={selectedBOMs}
				onchange={(boms, deleted) => {
					const existingGroups = new Set(groups);

					for (const bom of deleted ? [deleted] : boms) {
						for (const g of areas[bom] ?? []) {
							const id = groupNameToIDMap.get(g);
							if (id !== undefined) {
								existingGroups[deleted ? "delete" : "add"](id);
							}
						}
					}

					setGroups(Array.from(existingGroups));

					return false;
				}} />
			<label htmlFor={`unix_${num}`}>Unix Group</label >
			<MultiSelect
				id={`unix_${num}`}
				list={Array.from(new Set(groupNameToIDMap.keys())).sort(stringSort)}
				selected={selectedGroups}
				onchange={groups => setGroups(groups.map(groupname => groupNameToIDMap.get(groupname) ?? -1))} />
			<label htmlFor={`username_${num}`}>Username</label>
			<MultiSelect
				id={`username_${num}`}
				list={Array.from(new Set(userNameToIDMap.keys())).sort(stringSort)}
				selected={selectedUsers}
				onchange={users => setUsers(users.map(username => userNameToIDMap.get(username) ?? -1))} />
		</>;
	};

export default GroupUserFilter;