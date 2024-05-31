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

import type { SubDir } from "./rpc";
import { useEffect, useState } from "react";
import Table from './Table';
import { asTimeAgo, formatBytes, formatNumber } from './format';
import RPC from './rpc';

type SubDirParams = {
	id: number;
	path: string;
	isUser: boolean;
	treePath: string;
	justDisktree: boolean;
	setTreePath: (path: string) => void;
}

const pathJoin = (base: string, sub: string) => sub === "." ? base : base + "/" + sub,
	fileTypes = [
		"other",
		"temp",
		"vcf",
		"vcf.gz",
		"bcf",
		"sam",
		"bam",
		"cram",
		"fasta",
		"fastq",
		"fastq.gz",
		"ped/bed",
		"compressed",
		"text",
		"log",
		"dir"
	] as const,
	stringSort = new Intl.Collator().compare,
	sortPath = (a: SubDir, b: SubDir) => stringSort(a.SubDir, b.SubDir),
	sortNumFiles = (a: SubDir, b: SubDir) => a.NumFiles - b.NumFiles,
	sortSizeFiles = (a: SubDir, b: SubDir) => a.SizeFiles - b.SizeFiles,
	sortLastModifed = (a: SubDir, b: SubDir) => new Date(b.LastModified).valueOf() - new Date(a.LastModified).valueOf(),
	SubdirsComponent = ({ id, path, isUser, treePath, setTreePath, justDisktree }: SubDirParams) => {
		const [subdirs, setSubdirs] = useState<SubDir[]>([]);

		useEffect(() => {
			if (id === -1) {
				setSubdirs([]);

				return;
			}

			(isUser ? RPC.getBasedirsUserSubdirs : RPC.getBasedirsGroupSubdirs)(id, path)
				.then(setSubdirs);
		}, [id, path, isUser]);

		if (!subdirs || subdirs.length === 0) {
			return <></>;
		}

		return <details open id="subdirs" style={justDisktree ? { display: "none" } : undefined}>
			<summary><h1>Sub-Directories</h1></summary>
			<Table
				id="subDirsTable"
				table={subdirs}
				className="prettyTable"
				onRowClick={(row: SubDir) => setTreePath(pathJoin(path, row.SubDir))}
				caption="Sub Directories"
				rowExtra={row => ({ "className": pathJoin(path, row.SubDir) === treePath ? "selected" : undefined })}
				cols={[
					{
						title: "Path",
						key: "SubDir",
						sortFn: sortPath,
					},
					{
						title: "Number of Files",
						key: "NumFiles",
						sortFn: sortNumFiles,
						startReverse: true,
						formatter: formatNumber
					},
					{
						title: "Size",
						key: "SizeFiles",
						extra: size => ({ title: formatNumber(size) + " Bytes" }),
						sortFn: sortSizeFiles,
						startReverse: true,
						formatter: formatBytes
					},
					{
						title: "Last Modified",
						key: "LastModified",
						extra: title => ({ title }),
						sortFn: sortLastModifed,
						startReverse: true,
						formatter: asTimeAgo
					},
					{
						title: "File Usage",
						key: "FileUsage",
						formatter: (files: Record<number, number>) => Object.entries(files).sort((a, b) => b[1] - a[1]).map(e => `${fileTypes[parseInt(e[0])]}: ${formatBytes(e[1])}`).join(", ")
					}
				]} />
		</details>;
	};

export default SubdirsComponent;