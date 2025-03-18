/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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

type Children = string | Element | DocumentFragment | Children[];

type Properties = Record<string, string | Function>;

type PropertiesOrChildren = Properties | Children;

type EventSyscall = {
	time: number;
	file: string;
	host: string;
	opens: number;
	reads: number;
	bytes: number;
	closes: number;
	stats: number;
}

type EventError = {
	time: number;
	file: string;
	host: string;
	message: string;
	data?: Record<string, string>;
};

type Data = {
	events: EventSyscall[];
	errors: EventError[];
	complete: boolean;
};

let id = 0;

const amendNode = (node: Element, propertiesOrChildren: PropertiesOrChildren, children?: Children) => {
	const [p, c] = typeof propertiesOrChildren === "string" || propertiesOrChildren instanceof Node || propertiesOrChildren instanceof Array ? [{}, propertiesOrChildren] : [propertiesOrChildren, children ?? []];

	Object.entries(p).forEach(([key, value]) => node[value instanceof Function ? "addEventListener" : "setAttribute"](key, value as any));
	node.append(...[c as any].flat(Infinity));

	return node;
      },
      clearNode = (node: Element, propertiesOrChildren: PropertiesOrChildren = {}, children?: Children) => amendNode((node.replaceChildren(), node), propertiesOrChildren, children),
      {br, details, div, h2, li, option, select, span, summary, ul} = new Proxy({}, { "get": (_, element: keyof HTMLElementTagNameMap) => (props: PropertiesOrChildren = {}, children?: Children) => amendNode(document.createElementNS("http://www.w3.org/1999/xhtml", element), props, children) }) as { [K in keyof HTMLElementTagNameMap]: (props?: PropertiesOrChildren, children?: Children) => HTMLElementTagNameMap[K] },
      body = div(),
      formatTime = (time: number) => {
	      const d = new Date(time * 1000);

	      return d.toLocaleDateString() + " " + d.toLocaleTimeString()
      },
      showErr = (err: EventError) => li(`${formatTime(err.time)}, ${err.file} (${err.host}): ${err.message}`),
      display = (run: string, data: Data) => {
	let [opens, reads, bytes, closes, stats] = data.events.reduce((d, e) => (d[0] += e.opens ?? 0, d[1] += e.reads ?? 0, d[2] += e.bytes ?? 0, d[3] += e.closes ?? 0, d[4] += e.stats ?? 0, d), [0, 0, 0, 0, 0]);

	clearNode(body, [
		h2(run),
		div({"class": "tabs"}, [
			details({"name": "tabs", "open": "open"}, [
				summary("Details"),
				div([
					div("Run start time: " + formatTime(data.events.at(0)?.time ?? 0)),
					div("Run end time: " + formatTime(data.events.at(-1)?.time ?? 0)),
					div("Total Syscalls: " + (opens + reads + closes + stats).toLocaleString()),
					div("Total Opens: " + opens.toLocaleString()),
					div("Total Reads: " + reads.toLocaleString()),
					div("Total Bytes Read: " + bytes.toLocaleString()),
					div("Total Close: " + closes.toLocaleString()),
					div("Total Stats: " + stats.toLocaleString()),
				])
			]),
			data.errors.length === 0 ? [] : details({"name": "tabs"}, [
				summary("Errors"),
				ul(data.errors.length > 7 ? [
					data.errors.slice(0, 3).map(err => showErr(err)),
					li({"class": "expandErrors", "click": function(this: HTMLLIElement) {
						this.replaceWith(...data.errors.slice(3, -3).map(err => showErr(err)));
					}}, "Click here to expand remaining errors"),
					data.errors.slice(-3).map(err => showErr(err))
				] : data.errors.map(err => showErr(err)))
			])
		])
	]);
      };

(document.readyState === "complete" ? Promise.resolve() : new Promise(successFn => window.addEventListener("load", successFn, {"once": true})))
.then(() => fetch("/data.json"))
.then(j => j.json())
.then((data: Record<string, Data>) => {
	amendNode(document.body, [
			select([
			option({"click": () => body.replaceChildren()}, "-- Please select a run --"),
			Object.keys(data).map(val => option({"click": () => display(val, data[val])}, (data[val].complete ? "✓ " : "❌ ") + val))
		]),
		body
	]);
});
