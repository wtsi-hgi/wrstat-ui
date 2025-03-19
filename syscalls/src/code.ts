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

type syscalls = {
	time: number;
	opens: number;
	reads: number;
	bytes: number;
	closes: number;
	stats: number;
};

type EventSyscall = syscalls & {
	file: string;
	host: string;
};

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
	node.append(...[c as Element].flat(Infinity));

	return node;
      },
      clearNode = (node: Element, propertiesOrChildren: PropertiesOrChildren = {}, children?: Children) => amendNode((node.replaceChildren(), node), propertiesOrChildren, children),
      tags = <NS extends string>(ns: NS) => new Proxy({}, {"get": (_, element: string) => (props: PropertiesOrChildren = {}, children?: Children) => amendNode(document.createElementNS(ns, element), props, children) }) as NS extends "http://www.w3.org/1999/xhtml" ? {[K in keyof HTMLElementTagNameMap]: (props?: PropertiesOrChildren, children?: Children) => HTMLElementTagNameMap[K]} : NS extends "http://www.w3.org/2000/svg" ? {[K in keyof SVGElementTagNameMap]: (props?: PropertiesOrChildren, children?: Children) => SVGElementTagNameMap[K]} : Record<string, (props?: PropertiesOrChildren, children?: Children) => Element>,
      {br, details, div, h2, li, option, select, span, summary, ul} = tags("http://www.w3.org/1999/xhtml"),
      {polyline, svg} = tags("http://www.w3.org/2000/svg"),
      body = div(),
      formatTime = (time: number) => {
	const d = new Date(time * 1000);

	return d.toLocaleDateString() + " " + d.toLocaleTimeString()
      },
      setAndReturn = <K, V>(m: {set: (k: K, v: V) => void}, k: K, v: V) => {
	m.set(k, v);

	return v;
      },
      keys: (keyof syscalls)[] = ["opens", "reads", "closes", "stats", "bytes"],
      keyColours = ["#0f0", "#00f", "#f80", "#f0f", "#f00"],
      newSyscall = () => ({"time": 0, "opens": 0, "reads": 0, "bytes": 0, "closes": 0, "stats": 0}),
      maxY = 200,
      buildChart = (events: EventSyscall[]) => {
	const minutes = new Map<number, syscalls>(),
	      files = new Map<string, number>();

	for (const event of events) {
		const now = event.time / 60 | 0,
		      lastTime = files.get(event.file) ?? now - 1,
		      dt = now - lastTime;

		files.set(event.file, now);
		
		for (let i = lastTime + 1; i <= now; i++) {
			const min = minutes.get(i) ?? setAndReturn(minutes, i, newSyscall());

			for (const key of keys) {
				min[key] += (event[key] ?? 0) / dt;
			}
		}
	}

	const minTime = events[0].time / 60 | 0,
	      s = svg(),
	      lines = keys.map(() => [] as number[]),
	      maxes = keys.map(() => 0);

	
	for (const [, minute] of minutes) {
		for (const [n, key] of keys.entries()) {
			lines[n].push(minute[key]);
			maxes[n] = Math.max(maxes[n], minute[key]);
		}
	}

	const maxSyscalls = Math.max(...maxes.slice(0, 4));

	amendNode(s, {"viewBox": `0 0 ${(events.at(-1)!.time / 60 | 0) - minTime} ${maxY}`}, [
		lines.slice(0, 4).map((points, l) => polyline({"points": points.map((point, n) => `${n},${maxY - maxY * point / maxSyscalls}`).join(" "), "stroke": keyColours[l], "fill": "none"})),
		polyline({"points": lines[4].map((point, n) => `${n},${maxY - maxY * point / maxes[4]}`).join(" "), "stroke": keyColours[4], "fill": "none"})
	])

	return s;
      },
      showErr = (err: EventError) => li(`${formatTime(err.time)}, ${err.file} (${err.host}): ${err.message}`),
      display = (run: string, data: Data) => {
	let {opens, reads, bytes, closes, stats} = data.events.reduce((d, e) => (d.opens += e.opens ?? 0, d.reads += e.reads ?? 0, d.bytes += e.bytes ?? 0, d.closes += e.closes ?? 0, d.stats += e.stats ?? 0, d), newSyscall());

	clearNode(body, [
		h2(run),
		div({"class": "tabs"}, [
			details({"name": "tabs", "open": "open"}, [
				summary("Summary"),
				div([
					div("Run start time: " + formatTime(data.events.at(0)?.time ?? 0)),
					div("Run end time: " + formatTime(data.events.at(-1)?.time ?? 0)),
					div("Total Syscalls: " + (opens + reads + closes + stats).toLocaleString()),
					div("Total Opens: " + opens.toLocaleString()),
					div("Total Reads: " + reads.toLocaleString()),
					div("Total Bytes Read: " + bytes.toLocaleString()),
					div("Total Close: " + closes.toLocaleString()),
					div("Total Stats: " + stats.toLocaleString()),
					div("Total Errors: " + (data.errors?.length ?? 0).toLocaleString()),
				])
			]),
			data.errors?.length ?? 0 === 0 ? [] : details({"name": "tabs"}, [
				summary("Errors"),
				ul(data.errors.length > 7 ? [
					data.errors.slice(0, 3).map(err => showErr(err)),
					li({"class": "expandErrors", "click": function(this: HTMLLIElement) {
						this.replaceWith(...data.errors.slice(3, -3).map(err => showErr(err)));
					}}, "Click here to expand remaining errors"),
					data.errors.slice(-3).map(err => showErr(err))
				] : data.errors.map(err => showErr(err)))
			]),
			details({"name": "tabs"}, [
				summary("Charts"),
				div([
					select({"change": function(this: HTMLSelectElement) {
						(this.nextSibling as Element).replaceWith(buildChart(this.value === "..." ? data.events : data.events.filter(event => event.host === this.value)));
					}}, [
						option({"value": "..."}, "-- All Hosts --"),
						Array.from(data.events.reduce((hosts, event) => (hosts.add(event.host), hosts), new Set<string>())).map(host => option(host))
					]),
					buildChart(data.events),
					ul({"class": "graphKey"}, keys.map((key, n) => li([span({"style": "background-color: " + keyColours[n]}), span(key)])))
				])
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
			Object.entries(data).filter(([, e]) => e.events).map(([key, val]) => option({"click": () => display(key, val)}, (val.complete ? "✓ " : "❌ ") + key))
		]),
		body
	]);
});
