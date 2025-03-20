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

const amendNode = (node: Element, propertiesOrChildren: PropertiesOrChildren, children?: Children) => {
	const [p, c] = typeof propertiesOrChildren === "string" || propertiesOrChildren instanceof Node || propertiesOrChildren instanceof Array ? [{}, propertiesOrChildren] : [propertiesOrChildren, children ?? []];

	Object.entries(p).forEach(([key, value]) => node[value instanceof Function ? "addEventListener" : "setAttribute"](key, value as any));
	node.append(...[c as Element].flat(Infinity));

	return node;
      },
      clearNode = (node: Element, propertiesOrChildren: PropertiesOrChildren = {}, children?: Children) => amendNode((node.replaceChildren(), node), propertiesOrChildren, children),
      tags = <NS extends string>(ns: NS) => new Proxy({}, {"get": (_, element: string) => (props: PropertiesOrChildren = {}, children?: Children) => amendNode(document.createElementNS(ns, element), props, children) }) as NS extends "http://www.w3.org/1999/xhtml" ? {[K in keyof HTMLElementTagNameMap]: (props?: PropertiesOrChildren, children?: Children) => HTMLElementTagNameMap[K]} : NS extends "http://www.w3.org/2000/svg" ? {[K in keyof SVGElementTagNameMap]: (props?: PropertiesOrChildren, children?: Children) => SVGElementTagNameMap[K]} : Record<string, (props?: PropertiesOrChildren, children?: Children) => Element>,
      {br, details, div, h2, li, option, select, span, summary, ul} = tags("http://www.w3.org/1999/xhtml"),
      {g, line, polyline, svg, text} = tags("http://www.w3.org/2000/svg"),
      body = div(),
      formatTime = (time: number) => {
	const d = new Date(time * 1000);

	return d.toLocaleDateString() + " " + d.toLocaleTimeString()
      },
      keys: (keyof syscalls)[] = ["opens", "reads", "closes", "stats", "bytes"],
      keyColours = ["#0f0", "#00f", "#f80", "#f0f", "#f00"],
      newSyscall = (time = 0) => ({time, "opens": 0, "reads": 0, "bytes": 0, "closes": 0, "stats": 0}),
      range = function* <V>(start: number, stop: number, fn: (n: number) => V) {
	for (let i = start; i <= stop; i++) {
		yield fn(i);
	}
      },
      maxY = 200,
      buildChart = (events: EventSyscall[], startMinute: number, endMinute: number) => {
	const minutes = new Map(range(startMinute, endMinute,  n => [n, newSyscall(n)])),
	      files = new Map<string, syscalls>();

	for (const event of events) {
		const now = event.time / 60 | 0,
		      last = files.get(event.file) ?? newSyscall(event.time - 60);

		files.set(event.file, event);

		for (const key of keys) {
			if (!event[key]) {
				continue;
			}

			const lastTime = last[key] === 0 ? now - 1 : last.time / 60 | 0,
			      dt = now - lastTime;

			for (const i of range(lastTime + 1, now, n => n)) {
				minutes.get(i)![key] += (event[key] ?? 0) / dt;
			}
		}
	}

	const lines = keys.map(() => [] as [number, number][]),
	      maxes = keys.map(() => 0);
	
	for (const [, minute] of minutes) {
		for (const [n, key] of keys.entries()) {
			if (minute[key] === 0 && (lines[n].at(-1)?.[1] ?? 0) === 0) {
				continue
			}

			lines[n].push([minute.time, minute[key]]);
			maxes[n] = Math.max(maxes[n], minute[key]);
		}
	}

	for (const line of lines) {
	      while (line.at(-1)?.[1] === 0) {
		      line.pop();
	      }
	}

	const maxSyscalls = Math.max(...maxes.slice(0, 4));

	return svg({"viewBox": `0 0 ${endMinute - startMinute + 20} ${maxY + 100}`}, [
		g({"transform": "translate(10 5)"}, [
			lines.slice(0, 4).map((points, l) => polyline({"points": points.map(([n, point]) => `${n - startMinute},${maxY - maxY * point / maxSyscalls}`).join(" "), "stroke": keyColours[l], "fill": "none"})),
			polyline({"points": lines[4].map(([n, point]) => `${n - startMinute},${maxY - maxY * point / maxes[4]}`).join(" "), "stroke": keyColours[4], "fill": "none"}),
			line({"y1": maxY + "", "y2": maxY + "", "x2": "100%", "stroke": "#000"}),
			Array.from(range(Math.ceil(startMinute / 60), Math.floor(endMinute / 60), n => text({"transform": `translate(${n * 60 - startMinute}, ${maxY + 5}) rotate(90) scale(0.5)`}, formatTime(n * 3600))))
		])
	]);
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
			(data.errors?.length ?? 0) === 0 ? [] : details({"name": "tabs"}, [
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
						(this.nextSibling as Element).replaceWith(buildChart(this.value === "..." ? data.events : data.events.filter(event => event.host === this.value), data.events[0].time / 60 | 0, data.events.at(-1)!.time / 60 | 0));
					}}, [
						option({"value": "..."}, "-- All Hosts --"),
						Array.from(data.events.reduce((hosts, event) => (hosts.add(event.host), hosts), new Set<string>())).map(host => option(host))
					]),
					buildChart(data.events, data.events[0].time / 60 | 0, data.events.at(-1)!.time / 60 | 0),
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
		select({"change": function(this: HTMLSelectElement) {
			if (!this.value) {
				body.replaceChildren();
			} else {
				display(this.value, data[this.value]);
			}
		}}, [
			option({"value": ""}, "-- Please select a run --"),
			Object.entries(data).filter(([, e]) => e?.events).map(([key, val]) => option({"value": key}, (val.complete ? "✓ " : "❌ ") + key))
		]),
		body
	]);
});
