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
	syscalls: number;
	writes: number;
	writeBytes: number;
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
      {br, details, div, h2, input, label, li, optgroup, option, select, span, summary, ul} = tags("http://www.w3.org/1999/xhtml"),
      {g, line, polyline, svg, text} = tags("http://www.w3.org/2000/svg"),
      body = div(),
      formatTime = (time: number) => {
	const d = new Date(time * 1000);

	return d.toLocaleDateString() + " " + d.toLocaleTimeString()
      },
      keys: (keyof syscalls)[] = ["syscalls", "opens", "reads", "closes", "stats", "writes", "bytes", "writeBytes"],
      keyColours = ["#000", "#0f0", "#00f", "#f80", "#f0f", "#851", "#f00", "#f88"],
      newSyscall = (time = 0) => ({time, "opens": 0, "reads": 0, "bytes": 0, "closes": 0, "stats": 0, "syscalls": 0, "writes": 0, "writeBytes": 0}),
      range = function* <V>(start: number, stop: number, fn: (n: number) => V) {
	for (let i = start; i <= stop; i++) {
		yield fn(i);
	}
      },
      maxY = 200,
      buildMinuteByMinute = (events: EventSyscall[], startMinute: number, endMinute: number) => {
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
			      dt = now - lastTime,
			      v = event[key] / dt;

			for (const i of range(lastTime + 1, now, n => n)) {
				const m = minutes.get(i)!;

				m[key] += v;

				if (key.slice(-4) !== "ytes") {
					m.syscalls += v
				}
			}
		}
	}

	return minutes;
      },
      findDataMaxes = (events: EventSyscall[], startMinute: number, endMinute: number) => findMinuteMaxes(buildMinuteByMinute(events, startMinute, endMinute)),
      roundUpToTenth = (n: number) => {
	const tens = Math.pow(10, Math.floor(Math.log10(n)));

	return Math.ceil(10 * n / tens) * tens / 10 ;
      },
      findMinuteMaxes = (minutes: Map<number, syscalls>) => {
	const maxes: [number, number] = [0, 0];

	for (const [, minute] of minutes) {
		maxes[0] = Math.max(maxes[0], minute.syscalls);
		maxes[1] = Math.max(maxes[1], minute.bytes, minute.writeBytes);
	}

	maxes[0] = roundUpToTenth(maxes[0]);
	maxes[1] = roundUpToTenth(maxes[1]);

	return maxes;
      },
      prefixes = ["", "k","M","G","T","P","E","Z","R","Y","Q"],
      formatSI = (n: number) => {
	for (const prefix of prefixes) {
		if (n < 1000) {
			return n + prefix;
		}

		n = Math.floor(n / 100) / 10;
	}

	return "∞";
      },
      buildChart = (events: EventSyscall[], startMinute: number, endMinute: number, maxSyscalls: number, maxBytes: number, uniformY: boolean) => {
	const minutes = buildMinuteByMinute(events, startMinute, endMinute),
	      lines = keys.map(() => [] as [number, number][]);

	if (!uniformY) {
		[maxSyscalls, maxBytes] = findMinuteMaxes(minutes);
	}
	
	for (const [, minute] of minutes) {
		for (const [n, key] of keys.entries()) {
			lines[n].push([minute.time, minute[key]]);
		}
	}

	for (const line of lines) {
	      while (line.at(-1)?.[1] === 0) {
		      line.pop();
	      }
	}

	return svg({"viewBox": `0 0 ${endMinute - startMinute + 130} ${maxY + 110}`, "click": function(this:SVGSVGElement) {this.classList.toggle("zoom");}}, [
		g({"transform": "translate(0 10)"}, [
			g({"transform": "translate(50 5)"}, [
				lines.slice(6).map((points, l) => polyline({"points": points.map(([n, point]) => `${n - startMinute},${maxY - maxY * point / maxBytes}`).join(" "), "stroke": keyColours[6+l], "fill": "none", "class": keys[6+l]})),
				lines.slice(0, 6).map((points, l) => polyline({"points": points.map(([n, point]) => `${n - startMinute},${maxY - maxY * point / maxSyscalls}`).join(" "), "stroke": keyColours[l], "fill": "none", "class": keys[l]})),
				line({"y1": maxY + "", "y2": maxY + "", "x2": endMinute - startMinute + 10 + "", "stroke": "#000"}),
				Array.from(range(Math.ceil(startMinute / 60), Math.floor(endMinute / 60), n => text({"transform": `translate(${n * 60 - startMinute}, ${maxY + 5}) rotate(90) scale(0.5)`}, formatTime(n * 3600))))
			]),
			line({"x1": "50", "x2": "50", "y2": maxY+5+"", "stroke": "#000"}),
			Array.from(range(0, 5, n => text({"x": "40", "y": (5 - n) * 40 + 10 + "", "text-anchor": "end"}, formatSI(n * maxSyscalls / 5)))),
			line({"x1": endMinute - startMinute + 60 + "", "x2": endMinute - startMinute + 60 + "", "y2": maxY+5+"", "stroke": "#000"}),
			Array.from(range(0, 5, n => text({"x": endMinute - startMinute + 70 + "", "y": (5 - n) * 40 + 10 + ""}, (formatSI(n * maxBytes / 5) + "iB").replace(/([0-9])iB/, "$1B"))))
		])
	]);
      },
      showErr = (err: EventError) => li(`${formatTime(err.time)}, ${err.file} (${err.host}): ${err.message}`),
      display = (run: string, data: Data) => {
	let chart: SVGSVGElement,
	    uniformY = true,
	    hostData = data.events;

	const {opens, reads, bytes, closes, stats, writes, writeBytes} = data.events.reduce((d, e) => (d.opens += e.opens ?? 0, d.reads += e.reads ?? 0, d.bytes += e.bytes ?? 0, d.closes += e.closes ?? 0, d.stats += e.stats ?? 0, d.writes += e.writes ?? 0, d.writeBytes += e.writeBytes ?? 0, d), newSyscall()),
	      dataStart = data.events[0].time / 60 | 0,
	      dataEnd = data.events.at(-1)!.time / 60 | 0,
	      [maxSyscalls, maxBytes] = findDataMaxes(data.events, dataStart, dataEnd),
	      chartContent = div([
		select({"change": function(this: HTMLSelectElement) {
			chart.replaceWith(chart = buildChart(hostData = this.value === "..." ? data.events : data.events.filter(event => event.host === this.value), dataStart, dataEnd, maxSyscalls, maxBytes, uniformY));
		}}, [
			option({"value": "..."}, "-- All Hosts --"),
			Array.from(data.events.reduce((hosts, event) => (hosts.add(event.host), hosts), new Set<string>())).sort().map(host => option(host))
		]),
		br(),
		label({"for": "uniformY"}, "Uniform Y-axis: "),
		input({"id": "uniformY", "checked": "checked", "type": "checkbox", "click": function(this: HTMLInputElement) {
			chart.replaceWith(chart = buildChart(hostData, dataStart, dataEnd, maxSyscalls, maxBytes, uniformY = this.checked));
		}}),
		ul({"class": "graphKey"}, keys.map((key, n) => li([span({"style": "background-color: " + keyColours[n], "click": function(this: HTMLSpanElement) {
			this.setAttribute("style", chartContent.classList.toggle(keys[n]) ? "" : `background-color: ${keyColours[n]}`);
		}}), span(key.at(0)!.toUpperCase() + key.slice(1))]))),
		chart = buildChart(hostData, dataStart, dataEnd, maxSyscalls, maxBytes, uniformY)
	      ]);

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
					div("Total Writes: " + writes.toLocaleString()),
					div("Total Bytes Written: " + writeBytes.toLocaleString()),
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
				chartContent
			])
		])
	]);
      },
      reverse = <T>(arr: T[]) => {
	arr.reverse();

	return arr;
      },
      timeSort = (a: {time: number}, b: {time: number}) => a.time - b.time;

(document.readyState === "complete" ? Promise.resolve() : new Promise(successFn => window.addEventListener("load", successFn, {"once": true})))
.then(() => fetch("/data.json"))
.then(j => j.json())
.then((data: Record<string, Data>) => {
	amendNode(document.body, [
		select({"multiple": "multiple", "change": function(this: HTMLSelectElement) {
			const selected = Array.from(this.getElementsByTagName("option")).filter(e => e.selected).map(e => e.value);

			if (selected.length === 0) {
				body.replaceChildren();
			} else {
				display(selected.join(", "), selected.map(v => [v, data[v]] as [string, Data]).reduce((data, [runName, run]) => (data.events = data.events.concat((run?.events ?? []).map(e => (e.file += "/" + runName + "/", e))).sort(timeSort), data.errors = data.errors.concat(run?.errors ?? []).sort(timeSort), data.complete &&= run.complete, data), {"events": [], "errors": [], "complete": true} as Data));
			}
		}}, optgroup({"label": "Please select one or more runs"}, [
			reverse(Object.entries(data).filter(([, e]) => e?.events)).map(([key, val]) => option({"value": key}, (val.complete ? "✓ " : "❌ ") + key))
		])),
		body
	]);
});
