type Children = string | Element | DocumentFragment | Children[];

type Properties = Record<string, string | Function>;

type PropertiesOrChildren = Properties | Children;

type Summary = {
	filters: Record<string, number>;
}

type TopSummary = Summary & {
	Users: string[];
}

type UserSummary = Summary & {
	sessions: UserSession[];
}

type UserSession = {
	id: string;
	start: number;
	end: number;
}

type UserEvent = {
	data: string;
	time: number;
}

const amendNode = (node: Element, propertiesOrChildren: PropertiesOrChildren, children?: Children) => {
	const [p, c] = typeof propertiesOrChildren === "string" || propertiesOrChildren instanceof Node || propertiesOrChildren instanceof Array ? [{}, propertiesOrChildren] : [propertiesOrChildren, children ?? []];

	Object.entries(p).forEach(([key, value]) => node[value instanceof Function ? "addEventListener" : "setAttribute"](key, value as any));

	node.append(...[c as any].flat(Infinity));

	return node;
      },
      clearNode = (node: Element, propertiesOrChildren: PropertiesOrChildren = {}, children?: Children) => amendNode((node.replaceChildren(), node), propertiesOrChildren, children),
      {br, button, div, label, input} = new Proxy({}, {"get": (_, element: keyof HTMLElementTagNameMap) => (props: PropertiesOrChildren = {}, children?: Children) => amendNode(document.createElementNS("http://www.w3.org/1999/xhtml", element), props, children)}) as {[K in keyof HTMLElementTagNameMap]: (props?: PropertiesOrChildren, children?: Children) => HTMLElementTagNameMap[K]},
      rpc = (() => {
	const base = "/",
	      getData = <T>(url: string, body: string) => fetch(base + url, {"method": "POST", body}).then(j => j.json() as T);
	
	return {
		"getSummary": (startTime: number, endTime: number) => getData<TopSummary>("summary", JSON.stringify({startTime, endTime})),
		"getUser": (username: string, startTime: number, endTime: number) => getData<UserSummary>("user", JSON.stringify({username, startTime, endTime})),
		"getSession": (username: string, session: string) => getData<UserEvent[]>("session", JSON.stringify({username, session}))
	};
      })(),
      yesterday = (() => {
	const d = new Date();

	d.setDate(d.getDate() - 1);

	return d.toISOString().split("T")[0];
      })(),
      startTime = input({"id": "startTime", "type": "date", "value": yesterday}),
      endTime = input({"id": "endTime", "type": "date", "value": yesterday});

rpc.getSummary((+new Date()/1000|0) - 86400, +new Date()/1000|0);

amendNode(document.body, [
	div([
		label({"for": "startTime"}, "Start Time"),
		startTime,
		br(),
		label({"for": "endTime"}, "End Time"),
		endTime,
		br(),
		button({"click": () => {
			const start = startTime.valueAsNumber/1000|0,
			      end = (endTime.valueAsNumber/1000|0)+86400;

			if (isNaN(start) || isNaN(end) || start >= end) {
				alert("Invalid time range");

				return;
			}

			rpc.getSummary(start, end)
			.then(data => {
				console.log(data);
			});
		}}, "Go!")
	])
])
