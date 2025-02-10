type Children = string | Element | DocumentFragment | Children[];

type Properties = Record<string, string | Function>;

type PropertiesOrChildren = Properties | Children;

interface State {
	filterMinSize?: number;
	filterMaxSize?: number;
	filterMinDaysAgo?: number;
	filterMaxDaysAgo?: number;
	sinceLastAccess?: number;
	selectedID?: number;
	owners?: string[];
	treeTypes?: string[];
	groups?: number[];
	users?: number[];
	useCount?: boolean;
	useMTime?: boolean;
	byUser?: boolean;
	viewDiskList?: boolean;
	[x: string]: any;
}

type UserEvent = {
	Timestamp: number;
	State: State;
}

type Session = UserEvent[];

type User = Record<string, Session>;

type Analytics = Record<string, User>;

class Summary {
	#events = 0;
	#groups = 0;
	#users = 0;
	#diskTree = 0;
	#start = Number.MAX_SAFE_INTEGER;
	#end = 0;

	get start() {
		return this.#start;
	}

	addEvent(event: UserEvent) {
		this.#events++;
		this.#groups = +!!event.State.groups;
		this.#users = +!!event.State.users;
		this.#diskTree = +!!event.State["just"];

		if (event.Timestamp < this.#start) {
			this.#start = event.Timestamp;
		}

		if (event.Timestamp > this.#end) {
			this.#end = event.Timestamp;
		}
	}

	addTo(s: Summary) {
		s.#events = this.#events;
		s.#groups += this.#groups;
		s.#users+= this.#users;
		s.#diskTree += this.#diskTree;

		if (this.#start < s.#start) {
			s.#start = this.#start;
		}

		if (this.#end > s.#end) {
			s.#end = this.#end;
		}
	}

	summaryHTML() {
		return [
			span(`${new Date(this.#start * 1000)} - ${new Date(this.#end * 1000)}`),
			table([
				thead(tr([
					th("Groups"),
					th("Users"),
					th("Just Disktree")
				])),
				tbody(tr([
					td(this.#groups + ""),
					td(this.#users + ""),
					td(this.#diskTree + "")
				]))
			])
		]
	}
}

class SessionSummary extends Summary {
	#events: Session;

	constructor(session: Session) {
		super();

		this.#events = session.sort((a, b) => a.Timestamp - b.Timestamp);;

		for (const event of session) {
			this.addEvent(event);
		}
	}

	html() {
		return [
			this.summaryHTML(),
			ul(this.#events.map(event => li(a({"href": "https://wrstat.internal.sanger.ac.uk/?"+Object.entries(event.State).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join("&")}, new Date(event.Timestamp * 1000) + ""))))
		];
	}
}

class UserSummary extends Summary {
	#sessions: SessionSummary[] = [];
	username: string;

	constructor(username: string, user: User) {
		super();

		this.username = username;

		for (const session of Object.values(user)) {
			const s = new SessionSummary(session);

			s.addTo(this);

			this.#sessions.push(s);
		}

		this.#sessions.sort((a, b) => a.start - b.start);
	}

	html() {
		return [
			this.summaryHTML(),
			ul(this.#sessions.map(session => li(details([
				summary(new Date(session.start*1000)+""),
				session.html()
			]))))
		];
	}
}

class TopSummary extends Summary {
	#users: UserSummary[] = [];

	constructor(analytics: Analytics) {
		super();

		console.log(analytics, Object.entries(analytics))

		for (const [username, sessions] of Object.entries(analytics)) {
			const u = new UserSummary(username, sessions);

			u.addTo(this);

			this.#users.push(u);
		}

		this.#users.sort((a, b) => a.start - b.start)
	}

	html() {
		return [
			this.summaryHTML(),
			ul(this.#users.map(user => li(details([
				summary(user.username),
				user.html()
			]))))
		];
	}
}

const amendNode = (node: Element, propertiesOrChildren: PropertiesOrChildren, children?: Children) => {
	const [p, c] = typeof propertiesOrChildren === "string" || propertiesOrChildren instanceof Node || propertiesOrChildren instanceof Array ? [{}, propertiesOrChildren] : [propertiesOrChildren, children ?? []];

	Object.entries(p).forEach(([key, value]) => node[value instanceof Function ? "addEventListener" : "setAttribute"](key, value as any));

	node.append(...[c as any].flat(Infinity));

	return node;
      },
      clearNode = (node: Element, propertiesOrChildren: PropertiesOrChildren = {}, children?: Children) => amendNode((node.replaceChildren(), node), propertiesOrChildren, children),
      {a, br, button, details, div, label, li, input, span, summary, table, tbody, td, th, thead, tr, ul} = new Proxy({}, {"get": (_, element: keyof HTMLElementTagNameMap) => (props: PropertiesOrChildren = {}, children?: Children) => amendNode(document.createElementNS("http://www.w3.org/1999/xhtml", element), props, children)}) as {[K in keyof HTMLElementTagNameMap]: (props?: PropertiesOrChildren, children?: Children) => HTMLElementTagNameMap[K]},
      rpc = (() => {
	const base = "/",
	      getData = <T>(url: string, body: string) => fetch(base + url, {"method": "POST", body}).then(j => j.json() as T);
	
	return {
		"getAnalytics": (startTime: number, endTime: number) => getData<Analytics>("analytics", JSON.stringify({startTime, endTime})),
	};
      })(),
      yesterday = (() => {
	const d = new Date();

	d.setDate(d.getDate() - 1);

	return d.toISOString().split("T")[0];
      })(),
      startTime = input({"id": "startTime", "type": "date", "value": yesterday}),
      endTime = input({"id": "endTime", "type": "date", "value": yesterday}),
      today = endTime.valueAsNumber/1000|0;

rpc.getAnalytics(today - 86400, today).then(data => {
	const topLevelStats = div(),
	      setTopLevel = (data: Analytics) => clearNode(topLevelStats, new TopSummary(data).html());

	setTopLevel(data);

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

				rpc.getAnalytics(start, end)
				.then(data => setTopLevel(data));
			}}, "Go!")
		]),
		topLevelStats,
	]);
});
