const beaconURL = "/spyware",
	newSessionID = () => Date.now().toString(36),
	recordAnalytics = () => navigator.sendBeacon(beaconURL, sessionID),
	pushState = window.history.pushState;

let sessionID = newSessionID();

window.addEventListener("load", recordAnalytics);
window.addEventListener("popstate", recordAnalytics);
window.addEventListener("visibilitychange", () => {
	if (document.visibilityState !== "hidden") {
		sessionID = newSessionID();
	}

	recordAnalytics();
});
window.history.pushState = (data: any, unused: string, url?: string | URL) => {
	pushState.call(window.history, data, unused, url);

	recordAnalytics();
};