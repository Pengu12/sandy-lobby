// Lobby self-test: `node server/lobby_server.js` in one terminal (PORT 8799: `PORT=8799 node server/lobby_server.js`),
// then `node server/selftest.mjs`. Hosts a room, tries a wrong code and an old build, joins, passes an offer,
// and checks the joiner hears when the host goes. Needs Node 22+ (its built-in WebSocket client).
const url = "ws://127.0.0.1:8799";
const open = () => new Promise((r) => { const w = new WebSocket(url); w.onopen = () => r(w); });
const next = (w) => new Promise((r) => { w.onmessage = (e) => r(JSON.parse(e.data)); });
const host = await open();
host.send(JSON.stringify({ t: "host", v: 6, build: "abc", name: "Host" }));
const room = await next(host); console.log("host got", room.t, room.code, room.id, room.ice.length, "ice server(s)");
const bad = await open(); bad.send(JSON.stringify({ t: "join", code: "ZZZZZ", v: 6, build: "abc" })); console.log("wrong code:", (await next(bad)).msg);
const old = await open(); old.send(JSON.stringify({ t: "join", code: room.code, v: 6, build: "old" })); console.log("old build:", (await next(old)).msg);
const j = await open();
const hostNote = next(host);
j.send(JSON.stringify({ t: "join", code: room.code.toLowerCase(), v: 6, build: "abc", name: "Ann" }));
const joined = await next(j); console.log("joiner got", joined.t, "id", joined.id);
console.log("host told", JSON.stringify(await hostNote));
const sig = next(j); host.send(JSON.stringify({ t: "signal", to: joined.id, data: { type: "offer", sdp: "x".repeat(3000) } }));
const s = await sig; console.log("joiner got signal from", s.from, s.data.type, s.data.sdp.length, "chars");
const closed = next(j); host.close(); console.log("after host left, joiner got", (await closed).t);
process.exit(0);
