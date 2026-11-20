// Sandy's lobby server: room codes for playing together (docs/multiplayer.md, docs/lobby-server.md).
//
// It only introduces players. A host asks for a room and gets a short code; friends join with the
// code; the server then passes the WebRTC handshake (offers, answers, ICE candidates) between the
// host and each joiner until they're connected straight to each other. No game data goes through it.
//
// Plain Node.js (18 or later), no packages: `node server/lobby_server.js` (PORT, default 8787).
// Hosting services put it behind HTTPS, so the game connects to wss://<your-app>/.
// ICE_SERVERS (optional, JSON): the STUN / TURN servers handed to the players, e.g.
//   [{"urls":["stun:stun.l.google.com:19302"]},{"urls":["turn:turn.example.com:3478"],"username":"u","credential":"p"}]
//
// Messages (JSON text, one per WebSocket message):
//   game -> server: {t:"host", v, build, name, code?}    -> {t:"room", code, id:1, ice}   (code: one it would like)
//                   {t:"join", code, v, build, name}     -> {t:"joined", id, ice}   (and the host gets {t:"peer", id, name})
//                   {t:"signal", to, data}               -> the other one gets {t:"signal", from, data}
//   server -> game: {t:"error", msg}  {t:"left", id} (to the host)  {t:"closed"} (to joiners: the host went)

"use strict";
const http = require("http");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "8787", 10);
const ICE = JSON.parse(process.env.ICE_SERVERS || '[{"urls":["stun:stun.l.google.com:19302","stun:stun1.l.google.com:19302"]}]');
const MAX_PLAYERS = 10;          // the game's Net.MAX_PLAYERS
const MAX_MESSAGE = 64 * 1024;   // a handshake message is a few KB at most
const MAX_ROOMS = 2000;
const CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";   // no 0/O, 1/I/L
const CODE_LEN = 5;
const PING_MS = 25000;           // keeps idle hosts' connections open through proxies

const rooms = new Map();         // code -> {host: client, players: Map(id -> client), next, v, build}

// ------------------------------------------------------------------ the web server

const server = http.createServer((req, res) => {
  // Hosting services check this to see the server is up.
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(`Sandy lobby: ${rooms.size} room(s) open\n`);
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key || (req.headers.upgrade || "").toLowerCase() !== "websocket") {
    socket.destroy();
    return;
  }
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  socket.setNoDelay(true);
  const client = { socket, buf: Buffer.alloc(0), frags: [], room: null, id: 0, alive: true };
  socket.on("data", (d) => onData(client, d));
  socket.on("close", () => onClose(client));
  socket.on("error", () => socket.destroy());
});

setInterval(() => {
  for (const room of rooms.values()) {
    for (const c of room.players.values()) {
      if (!c.alive) { c.socket.destroy(); continue; }
      c.alive = false;
      sendFrame(c, 0x9, Buffer.alloc(0));   // ping; a pong marks it alive
    }
  }
}, PING_MS);

server.listen(PORT, () => console.log(`Sandy lobby listening on ${PORT}`));

// ------------------------------------------------------------------ WebSocket frames (RFC 6455)

function onData(c, data) {
  c.buf = Buffer.concat([c.buf, data]);
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1];
    const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_MESSAGE) { c.socket.destroy(); return; }
    const need = off + (masked ? 4 : 0) + len;
    if (c.buf.length < need) return;
    let payload = c.buf.subarray(off + (masked ? 4 : 0), need);
    if (masked) {
      const mask = c.buf.subarray(off, off + 4);
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
    }
    c.buf = c.buf.subarray(need);
    c.alive = true;
    if (op === 0x8) { sendFrame(c, 0x8, Buffer.alloc(0)); c.socket.end(); return; }   // close
    if (op === 0x9) { sendFrame(c, 0xa, payload); continue; }                          // ping -> pong
    if (op === 0xa) continue;                                                           // pong
    if (op === 0x1 || op === 0x2 || op === 0x0) {
      c.frags.push(payload);
      if (Buffer.concat(c.frags).length > MAX_MESSAGE) { c.socket.destroy(); return; }
      if (fin) {
        const text = Buffer.concat(c.frags).toString("utf8");
        c.frags = [];
        let msg = null;
        try { msg = JSON.parse(text); } catch (e) { /* not ours */ }
        if (msg && typeof msg === "object") onMessage(c, msg);
      }
    }
  }
}

function sendFrame(c, op, payload) {
  if (c.socket.destroyed) return;
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  c.socket.write(Buffer.concat([head, payload]));
}

function send(c, obj) {
  sendFrame(c, 0x1, Buffer.from(JSON.stringify(obj), "utf8"));
}

// ------------------------------------------------------------------ rooms

function newCode() {
  for (let tries = 0; tries < 50; tries++) {
    let code = "";
    for (let i = 0; i < CODE_LEN; i++) code += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(code)) return code;
  }
  return null;
}

function onMessage(c, m) {
  switch (m.t) {
    case "host": {
      if (c.room) return;
      if (rooms.size >= MAX_ROOMS) return send(c, { t: "error", msg: "The lobby is full right now. Try again soon." });
      // A host may ask for a code (tests use a fixed one); taken or malformed, it gets a new one.
      const want = String(m.code || "").toUpperCase();
      const ok = want.length === CODE_LEN && [...want].every((ch) => CODE_CHARS.includes(ch)) && !rooms.has(want);
      const code = ok ? want : newCode();
      if (!code) return send(c, { t: "error", msg: "Couldn't make a room code. Try again." });
      const room = { code, host: c, players: new Map([[1, c]]), next: 2, v: m.v, build: m.build };
      rooms.set(code, room);
      c.room = room;
      c.id = 1;
      send(c, { t: "room", code, id: 1, ice: ICE });
      console.log(`room ${code} opened (${rooms.size} open)`);
      return;
    }
    case "join": {
      if (c.room) return;
      const code = String(m.code || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
      const room = rooms.get(code);
      if (!room) return send(c, { t: "error", msg: `There's no game with the code ${code}. Check it with the host.` });
      if (room.v !== m.v || room.build !== m.build)
        return send(c, { t: "error", msg: `Your copy of Sandy isn't the same build as the host's (${m.build}, theirs ${room.build}). Get the host's latest copy.` });
      if (room.players.size >= MAX_PLAYERS) return send(c, { t: "error", msg: "That game is full." });
      const id = room.next++;
      room.players.set(id, c);
      c.room = room;
      c.id = id;
      send(c, { t: "joined", id, ice: ICE });
      send(room.host, { t: "peer", id, name: String(m.name || "").slice(0, 24) });
      return;
    }
    case "signal": {
      const room = c.room;
      if (!room) return;
      const to = room.players.get(Number(m.to));
      // Only between the host and a joiner (the game's peers only ever connect to the host).
      if (to && (c.id === 1 || to.id === 1)) send(to, { t: "signal", from: c.id, data: m.data });
      return;
    }
  }
}

function onClose(c) {
  const room = c.room;
  if (!room) return;
  c.room = null;
  room.players.delete(c.id);
  if (c.id === 1) {
    rooms.delete(room.code);
    for (const p of room.players.values()) send(p, { t: "closed" });
    console.log(`room ${room.code} closed (${rooms.size} open)`);
  } else {
    send(room.host, { t: "left", id: c.id });
  }
}
