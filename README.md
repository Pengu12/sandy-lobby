# Sandy lobby

Room codes for playing Sandy together. A host asks for a room and gets a short code; friends join with it; the lobby passes the WebRTC handshake between them until their games are connected directly. No game data goes through it.

Plain Node.js 18+, no packages: `node lobby_server.js` (PORT, default 8787). Optional `ICE_SERVERS` (JSON) adds STUN/TURN servers for the games. The source of truth is `server/` in the Sandy project; see its docs/lobby-server.md.
