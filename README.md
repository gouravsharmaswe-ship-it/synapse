# Synapse — Multiplayer in 10 Lines

Synapse is a tiny real-time JavaScript library for adding rooms, shared state, live cursors, and presence to a webpage. It uses a small Node.js WebSocket server and has no frontend framework.

**Repository:** [gouravsharmaswe-ship-it/synapse-multiplayer](https://github.com/gouravsharmaswe-ship-it/synapse-multiplayer)

Deploy this repository using the steps below to create its public demo.

## Add Synapse to a webpage

Copy `web/syncspace.js` beside your HTML file. The filename remains `syncspace.js` for existing adopters; it exposes the new `Synapse` global and the old `SyncSpace` alias.

```html
<script src="syncspace.js"></script>
<script>
const room = Synapse.connect({ url: 'wss://codebase-website-formation.onrender.com', room: 'quiz-night', name: 'Asha' });
room.on('state', (key, value) => console.log(key, value));
room.on('presence', peers => console.log(peers.map(peer => peer.name)));
room.on('cursor', (id, x, y, peer) => drawCursor(id, x, y, peer));
document.onmousemove = event => room.cursor(event.clientX / innerWidth, event.clientY / innerHeight);
room.set('score', 1);
</script>
```

Use `wss://` for an HTTPS page. For a local page served by the included server, use `ws://localhost:3000`.

## API

| Call | What it does |
|---|---|
| `Synapse.connect({ url, room, name, clientId, connectTimeout })` | Joins a room and returns a room connection. Room names may contain letters, numbers, `_`, and `-`, up to 32 characters. |
| `Synapse.randomRoom()` | Makes a short random room name. |
| `room.on('state', (key, value, byId) => {})` | Runs when shared state changes or a snapshot is restored after reconnect. |
| `room.on('presence', peers => {})` | Supplies the current online peer list. |
| `room.on('cursor', (id, x, y, peer) => {})` | Receives another peer's normalized cursor position. |
| `room.on('open', ({ reconnected }) => {})` | Runs after the room snapshot has been applied. |
| `room.on('reconnecting', (attempt, delayMs) => {})` | Reports automatic reconnect attempts. |
| `room.set(key, value)` | Shares a JSON value. Passing `undefined` deletes the key. Writes made offline queue and send after reconnect. |
| `room.get(key)` / `room.getAll()` | Reads the local room state mirror. |
| `room.peers()` / `room.me()` | Reads online peers or this connection's identity. |
| `room.cursor(x, y)` | Sends normalized coordinates from `0` to `1`. |
| `room.off(event, handler)` | Removes a listener. |
| `room.close()` | Closes this connection and disables reconnect. |

## Run locally

Requires Node.js 18 or newer.

```bash
git clone https://github.com/gouravsharmaswe-ship-it/synapse-multiplayer.git
cd synapse-multiplayer
npm install
npm start
```

Open `http://localhost:3000` in two or more tabs. Each tab can join the same room through the room link. The demo includes a shared notepad and counter, room ideas, reactions, live cursors, quick room shouts (press `/`), and a **Drop & Reconnect** control for trying state recovery.

## How reconnect works

The client keeps its room identity and retries a dropped connection with increasing delays. On reconnect, the server sends the current room snapshot and presence list. Synapse applies that snapshot, then sends queued offline writes. Room data lives in server memory, so it resets if the server restarts or sleeps long enough to be restarted.

## Deploy

The Node server serves both the demo page and the WebSocket endpoint from the same origin. Deploy the `syncspace` directory as a Node web service with build command `npm install` and start command `npm start`. Set the service's health check path to `/health`. Render's free service can sleep while idle; open the demo before presenting it.

The full wire format and server limits are described in [PROTOCOL.md](PROTOCOL.md). The current server uses in-memory rooms, limits rooms to 25 peers, and removes empty room records after 10 minutes.

## License

MIT
