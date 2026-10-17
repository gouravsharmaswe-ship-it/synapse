// Synapse Backend Server
// Real-time WebSocket room coordination, PostgreSQL persistence, and static HTTP hosting
// Reference: PROTOCOL.md

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const db = require('./db');

const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);

const LIMITS = {
  name: 32,
  key: 64,
  valueChars: 50000,
  keysPerRoom: 150,
  peersPerRoom: 50,
  cursorPerSec: 60,
  setPerSec: 40
};

const EMPTY_ROOM_TTL_MS = 15 * 60 * 1000; // 15 minutes
const WEB_DIR = path.join(__dirname, '..', 'web');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon'
};

// In-memory active room registry
// roomId -> { state: Map, version: number, peers: Map(id -> peer), emptySince: timestamp|null, padDebounceTimer: timer, lastPadTypist: { name, clientId, cursorPosition } }
const rooms = new Map();

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  const parsedUrl = new URL(req.url, 'http://localhost');

  // Health check endpoint (used by Render and uptime monitors)
  if (parsedUrl.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({
      ok: true,
      service: 'synapse',
      rooms: rooms.size,
      db: db.isDbConnected(),
      uptime: Math.round(process.uptime()),
      timestamp: Date.now()
    }));
  }

  // REST API endpoints for history
  if (parsedUrl.pathname.startsWith('/api/room/')) {
    const parts = parsedUrl.pathname.split('/');
    // format: /api/room/:roomCode/notepad-history or /api/room/:roomCode/counter-history
    const roomCode = parts[3];
    const resource = parts[4];

    if (validId(roomCode)) {
      if (resource === 'notepad-history') {
        return db.getPadVersions(roomCode, 50).then(versions => {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, roomCode, versions }));
        }).catch(err => {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: err.message }));
        });
      }

      if (resource === 'counter-history') {
        return db.getCounterActions(roomCode, 50).then(actions => {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, roomCode, actions }));
        }).catch(err => {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: err.message }));
        });
      }
    }
  }

  // Static web server
  const reqPath = parsedUrl.pathname === '/' ? '/index.html' : parsedUrl.pathname;
  const fullPath = path.normalize(path.join(WEB_DIR, reqPath));

  if (!fullPath.startsWith(WEB_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    return res.end('403 Forbidden');
  }

  fs.readFile(fullPath, (err, data) => {
    if (err) {
      const indexPath = path.join(WEB_DIR, 'index.html');
      fs.readFile(indexPath, (fallbackErr, fallbackData) => {
        if (fallbackErr) {
          res.writeHead(404, { 'Content-Type': 'text/plain' });
          return res.end('404 Not Found');
        }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(fallbackData);
      });
      return;
    }
    const ext = path.extname(fullPath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

const wss = new WebSocketServer({
  server,
  maxPayload: 32 * 1024, // 32 KB frame limit
  verifyClient: ({ origin }) => {
    if (ALLOWED_ORIGINS.length === 0 || !origin) return true;
    return ALLOWED_ORIGINS.includes(origin);
  }
});

const send = (ws, msg) => {
  if (ws && ws.readyState === 1) {
    try {
      ws.send(JSON.stringify(msg));
    } catch (e) {}
  }
};

const broadcast = (room, msg, exceptId = null) => {
  for (const peer of room.peers.values()) {
    if (peer.id !== exceptId) {
      send(peer.ws, msg);
    }
  }
};

const publicPeer = p => ({ id: p.id, name: p.name, color: p.color });

const COLORS = [
  '#4f46e5', // Royal Indigo
  '#059669', // Emerald
  '#d97706', // Amber
  '#db2777', // Fuchsia Pink
  '#0284c7', // Sky Blue
  '#7c3aed', // Violet
  '#dc2626', // Crimson Red
  '#16a34a'  // Forest Green
];

const validId = s => typeof s === 'string' && /^[A-Za-z0-9_-]{1,32}$/.test(s);

function allowRateLimit(peer, kind, maxPerSec) {
  const now = Date.now();
  const bucket = peer.buckets[kind] || (peer.buckets[kind] = { t: now, count: 0 });
  if (now - bucket.t >= 1000) {
    bucket.t = now;
    bucket.count = 0;
  }
  return ++bucket.count <= maxPerSec;
}

wss.on('connection', (ws) => {
  let peer = null;
  let room = null;
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return send(ws, { t: 'error', code: 'bad_json', message: 'Message must be valid JSON' });
    }

    if (!msg || typeof msg.t !== 'string') {
      return send(ws, { t: 'error', code: 'bad_message', message: 'Missing message type "t"' });
    }

    // 1. Join Room
    if (msg.t === 'join') {
      if (peer) {
        return send(ws, { t: 'error', code: 'already_joined', message: 'Already joined a room on this connection' });
      }
      if (!validId(msg.room)) {
        return send(ws, { t: 'error', code: 'bad_room', message: 'Room code must be 1-32 chars: letters, numbers, - or _' });
      }

      const id = validId(msg.clientId) ? msg.clientId : Math.random().toString(36).slice(2, 10);
      room = rooms.get(msg.room);

      if (!room) {
        // Load durable room state from database or init default
        const durableRoom = await db.getRoom(msg.room);
        const stateMap = new Map();
        stateMap.set('pad', durableRoom.pad_text || '');
        stateMap.set('counter', durableRoom.counter_value || 0);
        stateMap.set('tasks', durableRoom.tasks || []);

        room = {
          id: msg.room,
          state: stateMap,
          version: 1,
          peers: new Map(),
          emptySince: null,
          padDebounceTimer: null,
          lastPadTypist: null
        };
        rooms.set(msg.room, room);
      }

      // If reconnecting with same clientId, terminate old socket
      const existing = room.peers.get(id);
      if (!existing && room.peers.size >= LIMITS.peersPerRoom) {
        room = null;
        return send(ws, { t: 'error', code: 'room_full', message: `Room is full (max ${LIMITS.peersPerRoom})` });
      }

      if (existing) {
        try {
          existing.ws.removeAllListeners('close');
          existing.ws.terminate();
        } catch {}
        room.peers.delete(id);
      }

      const name = String(msg.name || 'Guest').trim().slice(0, LIMITS.name) || 'Guest';
      const colorHash = [...id].reduce((sum, ch) => sum + ch.charCodeAt(0), 0);
      const color = COLORS[colorHash % COLORS.length];

      peer = { id, name, color, ws, buckets: {} };
      room.peers.set(id, peer);
      room.emptySince = null;

      // Fetch latest history from database
      const [notepadHistory, counterHistory] = await Promise.all([
        db.getPadVersions(msg.room, 20),
        db.getCounterActions(msg.room, 30)
      ]);

      // Welcome handshake with complete room state, peer directory, and history
      send(ws, {
        t: 'welcome',
        id,
        room: msg.room,
        version: room.version,
        state: Object.fromEntries(room.state),
        peers: [...room.peers.values()].map(publicPeer),
        notepadHistory,
        counterHistory,
        dbConnected: db.isDbConnected()
      });

      // Announce arrival to room
      broadcast(room, { t: 'peer-join', peer: publicPeer(peer) }, id);
      return;
    }

    // Require join before any further action
    if (!peer || !room) {
      return send(ws, { t: 'error', code: 'not_joined', message: 'Must send "join" message first' });
    }

    // 2. State Mutation (`set`)
    if (msg.t === 'set') {
      if (typeof msg.key !== 'string' || !msg.key || msg.key.length > LIMITS.key) {
        return send(ws, { t: 'error', code: 'bad_key', message: `Key must be 1-${LIMITS.key} characters` });
      }
      if (!allowRateLimit(peer, 'set', LIMITS.setPerSec)) {
        return send(ws, { t: 'error', code: 'rate_limited', message: 'Too many updates (rate limit exceeded)' });
      }

      // Handle Collaborative Notepad updates
      if (msg.key === 'pad') {
        const newText = String(msg.value || '');
        if (newText.length > LIMITS.valueChars) {
          return send(ws, { t: 'error', code: 'too_big', message: `Notepad text exceeds character limit (${LIMITS.valueChars})` });
        }

        room.state.set('pad', newText);
        room.version++;

        // Broadcast text update immediately to all other peers for 0-latency typing
        broadcast(room, {
          t: 'state',
          key: 'pad',
          value: newText,
          by: peer.id,
          v: room.version
        }, peer.id);

        // Record active typist information
        room.lastPadTypist = {
          savedText: newText,
          displayName: peer.name,
          clientId: peer.id,
          cursorPosition: typeof msg.cursorPosition === 'number' ? msg.cursorPosition : null
        };

        // Notepad history debounce timer (2 seconds of inactivity triggers a durable version)
        if (room.padDebounceTimer) {
          clearTimeout(room.padDebounceTimer);
        }

        const roomCode = room.id;
        const typistSnapshot = { ...room.lastPadTypist };

        room.padDebounceTimer = setTimeout(async () => {
          try {
            const savedVersion = await db.savePadVersion(roomCode, typistSnapshot);
            // Broadcast new historical version to all room participants
            broadcast(room, {
              t: 'notepad_version_added',
              version: savedVersion
            });
            // Also notify the typist
            send(peer.ws, {
              t: 'notepad_version_added',
              version: savedVersion
            });
          } catch (err) {
            console.error('[Server] Failed to save notepad version:', err.message);
          }
        }, 2000);

        return;
      }

      // Handle Shared Tasks updates
      if (msg.key === 'tasks') {
        room.state.set('tasks', msg.value);
        room.version++;
        db.saveRoomTasks(room.id, msg.value).catch(() => {});

        broadcast(room, {
          t: 'state',
          key: 'tasks',
          value: msg.value,
          by: peer.id,
          v: room.version
        }, peer.id);
        return;
      }

      // General key-value update
      if (msg.value === undefined) {
        room.state.delete(msg.key);
      } else {
        const serialized = JSON.stringify(msg.value);
        if (serialized.length > LIMITS.valueChars) {
          return send(ws, { t: 'error', code: 'too_big', message: `Value size exceeds ${LIMITS.valueChars} characters` });
        }
        if (!room.state.has(msg.key) && room.state.size >= LIMITS.keysPerRoom) {
          return send(ws, { t: 'error', code: 'too_many_keys', message: `Room key limit reached (${LIMITS.keysPerRoom})` });
        }
        room.state.set(msg.key, msg.value);
      }

      room.version++;
      broadcast(room, {
        t: 'state',
        key: msg.key,
        value: msg.value,
        by: peer.id,
        v: room.version
      }, peer.id);
      return;
    }

    // 3. Counter Action with PostgreSQL Transaction Lock (Prevents Race Conditions)
    if (msg.t === 'counter_action') {
      const act = String(msg.action || 'inc');
      const amount = Number(msg.amount) || 1;

      try {
        const { resultingValue, actionRecord } = await db.applyCounterAction(room.id, {
          actionType: act,
          amount,
          displayName: peer.name,
          clientId: peer.id
        });

        room.state.set('counter', resultingValue);
        room.version++;

        // Broadcast updated counter value and history record to all peers (including sender for guaranteed sync)
        const counterUpdateMsg = {
          t: 'state',
          key: 'counter',
          value: resultingValue,
          by: peer.id,
          v: room.version
        };
        const actionAddedMsg = {
          t: 'counter_action_added',
          action: actionRecord
        };

        broadcast(room, counterUpdateMsg);
        send(peer.ws, counterUpdateMsg);

        broadcast(room, actionAddedMsg);
        send(peer.ws, actionAddedMsg);
      } catch (err) {
        console.error('[Server] counter_action error:', err.message);
        send(ws, { t: 'error', code: 'counter_error', message: 'Could not apply counter action' });
      }
      return;
    }

    // 4. Restore Notepad Version (Creates new version, never overwrites history)
    if (msg.t === 'restore_version') {
      const versionId = msg.versionId;
      try {
        const targetVersion = await db.getPadVersionById(room.id, versionId);
        if (!targetVersion) {
          return send(ws, { t: 'error', code: 'version_not_found', message: 'Requested version does not exist' });
        }

        const restoredText = targetVersion.saved_text;
        room.state.set('pad', restoredText);
        room.version++;

        // Broadcast restored text immediately
        const stateMsg = {
          t: 'state',
          key: 'pad',
          value: restoredText,
          by: peer.id,
          v: room.version
        };
        broadcast(room, stateMsg);
        send(peer.ws, stateMsg);

        // Record new version documenting the restoration
        const newVersionRecord = await db.savePadVersion(room.id, {
          savedText: restoredText,
          displayName: `${peer.name} (Restored #${targetVersion.id})`,
          clientId: peer.id,
          cursorPosition: 0
        });

        // Broadcast newly recorded version
        const versionAddedMsg = {
          t: 'notepad_version_added',
          version: newVersionRecord
        };
        broadcast(room, versionAddedMsg);
        send(peer.ws, versionAddedMsg);
      } catch (err) {
        console.error('[Server] restore_version error:', err.message);
        send(ws, { t: 'error', code: 'restore_error', message: 'Failed to restore version' });
      }
      return;
    }

    // 5. Live Cursor Streaming
    if (msg.t === 'cursor') {
      if (!allowRateLimit(peer, 'cursor', LIMITS.cursorPerSec)) return;
      if (typeof msg.x !== 'number' || typeof msg.y !== 'number') return;
      broadcast(room, {
        t: 'cursor',
        id: peer.id,
        x: msg.x,
        y: msg.y
      }, peer.id);
      return;
    }

    // 6. Quick Shouts / Room Chat
    if (msg.t === 'quick_shout') {
      const text = String(msg.message || '').trim().slice(0, 240);
      if (!text) return;
      const shoutObj = {
        t: 'quick_shout',
        id: Math.random().toString(36).slice(2, 9),
        message: text,
        from: peer.name,
        color: peer.color,
        clientId: peer.id,
        time: Date.now()
      };
      broadcast(room, shoutObj);
      send(peer.ws, shoutObj);
      return;
    }

    // 7. Live Emoji Reactions
    if (msg.t === 'reaction') {
      const emoji = String(msg.emoji || '🚀').slice(0, 8);
      broadcast(room, {
        t: 'reaction',
        emoji,
        from: peer.name,
        time: Date.now()
      }, peer.id);
      return;
    }

    // 8. Ping / Pong
    if (msg.t === 'ping') {
      return send(ws, { t: 'pong' });
    }

    send(ws, { t: 'error', code: 'unknown_type', message: `Unknown message type "${msg.t}"` });
  });

  ws.on('close', () => {
    if (!peer || !room) return;
    if (room.peers.get(peer.id) === peer) {
      room.peers.delete(peer.id);
      broadcast(room, { t: 'peer-leave', id: peer.id });
      if (room.peers.size === 0) {
        room.emptySince = Date.now();
      }
    }
  });

  ws.on('error', () => {});
});

// Periodic heartbeat & room garbage collection
const heartbeatInterval = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }

  const now = Date.now();
  for (const [roomId, r] of rooms) {
    if (r.emptySince && now - r.emptySince > EMPTY_ROOM_TTL_MS) {
      if (r.padDebounceTimer) clearTimeout(r.padDebounceTimer);
      rooms.delete(roomId);
    }
  }
}, 30000);
heartbeatInterval.unref();

// Server startup
async function start() {
  await db.initDb();
  server.listen(PORT, () => {
    console.log(`⚡ Synapse server listening on http://localhost:${PORT}`);
    console.log(`📡 WebSocket ready on ws://localhost:${PORT}`);
  });
}

if (require.main === module) {
  start().catch(err => {
    console.error('Fatal startup error:', err);
    process.exit(1);
  });
}

module.exports = { server, wss, rooms, start };
