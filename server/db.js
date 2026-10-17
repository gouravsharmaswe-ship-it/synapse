// Database layer for Synapse: PostgreSQL with resilient in-memory fallback
// Reads connection from process.env.DATABASE_URL
const { Pool } = require('pg');

let pool = null;
let isConnected = false;

// In-memory storage fallback used when DATABASE_URL is not set or DB is unreachable
const memoryStore = {
  rooms: new Map(), // roomCode -> { pad_text: '', counter_value: 0, tasks: [] }
  notepadVersions: new Map(), // roomCode -> array of version objects
  counterActions: new Map() // roomCode -> array of action objects
};

let versionSeq = 1;
let actionSeq = 1;

/**
 * Initialize database and execute idempotent migrations
 */
async function initDb() {
  const dbUrl = process.env.DATABASE_URL;

  if (!dbUrl) {
    console.log('ℹ️  [DB] DATABASE_URL not set; running with resilient in-memory storage.');
    return false;
  }

  try {
    const isProduction = process.env.NODE_ENV === 'production' || dbUrl.includes('render.com') || dbUrl.includes('sslmode=require');
    pool = new Pool({
      connectionString: dbUrl,
      ssl: isProduction ? { rejectUnauthorized: false } : undefined,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000
    });

    pool.on('error', (err) => {
      console.error('⚠️  [DB] Unexpected client pool error:', err.message);
    });

    // Test connection
    const client = await pool.connect();
    try {
      await runMigrations(client);
      isConnected = true;
      console.log('✅ [DB] Connected to PostgreSQL. Migrations verified.');
      return true;
    } finally {
      client.release();
    }
  } catch (err) {
    console.warn(`⚠️  [DB] Could not connect to PostgreSQL (${err.message}). Falling back to in-memory store.`);
    isConnected = false;
    return false;
  }
}

/**
 * Idempotent table and index migrations
 */
async function runMigrations(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS synapse_rooms (
      room_code VARCHAR(32) PRIMARY KEY,
      pad_text TEXT DEFAULT '',
      counter_value INTEGER DEFAULT 0,
      tasks JSONB DEFAULT '[]'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS synapse_notepad_versions (
      id SERIAL PRIMARY KEY,
      room_code VARCHAR(32) NOT NULL REFERENCES synapse_rooms(room_code) ON DELETE CASCADE,
      saved_text TEXT NOT NULL,
      char_count INTEGER NOT NULL,
      display_name VARCHAR(64) NOT NULL,
      client_id VARCHAR(64) NOT NULL,
      cursor_position INTEGER,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS synapse_counter_actions (
      id SERIAL PRIMARY KEY,
      room_code VARCHAR(32) NOT NULL REFERENCES synapse_rooms(room_code) ON DELETE CASCADE,
      action_type VARCHAR(16) NOT NULL,
      amount INTEGER NOT NULL,
      resulting_value INTEGER NOT NULL,
      display_name VARCHAR(64) NOT NULL,
      client_id VARCHAR(64) NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_notepad_versions_room ON synapse_notepad_versions(room_code, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_counter_actions_room ON synapse_counter_actions(room_code, created_at DESC);
  `);
}

function isDbConnected() {
  return isConnected;
}

/**
 * Ensure a room row exists
 */
async function ensureRoom(roomCode) {
  if (isConnected && pool) {
    try {
      await pool.query(
        `INSERT INTO synapse_rooms (room_code, pad_text, counter_value, tasks, updated_at)
         VALUES ($1, '', 0, '[]'::jsonb, NOW())
         ON CONFLICT (room_code) DO NOTHING`,
        [roomCode]
      );
    } catch (err) {
      console.error('[DB] ensureRoom error:', err.message);
    }
  }

  if (!memoryStore.rooms.has(roomCode)) {
    memoryStore.rooms.set(roomCode, { pad_text: '', counter_value: 0, tasks: [] });
  }
  if (!memoryStore.notepadVersions.has(roomCode)) {
    memoryStore.notepadVersions.set(roomCode, []);
  }
  if (!memoryStore.counterActions.has(roomCode)) {
    memoryStore.counterActions.set(roomCode, []);
  }
}

/**
 * Get durable room state
 */
async function getRoom(roomCode) {
  await ensureRoom(roomCode);

  if (isConnected && pool) {
    try {
      const res = await pool.query(
        `SELECT room_code, pad_text, counter_value, tasks FROM synapse_rooms WHERE room_code = $1`,
        [roomCode]
      );
      if (res.rows.length > 0) {
        const row = res.rows[0];
        const tasks = Array.isArray(row.tasks) ? row.tasks : (typeof row.tasks === 'string' ? JSON.parse(row.tasks) : []);
        // Mirror to memory
        memoryStore.rooms.set(roomCode, {
          pad_text: row.pad_text || '',
          counter_value: Number(row.counter_value) || 0,
          tasks
        });
        return {
          pad_text: row.pad_text || '',
          counter_value: Number(row.counter_value) || 0,
          tasks
        };
      }
    } catch (err) {
      console.error('[DB] getRoom query error:', err.message);
    }
  }

  return memoryStore.rooms.get(roomCode) || { pad_text: '', counter_value: 0, tasks: [] };
}

/**
 * Update current pad text in room
 */
async function saveRoomPad(roomCode, padText) {
  await ensureRoom(roomCode);
  const mem = memoryStore.rooms.get(roomCode);
  if (mem) mem.pad_text = padText;

  if (isConnected && pool) {
    try {
      await pool.query(
        `UPDATE synapse_rooms SET pad_text = $1, updated_at = NOW() WHERE room_code = $2`,
        [padText, roomCode]
      );
    } catch (err) {
      console.error('[DB] saveRoomPad error:', err.message);
    }
  }
}

/**
 * Save a new version to notepad history (called after 2s debounced inactivity)
 */
async function savePadVersion(roomCode, { savedText, displayName, clientId, cursorPosition }) {
  await ensureRoom(roomCode);
  const text = String(savedText || '');
  const charCount = text.length;
  const name = String(displayName || 'Guest').slice(0, 64);
  const cid = String(clientId || '').slice(0, 64);
  const pos = typeof cursorPosition === 'number' ? cursorPosition : null;

  // Save to memory store first
  const memVersion = {
    id: versionSeq++,
    room_code: roomCode,
    saved_text: text,
    char_count: charCount,
    display_name: name,
    client_id: cid,
    cursor_position: pos,
    created_at: new Date().toISOString()
  };
  const list = memoryStore.notepadVersions.get(roomCode) || [];
  list.unshift(memVersion);
  if (list.length > 100) list.pop(); // keep last 100 in memory
  memoryStore.notepadVersions.set(roomCode, list);

  // Update memory room pad
  const memRoom = memoryStore.rooms.get(roomCode);
  if (memRoom) memRoom.pad_text = text;

  // Persist to PostgreSQL if connected
  if (isConnected && pool) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE synapse_rooms SET pad_text = $1, updated_at = NOW() WHERE room_code = $2`,
        [text, roomCode]
      );
      const insertRes = await client.query(
        `INSERT INTO synapse_notepad_versions
           (room_code, saved_text, char_count, display_name, client_id, cursor_position, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, NOW())
         RETURNING id, room_code, saved_text, char_count, display_name, client_id, cursor_position, created_at`,
        [roomCode, text, charCount, name, cid, pos]
      );
      await client.query('COMMIT');
      if (insertRes.rows.length > 0) {
        return insertRes.rows[0];
      }
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      console.error('[DB] savePadVersion error:', err.message);
    } finally {
      client.release();
    }
  }

  return memVersion;
}

/**
 * Get notepad version history for a room
 */
async function getPadVersions(roomCode, limit = 50) {
  await ensureRoom(roomCode);

  if (isConnected && pool) {
    try {
      const res = await pool.query(
        `SELECT id, room_code, saved_text, char_count, display_name, client_id, cursor_position, created_at
         FROM synapse_notepad_versions
         WHERE room_code = $1
         ORDER BY created_at DESC
         LIMIT $2`,
        [roomCode, limit]
      );
      return res.rows;
    } catch (err) {
      console.error('[DB] getPadVersions error:', err.message);
    }
  }

  const list = memoryStore.notepadVersions.get(roomCode) || [];
  return list.slice(0, limit);
}

/**
 * Get single version by ID
 */
async function getPadVersionById(roomCode, versionId) {
  if (isConnected && pool) {
    try {
      const res = await pool.query(
        `SELECT id, room_code, saved_text, char_count, display_name, client_id, cursor_position, created_at
         FROM synapse_notepad_versions
         WHERE room_code = $1 AND id = $2`,
        [roomCode, versionId]
      );
      if (res.rows.length > 0) return res.rows[0];
    } catch (err) {
      console.error('[DB] getPadVersionById error:', err.message);
    }
  }

  const list = memoryStore.notepadVersions.get(roomCode) || [];
  return list.find(v => String(v.id) === String(versionId)) || null;
}

/**
 * Apply atomic counter action using PostgreSQL transaction + row-level lock (FOR UPDATE)
 * Prevents race conditions during simultaneous clicks
 */
async function applyCounterAction(roomCode, { actionType, amount, displayName, clientId }) {
  await ensureRoom(roomCode);
  const name = String(displayName || 'Guest').slice(0, 64);
  const cid = String(clientId || '').slice(0, 64);
  const act = String(actionType || 'inc');
  const delta = Number(amount) || 1;

  if (isConnected && pool) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Row-level lock guarantees serialization of concurrent counter operations
      const lockRes = await client.query(
        `SELECT counter_value FROM synapse_rooms WHERE room_code = $1 FOR UPDATE`,
        [roomCode]
      );

      let currentVal = 0;
      if (lockRes.rows.length > 0) {
        currentVal = Number(lockRes.rows[0].counter_value) || 0;
      }

      let resultingValue = currentVal;
      if (act === 'inc') resultingValue += delta;
      else if (act === 'dec') resultingValue -= delta;
      else if (act === 'reset') resultingValue = 0;
      else if (act === 'set') resultingValue = delta;

      // Update room counter
      await client.query(
        `UPDATE synapse_rooms SET counter_value = $1, updated_at = NOW() WHERE room_code = $2`,
        [resultingValue, roomCode]
      );

      // Record counter action history
      const actionRes = await client.query(
        `INSERT INTO synapse_counter_actions
           (room_code, action_type, amount, resulting_value, display_name, client_id, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, NOW())
         RETURNING id, room_code, action_type, amount, resulting_value, display_name, client_id, created_at`,
        [roomCode, act, delta, resultingValue, name, cid]
      );

      await client.query('COMMIT');

      const actionRecord = actionRes.rows[0];

      // Mirror to memory
      const memRoom = memoryStore.rooms.get(roomCode);
      if (memRoom) memRoom.counter_value = resultingValue;

      const actionsList = memoryStore.counterActions.get(roomCode) || [];
      actionsList.unshift(actionRecord);
      if (actionsList.length > 100) actionsList.pop();
      memoryStore.counterActions.set(roomCode, actionsList);

      return { resultingValue, actionRecord };
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      console.error('[DB] applyCounterAction transaction error:', err.message);
    } finally {
      client.release();
    }
  }

  // Memory fallback transaction simulation (single-threaded event loop)
  const memRoom = memoryStore.rooms.get(roomCode) || { counter_value: 0 };
  let resultingValue = Number(memRoom.counter_value) || 0;

  if (act === 'inc') resultingValue += delta;
  else if (act === 'dec') resultingValue -= delta;
  else if (act === 'reset') resultingValue = 0;
  else if (act === 'set') resultingValue = delta;

  memRoom.counter_value = resultingValue;
  memoryStore.rooms.set(roomCode, memRoom);

  const actionRecord = {
    id: actionSeq++,
    room_code: roomCode,
    action_type: act,
    amount: delta,
    resulting_value: resultingValue,
    display_name: name,
    client_id: cid,
    created_at: new Date().toISOString()
  };

  const actionsList = memoryStore.counterActions.get(roomCode) || [];
  actionsList.unshift(actionRecord);
  if (actionsList.length > 100) actionsList.pop();
  memoryStore.counterActions.set(roomCode, actionsList);

  return { resultingValue, actionRecord };
}

/**
 * Get counter action history for a room
 */
async function getCounterActions(roomCode, limit = 50) {
  await ensureRoom(roomCode);

  if (isConnected && pool) {
    try {
      const res = await pool.query(
        `SELECT id, room_code, action_type, amount, resulting_value, display_name, client_id, created_at
         FROM synapse_counter_actions
         WHERE room_code = $1
         ORDER BY created_at DESC
         LIMIT $2`,
        [roomCode, limit]
      );
      return res.rows;
    } catch (err) {
      console.error('[DB] getCounterActions error:', err.message);
    }
  }

  const list = memoryStore.counterActions.get(roomCode) || [];
  return list.slice(0, limit);
}

/**
 * Save room tasks / ideas
 */
async function saveRoomTasks(roomCode, tasks) {
  await ensureRoom(roomCode);
  const tasksArr = Array.isArray(tasks) ? tasks : [];
  const memRoom = memoryStore.rooms.get(roomCode);
  if (memRoom) memRoom.tasks = tasksArr;

  if (isConnected && pool) {
    try {
      await pool.query(
        `UPDATE synapse_rooms SET tasks = $1::jsonb, updated_at = NOW() WHERE room_code = $2`,
        [JSON.stringify(tasksArr), roomCode]
      );
    } catch (err) {
      console.error('[DB] saveRoomTasks error:', err.message);
    }
  }
}

module.exports = {
  initDb,
  isDbConnected,
  ensureRoom,
  getRoom,
  saveRoomPad,
  savePadVersion,
  getPadVersions,
  getPadVersionById,
  applyCounterAction,
  getCounterActions,
  saveRoomTasks
};
