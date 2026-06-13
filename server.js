const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

// === SQLite for leaderboard ===
let db = null;
try {
  const Database = require('better-sqlite3');
  const dataDir = '/app/data';
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  const dbPath = path.join(dataDir, 'leaderboard.db');
  db = new Database(dbPath);
  db.exec(`
    CREATE TABLE IF NOT EXISTS scores (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      score INTEGER NOT NULL,
      date TEXT NOT NULL,
      uid TEXT DEFAULT ''
    )
  `);
  // Create index for faster queries
  db.exec('CREATE INDEX IF NOT EXISTS idx_score ON scores(score DESC)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_uid ON scores(uid)');
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_uid_score ON scores(uid, score)');
  // Deduplicate: keep only best score per uid, delete duplicates
  try {
    db.exec(`DELETE FROM scores WHERE id NOT IN (SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY uid ORDER BY score DESC, id ASC) as rn FROM scores) WHERE rn = 1)`);
    console.log('[数据库] Deduplicated scores by uid');
  } catch(e2) { console.log('[数据库] Dedup skip:', e2.message); }
  // Clean up old buggy sync entries: uids with _digit_digit suffix (e.g. h260528_0_1082)
  // These were created by syncLocalScoresToGlobal with per-record unique uids
  try {
    const oldEntries = db.prepare("SELECT id, uid, name, score FROM scores WHERE uid GLOB '*_[0-9]*_[0-9]*'").all();
    if (oldEntries.length > 0) {
      // Group by base uid (everything before the first _digit pattern)
      const bestByBase = {};
      oldEntries.forEach(e => {
        const base = e.uid.replace(/_[0-9]+_[0-9]+$/, '');
        if (!bestByBase[base] || e.score > bestByBase[base].score) {
          bestByBase[base] = e;
        }
      });
      // Delete all old-pattern entries
      db.prepare("DELETE FROM scores WHERE uid GLOB '*_[0-9]*_[0-9]*'").run();
      // Re-insert best entries with clean base uid (only if no clean entry exists)
      Object.entries(bestByBase).forEach(([base, entry]) => {
        const existing = db.prepare('SELECT id FROM scores WHERE uid = ?').get(base);
        if (!existing) {
          db.prepare('INSERT INTO scores (name, score, date, uid) VALUES (?, ?, ?, ?)').run(entry.name, entry.score, entry.date, base);
        }
      });
      console.log(`[数据库] Cleaned ${oldEntries.length} old-pattern entries, restored ${Object.keys(bestByBase).length} best scores`);
    }
  } catch(e3) { console.log('[数据库] Old-pattern cleanup skip:', e3.message); }
  // === Family tree data table ===
  try {
    db.exec(`CREATE TABLE IF NOT EXISTS family_data (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      data TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )`);
    console.log('[数据库] SQLite family data table ready');
  } catch(fe) {
    console.log('[数据库] Family data table error:', fe.message);
  }
  console.log('[数据库] SQLite leaderboard ready');
} catch(e) {
  console.log('[数据库] SQLite not available, leaderboard disabled:', e.message);
}

const LEADERBOARD_MAX = 100; // Keep top 100

const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // Family tree cloud API
  if (req.method === 'GET' && req.url === '/api/family-data') {
    handleFamilyGet(req, res);
    return;
  }
  if (req.method === 'POST' && req.url === '/api/family-data') {
    handleFamilyPost(req, res);
    return;
  }

  res.writeHead(200);
  res.end('Color Linez WebSocket Server OK');
});

// === Family Tree Cloud API Handlers ===
function handleFamilyGet(req, res) {
  if (!db) {
    res.writeHead(503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, msg: 'Database unavailable' }));
    return;
  }
  try {
    const row = db.prepare('SELECT data FROM family_data WHERE id = 1').get();
    if (row) {
      const data = JSON.parse(row.data);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, data }));
    } else {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, msg: 'No data yet' }));
    }
  } catch(e) {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, msg: e.message }));
  }
}

function handleFamilyPost(req, res) {
  if (!db) {
    res.writeHead(503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, msg: 'Database unavailable' }));
    return;
  }
  let body = '';
  req.on('data', c => body += c);
  req.on('end', () => {
    try {
      const { data, role } = JSON.parse(body);
      if (!data || !data.people) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, msg: 'Invalid data' }));
        return;
      }
      if (role === 'guest') {
        res.writeHead(403, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, msg: 'Guests cannot save' }));
        return;
      }
      const now = new Date().toLocaleString('zh-CN');
      db.prepare('INSERT OR REPLACE INTO family_data (id, data, updated_at) VALUES (1, ?, ?)').run(JSON.stringify(data), now);
      console.log('[族谱] Data saved, people count:', data.people.length, 'by role:', role);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, msg: 'Saved', peopleCount: data.people.length }));
    } catch(e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, msg: e.message }));
    }
  });
}

const wss = new WebSocket.Server({ server });
const rooms = {};

// === Online count tracking ===
let onlineCount = 0;
function broadcastOnlineCount() {
  const msg = JSON.stringify({ type: 'online_count', count: onlineCount });
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  });
}

function genCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let s = '';
  for (let i = 0; i < 4; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

function broadcast(room, msg, exclude) {
  const raw = JSON.stringify(msg);
  if (room.host && room.host !== exclude && room.host.readyState === WebSocket.OPEN) {
    room.host.send(raw);
  }
  room.viewers.forEach(v => {
    if (v !== exclude && v.readyState === WebSocket.OPEN) v.send(raw);
  });
}

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.role = null;
  ws.playerName = '匿名';
  onlineCount++;
  broadcastOnlineCount();

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch(e) { return; }

    switch(data.type) {
      case 'create_room': {
        let code;
        do { code = genCode(); } while (rooms[code]);
        rooms[code] = { host: ws, viewers: [], relayViewer: null };
        ws.roomCode = code;
        ws.role = 'host';
        ws.playerName = data.name || '主机';
        ws.send(JSON.stringify({ type: 'room_created', code }));
        console.log('[房间创建] ' + code + ' by ' + ws.playerName);
        break;
      }
      case 'join_room': {
        const room = rooms[data.code];
        if (!room) {
          ws.send(JSON.stringify({ type: 'error', msg: '房间不存在' }));
          return;
        }
        if (room.host.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'error', msg: '房间已关闭' }));
          delete rooms[data.code];
          return;
        }
        room.viewers.push(ws);
        ws.roomCode = data.code;
        ws.role = 'viewer';
        ws.playerName = data.name || '观众';
        // Send joined confirmation with current state
        ws.send(JSON.stringify({ type: 'joined', code: data.code }));
        // Notify host
        room.host.send(JSON.stringify({ type: 'viewer_joined', name: ws.playerName, count: room.viewers.length }));
        // Notify all viewers about count
        const countMsg = JSON.stringify({ type: 'viewer_count', count: room.viewers.length });
        room.viewers.forEach(v => v.send(countMsg));
        console.log('[加入房间] ' + data.code + ' <- ' + ws.playerName);
        break;
      }
      case 'chat': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        broadcast(room, { type: 'chat', from: ws.playerName, text: data.text, isSystem: false });
        break;
      }
      case 'game_state': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        // Forward to all viewers
        const stateMsg = JSON.stringify({ type: 'game_state', state: data.state });
        room.viewers.forEach(v => {
          if (v.readyState === WebSocket.OPEN) v.send(stateMsg);
        });
        break;
      }
      case 'next_colors': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        const msg = JSON.stringify({ type: 'next_colors', colors: data.colors });
        room.viewers.forEach(v => {
          if (v.readyState === WebSocket.OPEN) v.send(msg);
        });
        break;
      }
      case 'score_update': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        const msg = JSON.stringify({ type: 'score_update', score: data.score, msg: data.msg });
        room.viewers.forEach(v => {
          if (v.readyState === WebSocket.OPEN) v.send(msg);
        });
        break;
      }
      case 'game_over': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        broadcast(room, { type: 'game_over', score: data.score });
        break;
      }
      case 'relay_request': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        room.host.send(JSON.stringify({ type: 'relay_request', from: ws.playerName }));
        break;
      }
      case 'relay_accept': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        room.relayViewer = ws;
        ws.role = 'relayer';
        broadcast(room, { type: 'relay_accepted', name: ws.playerName });
        break;
      }
      case 'relay_reject': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        // Tell the requester that relay was rejected
        if (room.relayViewer) {
          room.relayViewer.send(JSON.stringify({ type: 'relay_rejected' }));
        }
        // Also notify anyone who might have requested
        broadcast(room, { type: 'relay_rejected' });
        break;
      }
      case 'relay_move': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        room.host.send(JSON.stringify({ type: 'relay_move', from: ws.playerName, fromR: data.fromR, fromC: data.fromC, toR: data.toR, toC: data.toC }));
        break;
      }
      case 'relay_stop': {
        const room = rooms[ws.roomCode];
        if (!room) return;
        if (room.relayViewer) room.relayViewer.role = 'viewer';
        room.relayViewer = null;
        broadcast(room, { type: 'relay_stopped' });
        break;
      }
      case 'rename': {
        ws.playerName = data.name;
        break;
      }
      // === Leaderboard ===
      case 'submit_score': {
        if (!db) return;
        const name = (data.name || '匿名').substring(0, 8);
        const score = parseInt(data.score) || 0;
        const uid = (data.uid || '').substring(0, 20);
        if (score <= 0) return;
        const date = new Date().toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
        try {
          // Prevent duplicate: same uid+score combo already exists, skip
          const dup = db.prepare('SELECT id FROM scores WHERE uid = ? AND score = ?').get(uid, score);
          if (dup) {
            ws.send(JSON.stringify({ type: 'score_submitted', score, best: score, dup: true }));
            break;
          }
          // Each game is a separate record, same person can dominate the board
          db.prepare('INSERT INTO scores (name, score, date, uid) VALUES (?, ?, ?, ?)').run(name, score, date, uid);
          // Cleanup: only keep top N records to prevent unbounded growth
          db.prepare('DELETE FROM scores WHERE id NOT IN (SELECT id FROM scores ORDER BY score DESC LIMIT ?)').run(LEADERBOARD_MAX * 2);
          ws.send(JSON.stringify({ type: 'score_submitted', score, best: score }));
        } catch(e) {
          console.error('[积分榜] Error saving score:', e.message);
        }
        break;
      }
      case 'delete_score': {
        if (!db) return;
        // Admin only: delete a name from leaderboard
        const delName = (data.name || '').substring(0, 8);
        const delKey = data.key || '';
        if (delKey !== 'face_wall_2026' || !delName) return;
        try {
          const result = db.prepare('DELETE FROM scores WHERE name = ?').run(delName);
          console.log('[积分榜] Deleted ' + delName + ', rows affected: ' + result.changes);
          ws.send(JSON.stringify({ type: 'score_deleted', name: delName, removed: result.changes }));
        } catch(e) {
          console.error('[积分榜] Delete error:', e.message);
        }
        break;
      }
      case 'get_leaderboard': {
        if (!db) {
          ws.send(JSON.stringify({ type: 'leaderboard', scores: [] }));
          return;
        }
        try {
          // Each uid is a separate entry, same name can appear multiple times
          const rows = db.prepare(
            'SELECT name, score, date FROM scores ORDER BY score DESC LIMIT 20'
          ).all();
          ws.send(JSON.stringify({ type: 'leaderboard', scores: rows }));
        } catch(e) {
          ws.send(JSON.stringify({ type: 'leaderboard', scores: [] }));
        }
        break;
      }
      case 'get_scores_by_name': {
        if (!db) {
          ws.send(JSON.stringify({ type: 'my_scores', scores: [] }));
          return;
        }
        const searchName = (data.name || '').substring(0, 8);
        if (!searchName) {
          ws.send(JSON.stringify({ type: 'my_scores', scores: [] }));
          return;
        }
        try {
          const rows = db.prepare(
            'SELECT name, score, date FROM scores WHERE name = ? ORDER BY score DESC LIMIT 20'
          ).all(searchName);
          ws.send(JSON.stringify({ type: 'my_scores', scores: rows }));
        } catch(e) {
          ws.send(JSON.stringify({ type: 'my_scores', scores: [] }));
        }
        break;
      }
      case 'request_state': {
        const room = rooms[ws.roomCode];
        if (!room || ws.role !== 'viewer') return;
        // Ask host to send current state
        room.host.send(JSON.stringify({ type: 'send_state' }));
        break;
      }
    }
  });

  ws.on('close', () => {
    onlineCount = Math.max(0, onlineCount - 1);
    broadcastOnlineCount();
    const room = rooms[ws.roomCode];
    if (!room) return;
    if (ws.role === 'host') {
      // Host left, close room
      room.viewers.forEach(v => {
        v.send(JSON.stringify({ type: 'room_closed' }));
        v.roomCode = null;
        v.role = null;
      });
      delete rooms[ws.roomCode];
      console.log('[房间关闭] ' + ws.roomCode);
    } else {
      room.viewers = room.viewers.filter(v => v !== ws);
      if (room.relayViewer === ws) {
        room.relayViewer = null;
        room.host.send(JSON.stringify({ type: 'relay_stopped' }));
      }
      if (room.host.readyState === WebSocket.OPEN) {
        room.host.send(JSON.stringify({ type: 'viewer_left', name: ws.playerName, count: room.viewers.length }));
      }
      const countMsg = JSON.stringify({ type: 'viewer_count', count: room.viewers.length });
      room.viewers.forEach(v => v.send(countMsg));
      console.log('[离开房间] ' + ws.roomCode + ' <- ' + ws.playerName);
    }
  });
});

const port = process.env.PORT || 9000;
server.listen(port, () => {
  console.log('Color Linez WebSocket Server running on port ' + port);
});
