const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

// === SQLite for leaderboard ===
let db = null;
try {
  const Database = require('better-sqlite3');
  const dbPath = path.join(__dirname, 'leaderboard.db');
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
  console.log('[数据库] SQLite leaderboard ready');
} catch(e) {
  console.log('[数据库] SQLite not available, leaderboard disabled:', e.message);
}

const LEADERBOARD_MAX = 100; // Keep top 100

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('Color Linez WebSocket Server OK');
});

const wss = new WebSocket.Server({ server });
const rooms = {};

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
          // 1) Get best existing score for this uid
          const existing = db.prepare('SELECT MAX(score) as score FROM scores WHERE uid = ?').get(uid);
          const bestScore = existing ? existing.score : 0;
          const finalScore = Math.max(score, bestScore);
          // 2) Delete ALL entries for this uid
          db.prepare('DELETE FROM scores WHERE uid = ?').run(uid);
          // 3) Also delete entries with same name but different uid (user renamed, old uid leftover)
          db.prepare('DELETE FROM scores WHERE name = ? AND uid <> ?').run(name, uid);
          // 4) Insert single best record for this uid+name
          db.prepare('INSERT INTO scores (name, score, date, uid) VALUES (?, ?, ?, ?)').run(name, finalScore, date, uid);
          // 5) Periodic full dedup: keep only best score per name
          if (Math.random() < 0.1) {
            try {
              db.exec(`DELETE FROM scores WHERE id NOT IN (SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY name ORDER BY score DESC, id ASC) as rn FROM scores) WHERE rn = 1)`);
            } catch(de) {}
            db.prepare('DELETE FROM scores WHERE id NOT IN (SELECT id FROM scores ORDER BY score DESC LIMIT ?)').run(LEADERBOARD_MAX * 2);
          }
          ws.send(JSON.stringify({ type: 'score_submitted', score, best: finalScore }));
        } catch(e) {
          console.error('[积分榜] Error saving score:', e.message);
        }
        break;
      }
      case 'get_leaderboard': {
        if (!db) {
          ws.send(JSON.stringify({ type: 'leaderboard', scores: [] }));
          return;
        }
        try {
          // Group by name as safety net against duplicate names from different uids
          const rows = db.prepare(
            'SELECT name, MAX(score) as score, date FROM scores GROUP BY name ORDER BY score DESC LIMIT 20'
          ).all();
          ws.send(JSON.stringify({ type: 'leaderboard', scores: rows }));
        } catch(e) {
          ws.send(JSON.stringify({ type: 'leaderboard', scores: [] }));
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
