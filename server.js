const http = require('http');
const WebSocket = require('ws');
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
        ws.send(JSON.stringify({ type: 'joined', code: data.code }));
        room.host.send(JSON.stringify({ type: 'viewer_joined', name: ws.playerName, count: room.viewers.length }));
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
        const stateMsg = JSON.stringify({ type: 'game_state', state: data.state });
        room.viewers.forEach(v => {
          if (v.readyState === WebSocket.OPEN) v.send(stateMsg);
        });
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
    }
  });
  ws.on('close', () => {
    const room = rooms[ws.roomCode];
    if (!room) return;
    if (ws.role === 'host') {
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
