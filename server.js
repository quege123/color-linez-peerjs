const { PeerServer } = require('peer');
const port = process.env.PORT || 9000;
const server = PeerServer({
  port: port,
  path: '/myapp',
  allow_discovery: true,
  cors_options: { origin: '*' }
});
server.on('connection', (client) => {
  console.log('[连接] ' + client.getId());
});
server.on('disconnect', (client) => {
  console.log('[断开] ' + client.getId());
});
console.log('Color Linez PeerJS 信令服务器已启动，端口: ' + port);
