import { WebSocketServer } from 'ws';

// Stage 1 helper: echoes every message back to the sender, unchanged.
const PORT = 8080;

const wss = new WebSocketServer({ port: PORT });

wss.on('connection', (socket, request) => {
	console.log(`[open]    origin=${request.headers.origin ?? '-'} clients=${wss.clients.size}`);

	socket.on('message', (data, isBinary) => {
		const preview = isBinary ? `<binary ${Buffer.byteLength(data as Buffer)} bytes>` : data.toString();
		console.log(`[message] ${preview}`);
		socket.send(data as Buffer, { binary: isBinary });
	});

	socket.on('close', (code, reason) => {
		console.log(`[close]   code=${code} reason="${reason.toString()}" clients=${wss.clients.size}`);
	});

	socket.on('error', (error) => {
		console.error('[error]  ', error.message);
	});
});

console.log(`Sup. Echo server listening on ws://localhost:${PORT}`);
