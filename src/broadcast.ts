import { WebSocket, WebSocketServer } from 'ws';

// Stage 2 helper: every text message is sent to ALL connected clients, including the sender.
// Sending "/burst" makes the server broadcast 5 messages back-to-back (for the lastMessage experiment).
const PORT = 8081;
const BURST_SIZE = 5;

const wss = new WebSocketServer({ port: PORT });

const broadcast = (text: string) => {
	for (const client of wss.clients) {
		if (client.readyState === WebSocket.OPEN) {
			client.send(text);
		}
	}
};

wss.on('connection', (socket, request) => {
	console.log(`[open]    origin=${request.headers.origin ?? '-'} clients=${wss.clients.size}`);

	socket.on('message', (data, isBinary) => {
		if (isBinary) {
			console.log('[ignored] binary message');

			return;
		}

		const text = data.toString();
		console.log(`[message] ${text}`);

		if (text === '/burst') {
			for (let i = 1; i <= BURST_SIZE; i++) {
				broadcast(`burst ${i}/${BURST_SIZE}`);
			}

			return;
		}

		broadcast(text);
	});

	socket.on('close', (code, reason) => {
		console.log(`[close]   code=${code} reason="${reason.toString()}" clients=${wss.clients.size}`);
	});

	socket.on('error', (error) => {
		console.error('[error]  ', error.message);
	});
});

console.log(`Broadcast server listening on ws://localhost:${PORT}`);
