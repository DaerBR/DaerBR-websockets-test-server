import { WebSocket, WebSocketServer } from 'ws';

// Stage 2 helper: every text message is sent to ALL connected clients, including the sender.
// Sending "/burst" makes the server broadcast 5 messages back-to-back (for the lastMessage experiment).
//
// Stage 4 heartbeat (lessons/stage-4-reliability.md, §3):
//   "__ping__" -> the server answers "__pong__" to the sender only (never broadcast)
//   "/freeze"  -> the server keeps the connection open but stops answering this client's pings,
//                 which looks to the client exactly like a dead connection
//
// Stage 4 catch-up (lessons/stage-4-reliability.md, §7):
//   Every broadcast message gets an increasing id and is kept in a short history.
//   "__resume__:<lastId>" -> opts this connection in to JSON frames {"id":7,"text":"hi"}
//                            and replays every history message with id > lastId to it.
//   Connections that never send it (the Stage 2 /chat page) keep getting plain strings.
//
// Stage 4 duplicates (lessons/stage-4-reliability.md, §8):
//   "__reset__" -> sent before the replay when the client's lastId is one this server never issued
//                  (the server restarted and its ids began again at 1): "forget your lastId"
//   "/dupe"     -> re-sends the last 3 history messages to the sender only, to test duplicate handling
const PING = '__ping__';
const PONG = '__pong__';
const RESUME_PREFIX = '__resume__:';
const RESET = '__reset__';
const DUPE_COUNT = 3;
const PORT = 8081;
const BURST_SIZE = 5;
const HISTORY_SIZE = 100;

type ChatEvent = { id: number; text: string };

const wss = new WebSocketServer({ port: PORT });
const history: ChatEvent[] = [];
const idClients = new WeakSet<WebSocket>();
let nextId = 1;

const sendEvent = (client: WebSocket, event: ChatEvent) => {
	client.send(idClients.has(client) ? JSON.stringify(event) : event.text);
};

const broadcast = (text: string) => {
	const event: ChatEvent = { id: nextId++, text };
	history.push(event);

	if (history.length > HISTORY_SIZE) {
		history.shift();
	}

	for (const client of wss.clients) {
		if (client.readyState === WebSocket.OPEN) {
			sendEvent(client, event);
		}
	}
};

wss.on('connection', (socket, request) => {
	let frozen = false;
	console.log(`[open]    origin=${request.headers.origin ?? '-'} clients=${wss.clients.size}`);

	socket.on('message', (data, isBinary) => {
		if (isBinary) {
			console.log('[ignored] binary message');

			return;
		}

		const text = data.toString();

		if (text === PING) {
			if (frozen) {
				console.log('[ping]    ignored (frozen)');

				return;
			}

			socket.send(PONG);

			return;
		}

		if (text.startsWith(RESUME_PREFIX)) {
			let lastId = Number(text.slice(RESUME_PREFIX.length)) || 0;

			if (lastId >= nextId) {
				console.log(`[resume]  id=${lastId} was never issued (server restarted?), sending reset`);
				socket.send(RESET);
				lastId = 0;
			}

			const missed = history.filter((event) => event.id > lastId);
			idClients.add(socket);
			missed.forEach((event) => sendEvent(socket, event));
			console.log(`[resume]  after id=${lastId}, replayed ${missed.length}`);

			return;
		}

		console.log(`[message] ${text}`);

		if (text === '/freeze') {
			frozen = true;
			console.log('[freeze]  this client gets no more pongs');

			return;
		}

		if (text === '/dupe') {
			const recent = history.slice(-DUPE_COUNT);

			if (idClients.has(socket)) {
				recent.forEach((event) => sendEvent(socket, event));
			}

			console.log(`[dupe]    re-sent ${recent.length} message(s) to this client`);

			return;
		}

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
