import express from 'express';
import http from 'node:http';
import { WebSocket, WebSocketServer } from 'ws';

// Stage 5 server, rebuilt step by step (lessons/stage-5-server.md in the client repo).
// 5a: accepts connections, keeps per-client state in a Map.
// 5b: broadcasts every chat message to all clients (sender included).
// 5c: speaks the Stage 4 protocol. Control messages are answered to the sender only, never broadcast:
//   "__ping__"          -> "__pong__" (unless this client is frozen)
//   "__resume__:<id>"   -> switches this client to JSON frames {"id":7,"text":"hi"} and replays history after <id>;
//                          sends "__reset__" first if <id> was never issued (the server restarted)
//   "/freeze"           -> stop answering this client's pings (looks like a dead connection)
//   "/dupe"             -> re-sends the last DUPE_COUNT history messages to this client
//   "/who"              -> lists the connected clients
//   "/burst"            -> broadcasts BURST_SIZE messages back to back
// 5d: pings every client with a protocol ping frame (browsers answer automatically) every HEARTBEAT_INTERVAL_MS,
//     and terminates clients that didn't answer the previous ping.
// 5e: one HTTP server on PORT for both. Normal requests go to Express (GET /health, GET /messages?after=<id>);
//     "Upgrade: websocket" requests go through our upgrade handler, which accepts only WS_PATH.
// 5f: graceful shutdown on SIGINT (Ctrl+C) / SIGTERM: stop listening, close every client with 1001,
//     exit when all are gone or after SHUTDOWN_TIMEOUT_MS. A second signal exits at once.
const PORT = 8081;
const HEARTBEAT_INTERVAL_MS = 10000;
const BURST_SIZE = 5;
const DUPE_COUNT = 3;
const HISTORY_SIZE = 100;
const PING = '__ping__';
const PONG = '__pong__';
const RESET = '__reset__';
const RESUME_PREFIX = '__resume__:';
const WS_PATH = '/';
const SHUTDOWN_TIMEOUT_MS = 3000;
const REPEAT_SIGNAL_GRACE_MS = 1000;

type ChatEvent = { id: number; text: string };
type ClientState = { connectedAt: number; frozen: boolean; id: number; isAlive: boolean; wantsJson: boolean };

// Per-client state: one entry per connection. Added on connection, removed on close (otherwise it leaks).
const clients = new Map<WebSocket, ClientState>();
let nextClientId = 1;

// Server-wide state: shared by everyone, not tied to a connection.
const history: ChatEvent[] = [];
let nextId = 1;
const startedAt = Date.now();
let isShuttingDown = false;
let shutdownStartedAt = 0;

// The same event, in the format this client asked for.
const sendEvent = (socket: WebSocket, client: ClientState, event: ChatEvent) => {
	socket.send(client.wantsJson ? JSON.stringify(event) : event.text);
};

// "Send to everyone" is just a loop. Includes the sender: the server decides the order everyone sees.
const broadcast = (text: string) => {
	const event: ChatEvent = { id: nextId++, text };
	history.push(event);

	if (history.length > HISTORY_SIZE) {
		history.shift();
	}

	let reached = 0;

	for (const [socket, client] of clients) {
		if (socket.readyState === WebSocket.OPEN) {
			sendEvent(socket, client, event);
			reached += 1;
		}
	}

	console.log(`  -> #${event.id} broadcast to ${reached} client(s)`);
};

const resume = (socket: WebSocket, client: ClientState, lastIdText: string) => {
	let lastId = Number(lastIdText) || 0;
	client.wantsJson = true;

	if (lastId >= nextId) {
		console.log(`[#${client.id}] resume: id=${lastId} was never issued (server restarted?), sending reset`);
		socket.send(RESET);
		lastId = 0;
	}

	const missed = history.filter((event) => event.id > lastId);
	missed.forEach((event) => sendEvent(socket, client, event));
	console.log(`[#${client.id}] resume after id=${lastId}, replayed ${missed.length}`);
};

const describeClients = (asking: ClientState) => {
	const now = Date.now();

	return [...clients.values()]
		.map((other) => {
			const seconds = Math.round((now - other.connectedAt) / 1000);

			return other === asking ? `#${other.id} (you, ${seconds}s)` : `#${other.id} (${seconds}s)`;
		})
		.join(', ');
};

// HTTP routes. They read the same state as the WebSocket code: that's the point of one process.
const app = express();

app.get('/health', (_request, response) => {
	response.json({
		clients: clients.size,
		historySize: history.length,
		uptimeSeconds: Math.round((Date.now() - startedAt) / 1000),
	});
});

// "Refetching state over HTTP" (Stage 4 §7): the same data a resume replays, without a socket.
app.get('/messages', (request, response) => {
	const after = Number(request.query.after) || 0;
	response.json(history.filter((event) => event.id > after));
});

// Normal HTTP requests go to Express. Owns the port now.
const server = http.createServer(app);

// noServer: ws opens no port of its own; it only does handshakes we hand to it in 'upgrade'.
const wss = new WebSocketServer({ noServer: true });

// Every request with "Upgrade: websocket" lands here instead of in Express. `socket` is raw TCP, not answered yet.
server.on('upgrade', (request, socket, head) => {
	const path = new URL(request.url ?? '/', 'http://localhost').pathname;

	if (isShuttingDown) {
		console.log('[upgrade] rejected: shutting down');
		socket.write('HTTP/1.1 503 Service Unavailable\r\n\r\n');
		socket.destroy();

		return;
	}

	if (path !== WS_PATH) {
		console.log(`[upgrade] rejected: unknown path ${path}`);
		// Before the handshake we can only answer in plain HTTP, then hang up.
		socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
		socket.destroy();

		return;
	}

	// ws answers 101 Switching Protocols, then we fire the usual 'connection' event, so 5a–5d work unchanged.
	wss.handleUpgrade(request, socket, head, (ws) => {
		wss.emit('connection', ws, request);
	});
});

// One timer for the whole server. A client still not alive since the last round never answered our ping.
const heartbeat = setInterval(() => {
	for (const [socket, client] of clients) {
		if (!client.isAlive) {
			console.log(`[#${client.id}] no pong since the last ping: terminating`);
			// No close handshake: a dead client would never answer it. 'close' still fires, with code 1006.
			socket.terminate();
			continue;
		}

		client.isAlive = false;
		socket.ping();
	}
}, HEARTBEAT_INTERVAL_MS);

wss.on('close', () => {
	clearInterval(heartbeat);
});

server.on('listening', () => {
	console.log(`Server listening on http://localhost:${PORT} (HTTP) and ws://localhost:${PORT}${WS_PATH} (WebSocket)`);
});

// The server itself failed, e.g. the port is already in use (is `pnpm broadcast` still running?).
server.on('error', (error) => {
	console.error('[server error]', error.message);
});

server.listen(PORT);

// Runs once per client, after its handshake. `socket` is this client's connection.
wss.on('connection', (socket) => {
	const client: ClientState = {
		connectedAt: Date.now(),
		frozen: false,
		id: nextClientId++,
		isAlive: true,
		wantsJson: false,
	};
	clients.set(socket, client);
	console.log(`[#${client.id}] connected (clients: ${clients.size})`);

	// The browser sends this pong frame by itself, in reply to our socket.ping().
	socket.on('pong', () => {
		client.isAlive = true;
	});

	// The router: the first matching check handles the message and returns.
	socket.on('message', (data, isBinary) => {
		if (isBinary) {
			console.log(`[#${client.id}] binary message ignored`);

			return;
		}

		// `data` is a Buffer (raw bytes); text frames have to be decoded.
		const text = data.toString();

		// Not logged when answered: it comes every 10 s from every /shared-chat socket.
		if (text === PING) {
			if (client.frozen) {
				console.log(`[#${client.id}] ping ignored (frozen)`);

				return;
			}

			socket.send(PONG);

			return;
		}

		if (text.startsWith(RESUME_PREFIX)) {
			resume(socket, client, text.slice(RESUME_PREFIX.length));

			return;
		}

		console.log(`[#${client.id}] message: ${text}`);

		if (text === '/freeze') {
			client.frozen = true;
			console.log(`[#${client.id}] frozen: no more pongs`);

			return;
		}

		if (text === '/dupe') {
			const recent = history.slice(-DUPE_COUNT);
			recent.forEach((event) => sendEvent(socket, client, event));
			console.log(`[#${client.id}] re-sent ${recent.length} message(s)`);

			return;
		}

		if (text === '/who') {
			socket.send(`Connected: ${describeClients(client)}`);

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
		clients.delete(socket);
		const seconds = ((Date.now() - client.connectedAt) / 1000).toFixed(1);
		console.log(
			`[#${client.id}] closed code=${code} reason="${reason.toString()}" after ${seconds}s (clients: ${clients.size})`,
		);

		// During a shutdown, the last client to leave lets the process exit.
		if (isShuttingDown && clients.size === 0) {
			console.log('[shutdown] all clients closed, bye');
			process.exit(0);
		}
	});

	// Without an 'error' listener, Node would throw and crash the whole process. 'close' follows anyway.
	socket.on('error', (error) => {
		console.error(`[#${client.id}] error:`, error.message);
	});
});

const shutdown = (signal: NodeJS.Signals) => {
	if (isShuttingDown) {
		// One Ctrl+C reaches us more than once: the terminal signals pnpm, tsx and node together,
		// and each wrapper passes it on. Only a later, real second Ctrl+C forces the exit.
		if (Date.now() - shutdownStartedAt < REPEAT_SIGNAL_GRACE_MS) {
			return;
		}

		console.log(`[shutdown] second ${signal}: exiting now`);
		process.exit(1);
	}

	isShuttingDown = true;
	shutdownStartedAt = Date.now();
	console.log(`[shutdown] ${signal}: closing ${clients.size} client(s) with 1001`);

	// 1. No new connections. 2. 'close' on wss clears the heartbeat (see 5d).
	server.close();
	wss.close();

	if (clients.size === 0) {
		console.log('[shutdown] no clients, bye');
		process.exit(0);
	}

	// 3. Say goodbye. Each client answers, its 'close' handler removes it, and the last one exits (4).
	for (const socket of clients.keys()) {
		socket.close(1001, 'server shutting down');
	}

	// 5. A dead client never answers: don't wait for it forever.
	setTimeout(() => {
		console.log(`[shutdown] ${clients.size} client(s) didn't answer in time: terminating, bye`);

		for (const socket of clients.keys()) {
			socket.terminate();
		}

		process.exit(0);
	}, SHUTDOWN_TIMEOUT_MS);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
