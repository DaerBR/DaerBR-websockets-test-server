import express from 'express';
import http from 'node:http';
import { WebSocket, WebSocketServer } from 'ws';
import { getUser, login, logout } from './auth/authController.js';
import { getSessionById, readSessionId, sessionEvents } from './auth/sessions.js';

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
// 5e: one HTTP server on PORT for both. Normal requests go to Express (GET /api/health, GET /api/messages?after=<id>);
//     "Upgrade: websocket" requests go through our upgrade handler, which accepts only WS_PATH.
// 5f: graceful shutdown on SIGINT (Ctrl+C) / SIGTERM: stop listening, close every client with 1001,
//     exit when all are gone or after SHUTDOWN_TIMEOUT_MS. A second signal exits at once.
//
// Stage 6 (lessons/stage-6-auth-security.md in the client repo):
// 6a: everything the app talks to has a prefix (/api/*, /ws), so the Vite dev server can proxy it
//     and the browser sees a single origin.
// 6b: fake login with a real session cookie. POST /api/login {name, password} -> Set-Cookie: session=<random id>;
//     GET /api/user -> {name} or {name: null}; POST /api/logout forgets the session. The socket doesn't check it yet.
//     Handlers: auth/authController.ts; the session store: auth/sessions.ts.
// 6c: the socket needs a login. The handshake carries the session cookie; without a valid session the server
//     accepts the handshake and closes at once with UNAUTHORIZED_CLOSE_CODE, so the client can tell "go log in"
//     from "server down" (a refused handshake would only show up as 1006).
// 6d: identity comes from the server. Clients send only text; the server adds the author from the sender's session.
//     JSON clients get two kinds of frames:
//       {"type":"chat","id":7,"author":"Gena","text":"hi"}  -> everyone, kept in history (resume, /dupe, /api/messages)
//       {"type":"system","text":"..."}                      -> one client, no id, not in history (e.g. /who)
//     Plain-text clients get "Gena: hi" and the bare system text.
// 6e: Origin check against Cross-Site WebSocket Hijacking. Browsers attach the session cookie to handshakes from
//     any page, so the upgrade handler accepts only pages from ALLOWED_ORIGINS (403 otherwise, or with no Origin).
// 6f: limits per connection, three layers:
//     - transport: frames over MAX_PAYLOAD_BYTES -> ws closes with 1009 before reading them into memory;
//     - app validation: empty text is ignored, text over MAX_TEXT_LENGTH gets a system "too long" (socket stays open);
//     - rate limit: a token bucket (RATE_LIMIT_BURST, refilled RATE_LIMIT_PER_SECOND). Rejected messages get a system
//       "Slow down"; MAX_RATE_LIMIT_STRIKES rejections in a row close the connection with 1008. Pings don't count.
// 6g: authorization on every message, not just at connect. Each message re-reads the session by id: gone -> 4401.
//     "/kick <name>" is admin-only (role from the session *now*): closes that user's sockets with 4403.
//     A non-admin gets a system "Not allowed"; the socket stays open.
// 6h: eager revocation. When a session ends (logout), sessions.ts emits 'ended' and we close that session's sockets
//     with 4401 right away, instead of waiting for their next message (6g's check stays as the safety net).
const PORT = 8081;
const HEARTBEAT_INTERVAL_MS = 10000;
const BURST_SIZE = 5;
const DUPE_COUNT = 3;
const HISTORY_SIZE = 100;
const PING = '__ping__';
const PONG = '__pong__';
const RESET = '__reset__';
const RESUME_PREFIX = '__resume__:';
const WS_PATH = '/ws';
const SHUTDOWN_TIMEOUT_MS = 3000;
const REPEAT_SIGNAL_GRACE_MS = 1000;
// App close code (4000-4999 is ours), mirrors HTTP 401.
const UNAUTHORIZED_CLOSE_CODE = 4401;
// Pages allowed to open the socket. Exact matches only. In production: the real origin(s), from an env variable.
const ALLOWED_ORIGINS = new Set(['http://localhost:5173']);
// The ws default is 100 MiB. Generous on purpose: a safety net, not a UX rule.
const MAX_PAYLOAD_BYTES = 16 * 1024;
const MAX_TEXT_LENGTH = 500;
const RATE_LIMIT_BURST = 10;
const RATE_LIMIT_PER_SECOND = 2;
const MAX_RATE_LIMIT_STRIKES = 20;
const POLICY_VIOLATION_CLOSE_CODE = 1008;
// App close code, mirrors HTTP 403: we know who you are, and you're out.
const FORBIDDEN_CLOSE_CODE = 4403;
const KICK_PREFIX = '/kick ';

type ChatEvent = { author: string; id: number; text: string; type: 'chat' };
type SystemEvent = { text: string; type: 'system' };
type ClientState = {
	connectedAt: number;
	frozen: boolean;
	id: number;
	isAlive: boolean;
	lastRefillAt: number;
	name: string;
	// Rejected messages in a row; any accepted message resets it.
	rateLimitStrikes: number;
	// Not the session itself: a copy would go stale. Permissions are looked up by id on each message.
	sessionId: string;
	// The token bucket: one token per message.
	tokens: number;
	wantsJson: boolean;
};

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
	socket.send(client.wantsJson ? JSON.stringify(event) : `${event.author}: ${event.text}`);
};

// A message from the server to one client. Not a chat event: no id, never stored in history.
const sendSystem = (socket: WebSocket, client: ClientState, text: string) => {
	const event: SystemEvent = { text, type: 'system' };
	socket.send(client.wantsJson ? JSON.stringify(event) : text);
};

// "Send to everyone" is just a loop. Includes the sender: the server decides the order everyone sees.
// The author comes from the sender's session (stored at connection time), never from the message itself.
const broadcast = (sender: ClientState, text: string) => {
	const event: ChatEvent = { author: sender.name, id: nextId++, text, type: 'chat' };
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

// Refilled lazily: on each message, add what accumulated since the last one. No timers needed.
const takeToken = (client: ClientState) => {
	const now = Date.now();
	const refill = ((now - client.lastRefillAt) / 1000) * RATE_LIMIT_PER_SECOND;
	client.tokens = Math.min(RATE_LIMIT_BURST, client.tokens + refill);
	client.lastRefillAt = now;

	if (client.tokens < 1) {
		return false;
	}

	client.tokens -= 1;

	return true;
};

const describeClients = (asking: ClientState) => {
	const now = Date.now();

	return [...clients.values()]
		.map((other) => {
			const seconds = Math.round((now - other.connectedAt) / 1000);

			const label = `#${other.id} ${other.name}`;

			return other === asking ? `${label} (you, ${seconds}s)` : `${label} (${seconds}s)`;
		})
		.join(', ');
};

// HTTP routes. They read the same state as the WebSocket code: that's the point of one process.
const app = express();

// Parses JSON request bodies into request.body (undefined for other content types).
app.use(express.json());

app.get('/api/health', (_request, response) => {
	response.json({
		clients: clients.size,
		historySize: history.length,
		uptimeSeconds: Math.round((Date.now() - startedAt) / 1000),
	});
});

// "Refetching state over HTTP" (Stage 4 §7): the same data a resume replays, without a socket.
app.get('/api/messages', (request, response) => {
	const after = Number(request.query.after) || 0;
	response.json(history.filter((event) => event.id > after));
});

// Auth: handlers in auth/authController.ts.
app.get('/api/user', getUser);
app.post('/api/login', login);
app.post('/api/logout', logout);

// Normal HTTP requests go to Express. Owns the port now.
const server = http.createServer(app);

// noServer: ws opens no port of its own; it only does handshakes we hand to it in 'upgrade'.
const wss = new WebSocketServer({ maxPayload: MAX_PAYLOAD_BYTES, noServer: true });

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

	// Set by the browser, can't be changed by page scripts. Through the Vite proxy it's still the app's origin.
	const origin = request.headers.origin;

	if (origin === undefined || !ALLOWED_ORIGINS.has(origin)) {
		console.log(`[upgrade] rejected: origin ${origin ?? '(none)'} is not allowed`);
		socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
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

// A session ended: every socket opened with it is out, now. ClientState.sessionId (6g) is how we find them.
sessionEvents.on('ended', (sessionId) => {
	for (const [socket, client] of clients) {
		if (client.sessionId === sessionId) {
			console.log(`[#${client.id}] session ended: closing with 4401`);
			socket.close(UNAUTHORIZED_CLOSE_CODE, 'logged out');
		}
	}
});

server.on('listening', () => {
	console.log(`Server listening on http://localhost:${PORT} (HTTP) and ws://localhost:${PORT}${WS_PATH} (WebSocket)`);
});

// The server itself failed, e.g. the port is already in use (is `pnpm broadcast` still running?).
server.on('error', (error) => {
	console.error('[server error]', error.message);
});

server.listen(PORT);

// Runs once per client, after its handshake. `socket` is this client's connection,
// `request` is the handshake's HTTP request: its headers include the Cookie the browser attached.
wss.on('connection', (socket, request) => {
	const sessionId = readSessionId(request.headers.cookie);
	const session = sessionId === null ? null : getSessionById(sessionId);

	if (sessionId === null || !session) {
		console.log('[connection] no valid session: closing with 4401');
		// Not added to `clients`: it never becomes a chat client.
		socket.close(UNAUTHORIZED_CLOSE_CODE, 'unauthorized');

		return;
	}

	const client: ClientState = {
		connectedAt: Date.now(),
		frozen: false,
		id: nextClientId++,
		isAlive: true,
		lastRefillAt: Date.now(),
		name: session.name,
		rateLimitStrikes: 0,
		sessionId,
		tokens: RATE_LIMIT_BURST,
		wantsJson: false,
	};
	clients.set(socket, client);
	console.log(`[#${client.id}] "${client.name}" connected (clients: ${clients.size})`);

	// The browser sends this pong frame by itself, in reply to our socket.ping().
	socket.on('pong', () => {
		client.isAlive = true;
	});

	// The router: the first matching check handles the message and returns.
	socket.on('message', (data, isBinary) => {
		// After we called close(), frames already on their way still arrive. The connection is over: ignore them.
		if (socket.readyState !== WebSocket.OPEN) {
			return;
		}

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

		// Everything below costs a token. The heartbeat above never does: limiting it would look like a dead connection.
		if (!takeToken(client)) {
			client.rateLimitStrikes += 1;

			if (client.rateLimitStrikes >= MAX_RATE_LIMIT_STRIKES) {
				console.log(`[#${client.id}] rate limit: ${client.rateLimitStrikes} rejected in a row, closing with 1008`);
				socket.close(POLICY_VIOLATION_CLOSE_CODE, 'rate limit exceeded');

				return;
			}

			console.log(`[#${client.id}] rate limit: rejected "${text.slice(0, 40)}" (strike ${client.rateLimitStrikes})`);
			sendSystem(socket, client, 'Slow down: too many messages');

			return;
		}

		client.rateLimitStrikes = 0;

		// Authorization uses the session as it is *now*: logged out or expired since the handshake -> out.
		const session = getSessionById(client.sessionId);

		if (!session) {
			console.log(`[#${client.id}] session ended: closing with 4401`);
			socket.close(UNAUTHORIZED_CLOSE_CODE, 'session ended');

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
			sendSystem(socket, client, `Connected: ${describeClients(client)}`);

			return;
		}

		if (text.startsWith(KICK_PREFIX)) {
			if (session.role !== 'admin') {
				console.log(`[#${client.id}] not allowed: ${text}`);
				sendSystem(socket, client, 'Not allowed: /kick is for admins');

				return;
			}

			const target = text.slice(KICK_PREFIX.length).trim();
			let kicked = 0;

			for (const [otherSocket, other] of clients) {
				if (other.name === target) {
					otherSocket.close(FORBIDDEN_CLOSE_CODE, 'kicked by an admin');
					kicked += 1;
				}
			}

			console.log(`[#${client.id}] kicked ${kicked} connection(s) of "${target}"`);
			sendSystem(socket, client, `Kicked ${kicked} connection(s) of ${target}`);

			return;
		}

		if (text === '/burst') {
			for (let i = 1; i <= BURST_SIZE; i++) {
				broadcast(client, `burst ${i}/${BURST_SIZE}`);
			}

			return;
		}

		// App validation: the transport limit let it through, but is it a valid chat message?
		const chatText = text.trim();

		if (chatText === '') {
			return;
		}

		if (chatText.length > MAX_TEXT_LENGTH) {
			sendSystem(socket, client, `Message too long (max ${MAX_TEXT_LENGTH} characters)`);

			return;
		}

		broadcast(client, chatText);
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
