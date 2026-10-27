import { WebSocket, WebSocketServer } from 'ws';
import { z } from 'zod';

// Stage 3 helper: a JSON protocol with rooms, acks and typing indicators.
// The protocol is described in websockets-test/lessons/stage-3-message-design.md (§7).
//
// Test commands (send them as chat text):
//   /slowack  -> the message is delivered, but the ack arrives after SLOW_ACK_MS
//   /noack    -> the message is delivered, but the ack never arrives (client should time out)
//   /garbage  -> the server sends YOU three invalid messages (client validation should drop them)
const PORT = 8082;
const SLOW_ACK_MS = 3000;

const roomName = z.string().trim().min(1).max(40);

const clientMessageSchema = z.discriminatedUnion('type', [
	z.object({
		type: z.literal('join'),
		payload: z.object({ room: roomName, user: z.string().trim().min(1).max(40) }),
	}),
	z.object({
		type: z.literal('leave'),
		payload: z.object({ room: roomName }),
	}),
	z.object({
		type: z.literal('chat.send'),
		id: z.string().min(1).max(100),
		payload: z.object({ room: roomName, text: z.string().trim().min(1).max(2000) }),
	}),
	z.object({
		type: z.literal('typing'),
		payload: z.object({ room: roomName, isTyping: z.boolean() }),
	}),
]);

type ClientMessage = z.infer<typeof clientMessageSchema>;

type ServerMessage =
	| { type: 'chat.message'; payload: { id: string; room: string; user: string; text: string; ts: number } }
	| { type: 'ack'; payload: { id: string; ts: number } }
	| { type: 'typing'; payload: { room: string; user: string; isTyping: boolean } }
	| { type: 'error'; payload: { message: string; id?: string } };

type ClientState = { user: string | null; rooms: Set<string> };

const wss = new WebSocketServer({ port: PORT });
const clients = new Map<WebSocket, ClientState>();

const sendTo = (socket: WebSocket, message: ServerMessage) => {
	if (socket.readyState === WebSocket.OPEN) {
		socket.send(JSON.stringify(message));
	}
};

// Sends to everyone in the room except `except` (the sender).
const sendToRoom = (room: string, message: ServerMessage, except: WebSocket) => {
	for (const [socket, state] of clients) {
		if (socket !== except && state.rooms.has(room)) {
			sendTo(socket, message);
		}
	}
};

const sendGarbage = (socket: WebSocket) => {
	if (socket.readyState !== WebSocket.OPEN) {
		return;
	}

	socket.send('this is not JSON');
	socket.send(JSON.stringify({ type: 'chat.message', payload: { text: 42 } }));
	socket.send(JSON.stringify({ type: 'self-destruct', payload: {} }));
};

const handleMessage = (socket: WebSocket, state: ClientState, message: ClientMessage) => {
	switch (message.type) {
		case 'join': {
			state.user = message.payload.user;
			state.rooms.add(message.payload.room);
			console.log(`[join]    ${state.user} -> #${message.payload.room}`);

			return;
		}

		case 'leave': {
			const { room } = message.payload;

			if (state.rooms.delete(room) && state.user) {
				sendToRoom(room, { type: 'typing', payload: { room, user: state.user, isTyping: false } }, socket);
			}

			console.log(`[leave]   ${state.user ?? '?'} <- #${room}`);

			return;
		}

		case 'chat.send': {
			const { id } = message;
			const { room, text } = message.payload;

			if (!state.user || !state.rooms.has(room)) {
				sendTo(socket, { type: 'error', payload: { id, message: `Not joined to #${room}` } });

				return;
			}

			if (text === '/garbage') {
				sendGarbage(socket);

				return;
			}

			const ts = Date.now();
			console.log(`[message] #${room} ${state.user}: ${text}`);
			sendToRoom(room, { type: 'chat.message', payload: { id, room, user: state.user, text, ts } }, socket);

			if (text === '/noack') {
				return;
			}

			if (text === '/slowack') {
				setTimeout(() => sendTo(socket, { type: 'ack', payload: { id, ts } }), SLOW_ACK_MS);

				return;
			}

			sendTo(socket, { type: 'ack', payload: { id, ts } });

			return;
		}

		case 'typing': {
			const { room, isTyping } = message.payload;

			if (state.user && state.rooms.has(room)) {
				sendToRoom(room, { type: 'typing', payload: { room, user: state.user, isTyping } }, socket);
			}

			return;
		}
	}
};

wss.on('connection', (socket, request) => {
	const state: ClientState = { user: null, rooms: new Set() };
	clients.set(socket, state);
	console.log(`[open]    origin=${request.headers.origin ?? '-'} clients=${clients.size}`);

	socket.on('message', (data, isBinary) => {
		if (isBinary) {
			sendTo(socket, { type: 'error', payload: { message: 'Binary messages are not supported' } });

			return;
		}

		let json: unknown;

		try {
			json = JSON.parse(data.toString());
		} catch {
			sendTo(socket, { type: 'error', payload: { message: 'Message is not valid JSON' } });

			return;
		}

		const result = clientMessageSchema.safeParse(json);

		if (!result.success) {
			console.log(`[invalid] ${z.prettifyError(result.error)}`);
			sendTo(socket, { type: 'error', payload: { message: 'Invalid message', id: idOf(json) } });

			return;
		}

		handleMessage(socket, state, result.data);
	});

	socket.on('close', (code, reason) => {
		// Clear this user's typing indicator in every room they were in.
		if (state.user) {
			for (const room of state.rooms) {
				sendToRoom(room, { type: 'typing', payload: { room, user: state.user, isTyping: false } }, socket);
			}
		}

		clients.delete(socket);
		console.log(`[close]   code=${code} reason="${reason.toString()}" clients=${clients.size}`);
	});

	socket.on('error', (error) => {
		console.error('[error]  ', error.message);
	});
});

// If an invalid message still carried a string `id`, echo it so the client can fail that send.
const idOf = (json: unknown): string | undefined => {
	if (typeof json === 'object' && json !== null && 'id' in json && typeof json.id === 'string') {
		return json.id;
	}

	return undefined;
};

console.log(`Rooms server listening on ws://localhost:${PORT}`);
