import { WebSocket } from 'ws';

// Stage 5d test helper: a client that never answers the server's ping frames,
// which is what a sleeping laptop or a lost phone looks like to the server.
// Run `pnpm dead-client` next to `pnpm server` and watch the server terminate it.
const URL = 'ws://localhost:8081';

const startedAt = Date.now();
const elapsed = () => `${((Date.now() - startedAt) / 1000).toFixed(1)}s`;

// autoPong: false turns off the automatic pong reply that every client normally sends.
const socket = new WebSocket(URL, { autoPong: false });

socket.on('open', () => {
	console.log(`[${elapsed()}] connected to ${URL}, will never answer pings`);
});

socket.on('ping', () => {
	console.log(`[${elapsed()}] got a ping, not answering`);
});

socket.on('message', (data) => {
	console.log(`[${elapsed()}] message: ${data.toString()}`);
});

socket.on('close', (code) => {
	console.log(`[${elapsed()}] closed by the server, code=${code}`);
});

socket.on('error', (error) => {
	console.error(`[${elapsed()}] error:`, error.message);
});
