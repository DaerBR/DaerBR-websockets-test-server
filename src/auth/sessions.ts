import { randomBytes } from 'node:crypto';
import { EventEmitter } from 'node:events';

// Server-side session store: session id (the cookie's value) -> who it belongs to.
// Used by the HTTP routes (authController.ts) and by the WebSocket upgrade handler (server.ts).
export const SESSION_COOKIE = 'session';

export type Role = 'admin' | 'user';
type Session = { name: string; role: Role };

// In memory: a restart logs everyone out.
const sessions = new Map<string, Session>();

// Announces that a session is over, without knowing who listens (server.ts closes its sockets, 6h).
// Listeners run synchronously inside emit().
export const sessionEvents = new EventEmitter<{ ended: [sessionId: string] }>();

// 32 random bytes: meaningless by itself, impossible to guess. Only our Map turns it into a name.
export const createSession = (name: string, role: Role) => {
	const sessionId = randomBytes(32).toString('base64url');
	sessions.set(sessionId, { name, role });

	return sessionId;
};

// Returns the deleted session, or null if there was none.
export const deleteSession = (sessionId: string) => {
	const session = sessions.get(sessionId) ?? null;
	sessions.delete(sessionId);

	if (session) {
		sessionEvents.emit('ended', sessionId);
	}

	return session;
};

export const countSessions = () => sessions.size;

// The current state of a session, for checks made long after the handshake (6g). Null once it's gone.
export const getSessionById = (sessionId: string) => sessions.get(sessionId) ?? null;

// The raw header looks like "theme=dark; session=k3J9...". Parsed by hand: Express doesn't parse cookies,
// and the upgrade handler doesn't go through Express at all.
export const readSessionId = (cookieHeader: string | undefined) => {
	for (const part of cookieHeader?.split(';') ?? []) {
		const [key, ...value] = part.trim().split('=');

		if (key === SESSION_COOKIE) {
			return value.join('=');
		}
	}

	return null;
};

export const getSession = (cookieHeader: string | undefined) => {
	const sessionId = readSessionId(cookieHeader);

	return sessionId === null ? null : (sessions.get(sessionId) ?? null);
};
