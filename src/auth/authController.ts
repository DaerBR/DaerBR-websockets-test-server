import type { CookieOptions, Request, Response } from 'express';
import { z } from 'zod';
import {
	type Role,
	SESSION_COOKIE,
	countSessions,
	createSession,
	deleteSession,
	getSession,
	readSessionId,
} from './sessions.js';

// Handlers for /api/user, /api/login and /api/logout. The routes themselves are registered in server.ts.
const SESSION_MAX_AGE_MS = 24 * 60 * 60 * 1000;
// Everyone shares one password: there's no user database in this sandbox.
const DEMO_PASSWORD = 'Password123!';
// Who gets the admin role. In a real app: a column in the users table.
const ADMIN_NAMES = new Set(['Admin']);

// clearCookie must get the same options the cookie was set with, or the browser keeps it.
const cookieOptions: CookieOptions = {
	httpOnly: true,
	path: '/',
	sameSite: 'lax',
	// secure: true in production (HTTPS only). Off here: dev runs on plain http://.
};

const loginSchema = z.object({
	name: z.string().trim().min(1).max(20),
	password: z.string().min(1),
});

// "Who am I?" Not being logged in is a normal answer here, not an error.
export const getUser = (request: Request, response: Response) => {
	const session = getSession(request.headers.cookie);
	response.json({ name: session?.name ?? null, role: session?.role ?? null });
};

export const login = (request: Request, response: Response) => {
	const parsed = loginSchema.safeParse(request.body);

	if (!parsed.success) {
		response.status(400).json({ error: 'Name (1-20 characters) and password are required' });

		return;
	}

	const { name, password } = parsed.data;

	// Here a real app would look the user up in the database by name and compare the stored password hash
	// (bcrypt/argon2), answering the same 401 for an unknown name and for a wrong password.
	if (password !== DEMO_PASSWORD) {
		console.log(`[login] wrong password for "${name}"`);
		response.status(401).json({ error: 'Wrong name or password' });

		return;
	}

	const role: Role = ADMIN_NAMES.has(name) ? 'admin' : 'user';
	const sessionId = createSession(name, role);
	response.cookie(SESSION_COOKIE, sessionId, { ...cookieOptions, maxAge: SESSION_MAX_AGE_MS });
	console.log(`[login] "${name}" logged in as ${role} (sessions: ${countSessions()})`);
	response.json({ name, role });
};

export const logout = (request: Request, response: Response) => {
	const sessionId = readSessionId(request.headers.cookie);
	const session = sessionId === null ? null : deleteSession(sessionId);

	if (session) {
		console.log(`[logout] "${session.name}" logged out (sessions: ${countSessions()})`);
	}

	// Tells the browser to drop the cookie.
	response.clearCookie(SESSION_COOKIE, cookieOptions);
	response.status(204).end();
};
