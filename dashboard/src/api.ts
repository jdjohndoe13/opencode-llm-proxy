/**
 * Defensive client for the /api/dashboard contract.
 *
 * Every value coming off the wire is re-parsed with fallbacks (num/str/arr)
 * so a backend that drifts from the contract degrades to defaults instead of
 * throwing mid-render.
 */
import type {
  DashboardState,
  HistoryPage,
  InFlightRequest,
  ProxyEvent,
  ToggleResult,
} from './types';

const BASE = '/api/dashboard';

export class ApiError extends Error {
  readonly status: number;
  constructor(message: string, status = 0) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

/* ---------- defensive parsers ---------- */

function num(v: unknown, fallback = 0): number {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return fallback;
}

function str(v: unknown, fallback = ''): string {
  if (typeof v === 'string') return v;
  if (v === null || v === undefined) return fallback;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return fallback;
}

function bool(v: unknown, fallback = false): boolean {
  if (typeof v === 'boolean') return v;
  if (typeof v === 'number') return v !== 0;
  if (typeof v === 'string') return v === 'true' || v === '1';
  return fallback;
}

function arr(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

function obj(v: unknown): Record<string, unknown> {
  return typeof v === 'object' && v !== null ? (v as Record<string, unknown>) : {};
}

/* ---------- fetch helpers ---------- */

async function getJson(path: string, timeoutMs = 6000): Promise<unknown> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(`${BASE}${path}`, {
      headers: { accept: 'application/json' },
      cache: 'no-store',
      signal: ac.signal,
    });
    if (!res.ok) throw new ApiError(`GET ${path} → HTTP ${res.status}`, res.status);
    return (await res.json()) as unknown;
  } catch (err) {
    if (err instanceof ApiError) throw err;
    const msg = err instanceof Error ? err.message : String(err);
    throw new ApiError(`GET ${path} → ${msg}`);
  } finally {
    clearTimeout(timer);
  }
}

async function postJson(path: string, timeoutMs = 10000): Promise<unknown> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(`${BASE}${path}`, {
      method: 'POST',
      headers: { accept: 'application/json' },
      cache: 'no-store',
      signal: ac.signal,
    });
    if (!res.ok) throw new ApiError(`POST ${path} → HTTP ${res.status}`, res.status);
    return (await res.json()) as unknown;
  } catch (err) {
    if (err instanceof ApiError) throw err;
    const msg = err instanceof Error ? err.message : String(err);
    throw new ApiError(`POST ${path} → ${msg}`);
  } finally {
    clearTimeout(timer);
  }
}

/* ---------- parsers ---------- */

function parseInFlight(raw: unknown): InFlightRequest {
  const o = obj(raw);
  // contract: "age_seconds"; fall back to legacy "age_s" if present
  const age = o['age_seconds'] !== undefined ? o['age_seconds'] : o['age_s'];
  return {
    id: str(o['id'], '·'),
    model: str(o['model'], '—'),
    path: str(o['path'], '—'),
    stream: bool(o['stream']),
    state: str(o['state'], 'waiting'),
    started_at: num(o['started_at']),
    age_seconds: num(age),
    updated_at: num(o['updated_at'], NaN),
  };
}

function parseEvent(raw: unknown): ProxyEvent {
  const o = obj(raw);
  return { ts: num(o['ts']), action: str(o['action'], '·') };
}

export function parseState(raw: unknown): DashboardState {
  const o = obj(raw);
  const ka = obj(o['keepalive']);
  const pausedAt =
    o['paused_at'] === null || o['paused_at'] === undefined
      ? null
      : num(o['paused_at'], NaN);
  return {
    paused: bool(o['paused']),
    paused_at: pausedAt === null || Number.isNaN(pausedAt) ? null : pausedAt,
    uptime_s: num(o['uptime_s']),
    in_flight: arr(o['in_flight']).map(parseInFlight),
    keepalive: { delay: num(ka['delay']), interval: num(ka['interval']) },
    events: arr(o['events']).map(parseEvent),
  };
}

function parseHistoryItem(raw: unknown) {
  const o = obj(raw);
  const item = {
    model: str(o['model'], '—'),
    status: num(o['status']),
    duration_ms: num(o['duration_ms']),
    prompt_tokens: num(o['prompt_tokens']),
    completion_tokens: num(o['completion_tokens']),
    total_tokens: num(o['total_tokens']),
    start_time: str(o['start_time']),
    start_time_unix: num(o['start_time_unix']),
    stall_seconds: num(o['stall_seconds']),
  };
  if (o['paused_seconds'] !== undefined && o['paused_seconds'] !== null) {
    return { ...item, paused_seconds: num(o['paused_seconds']) };
  }
  return item;
}

export function parseHistory(raw: unknown): HistoryPage {
  const o = obj(raw);
  // contract: "entries"; fall back to legacy "items" if present
  const rawEntries = o['entries'] !== undefined ? o['entries'] : o['items'];
  const entries = arr(rawEntries).map(parseHistoryItem);
  const size = num(o['size'], entries.length || 20);
  return {
    total: num(o['total'], entries.length),
    page: num(o['page'], 1),
    size: size <= 0 ? 20 : size,
    pages: num(o['pages'], size > 0 ? Math.ceil(num(o['total'], entries.length) / size) : 1),
    entries,
  };
}

/* ---------- endpoints ---------- */

export async function fetchState(): Promise<DashboardState> {
  return parseState(await getJson('/state'));
}

export async function fetchHistory(page: number, size: number): Promise<HistoryPage> {
  return parseHistory(await getJson(`/history?page=${page}&size=${size}`));
}

export async function pause(): Promise<ToggleResult> {
  const o = obj(await postJson('/pause'));
  return { ok: bool(o['ok'], true), paused: bool(o['paused'], true) };
}

export async function resume(): Promise<ToggleResult> {
  const o = obj(await postJson('/resume'));
  return { ok: bool(o['ok'], true), paused: bool(o['paused'], false) };
}
