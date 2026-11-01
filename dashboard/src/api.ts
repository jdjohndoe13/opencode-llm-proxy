/**
 * Defensive client for the /api/dashboard contract.
 *
 * Every value coming off the wire is re-parsed with fallbacks (num/str/arr)
 * so a backend that drifts from the contract degrades to defaults instead of
 * throwing mid-render.
 */
import type {
  Concurrency,
  DashboardState,
  HistoryItem,
  HistoryPage,
  InFlightRequest,
  ProxyEvent,
  TempGuard,
  ToggleResult,
} from './types';

const BASE = '/api/dashboard';

export class ApiError extends Error {
  readonly status: number;
  /** parsed JSON body of the error response, when the server sent one
   *  (the 400 envelopes carry a one-line `error` reason) */
  readonly body: Record<string, unknown> | null;
  constructor(message: string, status = 0, body: Record<string, unknown> | null = null) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.body = body;
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

function numOr(v: unknown): number | null {
  const n = num(v, NaN);
  return Number.isFinite(n) ? n : null;
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

async function errorBody(res: Response): Promise<Record<string, unknown> | null> {
  try {
    const j = (await res.json()) as unknown;
    return typeof j === 'object' && j !== null ? (j as Record<string, unknown>) : null;
  } catch {
    return null; // non-JSON error body (proxy/html error page)
  }
}

async function postJson(path: string, body?: unknown, timeoutMs = 10000): Promise<unknown> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(`${BASE}${path}`, {
      method: 'POST',
      headers: {
        accept: 'application/json',
        ...(body !== undefined ? { 'content-type': 'application/json' } : {}),
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      cache: 'no-store',
      signal: ac.signal,
    });
    if (!res.ok) {
      // the 400 envelopes carry a one-line `error` reason — keep the body
      // attached so callers can show the server's own message
      throw new ApiError(`POST ${path} → HTTP ${res.status}`, res.status, await errorBody(res));
    }
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
  const r: InFlightRequest = {
    id: str(o['id'], '·'),
    model: str(o['model'], '—'),
    path: str(o['path'], '—'),
    stream: bool(o['stream']),
    state: str(o['state'], 'waiting'),
    started_at: num(o['started_at']),
    age_seconds: num(age),
    updated_at: num(o['updated_at'], NaN),
  };
  // optional phase split — absent on older backends, then the Age cell
  // shows the plain age as before
  const held = numOr(o['held_seconds']);
  if (held !== null) r.held_seconds = held;
  const up = numOr(o['upstream_seconds']);
  if (up !== null) r.upstream_seconds = up;
  return r;
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
  const s: DashboardState = {
    paused: bool(o['paused']),
    paused_at: pausedAt === null || Number.isNaN(pausedAt) ? null : pausedAt,
    uptime_s: num(o['uptime_s']),
    in_flight: arr(o['in_flight']).map(parseInFlight),
    keepalive: { delay: num(ka['delay']), interval: num(ka['interval']) },
    events: arr(o['events']).map(parseEvent),
  };
  // optional block — only attach when the backend sends it at all
  if (o['temp_guard'] !== undefined && o['temp_guard'] !== null) {
    s.temp_guard = parseTempGuard(o['temp_guard']);
  }
  // optional block — only attach when the backend implements the limiter
  if (o['concurrency'] !== undefined && o['concurrency'] !== null) {
    s.concurrency = parseConcurrency(o['concurrency']);
  }
  return s;
}

function parseTempGuard(raw: unknown): TempGuard {
  const o = obj(raw);
  const numOr = (v: unknown): number | null => {
    const n = num(v, NaN);
    return Number.isFinite(n) ? n : null;
  };
  const maxTemp = numOr(o['max_temp']);
  const checkedAt = numOr(o['checked_at']);
  const err = o['last_error'];
  const errStr = typeof err === 'string' && err !== '' ? err : null;
  const configEnabled = bool(o['config_enabled']);
  const userEnabled = bool(o['user_enabled']);
  const g: TempGuard = {
    config_enabled: configEnabled,
    user_enabled: userEnabled,
    // trust the server's "enabled" when present, else derive it
    enabled: o['enabled'] !== undefined ? bool(o['enabled'], configEnabled && userEnabled) : configEnabled && userEnabled,
    fail_open: bool(o['fail_open']),
    max_temp: maxTemp,
    checked_at: checkedAt,
    last_error: errStr,
  };
  // optional thresholds — only present when the guard is armed by config
  const opt = (key: string): number | undefined => {
    const n = numOr(o[key]);
    return n === null ? undefined : n;
  };
  const stall = opt('stall_above');
  const resume = opt('resume_below');
  if (stall !== undefined) g.stall_above = stall;
  if (resume !== undefined) g.resume_below = resume;
  const dStall = opt('default_stall_above');
  const dResume = opt('default_resume_below');
  if (dStall !== undefined) g.default_stall_above = dStall;
  if (dResume !== undefined) g.default_resume_below = dResume;
  return g;
}

/**
 * Concurrency block. `limit` 0 means UNLIMITED. Every field degrades to a
 * safe value: missing/garbage counts become 0, and a missing/garbage
 * `default_limit` falls back to the live limit so the UI degrades to
 * "not edited, reset unavailable" rather than claiming a wrong .env value.
 */
function parseConcurrency(raw: unknown): Concurrency {
  const o = obj(raw);
  const count = (v: unknown): number => {
    const n = num(v, NaN);
    if (!Number.isFinite(n)) return 0;
    return Math.max(0, Math.round(n));
  };
  const limit = count(o['limit']);
  const dflt = num(o['default_limit'], NaN);
  return {
    limit,
    active: count(o['active']),
    queued: count(o['queued']),
    default_limit: Number.isFinite(dflt) ? Math.max(0, Math.round(dflt)) : limit,
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
  const out: Record<string, unknown> = { ...item };
  // optional per-phase fields — older stats.jsonl lines lack them
  for (const key of ['paused_seconds', 'queued_seconds', 'total_seconds'] as const) {
    const v = numOr(o[key]);
    if (v !== null) out[key] = v;
  }
  return out as unknown as HistoryItem;
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

/** POST /tempguard — flips user_enabled. Returns the new effective value. */
export async function setTempGuard(enabled: boolean): Promise<{ ok: boolean; enabled: boolean }> {
  const o = obj(await postJson('/tempguard', { enabled }));
  return { ok: bool(o['ok'], true), enabled: bool(o['enabled'], enabled) };
}

/**
 * POST /tempguard/thresholds — edits the live hysteresis pair (in-memory,
 * a restart re-reads .env). Pass both numbers, or `reset` to go back to
 * the .env values. A rejected pair comes back as HTTP 400 with `error`.
 */
export interface ThresholdResult {
  ok: boolean;
  stall_above: number | null;
  resume_below: number | null;
  error: string | null;
}

export async function setTempThresholds(
  stallAbove: number,
  resumeBelow: number,
): Promise<ThresholdResult>;
export async function setTempThresholds(reset: { reset: true }): Promise<ThresholdResult>;
export async function setTempThresholds(
  a: number | { reset: true },
  b?: number,
): Promise<ThresholdResult> {
  const body = typeof a === 'number' ? { stall_above: a, resume_below: b } : a;
  try {
    const o = obj(await postJson('/tempguard/thresholds', body));
    return {
      ok: bool(o['ok'], true),
      stall_above: numOr(o['stall_above']),
      resume_below: numOr(o['resume_below']),
      error: typeof o['error'] === 'string' ? o['error'] : null,
    };
  } catch (err) {
    // Only a 400 is "the proxy rejected this pair" — its body carries the
    // reason, which beats the bare "HTTP 400" text the UI could show.
    // Anything else (5xx, transport) must keep throwing so the caller can
    // still raise the "proxy unreachable" banner.
    if (err instanceof ApiError && err.status === 400) {
      const reason = err.body ? str(err.body['error']) : '';
      return {
        ok: false,
        stall_above: null,
        resume_below: null,
        error: reason !== '' ? reason : null,
      };
    }
    throw err;
  }
}

/**
 * POST /concurrency — edits the live cap (in-memory, a restart re-reads
 * .env). `limit` is an integer 0..10000 where 0 = UNLIMITED, or pass
 * `{ reset: true }` to go back to the .env value. A rejected limit comes
 * back as HTTP 400 with a one-line `error`; the server's own reason is
 * preferred over the generic HTTP message when present.
 */
export interface ConcurrencyResult {
  ok: boolean;
  limit: number | null;
  error: string | null;
}

export async function setConcurrency(limit: number): Promise<ConcurrencyResult>;
export async function setConcurrency(reset: { reset: true }): Promise<ConcurrencyResult>;
export async function setConcurrency(
  a: number | { reset: true },
): Promise<ConcurrencyResult> {
  const body = typeof a === 'number' ? { limit: a } : a;
  try {
    const o = obj(await postJson('/concurrency', body));
    return {
      ok: bool(o['ok'], true),
      limit: numOr(o['limit']),
      error: typeof o['error'] === 'string' ? o['error'] : null,
    };
  } catch (err) {
    // Only a 400 is "the proxy rejected this cap" — its body carries the
    // reason. Anything else (5xx, transport) must keep throwing so the
    // caller can still raise the "proxy unreachable" banner.
    if (err instanceof ApiError && err.status === 400) {
      const reason = err.body ? str(err.body['error']) : '';
      return { ok: false, limit: null, error: reason !== '' ? reason : null };
    }
    throw err;
  }
}
