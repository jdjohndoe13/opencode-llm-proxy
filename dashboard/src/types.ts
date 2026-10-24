/**
 * Shapes for the /api/dashboard contract.
 *
 * The backend is being built in parallel; every field is parsed defensively
 * in api.ts (num()/str() fallbacks), so a slightly-different backend can
 * never crash the UI or the typecheck.
 */

export interface InFlightRequest {
  id: string;
  model: string;
  path: string;
  stream: boolean;
  /** "waiting" | "upstream" | "paused" | backend-specific extras */
  state: string;
  /** unix seconds (float) */
  started_at: number;
  /** seconds since the request started */
  age_seconds: number;
  /** unix seconds (float) — when the server last refreshed this entry */
  updated_at: number;
}

export interface KeepaliveConfig {
  delay: number;
  interval: number;
}

/**
 * GPU temperature guard (secondary runtime control).
 * enabled = config_enabled AND user_enabled (effective state).
 */
export interface TempGuard {
  /** effective: config_enabled && user_enabled */
  enabled: boolean;
  /** TEMP_CHECK_ENABLED env — false means the guard is off entirely */
  config_enabled: boolean;
  /** runtime switch, toggled via POST /tempguard */
  user_enabled: boolean;
  /** TEMP_CHECK_FAIL_OPEN env */
  fail_open: boolean;
  /** last observed temperature snapshot, null = never fetched */
  max_temp: number | null;
  /** unix seconds of the last temperature check, null = never */
  checked_at: number | null;
  /** last monitor failure message, null = healthy */
  last_error: string | null;
}

export interface ProxyEvent {
  /** unix seconds (float) */
  ts: number;
  /** "pause" | "resume" | backend-specific extras */
  action: string;
}

export interface DashboardState {
  paused: boolean;
  /** unix seconds (float) when the proxy was paused, null while running */
  paused_at: number | null;
  uptime_s: number;
  /** in_flight items use age_seconds + updated_at */
  in_flight: InFlightRequest[];
  keepalive: KeepaliveConfig;
  /** newest first */
  events: ProxyEvent[];
  /** present only when the backend implements the guard */
  temp_guard?: TempGuard;
}

export interface HistoryItem {
  model: string;
  /** upstream HTTP status (0 = aborted / no response) */
  status: number;
  duration_ms: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  /** ISO-ish UTC string, e.g. 2026-10-24T13:37:00Z */
  start_time: string;
  /** unix milliseconds (float) */
  start_time_unix: number;
  stall_seconds: number;
  /** optional — only present when the request was held by a pause */
  paused_seconds?: number;
}

export interface HistoryPage {
  total: number;
  page: number;
  size: number;
  /** total page count, reported by the server */
  pages: number;
  entries: HistoryItem[];
}

/** Envelope for POST /pause and POST /resume. */
export interface ToggleResult {
  ok: boolean;
  paused: boolean;
}

/** Live connection status of the dashboard against the proxy API. */
export type ConnStatus = 'ok' | 'down' | 'unknown';
