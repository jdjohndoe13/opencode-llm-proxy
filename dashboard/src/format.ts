/** Small formatting helpers. All pure; all tolerant of NaN/undefined. */

export function fmtInt(n: number): string {
  if (!Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}

/** 12.3s / 4m 07s / 2h 15m — compact durations for tables. */
export function fmtDurShort(seconds: number): string {
  if (!Number.isFinite(seconds)) return '—';
  const s = Math.max(0, seconds);
  if (s < 10) return `${s.toFixed(1)}s`;
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) {
    const m = Math.floor(s / 60);
    const r = Math.floor(s % 60);
    return `${m}m ${String(r).padStart(2, '0')}s`;
  }
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  return `${h}h ${String(m).padStart(2, '0')}m`;
}

/** Long uptime for the footer: 3d 04:12:59 */
export function fmtUptime(seconds: number): string {
  if (!Number.isFinite(seconds)) return '—';
  const s = Math.max(0, Math.floor(seconds));
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const clock = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')}`;
  return d > 0 ? `${d}d ${clock}` : clock;
}

/** "14:02:31" local time from unix seconds; returns '—' for bad input. */
export function fmtClock(unixSeconds: number): string {
  if (!Number.isFinite(unixSeconds) || unixSeconds <= 0) return '—';
  const d = new Date(unixSeconds * 1000);
  if (Number.isNaN(d.getTime())) return '—';
  return d.toLocaleTimeString('en-GB', { hour12: false });
}

/** "10-24 14:02:31" from unix milliseconds (stats.jsonl start_time_unix). */
export function fmtTimeFromMs(unixMs: number, isoFallback: string): string {
  if (Number.isFinite(unixMs) && unixMs > 0) {
    const d = new Date(unixMs);
    if (!Number.isNaN(d.getTime())) {
      const mo = String(d.getMonth() + 1).padStart(2, '0');
      const da = String(d.getDate()).padStart(2, '0');
      const hh = String(d.getHours()).padStart(2, '0');
      const mm = String(d.getMinutes()).padStart(2, '0');
      const ss = String(d.getSeconds()).padStart(2, '0');
      return `${mo}-${da} ${hh}:${mm}:${ss}`;
    }
  }
  if (isoFallback) {
    // "2026-10-24T14:02:31Z" → "10-24 14:02:31" (UTC, shown as-is)
    const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}:\d{2}:\d{2})/.exec(isoFallback);
    if (m) return `${m[2]}-${m[3]} ${m[4]}`;
    return isoFallback;
  }
  return '—';
}

/** "just now / 12s / 3m / 5h" — relative time for event log. */
export function fmtAgo(unixSeconds: number, nowUnix: number): string {
  if (!Number.isFinite(unixSeconds) || unixSeconds <= 0) return '—';
  const delta = Math.max(0, nowUnix - unixSeconds);
  if (delta < 5) return 'just now';
  if (delta < 60) return `${Math.floor(delta)}s ago`;
  if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
  if (delta < 86400) return `${Math.floor(delta / 3600)}h ago`;
  return `${Math.floor(delta / 86400)}d ago`;
}

export function fmtSecondsOpt(v: number | undefined): string {
  if (v === undefined || !Number.isFinite(v) || v <= 0) return '—';
  return v < 10 ? `${v.toFixed(1)}s` : `${Math.round(v)}s`;
}
