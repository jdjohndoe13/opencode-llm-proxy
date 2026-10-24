/**
 * llm-proxy ops console — plain TS, no framework.
 *
 * One static DOM skeleton, one 1s tick:
 *   - every tick: clock, interpolated ages, pause duration, button hint
 *   - every 2s:   GET /state (and GET /history when its params changed)
 *   - POST /pause|/resume behind a two-step confirm button
 *
 * All rendering is defensive: if the API is down the UI keeps the last
 * known state, flags it stale, and shows a "proxy unreachable" banner.
 */
import './styles.css';
import { ApiError, fetchHistory, fetchState, pause, resume, setTempGuard } from './api';
import {
  fmtAgo,
  fmtClock,
  fmtDurShort,
  fmtInt,
  fmtSecondsOpt,
  fmtTimeFromMs,
  fmtUptime,
} from './format';
import type { ConnStatus, DashboardState, HistoryPage } from './types';

/* ------------------------------------------------------------------ state */

let state: DashboardState | null = null;
let history: HistoryPage | null = null;
let historyError: string | null = null;

let conn: ConnStatus = 'unknown';
let everConnected = false;
let lastError = '';
let lastSyncAt = 0; // performance.now() of last successful /state
let ageOffsetMs = 0; // ms since last /state → interpolate ages between polls

let confirmTimer = 0;
let actionPending = false;
let guardPending = false;

let histPage = 1;
let histSize = 20;
let histLoading = false;

const STATE_MS = 2000;
const CONFIRM_MS = 5000;

/* ------------------------------------------------------------- dom setup */

const $ = <T extends HTMLElement>(sel: string): T => {
  const el = document.querySelector<T>(sel);
  if (!el) throw new Error(`missing element: ${sel}`);
  return el;
};

const $opt = <T extends HTMLElement>(sel: string): T | null =>
  document.querySelector<T>(sel);

function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  cls?: string,
  text?: string,
): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
}

const app = $<HTMLDivElement>('#app');

app.innerHTML = `
  <header class="masthead">
    <span class="logo">llm<b>·</b>proxy</span>
    <span class="sub">ops console</span>
    <span class="clock" id="clock">--:--:--</span>
  </header>

  <div class="conn" id="conn" role="alert">
    <span class="dot"></span>
    <span>PROXY UNREACHABLE</span>
    <span class="why" id="conn-why"></span>
  </div>

  <section class="hero">
    <div class="state-card" id="state-card">
      <div class="state-led"></div>
      <div class="state-text">
        <div class="state-label" id="state-label">···</div>
        <div class="state-detail" id="state-detail">connecting to proxy…</div>
      </div>
    </div>
    <button class="pause-btn" id="pause-btn" type="button" disabled>
      <span id="btn-label">···</span>
    </button>
  </section>

  <section class="tg-strip" id="tg-strip" hidden>
    <span class="tg-title">Temperature guard</span>
    <span class="tg-state" id="tg-state"><span class="tg-dot"></span><span id="tg-state-text">—</span></span>
    <button class="tg-switch" id="tg-switch" type="button"
            title="Toggle the GPU temperature guard. Off (or fail-open) forwards requests even when the monitor is down; the guard re-arms on proxy restart."></button>
    <span class="tg-facts">
      <span>max <b id="tg-temp">—</b></span>
      <span>checked <b id="tg-checked">—</b></span>
    </span>
    <span class="tg-hint">off = forward even when monitor is down · re-arms on restart</span>
    <span class="tg-error" id="tg-error">
      <span class="tg-tag" id="tg-tag">—</span>
      <span class="tg-msg" id="tg-msg"></span>
    </span>
  </section>

  <section class="panel">
    <div class="panel-head">
      <h2>In-flight requests</h2>
      <span class="count" id="iflight-count">0</span>
      <div class="right">
        <span class="scrolling" id="sync-mark">▮</span>
        <span id="sync-note">—</span>
      </div>
    </div>
    <div class="iflight-wrap">
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th>Path</th>
            <th>Stream</th>
            <th class="num">Age</th>
            <th>State</th>
          </tr>
        </thead>
        <tbody id="iflight-body"></tbody>
      </table>
      <div class="empty" id="iflight-empty">no requests in flight</div>
    </div>
  </section>

  <section class="panel">
    <div class="panel-head">
      <h2>Request history</h2>
      <span class="count" id="hist-count">—</span>
      <div class="right"><span id="hist-note"></span></div>
    </div>
    <div class="history-wrap">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Model</th>
            <th class="num">Status</th>
            <th class="num">Duration</th>
            <th class="num">Prompt</th>
            <th class="num">Completion</th>
            <th class="num">Total</th>
            <th class="num">Stall</th>
            <th class="num">Paused</th>
          </tr>
        </thead>
        <tbody id="hist-body"></tbody>
      </table>
      <div class="empty" id="hist-empty">—</div>
    </div>
    <div class="pager">
      <span class="pageinfo" id="page-info">—</span>
      <span class="spacer"></span>
      <label class="dim" for="page-size">rows</label>
      <select id="page-size">
        <option>10</option>
        <option selected>20</option>
        <option>50</option>
        <option>100</option>
      </select>
      <button id="page-prev" type="button">◀ prev</button>
      <button id="page-next" type="button">next ▶</button>
    </div>
  </section>

  <footer class="footer">
    <span class="kv">uptime <b id="f-uptime">—</b></span>
    <span class="kv">keepalive <b id="f-keepalive">—</b></span>
    <span class="kv">events</span>
    <span class="events" id="f-events"></span>
  </footer>
`;

const clockEl = $<HTMLSpanElement>('#clock');
const connEl = $<HTMLDivElement>('#conn');
const connWhy = $<HTMLSpanElement>('#conn-why');
const stateCard = $<HTMLDivElement>('#state-card');
const stateLabel = $<HTMLDivElement>('#state-label');
const stateDetail = $<HTMLDivElement>('#state-detail');
const pauseBtn = $<HTMLButtonElement>('#pause-btn');
const btnLabel = $<HTMLSpanElement>('#btn-label');
const iflightCount = $<HTMLSpanElement>('#iflight-count');
const iflightBody = $<HTMLTableSectionElement>('#iflight-body');
const iflightEmpty = $<HTMLDivElement>('#iflight-empty');
const syncNote = $<HTMLSpanElement>('#sync-note');
const syncMark = $<HTMLSpanElement>('#sync-mark');
const histCount = $<HTMLSpanElement>('#hist-count');
const histNote = $<HTMLSpanElement>('#hist-note');
const histBody = $<HTMLTableSectionElement>('#hist-body');
const histEmpty = $<HTMLDivElement>('#hist-empty');
const pageInfo = $<HTMLSpanElement>('#page-info');
const pageSize = $<HTMLSelectElement>('#page-size');
const pagePrev = $<HTMLButtonElement>('#page-prev');
const pageNext = $<HTMLButtonElement>('#page-next');
const fUptime = $<HTMLSpanElement>('#f-uptime');
const fKeepalive = $<HTMLSpanElement>('#f-keepalive');
const fEvents = $<HTMLSpanElement>('#f-events');
const tgStrip = $<HTMLElement>('#tg-strip');
const tgState = $<HTMLSpanElement>('#tg-state');
const tgStateText = $<HTMLSpanElement>('#tg-state-text');
const tgSwitch = $<HTMLButtonElement>('#tg-switch');
const tgTemp = $<HTMLSpanElement>('#tg-temp');
const tgChecked = $<HTMLSpanElement>('#tg-checked');
const tgError = $<HTMLElement>('#tg-error');
const tgTag = $<HTMLElement>('#tg-tag');
const tgMsg = $<HTMLElement>('#tg-msg');
const favIcon = $opt<HTMLLinkElement>('link[rel="icon"]');

/* ------------------------------------------------------------- rendering */

function setFavicon(paused: boolean): void {
  if (!favIcon) return;
  const color = paused ? 'ff5d55' : '2fd68c';
  favIcon.href =
    `data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E` +
    `%3Ccircle cx='8' cy='8' r='6' fill='%23${color}'/%3E%3C/svg%3E`;
}

function stateBadgeClass(stateName: string): string {
  switch (stateName) {
    case 'waiting':
      return 'badge st-waiting';
    case 'upstream':
      return 'badge st-upstream';
    case 'paused':
      return 'badge st-paused';
    default:
      return 'badge st-other';
  }
}

interface RowCells {
  tr: HTMLTableRowElement;
  age: HTMLTableCellElement;
  /** age_seconds as reported by the server at last sync */
  baseAge: number;
  /** unix seconds when the server computed baseAge (NaN if absent) */
  baseAt: number;
  /** model|path|stream|state — rebuild the row when this changes */
  sig: string;
}

const iflightRows = new Map<string, RowCells>();

function tickAge(row: RowCells, baseAge: number, baseAt: number): void {
  // prefer the per-entry updated_at for interpolation; fall back to the
  // global sync offset when the backend omits it
  let age: number;
  if (Number.isFinite(baseAt) && baseAt > 0) {
    age = baseAge + Math.max(0, Date.now() / 1000 - baseAt);
  } else {
    age = baseAge + ageOffsetMs / 1000;
  }
  row.age.textContent = fmtDurShort(age);
  row.age.classList.toggle('old', age > 30);
}

function renderInflight(): void {
  const reqs = state?.in_flight ?? [];
  const seen = new Set<string>();

  reqs.forEach((req, i) => {
    const key = req.id || `row-${i}`;
    seen.add(key);
    const sig = `${req.model}|${req.path}|${req.stream}|${req.state}`;
    let row = iflightRows.get(key);
    if (!row || row.sig !== sig) {
      // structure changed → rebuild the row
      if (row) row.tr.remove();
      const tr = el('tr');
      const tdModel = el('td', 'model', req.model);
      const tdPath = el('td', 'path', req.path);
      const tdStream = el('td');
      tdStream.appendChild(
        el('span', req.stream ? 'badge stream' : 'badge no-stream', req.stream ? 'SSE' : 'JSON'),
      );
      const tdAge = el('td', 'num age');
      const tdState = el('td');
      tdState.appendChild(el('span', stateBadgeClass(req.state), req.state));
      tr.append(tdModel, tdPath, tdStream, tdAge, tdState);
      iflightBody.appendChild(tr);
      row = { tr, age: tdAge, baseAge: req.age_seconds, baseAt: req.updated_at, sig };
      iflightRows.set(key, row);
    } else {
      row.baseAge = req.age_seconds;
      row.baseAt = req.updated_at;
    }
    tickAge(row, row.baseAge, row.baseAt);
    // keep DOM order stable if the server reorders
    const expected = iflightBody.children[i];
    if (expected !== row.tr) iflightBody.insertBefore(row.tr, expected ?? null);
  });

  for (const [key, row] of iflightRows) {
    if (!seen.has(key)) {
      row.tr.remove();
      iflightRows.delete(key);
    }
  }

  iflightEmpty.hidden = reqs.length > 0;
  iflightCount.textContent = String(reqs.length);
  iflightCount.classList.toggle('hot', reqs.length > 0);
}

function renderHero(): void {
  if (!state) {
    stateCard.classList.remove('paused');
    stateLabel.textContent = '···';
    stateDetail.textContent =
      conn === 'down' ? 'no data — proxy unreachable' : 'connecting to proxy…';
    return;
  }
  const stale = conn === 'down';
  stateCard.classList.toggle('paused', state.paused);
  stateLabel.textContent = state.paused ? 'PAUSED' : 'RUNNING';

  const parts: string[] = [];
  if (state.paused) {
    const since = state.paused_at ?? 0;
    const dur = since > 0 ? Date.now() / 1000 - since : NaN;
    parts.push(
      since > 0
        ? `held for <span class="hl">${fmtDurShort(dur)}</span> · since ${fmtClock(since)}`
        : 'holding all new requests',
    );
  } else {
    parts.push('forwarding requests');
  }
  const n = state.in_flight.length;
  if (n > 0) parts.push(`${n} in flight`);
  if (stale) parts.push('<span class="hl">stale data</span>');
  stateDetail.innerHTML = parts.join(' · ');
}

function renderButton(): void {
  if (actionPending) {
    pauseBtn.disabled = true;
    pauseBtn.classList.add('busy');
    pauseBtn.classList.remove('confirming', 'propose-pause', 'propose-resume');
    btnLabel.textContent = '··· WORKING';
    return;
  }
  pauseBtn.classList.remove('busy');
  if (conn === 'down' || !state) {
    pauseBtn.disabled = true;
    pauseBtn.classList.remove('confirming', 'propose-pause', 'propose-resume');
    btnLabel.textContent = 'PROXY UNREACHABLE';
    return;
  }
  pauseBtn.disabled = false;
  if (state.paused) {
    pauseBtn.classList.remove('confirming', 'propose-pause');
    pauseBtn.classList.add('propose-resume');
    btnLabel.textContent = '▶ RESUME TRAFFIC';
  } else if (pauseBtn.classList.contains('confirming')) {
    pauseBtn.classList.remove('propose-pause');
    pauseBtn.classList.add('confirming');
    btnLabel.textContent = '⚠ CONFIRM — HOLD ALL';
  } else {
    pauseBtn.classList.remove('propose-resume', 'confirming');
    pauseBtn.classList.add('propose-pause');
    btnLabel.textContent = '❚❚ PAUSE TRAFFIC';
  }
}

function renderTempGuard(): void {
  const g = state?.temp_guard;
  if (!g) {
    // backend doesn't expose the guard — hide the whole strip
    tgStrip.hidden = true;
    return;
  }
  tgStrip.hidden = false;

  // effective state pill: ON / OFF / INACTIVE (config off)
  const configOff = !g.config_enabled;
  tgState.className = configOff ? 'tg-state inactive' : g.enabled ? 'tg-state on' : 'tg-state off';
  tgStateText.textContent = configOff ? 'INACTIVE' : g.enabled ? 'ON' : 'OFF';

  // switch reflects user_enabled; disabled when config disables the guard
  const on = g.user_enabled && !configOff;
  tgSwitch.classList.toggle('on', on);
  tgSwitch.disabled = configOff || conn === 'down' || guardPending;
  tgSwitch.classList.toggle('busy', guardPending);
  tgSwitch.title = configOff
    ? 'Disabled by config (TEMP_CHECK_ENABLED=false) — the guard is off entirely.'
    : 'Toggle the GPU temperature guard. Off (or fail-open) forwards requests even when the monitor is down; the guard re-arms on proxy restart.';

  // live facts
  tgTemp.textContent = g.max_temp === null ? '—' : `${g.max_temp.toFixed(0)}°C`;
  tgChecked.textContent =
    g.checked_at === null ? 'never' : fmtAgo(g.checked_at, Date.now() / 1000);

  // error / failure-open / stalling states
  const hasErr = g.last_error !== null;
  if (hasErr) {
    if (g.fail_open) {
      tgError.className = 'tg-error warn show';
      tgTag.textContent = 'FAILING OPEN';
    } else {
      tgError.className = 'tg-error danger show';
      tgTag.textContent = 'STALLING REQUESTS';
    }
    tgMsg.textContent = g.last_error;
  } else {
    tgError.className = 'tg-error';
  }
}

function renderHistory(): void {
  if (historyError) {
    histBody.replaceChildren();
    histEmpty.hidden = false;
    histEmpty.textContent = `history unavailable — ${historyError}`;
    histCount.textContent = '—';
    pageInfo.textContent = '—';
    pagePrev.disabled = true;
    pageNext.disabled = true;
    return;
  }
  if (!history) {
    histEmpty.hidden = false;
    histEmpty.textContent = histLoading ? 'loading…' : '—';
    return;
  }
  const { entries, total, page, size, pages } = history;
  histEmpty.hidden = entries.length > 0;
  if (entries.length === 0) histEmpty.textContent = 'no history yet';
  histCount.textContent = fmtInt(total);

  const frag = document.createDocumentFragment();
  for (const item of entries) {
    const tr = el('tr');
    const td = (cls: string, text: string): HTMLTableCellElement => {
      const td2 = el('td', cls, text);
      tr.appendChild(td2);
      return td2;
    };
    // column order must match the <thead>: time, model, status, duration,
    // prompt, completion, total, stall, paused
    td('path', fmtTimeFromMs(item.start_time_unix, item.start_time));
    td('model', item.model);
    const tdStatus = td('num', '');
    const ok = item.status >= 200 && item.status < 300;
    tdStatus.appendChild(el('span', `badge ${ok ? 'ok' : 'err'}`, String(item.status || '—')));
    td('num', fmtDurShort(item.duration_ms / 1000));
    td('num', fmtInt(item.prompt_tokens));
    td('num', fmtInt(item.completion_tokens));
    td('num', fmtInt(item.total_tokens));
    const stall = td('num', fmtSecondsOpt(item.stall_seconds));
    if (item.stall_seconds > 0) stall.classList.add('stall');
    const paused = td('num', fmtSecondsOpt(item.paused_seconds));
    if (item.paused_seconds !== undefined && item.paused_seconds > 0) {
      paused.classList.add('stall');
    }
    frag.appendChild(tr);
  }
  histBody.replaceChildren(frag);

  const from = total === 0 ? 0 : (page - 1) * size + 1;
  const to = Math.min(total, page * size);
  pageInfo.innerHTML = `<b>${from}–${to}</b> of <b>${fmtInt(total)}</b> · page <b>${page}</b>/${pages}`;
  pagePrev.disabled = page <= 1 || histLoading;
  pageNext.disabled = page >= pages || histLoading;
}

function renderFooter(): void {
  if (!state) {
    fUptime.textContent = '—';
    fKeepalive.textContent = '—';
    fEvents.replaceChildren();
    return;
  }
  fUptime.textContent = fmtUptime(state.uptime_s);
  fKeepalive.textContent = `delay ${fmtDurShort(state.keepalive.delay)} · interval ${fmtDurShort(state.keepalive.interval)}`;

  const now = Date.now() / 1000;
  // contract: events arrive newest-first; sort defensively anyway
  const evs = [...state.events].sort((a, b) => b.ts - a.ts).slice(0, 3);
  fEvents.replaceChildren();
  if (evs.length === 0) {
    fEvents.appendChild(el('span', 'dim', 'none recorded'));
  }
  for (const ev of evs) {
    const kind = ev.action === 'pause' ? 'pause' : ev.action === 'resume' ? 'resume' : 'other';
    const chip = el('span', `ev ${kind}`);
    chip.appendChild(el('span', 'ev-dot'));
    chip.appendChild(el('span', undefined, ev.action));
    chip.appendChild(el('span', 'ev-when', fmtAgo(ev.ts, now)));
    fEvents.appendChild(chip);
  }
}

function renderSync(): void {
  if (conn === 'down' && everConnected) {
    syncNote.textContent = `lost · ${Math.round(ageOffsetMs / 1000)}s ago`;
    syncMark.style.color = 'var(--red)';
  } else if (conn === 'ok') {
    syncNote.textContent = 'live';
    syncMark.style.color = '';
  } else {
    syncNote.textContent = conn === 'down' ? 'offline' : '—';
    syncMark.style.color = conn === 'down' ? 'var(--red)' : '';
  }
}

function renderConn(): void {
  connEl.classList.toggle('show', conn === 'down');
  if (conn === 'down') connWhy.textContent = `retrying every 2s — ${lastError}`;
}

/* ------------------------------------------------------------- polling */

function setConnDown(err: unknown): void {
  conn = 'down';
  let msg = err instanceof ApiError ? err.message : String(err);
  // keep the banner readable — never dump raw HTML bodies into the UI
  if (msg.length > 120) msg = msg.slice(0, 117) + '…';
  lastError = msg;
  renderConn();
}

async function pollState(): Promise<void> {
  try {
    const s = await fetchState();
    state = s;
    everConnected = true;
    if (conn !== 'ok') {
      conn = 'ok';
      lastError = '';
      renderConn();
    }
    lastSyncAt = performance.now();
    ageOffsetMs = 0;
    setFavicon(s.paused);
  } catch (err) {
    setConnDown(err);
  }
  renderHero();
  renderInflight();
  renderButton();
  renderTempGuard();
  renderFooter();
  renderSync();
}

async function pollHistory(): Promise<void> {
  if (histLoading) return;
  histLoading = true;
  histNote.textContent = '…';
  try {
    const h = await fetchHistory(histPage, histSize);
    history = h;
    historyError = null;
  } catch (err) {
    // history failure alone must NOT disable the pause control — only
    // /state failures mean the proxy is unreachable.
    let msg = err instanceof ApiError ? err.message : String(err);
    if (msg.length > 120) msg = msg.slice(0, 117) + '…';
    historyError = msg;
  } finally {
    histLoading = false;
    histNote.textContent = '';
    renderHistory();
    renderSync();
  }
}

/* ------------------------------------------------------------- actions */

function clearConfirm(): void {
  if (confirmTimer) {
    window.clearTimeout(confirmTimer);
    confirmTimer = 0;
  }
  pauseBtn.classList.remove('confirming');
}

pauseBtn.addEventListener('click', () => {
  if (!state || actionPending || conn === 'down') return;
  const target = state.paused ? 'resume' : 'pause';

  if (target === 'pause' && !pauseBtn.classList.contains('confirming')) {
    // step 1 of 2: surface the warning before the destructive action
    pauseBtn.classList.add('confirming');
    btnLabel.textContent = '⚠ CONFIRM — HOLD ALL';
    confirmTimer = window.setTimeout(() => {
      clearConfirm();
      renderButton();
    }, CONFIRM_MS);
    return;
  }

  clearConfirm();
  actionPending = true;
  renderButton();
  const call = target === 'pause' ? pause() : resume();
  call
    .then((res) => {
      // optimistic: trust the envelope unless it contradicts us
      if (res.ok || res.paused === (target === 'pause')) {
        if (state) {
          state = {
            ...state,
            paused: res.paused,
            paused_at: res.paused ? Date.now() / 1000 : null,
          };
        }
      }
    })
    .catch((err) => {
      setConnDown(err);
    })
    .finally(() => {
      actionPending = false;
      void pollState();
    });
});

tgSwitch.addEventListener('click', () => {
  const s = state;
  const g = s?.temp_guard;
  if (!s || !g || !g.config_enabled || guardPending || conn === 'down') return;
  const target = !g.user_enabled;
  // optimistic flip — the next /state poll re-checks against the server.
  // apply() merges into the CURRENT state so a /state poll landing while
  // the POST is in flight is not clobbered.
  guardPending = true;
  const apply = (enabled: boolean): void => {
    const cur = state;
    if (!cur?.temp_guard) return;
    const g0 = cur.temp_guard;
    state = { ...cur, temp_guard: { ...g0, user_enabled: enabled, enabled: g0.config_enabled && enabled } };
  };
  apply(target);
  renderTempGuard();
  setTempGuard(target)
    .then((res) => {
      if (!res.ok || res.enabled !== target) apply(!target); // server disagreed
    })
    .catch((err) => {
      if (err instanceof ApiError && err.status === 400) {
        // malformed body / unknown endpoint — revert the flip, keep the console alive
        apply(!target);
      } else {
        setConnDown(err);
        apply(!target);
      }
    })
    .finally(() => {
      guardPending = false;
      renderTempGuard();
      void pollState();
    });
});

pagePrev.addEventListener('click', () => {
  if (histPage > 1) {
    histPage -= 1;
    void pollHistory();
  }
});

pageNext.addEventListener('click', () => {
  histPage += 1;
  void pollHistory();
});

pageSize.addEventListener('change', () => {
  const n = Number(pageSize.value);
  histSize = Number.isFinite(n) && n > 0 ? n : 20;
  histPage = 1;
  void pollHistory();
});

/* ------------------------------------------------------------- tick */

function tick(): void {
  const now = new Date();
  clockEl.textContent = now.toLocaleTimeString('en-GB', { hour12: false });
  ageOffsetMs = performance.now() - lastSyncAt;

  for (const row of iflightRows.values()) tickAge(row, row.baseAge, row.baseAt);
  renderHero();
  renderSync();
}

/* ------------------------------------------------------------- boot */

setFavicon(false);
void pollState();
void pollHistory();
window.setInterval(tick, 1000);
window.setInterval(() => {
  void pollState();
}, STATE_MS);
