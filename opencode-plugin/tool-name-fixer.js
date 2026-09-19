/**
 * tool-name-fixer — OpenCode v1 plugin (anomalyco/opencode fork, npm opencode-ai 1.18.x)
 *
 * Purpose
 *   1. Postmortem log for hallucinated tool calls: OpenCode funnels unknown
 *      tool names into the hidden builtin tool `invalid`. When that happens we
 *      append one JSON line to a log file. This hook NEVER throws (so the
 *      builtin "invalid" tool still runs and its output reaches the model).
 *   2. Pending-nudge flow: when an assistant *text* part contains the sentinel
 *      string, remember the session; when that session next goes idle, inject
 *      a continuation prompt (docs-sanctioned session.prompt form, no noReply
 *      — intentionally triggers a new AI turn) telling the model to re-check
 *      real tool names before calling anything.
 *
 * Notes
 *   - Plain JS (ESM). No external npm deps. No "@opencode-ai/plugin" import —
 *     plain object hooks are all that is needed.
 *   - Exactly ONE named export (ToolNameFixer). In OpenCode v1 every named
 *     export is registered as an independent plugin, so extra exports here
 *     would register extra plugin instances.
 *   - All state is in-memory and process-local; entries auto-expire.
 *   - Designed to never crash the host: every hook body is wrapped.
 */

const SENTINEL = "[tool-name-fixer]";

const CONTINUATION_TEXT =
  "[tool-name-fixer] Your previous message contained an invalid tool call (a tool name that does not exist). Before continuing: re-check the available tool names (they were listed in your system context), pick the correct existing tool that matches the intent, and call it. Do not invent tool names again.";

const CLEAN_AFTER_MS = 5 * 60 * 1000;     // pending entries older than 5 min are dropped
const NUDGE_DEBOUNCE_MS = 5 * 1000;       // max one nudge per session per 5 seconds
const LAST_NUDGE_TTL_MS = 30 * 60 * 1000; // housekeeping for the debounce map

const WIN32_DEFAULT_LOG = "C:/Users/user/.config/opencode/tool-name-fixer.log.jsonl";
const GENERIC_LOG_NAME = "tool-name-fixer.log.jsonl";

/** Load a node builtin: require-first (Bun exposes require in ESM), dynamic import fallback (Node ESM). */
async function loadBuiltin(name) {
  if (typeof require === "function") {
    try {
      const mod = require(name);
      return mod && mod.default ? mod.default : mod;
    } catch {}
  }
  try {
    const mod = await import(name);
    return mod && mod.default ? mod.default : mod;
  } catch {}
  return null;
}

function str(value) {
  return typeof value === "string" ? value : "";
}

function envLogOverride() {
  try {
    if (typeof process === "undefined") return "";
    const value = (process.env || {}).TOOL_NAME_FIXER_LOG;
    return typeof value === "string" && value.trim() ? value.trim() : "";
  } catch {
    return "";
  }
}

function resolveLogPath(os, path) {
  const fromEnv = envLogOverride();
  if (fromEnv) return fromEnv;
  try {
    if (typeof process !== "undefined" && process.platform === "win32") {
      return WIN32_DEFAULT_LOG; // spec default for win32
    }
  } catch {}
  try {
    const home = os && typeof os.homedir === "function" ? os.homedir() : "";
    if (home && path && typeof path.join === "function") {
      return path.join(home, ".config", "opencode", GENERIC_LOG_NAME);
    }
  } catch {}
  return GENERIC_LOG_NAME;
}

function appendInvalidToolLog(fs, path, logPath, entry) {
  try {
    if (!fs || !logPath) return false;
    try {
      if (path && typeof path.dirname === "function") {
        fs.mkdirSync(path.dirname(logPath), { recursive: true });
      }
    } catch {}
    fs.appendFileSync(logPath, JSON.stringify(entry) + "\n", "utf8");
    return true;
  } catch {
    return false; // logging failures must never break tool execution
  }
}

/**
 * Scan a message.* bus event for an assistant text part containing SENTINEL.
 * Defensive on purpose: event shapes are checked at runtime.
 * Returns { hit, sessionID, messageID, how } or null.
 */
function detectSentinel(event) {
  try {
    if (!event || typeof event !== "object") return null;
    const type = str(event.type);
    if (!type || type.indexOf("message.") !== 0) return null; // message.part.updated / message.updated / ...

    const properties =
      event.properties && typeof event.properties === "object" ? event.properties : {};
    const info =
      properties.info && typeof properties.info === "object" ? properties.info : null;
    const part =
      properties.part && typeof properties.part === "object" ? properties.part : null;

    let text = "";
    let role = "";

    if (part) {
      if (part.type && part.type !== "text") return null; // non-text part: no signal
      text = str(part.text) || str(part.content);
      role = str(part.role);
    }

    if (info && typeof info === "object") {
      role = role || str(info.role); // role may live on part or on the message info
      if (!text) {
        if (Array.isArray(info.parts)) {
          text = info.parts
            .filter((p) => p && typeof p === "object" && (!p.type || p.type === "text"))
            .map((p) => str(p.text) || str(p.content))
            .join("\n");
        }
        if (!text) text = str(info.text);
      }
    }

    if (!text && typeof properties.delta === "string") {
      text = properties.delta; // incremental deltas are best-effort (sentinel may split)
    }

    const sessionID =
      (info ? str(info.sessionID) : "") ||
      (part ? str(part.sessionID) : "") ||
      str(properties.sessionID) ||
      str(event.sessionID);
    const messageID =
      (info ? str(info.id) : "") || (part ? str(part.messageID) : "") || str(properties.messageID);

    if (role === "assistant" && text.indexOf(SENTINEL) !== -1) {
      return { hit: true, sessionID, messageID, how: "part" };
    }

    if (role !== "assistant") {
      // Fallback: raw search of the serialized payload — still require an
      // assistant role marker inside the payload before flagging.
      try {
        const raw = JSON.stringify(event);
        if (raw && raw.indexOf(SENTINEL) !== -1 && /"role"\s*:\s*"assistant"/.test(raw)) {
          return { hit: true, sessionID, messageID, how: "raw" };
        }
      } catch {}
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * Session-idle signals: "session.idle", or "session.status" whose
 * status.type === "idle". Busy/unknown -> null (never fire on busy).
 */
function idleSessionFromEvent(event) {
  try {
    if (!event || typeof event !== "object") return null;
    const type = str(event.type);
    if (type !== "session.idle" && type !== "session.status") return null;

    const properties =
      event.properties && typeof event.properties === "object" ? event.properties : {};
    const info = properties.info && typeof properties.info === "object" ? properties.info : null;

    const sessionID = str(properties.sessionID) || (info ? str(info.sessionID) : "") || str(event.sessionID);
    if (!sessionID) return null;

    if (type === "session.idle") return sessionID;

    const status =
      properties.status !== undefined ? properties.status : info ? info.status : undefined;
    let statusType = "";
    if (status && typeof status === "object") statusType = str(status.type);
    else if (typeof status === "string") statusType = status;
    return statusType === "idle" ? sessionID : null;
  } catch {
    return null;
  }
}

export const ToolNameFixer = async (ctx) => {
  const fs = await loadBuiltin("node:fs");
  const os = await loadBuiltin("node:os");
  const path = await loadBuiltin("node:path");

  const logPath = resolveLogPath(os, path);

  // ---- in-memory state ----
  const pendingNudges = new Set();  // sessionIDs flagged by sentinel detection
  const pendingRecords = new Map(); // sessionID -> { ts, messageIDs: Set<string> }
  const lastNudgeAt = new Map();    // sessionID -> epoch ms of last nudge actually sent

  function cleanup(now) {
    try {
      for (const [sessionID, rec] of pendingRecords) {
        if (!rec || now - (rec.ts || 0) >= CLEAN_AFTER_MS) {
          pendingRecords.delete(sessionID);
          pendingNudges.delete(sessionID);
        }
      }
      for (const [sessionID, ts] of lastNudgeAt) {
        if (now - ts >= LAST_NUDGE_TTL_MS) lastNudgeAt.delete(sessionID);
      }
    } catch {}
  }

  async function fireNudge(sessionID) {
    try {
      if (!sessionID) return false;

      const now = Date.now();
      const last = lastNudgeAt.get(sessionID) || 0;
      if (now - last < NUDGE_DEBOUNCE_MS) return false; // 5s duplicate suppression

      const client = ctx && typeof ctx === "object" ? ctx.client : null;
      const sessionApi = client && typeof client === "object" ? client.session : null;
      if (!sessionApi || typeof sessionApi.prompt !== "function") return false; // defensive API check

      await sessionApi.prompt({
        path: { id: sessionID },
        body: { parts: [{ type: "text", text: CONTINUATION_TEXT }] },
        // NOTE: no noReply here — this form intentionally triggers a new AI turn.
      });
      lastNudgeAt.set(sessionID, Date.now());
      return true;
    } catch (err) {
      try {
        const msg = err && err.message ? err.message : String(err);
        console.error(
          "[tool-name-fixer] continuation prompt failed for session " + sessionID + ": " + msg
        );
      } catch {}
      return false;
    }
  }

  return {
    "tool.execute.before": async (input, output) => {
      try {
        const inArgs = input && typeof input === "object" ? input : {};
        if (str(inArgs.tool) !== "invalid") return;

        const outObj = output && typeof output === "object" ? output : {};
        const args = outObj.args && typeof outObj.args === "object" ? outObj.args : {};

        appendInvalidToolLog(fs, path, logPath, {
          time: new Date().toISOString(),
          sessionID: str(inArgs.sessionID),
          bogusTool: args.tool !== undefined ? args.tool : null,
          error: args.error !== undefined ? args.error : null,
        });
      } catch {
        // NEVER throw from tool.execute.before — let the builtin "invalid" tool run.
      }
    },

    event: async (container) => {
      try {
        let event = container && typeof container === "object" ? container : null;
        if (event && event.event && typeof event.event === "object") event = event.event; // ({ event }) unwrap
        if (!event || typeof event !== "object") return;

        cleanup(Date.now());

        // 1) Sentinel detection in assistant text parts.
        const signal = detectSentinel(event);
        if (signal && signal.hit && signal.sessionID) {
          let rec = pendingRecords.get(signal.sessionID);
          if (!rec) {
            rec = { ts: Date.now(), messageIDs: new Set() };
            pendingRecords.set(signal.sessionID, rec);
          }
          if (!rec.messageIDs.has(signal.messageID)) {
            rec.messageIDs.add(signal.messageID);
            rec.ts = Date.now();
            pendingNudges.add(signal.sessionID);
          }
          // Same message seen again -> dedupe (no re-flag, no double nudge).
        }

        // 2) Idle trigger -> clear entry FIRST, then fire continuation prompt.
        const idleSession = idleSessionFromEvent(event);
        if (idleSession && pendingNudges.has(idleSession)) {
          pendingNudges.delete(idleSession); // clear FIRST — avoids re-entry loops
          pendingRecords.delete(idleSession);
          await fireNudge(idleSession);
        }
      } catch {
        // Never crash the host.
      }
    },
  };
};
