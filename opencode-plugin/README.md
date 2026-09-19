# tool-name-fixer — OpenCode v1 plugin

A small, dependency-free OpenCode v1 (anomalyco/opencode fork, npm `opencode-ai` 1.18.x) plugin that watches for **hallucinated tool names** and nudges the model back to real tools.

## What it detects

1. **Invalid tool calls** — OpenCode funnels calls to unknown/hallucinated tool names into the hidden builtin tool named `invalid`. On every such call (`tool.execute.before` with `input.tool === "invalid"`) the plugin appends one postmortem log line. It never throws, so the builtin's error output still reaches the model normally.
2. **Sentinel echo** — a manual/programmatic intent channel: if an **assistant text part** contains the literal sentinel string, the session is flagged for a continuation nudge (deduped per message via `messageID`).

## Sentinel

```
[tool-name-fixer]
```

Detection is defensive about event shapes (`message.part.updated` / `message.updated`): it requires the part to be type `text` and the message role `assistant` (role may live on `part.role` or `info.role`); as a fallback it raw-searches the serialized event payload for the sentinel, but only flags if an assistant role marker is present in the payload.

## Behavior

- **Invalid-tool observation** — log entry shape, one JSON object per JSONL line:

  ```json
  {"time":"2026-01-01T00:00:00.000Z","sessionID":"ses_x","bogusTool":"docker_list_image","error":"No tool named ..."}
  ```

- **Auto-continuation** — when a flagged session emits `session.idle`, or `session.status` with `status.type === "idle"` (busy status is *skipped*; it only fires on idle signals), the pending entry is cleared **first** (re-entry protection), then this prompt is injected via `client.session.prompt` (no `noReply` — it intentionally triggers a new AI turn):

  ```
  [tool-name-fixer] Your previous message contained an invalid tool call (a tool name that does not exist). Before continuing: re-check the available tool names (they were listed in your system context), pick the correct existing tool that matches the intent, and call it. Do not invent tool names again.
  ```

- **Guards / limits** — 5-second per-session debounce lock on sent nudges; pending entries older than 5 minutes are auto-cleaned; all state is in-memory per OpenCode process; the prompt API is checked defensively (`client.session.prompt` must exist); failures are logged to console, never thrown; hooks are fully try/catch wrapped so the host never crashes.

## Install path used

- Source copy: `B:\intersub\temp\llm-proxy\opencode-plugin\tool-name-fixer.js`
- Installed (global OpenCode plugins dir, applies to all sessions):

  ```
  C:\Users\user\.config\opencode\plugins\tool-name-fixer.js
  ```

OpenCode v1 registers each named export in the file as an independent plugin; this file exports exactly one plugin: `ToolNameFixer`.

## How to disable

- Delete/rename the file `C:\Users\user\.config\opencode\plugins\tool-name-fixer.js` and restart OpenCode, or
- run OpenCode with `opencode --pure`.

## Viewing the log file

- Default path (win32): `C:\Users\user\.config\opencode\tool-name-fixer.log.jsonl` (i.e. `%USERPROFILE%\.config\opencode\tool-name-fixer.log.jsonl`). The file is created on the first invalid-tool event; the directory is created as needed.
- Override the location with the `TOOL_NAME_FIXER_LOG` environment variable.
- Tail it with PowerShell: `Get-Content "$env:USERPROFILE\.config\opencode\tool-name-fixer.log.jsonl" -Tail 20` (or plain `type` in cmd).
