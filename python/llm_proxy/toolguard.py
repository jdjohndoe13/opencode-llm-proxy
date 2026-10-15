"""Port of the tool-name guard from completions.lua.

Hallucinated tool names lifted from response markup are validated against
the request's tools[].function.name list:
  kept   when its name is in the allowed set;
  remapped (mode "remap") to one assured real tool when confidently
         matching:
           R1: levenshtein(name, candidate) <= 2 with a unique minimum,
           R2: same server prefix and >= 1 shared alpha token (len >= 5)
               with exactly one candidate matching in that server;
  dropped  otherwise: removed from the emission and reported via a
             corrective note appended to the content.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from .markup import json_encode

TOOLNAME_NOTE = "[tool-name-fixer]"

_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9]*")
_PREFIX_RE = re.compile(r"^[^_]+")


def normalize_guard(v: str | None) -> str:
    v = str(v or "").lower()
    if v in ("off", "strip", "remap"):
        return v
    return "remap"


def normalize_bool(v: str | None) -> bool:
    if v is None:
        return True
    v = str(v).lower()
    return not (v in ("0", "false", "off", "no"))


def levenshtein(a: str, b: str) -> int:
    """Bounded Levenshtein (single-row DP). Strings longer than 64 chars are
    compared over their 64-char tail, exactly like the Lua port."""
    if len(a) > 64:
        a = a[-64:]
    if len(b) > 64:
        b = b[-64:]
    m, n = len(a), len(b)
    if m == 0:
        return n
    if n == 0:
        return m
    prev = list(range(n + 1))
    for i in range(1, m + 1):
        cur = [i] + [0] * n
        ai = a[i - 1]
        for j in range(1, n + 1):
            cost = 0 if b[j - 1] == ai else 1
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
        prev = cur
    return prev[n]


def name_tokens(name: str | None) -> set[str]:
    """Lowercase alpha tokens of a tool name:
    "chrome-devtools_Take_Snapshot" -> chrome, devtools, take, snapshot."""
    return {t for t in _TOKEN_RE.findall(str(name or "").lower())}


def server_prefix(name: str | None) -> str:
    """Everything before the first '_' ("chrome-devtools" in
    "chrome-devtools_take_snapshot"), else ""."""
    m = _PREFIX_RE.match(str(name or ""))
    return m.group(0) if m else ""


def suffix_tokens(name: str | None) -> set[str]:
    """Alpha tokens of the part AFTER the server prefix (distinctive intent
    tokens, not the server name itself)."""
    n = str(name or "")
    p = server_prefix(n)
    if p != "" and len(n) > len(p) + 1 and n[len(p)] == "_":
        n = n[len(p) + 1:]
    return name_tokens(n)


def allowed_names(request_body: dict) -> list[str] | None:
    """Tool names allowed by the request (tools[].function.name, deduped,
    order-preserving). None when the request has no "tools" array (the guard
    is skipped); an empty array yields an empty list (every markup call is
    dropped)."""
    tools = request_body.get("tools")
    if not isinstance(tools, list):
        return None
    names: list[str] = []
    seen: set[str] = set()
    for tool in tools:
        fn = tool.get("function") if isinstance(tool, dict) else None
        if isinstance(fn, dict):
            name = fn.get("name")
            if isinstance(name, str) and name != "" and name not in seen:
                seen.add(name)
                names.append(name)
    return names


class ToolNameGuard:
    """Guard mode + fix-log writer. Mirrors the Lua guard_tool_calls and
    log_toolname_fix."""

    def __init__(self, mode: str, retry_enabled: bool, log_path: Path | str):
        self.mode = normalize_guard(mode)
        self.retry_enabled = retry_enabled
        self.log_path = Path(log_path)

    def _log_fix(self, bogus: str, action: str, mapped: str, model: str | None) -> None:
        entry = {
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "model": model,
            "bogus": bogus,
            "action": action,
            "mapped": mapped,
        }
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json_encode(entry) + "\n")
        except OSError as err:
            import logging

            logging.warning("failed to open toolname log %s: %s", self.log_path, err)

    def guard_calls(
        self,
        calls: list[dict],
        allowed: list[str] | None,
        model: str | None,
        noisy: bool = True,
    ) -> tuple[list[dict], list[str], int, int]:
        """Returns (kept, dropped, n_remap, n_strip). dropped holds the
        original bogus names (one entry per dropped call, duplicates
        included). When noisy is False (probe runs), decisions are not
        written to the toolname log."""
        if allowed is None or self.mode == "off":
            return calls, [], 0, 0

        kept: list[dict] = []
        dropped: list[str] = []
        n_remap, n_strip = 0, 0
        allowed_set = set(allowed)
        allowed_low = [a.lower() for a in allowed]

        for call in calls:
            name = str(call.get("name") or "")
            if name in allowed_set:
                kept.append(call)
            elif self.mode == "remap" and name != "":
                mapped = None
                # R1: closest allowed name within distance 2, unique minimum.
                lname = name.lower()
                best, bd, bd_count = None, None, 0
                for i, cand in enumerate(allowed):
                    d = levenshtein(lname, allowed_low[i])
                    if bd is None or d < bd:
                        best, bd, bd_count = cand, d, 1
                    elif d == bd:
                        bd_count += 1
                if bd is not None and bd <= 2 and bd_count == 1:
                    mapped = best

                # R2: same server prefix, >= 1 shared distinctive token
                # (len >= 5) after the server prefix, exactly one candidate
                # in that server.
                if mapped is None:
                    prefix = server_prefix(name)
                    if prefix != "":
                        btokens = suffix_tokens(name)
                        cands: set[str] = set()
                        for i, cand in enumerate(allowed):
                            if cand != name and server_prefix(cand) == prefix:
                                ctokens = suffix_tokens(cand)
                                for tok in btokens:
                                    if len(tok) >= 5 and tok in ctokens:
                                        cands.add(cand)
                                        break
                        if len(cands) == 1:
                            mapped = next(iter(cands))

                if mapped is not None:
                    n_remap += 1
                    if noisy:
                        self._log_fix(name, "remap", mapped, model)
                    kept.append({"id": call.get("id"), "name": mapped,
                                 "arguments": call.get("arguments")})
                else:
                    n_strip += 1
                    if noisy:
                        self._log_fix(name, "drop", "", model)
                    dropped.append(name)
            else:
                n_strip += 1
                if name != "" and noisy:
                    self._log_fix(name, "drop", "", model)
                dropped.append(name)

        return kept, dropped, n_remap, n_strip
