"""Port of the antml/DSML tool-call markup parsing from completions.lua.

Faithful port of the Lua helpers: match_name, find_open_tag, find_close_tag,
extract_tool_calls, strip_tool_calls, split_text.

Deviation from the Lua (documented): the Lua scans bytes; Python scans
code points. For the ASCII markup tags this is identical, and the chunking
requirement (never split a code point, concatenation == original) is
preserved.
"""
from __future__ import annotations

import json
import re

CHUNK_SIZE = 16

_NAME_RE = re.compile(r'name="([^"]*)"')


def json_encode(obj) -> str:
    """Compact JSON, mirroring cjson.encode's default output."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _is_prefix_char(c: str) -> bool:
    return c in (" ", "\t", "\n", "\r", "|")


def _match_name(s: str, j: int, name: str) -> int | None:
    """After a literal '<' at index j (with an optional '/'), match the
    backend's tag prefix (whitespace, '|', optional "DSML") followed by
    *name*. Returns the index of the first char of *name*, or None."""
    k = j + 1
    if k < len(s) and s[k] == "/":
        k += 1
    while k < len(s) and _is_prefix_char(s[k]):
        k += 1
    if s[k:k + 4] == "DSML":
        k += 4
        while k < len(s) and _is_prefix_char(s[k]):
            k += 1
    if s[k:k + len(name)] == name:
        return k
    return None


def _find_open_tag(s: str, frm: int, name: str) -> tuple[int, int] | None:
    """Find the next '<name' open tag at/after index frm; return
    (index_of_lt, index_of_gt) or None."""
    i = frm
    while True:
        lt = s.find("<", i)
        if lt < 0:
            return None
        if s[lt + 1:lt + 2] != "/":
            k = _match_name(s, lt, name)
            if k is not None:
                gt = s.find(">", k + len(name))
                if gt >= 0:
                    return lt, gt
        i = lt + 1


def _find_close_tag(s: str, frm: int, name: str) -> tuple[int, int] | None:
    """Find the next '</name' close tag at/after index frm; return
    (index_of_lt, index_of_gt) or None."""
    i = frm
    while True:
        lt = s.find("</", i)
        if lt < 0:
            return None
        k = _match_name(s, lt, name)
        if k is not None:
            gt = s.find(">", k + len(name))
            if gt >= 0:
                return lt, gt
        i = lt + 1


def extract_tool_calls(text: str) -> list[dict]:
    """Parse <invoke>...</invoke> blocks (with the optional <| |DSML|> tag
    prefixes) into {id, name, arguments} where arguments is a JSON object of
    the <parameter name="..."> values. Mirrors the Lua extract_tool_calls."""
    calls: list[dict] = []
    pos = 0
    while True:
        ot = _find_open_tag(text, pos, "invoke")
        if not ot:
            break
        lt, gt = ot
        ct = _find_close_tag(text, gt + 1, "invoke")
        if not ct:
            break
        clt, cgt = ct
        m = _NAME_RE.search(text, lt)
        name = m.group(1) if m else ""
        params: dict[str, str] = {}
        body = text[gt + 1:clt]
        p = 0
        while True:
            pot = _find_open_tag(body, p, "parameter")
            if not pot:
                break
            plt, pgt = pot
            pct = _find_close_tag(body, pgt + 1, "parameter")
            if not pct:
                break
            pclt, pcgt = pct
            pm = _NAME_RE.search(body, plt)
            if pm:
                params[pm.group(1)] = body[pgt + 1:pclt]
            p = pcgt + 1
        calls.append({"id": None, "name": name, "arguments": json_encode(params)})
        pos = cgt + 1
    return calls


def strip_tool_calls(text: str) -> tuple[str, list[dict]]:
    """Remove <tool_calls>...</tool_calls> and bare <invoke>...</invoke>
    blocks, returning (cleaned_text, calls). Mirrors the Lua strip_tool_calls."""
    calls: list[dict] = []

    def strip_blocks(t: str, name: str) -> str:
        out: list[str] = []
        pos = 0
        while True:
            ot = _find_open_tag(t, pos, name)
            if not ot:
                out.append(t[pos:])
                break
            lt, gt = ot
            ct = _find_close_tag(t, gt + 1, name)
            if not ct:
                out.append(t[pos:])
                break
            clt, cgt = ct
            out.append(t[pos:lt])
            calls.extend(extract_tool_calls(t[lt:cgt + 1]))
            pos = cgt + 1
        return "".join(out)

    t = strip_blocks(text, "tool_calls")
    t = strip_blocks(t, "invoke")
    return re.sub(r"\s+$", "", t), calls


def split_text(s: str, max_len: int = CHUNK_SIZE) -> list[str]:
    """Partition *s* into pieces of at most max_len code points, breaking on
    ASCII spaces when possible. Concatenating the parts reproduces *s*
    exactly and every part is a whole number of code points. Mirrors the Lua
    split_text (which is byte-based; same guarantees hold here)."""
    parts: list[str] = []
    n = len(s)
    pos = 0
    while pos < n:
        cut: int | None = None
        count = 0
        i = pos
        while i < n and count < max_len:
            if s[i] == " ":
                cut = i - 1
            i += 1
            count += 1
        if cut is not None and cut >= pos:
            endpos = cut
        else:
            endpos = i - 1
        parts.append(s[pos:endpos + 1])
        pos = endpos + 1
    return parts