"""Port of the SSE emission from completions.lua.

Pure functions returning the ordered list of SSE payload strings (the
server layer prefixes each with "data: " and appends "\\n\\n", exactly like
the Lua sse() helper which uses \\n\\n — not \\r\\n).
"""
from __future__ import annotations

import random
from typing import Any

from .markup import CHUNK_SIZE, json_encode, split_text, strip_tool_calls
from .toolguard import TOOLNAME_NOTE, ToolNameGuard

HEX = "0123456789abcdef"


def _field_str(v: Any) -> str:
    """Lua field_str: null/None becomes "", anything else is stringified."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    return json_encode(v)


def random_hex(length: int) -> str:
    return "".join(random.choice(HEX) for _ in range(length))


def sse_error(message: str) -> str:
    """SSE payload for an upstream failure after the handshake was sent."""
    return json_encode({"error": {"message": message, "type": "upstream_error"}})


def keepalive_chunk(is_chat: bool, cid: str, created: int, model: str) -> str:
    """Fake empty chunk mirroring the upstream chunk shape (see Lua
    send_keepalive)."""
    if is_chat:
        chunk = {
            "id": cid, "object": "chat.completion.chunk",
            "created": created, "model": model,
            "choices": [{"index": 0, "delta": {}, "logprobs": None,
                         "finish_reason": None}],
        }
    else:
        chunk = {
            "id": cid, "object": "text_completion",
            "created": created, "model": model,
            "choices": [{"index": 0, "text": "", "logprobs": None,
                         "finish_reason": None, "stop_reason": None}],
        }
    return json_encode(chunk)


def _chat_delta(base: dict, delta: dict) -> str:
    return json_encode({
        "id": base["id"], "object": "chat.completion.chunk",
        "created": base["created"], "model": base["model"],
        "choices": [{"index": 0, "delta": delta, "logprobs": None,
                     "finish_reason": None, "token_ids": None}],
    })


def _tool_call_deltas(base: dict, call: dict, index: int) -> list[str]:
    """One tool call as OpenAI-style delta.tool_calls events: a first event
    carrying id/type/function.name, then one event per arguments fragment."""
    out = []
    cid = call.get("id") or ("call_" + random_hex(24))
    out.append(json_encode({
        "id": base["id"], "object": "chat.completion.chunk",
        "created": base["created"], "model": base["model"],
        "choices": [{"index": 0, "delta": {"content": None, "tool_calls": [{
            "id": cid, "type": "function", "index": index,
            "function": {"name": call.get("name"), "arguments": ""},
        }]}, "logprobs": None, "finish_reason": None}],
    }))
    for part in split_text(call.get("arguments") or "", CHUNK_SIZE):
        out.append(json_encode({
            "id": base["id"], "object": "chat.completion.chunk",
            "created": base["created"], "model": base["model"],
            "choices": [{"index": 0, "delta": {"content": None, "tool_calls": [{
                "id": None, "type": None, "index": index,
                "function": {"name": None, "arguments": part},
            }]}, "logprobs": None, "finish_reason": None}],
        }))
    return out


def emit_chat_stream(obj: dict, allowed: list[str] | None,
                     guard: ToolNameGuard | None) -> tuple[list[str], dict]:
    """Emit a chat.completion response as chat.completion.chunk SSE payload
    strings. Returns (payloads, stats) where stats carries the toolname
    remap/strip counts for the stats log. Mirrors the Lua emit_chat_stream."""
    out: list[str] = []
    stats = {"remap": 0, "strip": 0}
    choice = obj["choices"][0] if obj.get("choices") else None
    msg = choice.get("message") if choice else None
    finish_reason = (choice or {}).get("finish_reason") or "stop"
    base = {"id": obj.get("id"), "object": "chat.completion.chunk",
            "created": obj.get("created"), "model": obj.get("model")}

    first = {
        "id": obj.get("id"), "object": "chat.completion.chunk",
        "created": obj.get("created"), "model": obj.get("model"),
        "choices": [{"index": 0,
                     "delta": {"role": "assistant", "content": ""},
                     "logprobs": None, "finish_reason": None}],
    }
    if obj.get("prompt_token_ids") is not None:
        first["prompt_token_ids"] = obj["prompt_token_ids"]
    if obj.get("prompt_text") is not None:
        first["prompt_text"] = obj["prompt_text"]
    out.append(json_encode(first))

    calls: list[dict] = []
    reasoning = _field_str(msg.get("reasoning")) if msg else ""
    content = _field_str(msg.get("content")) if msg else ""

    if msg and isinstance(msg.get("tool_calls"), list):
        for tc in msg["tool_calls"]:
            fn = tc.get("function") if isinstance(tc, dict) else None
            calls.append({
                "id": tc.get("id") if isinstance(tc, dict) else None,
                "name": fn.get("name") if isinstance(fn, dict) else None,
                "arguments": _field_str(fn.get("arguments")) if isinstance(fn, dict) else "",
            })

    reasoning, calls_r = strip_tool_calls(reasoning)
    content, calls_c = strip_tool_calls(content)

    if guard is not None:
        kept_r, drop_r, remap_r, strip_r = guard.guard_calls(calls_r, allowed, obj.get("model"))
        kept_c, drop_c, remap_c, strip_c = guard.guard_calls(calls_c, allowed, obj.get("model"))
    else:
        kept_r, drop_r, remap_r, strip_r = calls_r, [], 0, 0
        kept_c, drop_c, remap_c, strip_c = calls_c, [], 0, 0
    fix_strips = strip_r + strip_c
    fix_names = list(drop_r) + list(drop_c)
    stats["remap"] = remap_r + remap_c
    stats["strip"] = fix_strips
    calls.extend(kept_r)
    calls.extend(kept_c)

    if fix_strips > 0:
        uniq: set[str] = set()
        listed: list[str] = []
        for nf in fix_names:
            nf = str(nf or "")
            if nf not in uniq:
                uniq.add(nf)
                listed.append("`" + nf + "`")
        verb = " is not an available tool." if len(listed) == 1 else " are not available tools."
        note = (TOOLNAME_NOTE + " Tool call skipped: " + ", ".join(listed) + verb
                + " Double-check the tool name and continue with the correct available tool.")
        content = note if content == "" else content + "\n\n" + note

    if calls:
        finish_reason = "tool_calls"

    if reasoning != "":
        for part in split_text(reasoning, CHUNK_SIZE):
            out.append(_chat_delta(base, {"reasoning": part}))

    if content != "":
        for part in split_text(content, CHUNK_SIZE):
            out.append(_chat_delta(base, {"content": part}))

    for i, call in enumerate(calls):
        out.extend(_tool_call_deltas(base, call, i))

    out.append(json_encode({
        "id": obj.get("id"), "object": "chat.completion.chunk",
        "created": obj.get("created"), "model": obj.get("model"),
        "choices": [{"index": 0, "delta": {}, "logprobs": None,
                     "finish_reason": finish_reason, "stop_reason": None,
                     "token_ids": None}],
    }))

    if obj.get("usage") is not None:
        usage: dict = {
            "id": obj.get("id"), "object": "chat.completion.chunk",
            "created": obj.get("created"), "model": obj.get("model"),
            "choices": [], "usage": obj["usage"],
        }
        if obj.get("system_fingerprint") is not None:
            usage["system_fingerprint"] = obj["system_fingerprint"]
        out.append(json_encode(usage))

    out.append("[DONE]")
    return out, stats


def _probe_strips(obj: dict, allowed: list[str], guard: ToolNameGuard) -> int:
    """Pure (silent) probe used before the optional one-shot resend: returns
    how many markup-converted calls the guard would strip from this response
    object, without logging or emitting anything. Mirrors analyze_response."""
    if not obj or not allowed:
        return 0
    choice = obj.get("choices")
    msg = choice[0].get("message") if choice else None
    if not msg:
        return 0
    calls_r = strip_tool_calls(_field_str(msg.get("reasoning")))[1]
    calls_c = strip_tool_calls(_field_str(msg.get("content")))[1]
    if not calls_r and not calls_c:
        return 0
    _, _, _, strip_r = guard.guard_calls(calls_r, allowed, obj.get("model"), noisy=False)
    _, _, _, strip_c = guard.guard_calls(calls_c, allowed, obj.get("model"), noisy=False)
    return strip_r + strip_c


def emit_text_stream(obj: dict) -> list[str]:
    """Emit a text_completion response as text_completion SSE payload
    strings, chunking choices[0].text and carrying finish_reason on the
    last text event, then [DONE]."""
    out: list[str] = []
    choice = obj["choices"][0] if obj.get("choices") else None
    text = (choice or {}).get("text") or ""
    finish_reason = (choice or {}).get("finish_reason") or "stop"
    parts = split_text(text, CHUNK_SIZE)

    for i, part in enumerate(parts):
        last = i == len(parts) - 1
        chunk: dict = {
            "id": obj.get("id"), "object": "text_completion",
            "created": obj.get("created"), "model": obj.get("model"),
            "choices": [{"index": 0, "text": part, "logprobs": None,
                         "finish_reason": finish_reason if last else None,
                         "stop_reason": None, "prompt_token_ids": None,
                         "token_ids": None}],
        }
        if last and obj.get("system_fingerprint") is not None:
            chunk["system_fingerprint"] = obj["system_fingerprint"]
        out.append(json_encode(chunk))

    out.append("[DONE]")
    return out
