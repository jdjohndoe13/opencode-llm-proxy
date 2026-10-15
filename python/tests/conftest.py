"""Shared helpers: port of the selftest.lua reassemble() (lines 49-87)."""
from __future__ import annotations

import json

TOOLS = [
    "bash", "read", "edit", "grep", "glob",
    "chrome-devtools_click",
    "chrome-devtools_evaluate_script",
    "chrome-devtools_navigate_page",
    "chrome-devtools_take_screenshot",
    "chrome-devtools_take_snapshot",
    "chrome-devtools_upload_file",
    "chrome-devtools_wait_for",
    "firefox-devtools_take_snapshot",
    "firefox-devtools_upload_file_by_uid",
]

SENTINEL = "[tool-name-fixer]"
BOGUS = "chrome-devtools_snapshot_for_upload_ready_check"


def chat_completion(model: str, message: dict, finish: str) -> dict:
    """Build a chat.completion object the way the vLLM backend returns it."""
    return {
        "id": "chatcmpl-test", "object": "chat.completion",
        "created": 1789837609, "model": model,
        "choices": [{"index": 0, "message": message,
                     "finish_reason": finish, "logprobs": None}],
        "usage": None,
    }


def reassemble(whole: str) -> tuple[str, dict, str]:
    """Parse `data: <json>` lines and aggregate the delta stream into one
    message: (content, calls by index, finish)."""
    parts: list[str] = []
    calls: dict[int, dict] = {}
    finish = ""
    for line in whole.splitlines():
        if not line.startswith("data: "):
            continue
        payload = line[len("data: "):]
        if payload == "[DONE]":
            continue
        try:
            j = json.loads(payload)
        except ValueError:
            continue
        choices = j.get("choices")
        if isinstance(choices, list) and choices:
            choice = choices[0]
            delta = choice.get("delta") or {}
            if isinstance(delta.get("content"), str):
                parts.append(delta["content"])
            tcs = delta.get("tool_calls")
            if isinstance(tcs, list):
                for tc in tcs:
                    idx = tc.get("index") or 0
                    c = calls.setdefault(idx, {"id": "", "name": "", "args": ""})
                    if isinstance(tc.get("id"), str) and tc["id"] != "":
                        c["id"] = tc["id"]
                    fn = tc.get("function")
                    if isinstance(fn, dict):
                        if isinstance(fn.get("name"), str) and fn["name"] != "":
                            c["name"] = fn["name"]
                        if isinstance(fn.get("arguments"), str):
                            c["args"] += fn["arguments"]
            if isinstance(choice.get("finish_reason"), str) and choice["finish_reason"]:
                finish = choice["finish_reason"]
    return "".join(parts), calls, finish
