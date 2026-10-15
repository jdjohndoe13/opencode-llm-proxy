"""Port of the selftest.lua T1-T6 cases: emit_chat_stream + the guard."""
import json

from llm_proxy.sse import emit_chat_stream, emit_text_stream
from llm_proxy.toolguard import ToolNameGuard
from tests.conftest import BOGUS, SENTINEL, TOOLS, chat_completion, reassemble

CP = "</" + "parameter" + ">"


def P(name, value):
    return f'<parameter name="{name}">{value}{CP}'


def _whole(payloads):
    return "".join("data: " + p + "\n\n" for p in payloads)


def _guard(tmp_path):
    return ToolNameGuard("remap", True, tmp_path / "fixes.jsonl")


def _emit(obj, tmp_path, tools=TOOLS):
    payloads, stats = emit_chat_stream(obj, tools, _guard(tmp_path))
    return _whole(payloads), stats


def test_t1_incident_replay(tmp_path):
    msg = {"role": "assistant",
           "content": '<invoke name="' + BOGUS + '"></invoke>',
           "reasoning": "", "tool_calls": None}
    whole, stats = _emit(chat_completion("glm-5.3-flash", msg, "stop"),
                         tmp_path)
    content, calls, finish = reassemble(whole)
    assert '"' + BOGUS + '"' not in whole
    assert len(calls) == 0
    assert finish == "stop"
    assert SENTINEL in content
    assert "`" + BOGUS + "`" in content
    assert "<invoke" not in whole
    assert stats["strip"] == 1 and stats["remap"] == 0


def test_t2_valid_markup_lifted(tmp_path):
    msg = {"role": "assistant",
           "content": '<invoke name="chrome-devtools_take_snapshot">'
                      + P("pageId", "28") + "</invoke>",
           "reasoning": ""}
    whole, stats = _emit(chat_completion("qwen-3.8", msg, "stop"), tmp_path)
    content, calls, finish = reassemble(whole)
    tc = calls.get(0)
    assert tc is not None and tc["name"] == "chrome-devtools_take_snapshot"
    assert "pageId" in tc["args"]
    assert finish == "tool_calls"
    assert SENTINEL not in content
    assert stats["remap"] == 0 and stats["strip"] == 0


def test_t3_unique_remap(tmp_path):
    msg = {"role": "assistant",
           "content": '<invoke name="chrome-devtools_snapshot"></invoke>',
           "reasoning": ""}
    whole, _ = _emit(chat_completion("glm-5.3-flash", msg, "stop"), tmp_path)
    content, calls, finish = reassemble(whole)
    tc = calls.get(0)
    assert tc is not None and tc["name"] == "chrome-devtools_take_snapshot"
    assert finish == "tool_calls"
    assert SENTINEL not in content


def test_t4_ambiguous_dropped(tmp_path):
    msg = {"role": "assistant",
           "content": '<invoke name="chrome-devtools_snapshot_for_upload">'
                      '</invoke>',
           "reasoning": ""}
    whole, _ = _emit(chat_completion("glm-5.3-flash", msg, "stop"), tmp_path)
    content, calls, finish = reassemble(whole)
    assert len(calls) == 0
    assert "chrome-devtools_snapshot_for_upload" in content


def test_t5_namespace_params(tmp_path):
    msg = {"role": "assistant", "reasoning": "",
           "content": '<invoke name="chrome-devtools_upload_file">'
                      + P("uid", "snap-1")
                      + P("filePaths", '["C:/x.png"]') + "</invoke>"}
    whole, _ = _emit(chat_completion("glm-5.3-flash", msg, "stop"), tmp_path)
    content, calls, finish = reassemble(whole)
    tc = calls.get(0)
    assert tc is not None and tc["name"] == "chrome-devtools_upload_file"
    assert "uid" in tc["args"] and "filePaths" in tc["args"]
    assert SENTINEL not in content


def test_t6_no_tools_legacy(tmp_path):
    msg = {"role": "assistant",
           "content": '<invoke name="' + BOGUS + '"></invoke>',
           "reasoning": ""}
    payloads, stats = emit_chat_stream(
        chat_completion("glm-5.3-flash", msg, "stop"), None, _guard(tmp_path))
    whole = _whole(payloads)
    assert '"' + BOGUS + '"' in whole
    assert SENTINEL not in whole
    assert stats["remap"] == 0 and stats["strip"] == 0


def test_message_tool_calls_passthrough(tmp_path):
    msg = {"role": "assistant", "content": None,
           "tool_calls": [{"id": "call_1",
                           "function": {"name": "anything_goes",
                                        "arguments": '{"a":1}'}}]}
    whole, stats = _emit(chat_completion("m", msg, "tool_calls"), tmp_path)
    content, calls, finish = reassemble(whole)
    assert finish == "tool_calls"
    assert calls[0]["name"] == "anything_goes"
    assert calls[0]["id"] == "call_1"
    assert calls[0]["args"] == '{"a":1}'
    assert stats == {"remap": 0, "strip": 0}


def test_reasoning_and_content_and_usage_streamed(tmp_path):
    msg = {"role": "assistant", "content": "hello world",
           "reasoning": "thinking hard", "tool_calls": None}
    obj = chat_completion("m", msg, "stop")
    obj["usage"] = {"prompt_tokens": 5, "completion_tokens": 2,
                    "total_tokens": 7}
    whole, _ = _emit(obj, tmp_path)
    content, calls, finish = reassemble(whole)
    assert content == "hello world"
    assert finish == "stop"
    # reasoning is chunked too (word boundaries): aggregate the deltas
    reasoning = ""
    for line in whole.splitlines():
        if line.startswith("data: ") and line != "data: [DONE]":
            ch = json.loads(line[6:])["choices"]
            if ch:
                reasoning += ch[0]["delta"].get("reasoning") or ""
    assert reasoning == "thinking hard"
    assert '"usage"' in whole
    assert whole.rstrip().endswith("data: [DONE]")


def test_text_stream_chunks():
    obj = {"id": "c", "object": "text_completion", "created": 1,
           "model": "m",
           "choices": [{"index": 0, "text": "x" * 40,
                        "finish_reason": "length"}]}
    payloads = emit_text_stream(obj)
    assert payloads[-1] == "[DONE]"
    texts = [json.loads(p)["choices"][0]["text"] for p in payloads[:-1]]
    assert "".join(texts) == "x" * 40
    last = json.loads(payloads[-2])
    assert last["choices"][0]["finish_reason"] == "length"
