"""Unit tests for the tool-name guard (Lua guard_tool_calls port)."""
import json

from llm_proxy.toolguard import (
    ToolNameGuard, allowed_names, levenshtein, name_tokens,
    normalize_bool, normalize_guard, server_prefix, suffix_tokens,
)
from tests.conftest import BOGUS, TOOLS


def _guard(tmp_path, mode="remap"):
    return ToolNameGuard(mode, True, tmp_path / "toolname_fixes.jsonl")


def test_levenshtein():
    assert levenshtein("kitten", "sitting") == 3
    assert levenshtein("snapshot", "snapshot") == 0
    assert levenshtein("take_snapshot", "take_shapshot") == 1
    assert levenshtein("", "abc") == 3


def test_tokens_and_prefix():
    assert name_tokens("chrome-devtools_Take_Snapshot") == {
        "chrome", "devtools", "take", "snapshot"}
    assert server_prefix("chrome-devtools_take_snapshot") == "chrome-devtools"
    assert server_prefix("bash") == "bash"
    assert suffix_tokens("chrome-devtools_take_snapshot") == {
        "take", "snapshot"}
    assert "snapshot" in suffix_tokens("chrome-devtools_snapshot_for_upload")


def test_normalize():
    assert normalize_guard("OFF") == "off"
    assert normalize_guard("bogus") == "remap"
    assert normalize_guard(None) == "remap"
    assert normalize_bool("0") is False
    assert normalize_bool(None) is True
    assert normalize_bool("true") is True


def test_allowed_names():
    assert allowed_names({"tools": []}) == []
    assert allowed_names({}) is None
    body = {"tools": [{"function": {"name": "bash"}},
                      {"function": {"name": "read"}},
                      {"function": {"name": "bash"}},
                      {"type": "nope"}]}
    assert allowed_names(body) == ["bash", "read"]


def test_kept_when_allowed(tmp_path):
    g = _guard(tmp_path)
    calls = [{"name": "bash", "arguments": "{}"}]
    kept, dropped, remap, strip = g.guard_calls(calls, TOOLS, "m")
    assert kept == calls and dropped == [] and remap == 0 and strip == 0


def test_off_mode_passes_through(tmp_path):
    g = _guard(tmp_path, "off")
    calls = [{"name": "totally_bogus", "arguments": "{}"}]
    kept, dropped, remap, strip = g.guard_calls(calls, TOOLS, "m")
    assert kept == calls and dropped == []


def test_no_tools_skips_guard(tmp_path):
    g = _guard(tmp_path)
    calls = [{"name": "totally_bogus", "arguments": "{}"}]
    kept, dropped, remap, strip = g.guard_calls(calls, None, "m")
    assert kept == calls and dropped == []


def test_levenshtein_remap(tmp_path):
    g = _guard(tmp_path)
    calls = [{"name": "chrome-devtools_take_shapshot", "arguments": "{}"}]
    kept, dropped, remap, strip = g.guard_calls(calls, TOOLS, "m")
    assert remap == 1 and strip == 0
    assert kept[0]["name"] == "chrome-devtools_take_snapshot"


def test_token_remap_unique(tmp_path):
    g = _guard(tmp_path)
    calls = [{"name": "chrome-devtools_snapshot", "arguments": "{}"}]
    kept, dropped, remap, strip = g.guard_calls(calls, TOOLS, "m")
    # both take_snapshot and wait_for... no: only token >=5 "snapshot"
    # matches take_snapshot uniquely among chrome-devtools_* tools.
    assert remap == 1 and strip == 0
    assert kept[0]["name"] == "chrome-devtools_take_snapshot"


def test_ambiguous_dropped(tmp_path):
    g = _guard(tmp_path)
    calls = [{"name": "chrome-devtools_snapshot_for_upload", "arguments": "{}"}]
    kept, dropped, remap, strip = g.guard_calls(calls, TOOLS, "m")
    assert remap == 0 and strip == 1
    assert kept == [] and dropped == ["chrome-devtools_snapshot_for_upload"]


def test_strip_mode_drops(tmp_path):
    g = _guard(tmp_path, "strip")
    calls = [{"name": "chrome-devtools_snapshot", "arguments": "{}"}]
    kept, dropped, remap, strip = g.guard_calls(calls, TOOLS, "m")
    assert remap == 0 and strip == 1 and kept == []


def test_bogus_incident_name_dropped(tmp_path):
    g = _guard(tmp_path)
    calls = [{"name": BOGUS, "arguments": "{}"}]
    kept, dropped, remap, strip = g.guard_calls(calls, TOOLS, "m")
    assert remap == 0 and strip == 1 and kept == []


def test_fix_log_written(tmp_path):
    g = _guard(tmp_path)
    calls = [{"name": "chrome-devtools_snapshot", "arguments": "{}"},
             {"name": "chrome-devtools_snapshot_for_upload", "arguments": "{}"}]
    g.guard_calls(calls, TOOLS, "test-model")
    lines = (tmp_path / "toolname_fixes.jsonl").read_text(
        encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    e1 = json.loads(lines[0])
    assert e1["action"] == "remap" and e1["mapped"] == "chrome-devtools_take_snapshot"
    assert e1["bogus"] == "chrome-devtools_snapshot" and e1["model"] == "test-model"
    assert e1["time"].endswith("Z")
    e2 = json.loads(lines[1])
    assert e2["action"] == "drop" and e2["mapped"] == ""


def test_probe_not_noisy(tmp_path):
    g = _guard(tmp_path)
    calls = [{"name": "chrome-devtools_snapshot", "arguments": "{}"}]
    g.guard_calls(calls, TOOLS, "m", noisy=False)
    assert not (tmp_path / "toolname_fixes.jsonl").exists()
