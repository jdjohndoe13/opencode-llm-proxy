"""Ports of the selftest.lua parser cases (T7) + markup edge cases.

The closing tag is built at runtime: the literal form is cut off by the
output filter of this assistant (the same truncation that silently killed
the earlier fixer lanes).
"""
from llm_proxy.markup import extract_tool_calls, split_text, strip_tool_calls

CP = "</" + "parameter" + ">"


def P(name, value):
    return f'<parameter name="{name}">{value}{CP}'


def test_t7_wrapper_name_and_params():
    text = ("<tool_calls><invoke name=\"read\">" + P("filePath", "a.lua")
            + "</invoke></tool_calls>")
    found = extract_tool_calls(text)
    assert len(found) == 1
    assert found[0]["name"] == "read"
    assert "filePath" in found[0]["arguments"]
    assert "a.lua" in found[0]["arguments"]


def test_bare_invoke_extracted():
    found = extract_tool_calls('<invoke name="bash"></invoke>')
    assert found == [{"id": None, "name": "bash", "arguments": "{}"}]


def test_dsml_prefixed_form():
    text = ("<|DSML|invoke name=\"grep\">" + P("pattern", "x")
            + "</|DSML|invoke>")
    found = extract_tool_calls(text)
    assert len(found) == 1 and found[0]["name"] == "grep"
    assert "pattern" in found[0]["arguments"]


def test_multiple_calls_indexed_in_order():
    text = ('<invoke name="read">' + P("a", "1") + "</invoke>"
            + '<invoke name="edit">' + P("b", "2") + "</invoke>")
    found = extract_tool_calls(text)
    assert [c["name"] for c in found] == ["read", "edit"]


def test_strip_removes_markup_and_returns_calls():
    text = ("Answer here. <tool_calls><invoke name=\"read\">"
            + P("filePath", "x") + "</invoke></tool_calls>")
    clean, calls = strip_tool_calls(text)
    assert clean == "Answer here."
    assert len(calls) == 1 and calls[0]["name"] == "read"


def test_split_concat_roundtrip():
    for s in ["", "x", "a" * 40, "the quick brown fox jumps over the lazy dog",
              "Привет мир 你好世界", "emoji: 🎉🎉🎉 tail", "a b", "a", "a b c"]:
        assert "".join(split_text(s)) == s, s


def test_split_no_codepoint_split_and_size():
    for s in ["x" * 100, "Привет" * 20, "🎉" * 20, "a 🎉 b" * 10]:
        for part in split_text(s):
            assert len(part) <= 16
            part.encode("utf-8")  # re-encodes fine: no split codepoints
