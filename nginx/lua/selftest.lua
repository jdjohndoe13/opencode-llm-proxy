-- Mock-based selftest for completions.lua (antml parsing + tool-name guard).
--
-- Run inside the proxy container (the /etc/nginx/lua bind mount is the live
-- source):
--   docker exec \
--     -e LUA_PATH='/etc/nginx/lua/?.lua;;' \
--     -e LUA_CPATH='/usr/local/openresty/lualib/?.so;;' \
--     llm-proxy-lua /usr/local/openresty/luajit/bin/luajit /etc/nginx/lua/selftest.lua
--
-- It stubs ngx, loads completions.lua through its _M._* test hooks and
-- replays the real incident payload plus the guard cases. Exits 0 when every
-- check passed, 1 otherwise.

local out
local ngx_mock

local function reset_output()
    out = {}
end

local function concat()
    return table.concat(out, "")
end

local results = { ok = 0, fail = {} }
local function check(name, cond, detail)
    if cond then
        results.ok = results.ok + 1
    else
        results.fail[#results.fail + 1] = name .. (detail and (": " .. tostring(detail)) or "")
    end
end

local function findi(s, needle)
    return (s:lower()):find((needle:lower()), 1, true) ~= nil
end

local DEBUG = os.getenv("SELFTEST_DEBUG") ~= nil
local function dbg(tag, whole)
    if DEBUG then
        io.stderr:write("--- " .. tag .. " ---\n" .. whole .. "\n")
    end
end

-- Real consumers (OpenCode) reassemble the delta stream into one message;
-- several checks only make sense on that aggregate, not on the raw SSE text
-- (chunk borders and JSON quoting break plain substring searches). Returns
-- (content, calls, finish) where calls maps emit-index -> {id, name, args}.
local function reassemble(whole)
    local cjson = require("cjson.safe")
    local parts = {}
    local calls = {}
    local finish = ""
    for payload in whole:gmatch("data: (.-)\r?\n") do
        if payload ~= "[DONE]" then
            local j = cjson.decode(payload)
            if j and type(j.choices) == "table" and j.choices[1] then
                local choice = j.choices[1]
                local delta = choice.delta or {}
                if type(delta.content) == "string" then
                    parts[#parts + 1] = delta.content
                end
                local tcs = delta.tool_calls
                if type(tcs) == "table" then
                    for _, tc in ipairs(tcs) do
                        local idx = tc.index or 0
                        local c = calls[idx]
                        if not c then
                            c = { id = "", name = "", args = "" }
                            calls[idx] = c
                        end
                        if type(tc.id) == "string" and tc.id ~= "" then c.id = tc.id end
                        local fn = tc["function"]
                        if type(fn) == "table" then
                            if type(fn.name) == "string" and fn.name ~= "" then c.name = fn.name end
                            if type(fn.arguments) == "string" then c.args = c.args .. fn.arguments end
                        end
                    end
                end
                if type(choice.finish_reason) == "string" and choice.finish_reason ~= "" then
                    finish = choice.finish_reason
                end
            end
        end
    end
    return table.concat(parts), calls, finish
end

-- Build a chat.completion object the way the vLLM backend returns it.
local function chat_completion(model, message, finish)
    return {
        id = "chatcmpl-test", object = "chat.completion", created = 1789837609, model = model,
        choices = { { index = 0, message = message, finish_reason = finish, logprobs = nil } },
        usage = nil,
    }
end

-- Available tool names mirroring the incident session subset.
local TOOLS = {
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
}

local function set_tools(names)
    ngx_mock.ctx._tool_names = names
end

-- The guard note text must be the sentinel followed by the corrected info.
local SENTINEL = "[tool-name-fixer]"
local BOGUS = "chrome-devtools_snapshot_for_upload_ready_check"

-- ---------------------------------------------------------------- cases
reset_output()
ngx_mock = {
    null = setmetatable({}, { __tostring = function() return "userdata: NULL" end }),
    print = function(s) out[#out + 1] = s end,
    flush = function() end,
    log = function() end,
    ERR = 4, WARN = 5, NOTICE = 6, INFO = 7,
    now = function() return 1700000000 end,
    worker = { pid = function() return 1 end },
    var = {
        temp_check_enabled = "0", top_p = "-1",
        keepalive_delay = "30", keepalive_interval = "5",
        upstream_timeout = "600", upstream_retries = "1",
        tool_name_guard = "remap",
    },
    socket = {},
    shared = nil,
    ctx = {},
}
ngx = ngx_mock
local M = dofile("/etc/nginx/lua/completions.lua")
set_tools(TOOLS)

-- T1: the real incident response replayed (antml markup with a hallucinated
-- name, no params). Must NOT emit a tool_call; must strip the markup and
-- surface the corrective note as content; finish_reason stays "stop".
do
    reset_output()
    ngx_mock.ctx = { _tool_names = TOOLS }
    local msg = {
        role = "assistant",
        content = '<invoke name="' .. BOGUS .. '">' .. "</invoke>",
        reasoning = "",
        tool_calls = ngx.null,
    }
    M._emit_chat_stream(chat_completion("glm-5.3-flash", msg, "stop"))
    local whole = concat()
    local content, calls, finish = reassemble(whole)
    check("T1 incident: no bogus tool_call delta", not whole:find('"name":"' .. BOGUS .. '"', 1, true))
    check("T1 incident: no tool_call emitted", #calls == 0)
    check("T1 incident: finish stays stop", finish == "stop")
    check("T1 incident: sentinel note emitted", content:find(SENTINEL, 1, true) ~= nil)
    check("T1 incident: bogus name is quoted in the note", content:find("`" .. BOGUS .. "`", 1, true) ~= nil)
    check("T1 incident: raw markup gone", whole:find("<invoke", 1, true) == nil)
    dbg("T1", whole)
end

-- T2: valid antml markup is lifted into a real tool_call.
do
    reset_output()
    local msg = {
        role = "assistant",
        content = '<invoke name="chrome-devtools_take_snapshot">'
            .. '<parameter name="pageId">28</parameter>'
            .. "</invoke>",
        reasoning = "",
    }
    M._emit_chat_stream(chat_completion("qwen-3.8-flash-next", msg, "stop"))
    local whole = concat()
    local content, calls, finish = reassemble(whole)
    local tc = calls[0]
    check("T2 valid antml: tool_call emitted for take_snapshot",
        tc ~= nil and tc.name == "chrome-devtools_take_snapshot", tostring(tc and tc.name))
    check("T2 valid antml: args contain pageId",
        tc ~= nil and tc.args:find("pageId", 1, true) ~= nil, tostring(tc and tc.args))
    check("T2 valid antml: finish is tool_calls", finish == "tool_calls")
    check("T2 valid antml: no note", content:find(SENTINEL, 1, true) == nil)
    dbg("T2", whole)
end

-- T3: unambiguous same-server miss gets remapped (R2: unique token match).
do
    reset_output()
    local msg = {
        role = "assistant",
        content = '<invoke name="chrome-devtools_snapshot"></invoke>',
        reasoning = "",
    }
    M._emit_chat_stream(chat_completion("glm-5.3-flash", msg, "stop"))
    local whole = concat()
    local content, calls, finish = reassemble(whole)
    local tc = calls[0]
    check("T3 remap: emitted under the real name",
        tc ~= nil and tc.name == "chrome-devtools_take_snapshot", tostring(tc and tc.name))
    check("T3 remap: finish is tool_calls", finish == "tool_calls")
    check("T3 remap: no note", content:find(SENTINEL, 1, true) == nil)
    dbg("T3", whole)
end

-- T4: ambiguous same-server miss is NOT remapped (multiple token matches):
-- tokens snapshot + upload both fire -> dropped with note.
do
    reset_output()
    local msg = {
        role = "assistant",
        content = '<invoke name="chrome-devtools_snapshot_for_upload"></invoke>',
        reasoning = "",
    }
    M._emit_chat_stream(chat_completion("glm-5.3-flash", msg, "stop"))
    local whole = concat()
    local content, calls = reassemble(whole)
    check("T4 ambiguous: no tool_call emitted", #calls == 0)
    check("T4 ambiguous: note emitted with bogus name",
        content:find("chrome-devtools_snapshot_for_upload", 1, true) ~= nil)
end

-- T5: antml-namespaced markup parses too (close tag + parameter names).
do
    reset_output()
    local msg = {
        role = "assistant",
        reasoning = "",
        content = '<invoke name="chrome-devtools_upload_file">'
            .. '<parameter name="uid">snap-1</parameter>'
            .. '<parameter name="filePaths">["C:/x.png"]</parameter>'
            .. "</invoke>",
    }
    M._emit_chat_stream(chat_completion("glm-5.3-flash", msg, "stop"))
    local whole = concat()
    local content, calls, finish = reassemble(whole)
    local tc = calls[0]
    check("T5 antml namespace: tool_call emitted",
        tc ~= nil and tc.name == "chrome-devtools_upload_file", tostring(tc and tc.name))
    check("T5 antml namespace: args carried",
        tc ~= nil and tc.args:find("uid", 1, true) ~= nil and tc.args:find("filePaths", 1, true) ~= nil,
        tostring(tc and tc.args))
    check("T5 antml namespace: no note", content:find(SENTINEL, 1, true) == nil)
    dbg("T5", whole)
end

-- T6: request without a tools array skips the guard (legacy pass-through).
do
    reset_output()
    ngx_mock.ctx = {}
    local msg = {
        role = "assistant",
        content = '<invoke name="' .. BOGUS .. '">' .. "</invoke>",
        reasoning = "",
    }
    M._emit_chat_stream(chat_completion("glm-5.3-flash", msg, "stop"))
    local whole = concat()
    check("T6 no-tools request: legacy tool_call emitted",
        whole:find('"name":"' .. BOGUS .. '"', 1, true) ~= nil)
    check("T6 no-tools request: no note", whole:find(SENTINEL, 1, true) == nil)
end

-- T7: parser-level regression checks via _extract_tool_calls.
do
    local found = M._extract_tool_calls('<tool_calls>'
        .. '<invoke name="read"><parameter name="filePath">a.lua</parameter></invoke>'
        .. '</tool_calls>')
    check("T7 parser: wrapper + name + params", #found == 1 and found[1].name == "read"
        and found[1].arguments:find("filePath", 1, true) ~= nil, tostring(found[1] and found[1].arguments))
end

-- ---------------------------------------------------------------- summary
print("")
if #results.fail == 0 then
    print("SELFTEST PASS (" .. results.ok .. " checks)")
else
    print("SELFTEST FAIL (" .. #results.fail .. " of " .. (results.ok + #results.fail) .. " checks failed)")
    for _, f in ipairs(results.fail) do print("  FAIL: " .. f) end
end
