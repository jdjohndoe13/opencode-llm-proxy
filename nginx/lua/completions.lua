-- OpenResty (ngx_lua) reimplementation of njs/completions.js.
--
-- Rewrites /v1/completions and /v1/chat/completions request bodies
-- (top_p=0.95, stream=false, stream_options removed), forwards the modified
-- body to the backend over a cosocket, and converts the always non-streaming
-- backend response into an SSE stream when the client asked for stream=true.
--
-- Unlike the njs version, a client disconnect is detected via ngx.on_abort
-- while the upstream call is in flight: the callback closes the cosocket so
-- the backend (vLLM) aborts generation instead of running to completion.

local cjson = require "cjson"
local socket = ngx.socket

local LOG_DIR = "/var/log/nginx/completions"
local STATS_LOG = LOG_DIR .. "/stats.jsonl"
local CHUNK_SIZE = 16
local POLL_MS = 1000
local HEX = "0123456789abcdef"

-- GPU temperature monitor. The endpoint returns a JSON object mapping GPU
-- names to temperatures, e.g. {"gpu_0":48,"gpu_1":49,...}. The proxy stalls
-- (stops forwarding to the upstream) while any temperature is above
-- TEMP_CHECK_STALL_ABOVE and only resumes once every temperature is below
-- TEMP_CHECK_RESUME_BELOW (hysteresis so the machine has time to cool down).
--
-- Every tuning value is injected from the environment into the nginx config
-- as a `set $var` directive (see default.conf.template) and read back here
-- via ngx.var. The whole monitor is armed only when TEMP_CHECK_ENABLED is
-- "1" or "true" (case-insensitive); otherwise all TEMP_CHECK_* variables are
-- ignored and the proxy behaves as if every check passed. When enabled there
-- are deliberately no defaults: the container refuses to start when a
-- variable is missing, and the checks below fail loaded requests if one ever
-- slips through.

local function require_var(name, v)
    if not v or v == "" then
        error("missing configuration variable " .. name .. " (set it in docker-compose.openresty.yaml)", 2)
    end
    return v
end

local function require_number(name, v)
    local n = tonumber(require_var(name, v))
    if not n then
        error("invalid numeric value for " .. name .. ": " .. tostring(v), 2)
    end
    return n
end

-- Splits a full endpoint URL into (host, port, path). Returns nil when the
-- URL is missing or malformed.
local function parse_temp_endpoint(url)
    local rest = (url or ""):gsub("^https?://", "")
    if rest == "" then return nil end
    local hostport, path = rest:match("^([^/]+)(/.*)$")
    if not hostport then hostport, path = rest, "/" end
    local host, port = hostport:match("^(.*):(%d+)$")
    if not host then host, port = hostport, "80" end
    return host, port, path
end

-- Temperature checks are armed only when TEMP_CHECK_ENABLED is exactly "1"
-- or "true" (case-insensitive). When disabled, every other TEMP_CHECK_*
-- variable is ignored and the proxy behaves as if all checks passed.
local TEMP_CHECKS_ENABLED = (tostring(ngx.var.temp_check_enabled or ""):lower() == "1")
    or (tostring(ngx.var.temp_check_enabled or ""):lower() == "true")

local TEMP_CHECK_ENDPOINT
local TEMP_CHECK_STALL_ABOVE
local TEMP_CHECK_RESUME_BELOW
local TEMP_CHECK_POLL_INTERVAL
local TEMP_CHECK_CACHE_TTL
local TEMP_CHECK_HTTP_TIMEOUT
local TEMP_CHECK_STALL_LOG
local TEMP_CHECK_HOST, TEMP_CHECK_PORT, TEMP_CHECK_PATH

if TEMP_CHECKS_ENABLED then
    TEMP_CHECK_ENDPOINT = require_var("temp_check_endpoint", ngx.var.temp_check_endpoint)
    TEMP_CHECK_STALL_ABOVE = require_number("temp_check_stall_above", ngx.var.temp_check_stall_above)
    TEMP_CHECK_RESUME_BELOW = require_number("temp_check_resume_below", ngx.var.temp_check_resume_below)
    TEMP_CHECK_POLL_INTERVAL = require_number("temp_check_poll_interval", ngx.var.temp_check_poll_interval)
    TEMP_CHECK_CACHE_TTL = require_number("temp_check_cache_ttl", ngx.var.temp_check_cache_ttl)
    TEMP_CHECK_HTTP_TIMEOUT = require_number("temp_check_http_timeout", ngx.var.temp_check_http_timeout)
    TEMP_CHECK_STALL_LOG = require_var("temp_check_stall_log", ngx.var.temp_check_stall_log)

    TEMP_CHECK_HOST, TEMP_CHECK_PORT, TEMP_CHECK_PATH = parse_temp_endpoint(TEMP_CHECK_ENDPOINT)
    if not TEMP_CHECK_HOST then
        error("invalid temp_check_endpoint URL, expected http://host:port/path: " .. tostring(TEMP_CHECK_ENDPOINT), 2)
    end
end

-- Keep-alive heartbeat for streaming clients: fake empty SSE chunks start
-- emitting KEEPALIVE_DELAY seconds after the request was fully received and
-- repeat every KEEPALIVE_INTERVAL seconds until the upstream response
-- arrives, so clients with a first-byte timeout shorter than the backend
-- latency don't abort the stream.
local KEEPALIVE_DELAY = require_number("keepalive_delay", ngx.var.keepalive_delay)
local KEEPALIVE_INTERVAL = require_number("keepalive_interval", ngx.var.keepalive_interval)

math.randomseed(math.floor(ngx.now() * 1000) + ngx.worker.pid())

local _M = {}

-- ngx.null is a truthy lightuserdata (tostring(ngx.null) == "userdata: NULL"),
-- so treat it like nil when reading optional backend fields.
local function is_null(v)
    return v == nil or v == ngx.null
end

local function field_str(v)
    if is_null(v) then return "" end
    return tostring(v)
end

-- Token count for the stats log: any missing or invalid value becomes 0
-- (e.g. error responses carry no usage object, so nothing can be deduced).
local function stat_num(v)
    if type(v) == "number" then return math.floor(v + 0.5) end
    return 0
end

local function is_prefix_char(c)
    return c == " " or c == "\t" or c == "\n" or c == "\r" or c == "|"
end

-- After a literal '<' at position j (with an optional '/'), match the
-- backend's tag prefix (whitespace, '|', optional "DSML") followed by 'name'.
-- Returns the position of the first char of 'name', or nil.
local function match_name(s, j, name)
    local k = j + 1
    if s:sub(k, k) == "/" then k = k + 1 end
    while k <= #s and is_prefix_char(s:sub(k, k)) do k = k + 1 end
    if s:sub(k, k + 3) == "DSML" then
        k = k + 4
        while k <= #s and is_prefix_char(s:sub(k, k)) do k = k + 1 end
    end
    if s:sub(k, k + #name - 1) == name then
        return k
    end
    return nil
end

local function find_open_tag(s, from, name)
    local i = from
    while true do
        local lt = s:find("<", i, true)
        if not lt then return nil end
        if s:sub(lt + 1, lt + 1) ~= "/" then
            local k = match_name(s, lt, name)
            if k then
                local gt = s:find(">", k + #name, true)
                if gt then return lt, gt end
            end
        end
        i = lt + 1
    end
end

local function find_close_tag(s, from, name)
    local i = from
    while true do
        local lt = s:find("</", i, true)
        if not lt then return nil end
        local k = match_name(s, lt, name)
        if k then
            local gt = s:find(">", k + #name, true)
            if gt then return lt, gt end
        end
        i = lt + 1
    end
end

-- Parses <invoke>...</invoke> blocks (with the optional <| |DSML|> tag
-- prefixes) into {id=nil, name, arguments} where arguments is a JSON object
-- of the <parameter name="..." ...>...</parameter> values.
local function extract_tool_calls(text)
    local calls = {}
    local pos = 1
    while true do
        local lt, gt = find_open_tag(text, pos, "invoke")
        if not lt then break end
        local clt, cgt = find_close_tag(text, gt + 1, "invoke")
        if not clt then break end
        local name = text:match('name="([^"]*)"', lt) or ""
        local params = {}
        local body = text:sub(gt + 1, clt - 1)
        local p = 1
        while true do
            local plt, pgt = find_open_tag(body, p, "parameter")
            if not plt then break end
            local pclt, pcgt = find_close_tag(body, pgt + 1, "parameter")
            if not pclt then break end
            local pname = body:match('name="([^"]*)"', plt)
            if pname then
                params[pname] = body:sub(pgt + 1, pclt - 1)
            end
            p = pcgt + 1
        end
        calls[#calls + 1] = { id = nil, name = name, arguments = cjson.encode(params) }
        pos = cgt + 1
    end
    return calls
end

-- Removes <tool_calls>...</tool_calls> and bare <invoke>...</invoke> blocks,
-- returning the cleaned text and the parsed tool calls.
local function strip_tool_calls(text)
    local calls = {}
    local function strip_blocks(t, name)
        local out = {}
        local pos = 1
        while true do
            local lt, gt = find_open_tag(t, pos, name)
            if not lt then
                out[#out + 1] = t:sub(pos)
                break
            end
            local clt, cgt = find_close_tag(t, gt + 1, name)
            if not clt then
                out[#out + 1] = t:sub(pos)
                break
            end
            out[#out + 1] = t:sub(pos, lt - 1)
            local found = extract_tool_calls(t:sub(lt, cgt))
            for i = 1, #found do calls[#calls + 1] = found[i] end
            pos = cgt + 1
        end
        return table.concat(out)
    end
    local t = strip_blocks(text, "tool_calls")
    t = strip_blocks(t, "invoke")
    return t:gsub("%s+$", ""), calls
end

-- Byte length of the UTF-8 sequence starting at byte position i of s
-- (1 for ASCII, 2/3/4 for multi-byte sequences). Never returns more bytes
-- than remain in the string.
local function char_byte_len(s, i)
    local b = s:byte(i)
    if not b then return 0 end
    if b < 0x80 then return 1 end
    if b >= 0xF0 then return math.min(4, #s - i + 1) end
    if b >= 0xE0 then return math.min(3, #s - i + 1) end
    if b >= 0xC0 then return math.min(2, #s - i + 1) end
    return 1
end

-- Partitions a UTF-8 string into substrings of at most max_len code points,
-- breaking on word boundaries (ASCII spaces) when possible. The cut always
-- falls on a character boundary, so multi-byte characters (Cyrillic,
-- box-drawing, emoji, ...) are never split in half and every chunk is valid
-- UTF-8 on its own. Concatenating the parts reproduces the input exactly.
local function split_text(s, max_len)
    local parts = {}
    local n = #s
    local pos = 1
    while pos <= n do
        -- Scan up to max_len code points from pos, remembering the last
        -- word boundary (byte position just before a space) seen so far.
        local cut
        local count = 0
        local i = pos
        while i <= n and count < max_len do
            if s:sub(i, i) == " " then cut = i - 1 end
            i = i + char_byte_len(s, i)
            count = count + 1
        end
        local endpos
        if cut and cut >= pos then
            endpos = cut
        else
            endpos = i - 1
        end
        parts[#parts + 1] = s:sub(pos, endpos)
        pos = endpos + 1
    end
    return parts
end

local function random_hex(len)
    local t = {}
    for i = 1, len do
        t[i] = HEX:sub(math.random(16), math.random(16))
    end
    return table.concat(t)
end

-- Writes a request/response dump to LOG_DIR/<unix-ms>.<kind>.json. Dumping
-- must never break the proxy path, so write errors are only logged.
local function dump(ts, kind, data)
    if not data then return end
    local f, err = io.open(LOG_DIR .. "/" .. ts .. "." .. kind .. ".json", "wb")
    if not f then
        ngx.log(ngx.ERR, "failed to write " .. kind .. " dump: " .. tostring(err))
        return
    end
    f:write(data)
    f:close()
end

local function flush_print(s)
    ngx.print(s)
    ngx.flush(true)
end

local function sse(data)
    flush_print("data: " .. data .. "\n\n")
end

-- Relays an upstream failure to a streaming client: since the SSE handshake
-- was already sent, the error can no longer be a plain HTTP status, so it is
-- sent as an SSE event followed by [DONE].
local function sse_error(message)
    sse(cjson.encode({ error = { message = message, type = "upstream_error" } }))
end

-- Emits fake empty SSE chunks while the upstream response is still pending,
-- so clients whose first-byte timeout is shorter than the backend latency do
-- not abort the stream. The SSE handshake itself is deferred until the first
-- keep-alive chunk is due (KEEPALIVE_DELAY after the request was received):
-- as long as the upstream answers before that, a non-200 result can still be
-- relayed as a plain HTTP error instead of an SSE event. No-op for
-- non-streaming requests. The chunks mirror the upstream chunk shape with an
-- empty delta/text.
local function send_keepalive()
    if not ngx.ctx._want_stream then return end
    local req_done = ngx.ctx._req_done
    if not req_done then return end
    local now = ngx.now()
    if not ngx.ctx._sse_started then
        if now - req_done < KEEPALIVE_DELAY then return end
        ngx.status = ngx.HTTP_OK
        ngx.header["Content-Type"] = "text/event-stream"
        ngx.header["Cache-Control"] = "no-cache"
        ngx.send_headers()
        ngx.ctx._sse_started = true
    else
        local last = ngx.ctx._last_keepalive or req_done
        if now - last < KEEPALIVE_INTERVAL then return end
    end
    ngx.ctx._last_keepalive = now

    local chunk
    if ngx.ctx._keepalive_chat then
        chunk = {
            id = ngx.ctx._keepalive_id, object = "chat.completion.chunk",
            created = ngx.ctx._keepalive_created, model = ngx.ctx._keepalive_model,
            choices = { { index = 0, delta = {}, logprobs = ngx.null, finish_reason = ngx.null } },
        }
    else
        chunk = {
            id = ngx.ctx._keepalive_id, object = "text_completion",
            created = ngx.ctx._keepalive_created, model = ngx.ctx._keepalive_model,
            choices = { { index = 0, text = "", logprobs = ngx.null, finish_reason = ngx.null, stop_reason = ngx.null } },
        }
    end
    sse(cjson.encode(chunk))
end

local function chat_delta(base, delta)
    return {
        id = base.id, object = "chat.completion.chunk", created = base.created, model = base.model,
        choices = { { index = 0, delta = delta, logprobs = ngx.null, finish_reason = ngx.null, token_ids = ngx.null } },
    }
end

-- Emits one tool call as OpenAI-style delta.tool_calls events: a first event
-- carrying id/type/function.name, then one event per arguments fragment.
local function emit_tool_call_deltas(base, call, index)
    local id = call.id or ("call_" .. random_hex(24))
    sse(cjson.encode({
        id = base.id, object = "chat.completion.chunk", created = base.created, model = base.model,
        choices = { {
            index = 0,
            delta = { content = ngx.null, tool_calls = { { id = id, type = "function", index = index, ["function"] = { name = call.name, arguments = "" } } } },
            logprobs = ngx.null, finish_reason = ngx.null,
        } },
    }))

    local args = call.arguments or ""
    local parts = split_text(args, CHUNK_SIZE)
    for i = 1, #parts do
        sse(cjson.encode({
            id = base.id, object = "chat.completion.chunk", created = base.created, model = base.model,
            choices = { {
                index = 0,
                delta = { content = ngx.null, tool_calls = { { id = ngx.null, type = ngx.null, index = index, ["function"] = { name = ngx.null, arguments = parts[i] } } } },
                logprobs = ngx.null, finish_reason = ngx.null,
            } },
        }))
    end
end

-- Emits a chat.completion response as chat.completion.chunk SSE events,
-- mirroring the upstream streaming format.
local function emit_chat_stream(obj)
    local choice = (obj.choices and #obj.choices > 0) and obj.choices[1] or nil
    local msg = choice and choice.message or nil
    local finish_reason = (choice and choice.finish_reason) or "stop"
    local base = { id = obj.id, object = "chat.completion.chunk", created = obj.created, model = obj.model }

    local first = {
        id = obj.id, object = "chat.completion.chunk", created = obj.created, model = obj.model,
        choices = { { index = 0, delta = { role = "assistant", content = "" }, logprobs = ngx.null, finish_reason = ngx.null } },
    }
    if not is_null(obj.prompt_token_ids) then first.prompt_token_ids = obj.prompt_token_ids end
    if not is_null(obj.prompt_text) then first.prompt_text = obj.prompt_text end
    sse(cjson.encode(first))

    local calls = {}
    local reasoning = msg and field_str(msg.reasoning) or ""
    local content = msg and field_str(msg.content) or ""

    if msg and type(msg.tool_calls) == "table" then
        for i = 1, #msg.tool_calls do
            local tc = msg.tool_calls[i]
            local fn = tc["function"]
            calls[#calls + 1] = {
                id = not is_null(tc.id) and tc.id or nil,
                name = fn and not is_null(fn.name) and fn.name or nil,
                arguments = fn and field_str(fn.arguments) or "",
            }
        end
    end
    local stripped_r, calls_r = strip_tool_calls(reasoning)
    reasoning = stripped_r
    for i = 1, #calls_r do calls[#calls + 1] = calls_r[i] end
    local stripped_c, calls_c = strip_tool_calls(content)
    content = stripped_c
    for i = 1, #calls_c do calls[#calls + 1] = calls_c[i] end

    if #calls > 0 then finish_reason = "tool_calls" end

    if reasoning ~= "" then
        local parts = split_text(reasoning, CHUNK_SIZE)
        for i = 1, #parts do
            sse(cjson.encode(chat_delta(base, { reasoning = parts[i] })))
        end
    end

    if content ~= "" then
        local parts = split_text(content, CHUNK_SIZE)
        for i = 1, #parts do
            sse(cjson.encode(chat_delta(base, { content = parts[i] })))
        end
    end

    for i = 1, #calls do
        emit_tool_call_deltas(base, calls[i], i - 1)
    end

    sse(cjson.encode({
        id = obj.id, object = "chat.completion.chunk", created = obj.created, model = obj.model,
        choices = { { index = 0, delta = {}, logprobs = ngx.null, finish_reason = finish_reason, stop_reason = ngx.null, token_ids = ngx.null } },
    }))

    if not is_null(obj.usage) then
        local usage = {
            id = obj.id, object = "chat.completion.chunk", created = obj.created, model = obj.model,
            choices = cjson.empty_array, usage = obj.usage,
        }
        if not is_null(obj.system_fingerprint) then usage.system_fingerprint = obj.system_fingerprint end
        sse(cjson.encode(usage))
    end

    sse("[DONE]")
end

-- Emits a /v1/completions (text_completion) response as text_completion SSE
-- events, chunking choices[1].text and carrying finish_reason on the last
-- text event, then [DONE].
local function emit_text_stream(obj)
    local choice = (obj.choices and #obj.choices > 0) and obj.choices[1] or nil
    local text = (choice and choice.text) or ""
    local finish_reason = (choice and choice.finish_reason) or "stop"
    local parts = split_text(text, CHUNK_SIZE)

    for i = 1, #parts do
        local last = i == #parts
        local chunk = {
            id = obj.id, object = "text_completion", created = obj.created, model = obj.model,
            choices = { {
                index = 0, text = parts[i], logprobs = ngx.null,
                finish_reason = last and finish_reason or ngx.null,
                stop_reason = ngx.null, prompt_token_ids = ngx.null, token_ids = ngx.null,
            } },
        }
        if last and not is_null(obj.system_fingerprint) then chunk.system_fingerprint = obj.system_fingerprint end
        sse(cjson.encode(chunk))
    end

    sse("[DONE]")
end

-- Abort-aware reads: use the short poll timeout and return nil, "aborted"
-- when the client disconnected while waiting, so the handler coroutine
-- (which owns the cosocket) can close the upstream connection itself.
-- on_wait, when given, is called on every receive timeout so the caller can
-- keep the client's first-byte timeout at bay (e.g. emit keep-alive chunks).
local function recv_line_poll(sock, state, on_wait)
    while true do
        if state.aborted then return nil, "aborted" end
        local line, err = sock:receive("*l")
        if line then return line, nil end
        if err ~= "timeout" then return nil, err end
        if on_wait then on_wait() end
    end
end

local function recv_bytes_poll(sock, n, state, on_wait)
    local parts = {}
    local got = 0
    while got < n do
        if state.aborted then return nil, "aborted" end
        local data, err = sock:receive(n - got)
        if data then
            parts[#parts + 1] = data
            got = got + #data
        elseif err ~= "timeout" then
            return nil, err
        elseif on_wait then
            on_wait()
        end
    end
    return table.concat(parts)
end

local function receive_chunked_poll(sock, state, on_wait)
    local parts = {}
    while true do
        if state.aborted then return nil, "aborted" end
        local line, err = recv_line_poll(sock, state, on_wait)
        if not line then return nil, err end
        local size = tonumber(line:match("^%x+"))
        if not size then return nil, "bad chunk size" end
        if size == 0 then
            while true do
                local tl, terr = recv_line_poll(sock, state, on_wait)
                if not tl then break end
                if tl == "" then break end
            end
            break
        end
        local chunk, cerr = recv_bytes_poll(sock, size, state, on_wait)
        if not chunk then return nil, cerr end
        parts[#parts + 1] = chunk
        recv_bytes_poll(sock, 2, state, on_wait)
    end
    return table.concat(parts)
end

local function receive_until_close_poll(sock, state, on_wait)
    local parts = {}
    while true do
        if state.aborted then return nil, "aborted" end
        local data, err = sock:receive(4096)
        if data then
            parts[#parts + 1] = data
        elseif err == "timeout" then
            -- keep polling the abort flag / keep-alive cadence
            if on_wait then on_wait() end
        elseif err == "closed" then
            break
        else
            return nil, err
        end
    end
    return table.concat(parts)
end

-- Fetches the current GPU temperatures from the monitor endpoint over a
-- cosocket. Returns a list of numeric temperatures, or nil on any failure.
local function fetch_temperatures()
    local sock = ngx.socket.tcp()
    sock:settimeout(TEMP_CHECK_HTTP_TIMEOUT)
    local ok, err = sock:connect(TEMP_CHECK_HOST, TEMP_CHECK_PORT)
    if not ok then
        ngx.log(ngx.ERR, "temperature endpoint connect failed: " .. tostring(err))
        return nil
    end
    local req = "GET " .. TEMP_CHECK_PATH .. " HTTP/1.1\r\n"
        .. "Host: " .. TEMP_CHECK_HOST .. ":" .. TEMP_CHECK_PORT .. "\r\n"
        .. "Connection: close\r\n\r\n"
    local _, werr = sock:send(req)
    if werr then
        sock:close()
        return nil
    end

    local status_line = sock:receive("*l")
    if not status_line then
        sock:close()
        return nil
    end
    local status = tonumber(status_line:match("^HTTP/%d+%.?%d*%s+(%d+)"))
    local content_length
    local chunked = false
    while true do
        local line = sock:receive("*l")
        if not line or line == "" then break end
        local name, value = line:match("^([^:]+):%s*(.*)$")
        if name then
            local lname = string.lower(name)
            if lname == "content-length" then content_length = tonumber(value) end
            if lname == "transfer-encoding" and value:lower():find("chunked", 1, true) then chunked = true end
        end
    end

    local body = ""
    if chunked then
        while true do
            local size_line = sock:receive("*l")
            if not size_line then break end
            local size = tonumber(size_line:match("^%x+"))
            if not size or size == 0 then break end
            local data = sock:receive(size)
            if not data then break end
            body = body .. data
            sock:receive(2)
        end
    elseif content_length then
        local data = sock:receive(content_length)
        if data then body = data end
    else
        while true do
            local data, rerr = sock:receive(4096)
            if not data then break end
            body = body .. data
        end
    end
    sock:close()

    if status ~= 200 then
        ngx.log(ngx.ERR, "temperature endpoint returned status " .. tostring(status))
        return nil
    end
    local okj, obj = pcall(cjson.decode, body)
    if not okj or type(obj) ~= "table" then
        ngx.log(ngx.ERR, "temperature endpoint returned invalid JSON")
        return nil
    end
    local temps = {}
    for _, v in pairs(obj) do
        if type(v) == "number" then temps[#temps + 1] = v end
    end
    if #temps == 0 then
        ngx.log(ngx.ERR, "temperature endpoint returned no numeric values")
        return nil
    end
    return temps
end

-- Stores the highest observed temperature in the shared dict so concurrent
-- requests/workers reuse one snapshot instead of each hammering the monitor.
local function update_temp_cache(temps)
    local max = 0
    for i = 1, #temps do
        if temps[i] > max then max = temps[i] end
    end
    local d = ngx.shared.temps
    if d then
        d:set("max_temp", max)
        d:set("checked_at", ngx.now())
    end
    return max
end

-- Returns (max_temp, checked_at) from the shared cache when the snapshot is
-- still fresh, otherwise (nil, nil) so the caller fetches a new one.
local function read_temp_cache()
    local d = ngx.shared.temps
    if not d then return nil, nil end
    local max = d:get("max_temp")
    local checked_at = d:get("checked_at")
    if not max or not checked_at then return nil, nil end
    if ngx.now() - checked_at > TEMP_CHECK_CACHE_TTL then return nil, nil end
    return max, checked_at
end

-- Stalls the request until every GPU temperature is safely below
-- TEMP_CHECK_RESUME_BELOW. If any temperature is above
-- TEMP_CHECK_STALL_ABOVE the request is not forwarded to the upstream and we
-- keep polling the monitor (at most once per second, shared across all
-- requests) until everything cools down. If the monitor cannot be reached the
-- proxy conservatively keeps stalling. Returns true when it is safe to
-- proceed, false if the client disconnected while waiting. When temperature
-- checks are disabled it always returns true immediately (as if every check
-- passed).
local function wait_for_cool_down(state)
    if not TEMP_CHECKS_ENABLED then
        return true
    end
    local stalled = false
    while true do
        if state.aborted then return false end

        local max = read_temp_cache()
        if not max then
            local temps = fetch_temperatures()
            if not temps then
                ngx.log(ngx.ERR, "temperature check failed; conservatively stalling request")
                if not ngx.ctx._stall_start then ngx.ctx._stall_start = ngx.now() end
                stalled = true
            else
                max = update_temp_cache(temps)
            end
        end

        if max then
            if stalled then
                if max < TEMP_CHECK_RESUME_BELOW then
                    ngx.ctx._stall_end = ngx.now()
                    ngx.log(ngx.NOTICE, "GPU cooled to " .. max .. " (<" .. TEMP_CHECK_RESUME_BELOW .. "), resuming request")
                    return true
                end
            elseif max > TEMP_CHECK_STALL_ABOVE then
                if not ngx.ctx._stall_start then ngx.ctx._stall_start = ngx.now() end
                ngx.log(ngx.NOTICE, "GPU temperature " .. max .. " > " .. TEMP_CHECK_STALL_ABOVE .. ", stalling request")
                stalled = true
            else
                return true
            end
        end

        -- Wait one poll interval, waking periodically to notice client aborts
        -- and to keep streaming clients fed with keep-alive chunks.
        local deadline = ngx.now() + TEMP_CHECK_POLL_INTERVAL
        while ngx.now() < deadline do
            if state.aborted then return false end
            send_keepalive()
            ngx.sleep(0.2)
        end
    end
end

function _M.completions()
    ngx.ctx._start = ngx.now()
    ngx.ctx._uri = ngx.var.uri

    ngx.req.read_body()
    ngx.ctx._req_done = ngx.now() -- "last byte of the request" moment, base of the keep-alive schedule
    local raw = ngx.req.get_body_data()
    if not raw or raw == "" then
        ngx.status = ngx.HTTP_BAD_REQUEST
        ngx.print("empty request body")
        return ngx.exit(ngx.HTTP_BAD_REQUEST)
    end

    local ts = math.floor(ngx.now() * 1000)
    dump(ts, "req", raw)

    local ok, obj = pcall(cjson.decode, raw)
    if not ok or type(obj) ~= "table" then
        ngx.status = ngx.HTTP_BAD_REQUEST
        ngx.print("invalid JSON in request body")
        return ngx.exit(ngx.HTTP_BAD_REQUEST)
    end

    local want_stream = obj.stream == true
    obj.top_p = 0.95
    obj.stream = false
    obj.stream_options = nil
    local new_body = cjson.encode(obj)

    -- Keep-alive context for streaming clients (see send_keepalive).
    ngx.ctx._want_stream = want_stream
    ngx.ctx._keepalive_chat = ngx.var.uri:find("/chat/", 1, true) ~= nil
    ngx.ctx._keepalive_id = (type(obj.id) == "string" and obj.id ~= "") and obj.id or "chatcmpl-keepalive"
    ngx.ctx._keepalive_model = type(obj.model) == "string" and obj.model or "unknown"
    ngx.ctx._keepalive_created = math.floor(ngx.now())
    ngx.ctx._req_model = type(obj.model) == "string" and obj.model or nil

    -- Register the abort handler before the cool-down stall so a client
    -- disconnect during the stall (or while waiting for the upstream) is
    -- noticed. The callback runs in a separate light thread, so it must only
    -- set a flag: the cosocket is owned by the handler coroutine and cannot be
    -- closed from here ("socket busy reading"). The handler polls the flag
    -- with short socket timeouts and closes the upstream socket itself.
    local state = { aborted = false }
    local okab, ab_err = ngx.on_abort(function()
        state.aborted = true
        ngx.ctx._aborted = true
        ngx.log(ngx.WARN, "client aborted while waiting for upstream")
    end)
    if not okab then
        ngx.log(ngx.ERR, "failed to register abort handler: " .. tostring(ab_err))
    end

    -- Streaming clients: the SSE handshake is deferred until the first
    -- keep-alive chunk is due (KEEPALIVE_DELAY after the request was
    -- received, see send_keepalive). While the upstream answers within that
    -- window, non-200 results are relayed as plain HTTP errors; only once the
    -- handshake has gone out is a later failure sent as an SSE error event.

    -- Cool-down guard: while any GPU temperature is above
    -- TEMP_CHECK_STALL_ABOVE, do not forward the request to the upstream.
    -- Keep polling the monitor (about once per second) until every
    -- temperature drops below TEMP_CHECK_RESUME_BELOW.
    if not wait_for_cool_down(state) then
        ngx.log(ngx.INFO, "client aborted while stalling for cool-down")
        return ngx.exit(444)
    end

    -- From this point on the request is actually forwarded to the proxied
    -- server: this is the "sent to the backend" moment recorded in the stats
    -- log (the cool-down stall above is deliberately excluded from it).
    ngx.ctx._sent_at = ngx.now()

    local path = ngx.var.uri
    local args = ngx.var.args
    if args and args ~= "" then path = path .. "?" .. args end

    local backend = ngx.var.llm_backend or "host.docker.internal:21025"
    local host, port = backend:match("^(.*):(%d+)$")
    if not host then
        host, port = backend, "80"
    end

    local sock = ngx.socket.tcp()
    sock:settimeout(POLL_MS)

    local okc, cerr = sock:connect(host, tonumber(port))
    if not okc then
        ngx.log(ngx.ERR, "connect to " .. backend .. " failed: " .. tostring(cerr))
        if ngx.ctx._sse_started then
            sse_error("upstream connect failed: " .. tostring(cerr))
            sse("[DONE]")
            return ngx.exit(ngx.HTTP_OK)
        end
        return ngx.exit(ngx.HTTP_BAD_GATEWAY)
    end

    local lines = { ngx.req.get_method() .. " " .. path .. " HTTP/1.1", "Host: " .. backend }
    local h = ngx.req.get_headers()
    local skip = { host = true, ["content-length"] = true, connection = true, ["accept-encoding"] = true }
    for k, v in pairs(h) do
        local lk = string.lower(k)
        if not skip[lk] then
            if type(v) == "table" then
                for i = 1, #v do lines[#lines + 1] = k .. ": " .. v[i] end
            else
                lines[#lines + 1] = k .. ": " .. v
            end
        end
    end
    lines[#lines + 1] = "Accept-Encoding: identity"
    lines[#lines + 1] = "Connection: close"
    lines[#lines + 1] = "Content-Length: " .. #new_body
    local request = table.concat(lines, "\r\n") .. "\r\n\r\n" .. new_body

    local okw, werr = sock:send(request)
    if not okw then
        if state.aborted then
            ngx.log(ngx.INFO, "client aborted, discarding upstream send error")
            return ngx.exit(444)
        end
        ngx.log(ngx.ERR, "failed to send request to " .. backend .. ": " .. tostring(werr))
        sock:close()
        if ngx.ctx._sse_started then
            sse_error("upstream send failed: " .. tostring(werr))
            sse("[DONE]")
            return ngx.exit(ngx.HTTP_OK)
        end
        return ngx.exit(ngx.HTTP_BAD_GATEWAY)
    end

    local status_line, sl_err = recv_line_poll(sock, state, send_keepalive)
    if not status_line then
        if state.aborted then
            ngx.log(ngx.INFO, "client aborted, closing upstream connection")
            sock:close()
            return ngx.exit(444)
        end
        ngx.log(ngx.ERR, "failed to read status line from " .. backend .. ": " .. tostring(sl_err))
        sock:close()
        if ngx.ctx._sse_started then
            sse_error("upstream error: " .. tostring(sl_err))
            sse("[DONE]")
            return ngx.exit(ngx.HTTP_OK)
        end
        return ngx.exit(ngx.HTTP_BAD_GATEWAY)
    end

    local status = tonumber(status_line:match("^HTTP/%d+%.?%d*%s+(%d+)")) or 500
    ngx.ctx._upstream_status = status
    local headers = {}
    local content_length = nil
    local chunked = false
    while true do
        if state.aborted then break end
        local line, herr = recv_line_poll(sock, state, send_keepalive)
        if not line then break end
        if line == "" then break end
        local name, value = line:match("^([^:]+):%s*(.*)$")
        if name then
            headers[string.lower(name)] = value
            if string.lower(name) == "content-length" then content_length = tonumber(value) end
            if string.lower(name) == "transfer-encoding" and value:lower():find("chunked", 1, true) then chunked = true end
        end
    end
    if state.aborted then
        ngx.log(ngx.INFO, "client aborted, closing upstream connection")
        sock:close()
        return ngx.exit(444)
    end

    local body, err
    if chunked then
        body, err = receive_chunked_poll(sock, state, send_keepalive)
    elseif content_length then
        body, err = recv_bytes_poll(sock, content_length, state, send_keepalive)
    else
        body, err = receive_until_close_poll(sock, state, send_keepalive)
    end

    if state.aborted then
        ngx.log(ngx.INFO, "client aborted, upstream connection closed")
        sock:close()
        return ngx.exit(444)
    end

    sock:close()
    state.sock = nil
    ngx.ctx._upstream_done = ngx.now() -- upstream exchange finished (body read or read failure)

    if not body then
        ngx.log(ngx.ERR, "failed to read response body from " .. backend .. ": " .. tostring(err))
        if ngx.ctx._sse_started then
            sse_error("upstream read failed: " .. tostring(err))
            sse("[DONE]")
            return ngx.exit(ngx.HTTP_OK)
        end
        return ngx.exit(ngx.HTTP_BAD_GATEWAY)
    end

    dump(ts, "resp", body)

    -- Parse the upstream JSON once: usage/model are stashed for the stats
    -- log (zeros when they cannot be deduced, e.g. a non-200 response), and
    -- the decoded object is reused below for the SSE emission.
    local jobj
    if status == 200 and body ~= "" then
        local okj, decoded = pcall(cjson.decode, body)
        if okj and type(decoded) == "table" then
            jobj = decoded
            ngx.ctx._resp_usage = decoded.usage
            ngx.ctx._resp_model = decoded.model
        end
    end

    if want_stream and ngx.ctx._sse_started then
        -- The SSE handshake was already sent (the upstream took longer than
        -- KEEPALIVE_DELAY); a non-200 result is relayed as an SSE event so
        -- the client's parser still gets a stream.
        if status ~= 200 then
            if body == "" then
                sse_error("upstream returned status " .. tostring(status))
            else
                sse(body)
            end
            sse("[DONE]")
            return ngx.exit(ngx.HTTP_OK)
        end
        if body == "" then
            sse("[DONE]")
            return ngx.exit(ngx.HTTP_OK)
        end
    else
        ngx.status = status
        ngx.header["Content-Type"] = headers["content-type"] or "application/json"
        if headers["cache-control"] then ngx.header["Cache-Control"] = headers["cache-control"] end

        if status ~= 200 then
            ngx.print(body)
            return ngx.exit(ngx.HTTP_OK)
        end

        if not want_stream then
            ngx.print(body)
            return ngx.exit(ngx.HTTP_OK)
        end

        ngx.status = ngx.HTTP_OK
        ngx.header["Content-Type"] = "text/event-stream"
        ngx.header["Cache-Control"] = "no-cache"
        ngx.send_headers()
        ngx.ctx._sse_started = true

        if body == "" then
            sse("[DONE]")
            return ngx.exit(ngx.HTTP_OK)
        end
    end

    -- The body was already decoded above (for the stats usage); reaching this
    -- point with no object means the JSON was unparsable.
    if not jobj then
        sse(body)
        sse("[DONE]")
        return ngx.exit(ngx.HTTP_OK)
    end

    local ok_emit, err_emit = pcall(function()
        if jobj.object == "chat.completion" or path:find("/chat/", 1, true) then
            emit_chat_stream(jobj)
        else
            emit_text_stream(jobj)
        end
    end)
    if not ok_emit then
        ngx.log(ngx.WARN, "SSE emission failed (client aborted?): " .. tostring(err_emit))
    end

    return ngx.exit(ngx.HTTP_OK)
end

-- Runs in the log phase (after the response has been fully sent).
--
-- stats.jsonl: one JSON line per request that was actually forwarded to the
-- upstream (i.e. past the cool-down stall; requests rejected earlier —
-- invalid bodies, client aborts during the stall — are not recorded here).
-- start_time_unix / start_time / duration_ms refer to the upstream send:
-- start is the moment the request was sent to the proxied server (after the
-- GPU cool-down wait), duration runs until the response was fully received
-- from it (or until the log phase, if the exchange was cut short). Token
-- counts come from the upstream response usage and are 0 when they cannot be
-- deduced (e.g. a 404), status is the upstream HTTP status (or the status
-- this request ended with when the upstream never answered).
--
-- Then writes the existing stall.log line:
--   {"time":"...","ts":...,"uri":"/v1/completions","status":200,"aborted":false,"total_s":1.234,"stall_s":0.0}
-- Tooling can sum total_s (time from incoming to last byte returned) and
-- stall_s (time spent waiting for the GPUs to cool down) across all lines.
function _M.log_request()
    local c = ngx.ctx
    local ended = ngx.now()
    local start = c._start or ended

    local stall_s = 0
    if c._stall_start then
        stall_s = math.floor(((c._stall_end or ended) - c._stall_start) * 1000 + 0.5) / 1000
    end

    if c._sent_at then
        local sent_at = c._sent_at
        local usage = c._resp_usage
        if is_null(usage) or type(usage) ~= "table" then usage = {} end
        local model = c._resp_model
        if is_null(model) or model == "" then model = c._req_model end
        if is_null(model) or model == "" then model = "unknown" end

        local entry = {
            model = model,
            prompt_tokens = stat_num(usage.prompt_tokens),
            total_tokens = stat_num(usage.total_tokens),
            completion_tokens = stat_num(usage.completion_tokens),
            status = c._upstream_status or ngx.status,
            stall_seconds = stall_s,
            start_time_unix = math.floor(sent_at * 1000 + 0.5),
            start_time = os.date("!%Y-%m-%dT%H:%M:%SZ", math.floor(sent_at + 0.5)),
            duration_ms = math.floor(((c._upstream_done or ended) - sent_at) * 1000 + 0.5),
        }

        local f, err = io.open(STATS_LOG, "a")
        if not f then
            ngx.log(ngx.ERR, "failed to open stats log " .. STATS_LOG .. ": " .. tostring(err))
        else
            f:write(cjson.encode(entry) .. "\n")
            f:close()
        end
    end

    if not TEMP_CHECKS_ENABLED or not TEMP_CHECK_STALL_LOG then return end

    local entry = {
        time = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        ts = start,
        uri = c._uri or ngx.var.uri,
        status = ngx.status,
        aborted = c._aborted == true,
        total_s = math.floor((ended - start) * 1000 + 0.5) / 1000,
        stall_s = stall_s,
    }

    if c._stall_start then
        entry.stall_start = c._stall_start
        entry.stall_end = c._stall_end or ended
    end

    local f, err = io.open(TEMP_CHECK_STALL_LOG, "a")
    if not f then
        ngx.log(ngx.ERR, "failed to open stall log " .. TEMP_CHECK_STALL_LOG .. ": " .. tostring(err))
        return
    end
    f:write(cjson.encode(entry) .. "\n")
    f:close()
end

return _M
