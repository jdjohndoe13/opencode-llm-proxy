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
local CHUNK_SIZE = 16
local POLL_MS = 1000
local HEX = "0123456789abcdef"

-- GPU temperature monitor. The endpoint returns a JSON object mapping GPU
-- names to temperatures, e.g. {"gpu_0":48,"gpu_1":49,...}. The address is
-- taken from the TEMP_ENDPOINT environment variable (a full URL to the
-- /temperatures endpoint, e.g. http://host.docker.internal:18082/temperatures),
-- falling back to that default when unset. The proxy stalls (stops forwarding
-- to the upstream) while any temperature is above STALL_ABOVE and only resumes
-- once every temperature is below RESUME_BELOW (hysteresis so the machine has
-- time to cool down).
local STALL_ABOVE = 75
local RESUME_BELOW = 65
local TEMP_POLL_INTERVAL = 1.0  -- seconds between temperature checks
local TEMP_CACHE_TTL = 0.9      -- seconds a cached snapshot is considered fresh
local TEMP_HTTP_TIMEOUT = 5000  -- ms for the cosocket temperature request

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

local TEMP_ENDPOINT = ngx.var.temp_endpoint
if not TEMP_ENDPOINT or TEMP_ENDPOINT == "" then
    TEMP_ENDPOINT = "http://host.docker.internal:18082/temperatures"
end
local TEMP_HOST, TEMP_PORT, TEMP_PATH = parse_temp_endpoint(TEMP_ENDPOINT)

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

-- Partitions a string into substrings of at most max_len characters, breaking
-- on word boundaries when possible. Concatenating the parts reproduces the
-- input exactly.
local function split_text(s, max_len)
    local parts = {}
    local rest = s
    while #rest > max_len do
        local nchars
        local start = 1
        while true do
            local sp = rest:find(" ", start, true)
            if not sp or sp > max_len then break end
            nchars = sp - 1
            start = sp + 1
        end
        if not nchars or nchars < 1 then nchars = max_len end
        parts[#parts + 1] = rest:sub(1, nchars)
        rest = rest:sub(nchars + 1)
    end
    if #rest > 0 then parts[#parts + 1] = rest end
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
local function recv_line_poll(sock, state)
    while true do
        if state.aborted then return nil, "aborted" end
        local line, err = sock:receive("*l")
        if line then return line, nil end
        if err ~= "timeout" then return nil, err end
    end
end

local function recv_bytes_poll(sock, n, state)
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
        end
    end
    return table.concat(parts)
end

local function receive_chunked_poll(sock, state)
    local parts = {}
    while true do
        if state.aborted then return nil, "aborted" end
        local line, err = recv_line_poll(sock, state)
        if not line then return nil, err end
        local size = tonumber(line:match("^%x+"))
        if not size then return nil, "bad chunk size" end
        if size == 0 then
            while true do
                local tl, terr = recv_line_poll(sock, state)
                if not tl then break end
                if tl == "" then break end
            end
            break
        end
        local chunk, cerr = recv_bytes_poll(sock, size, state)
        if not chunk then return nil, cerr end
        parts[#parts + 1] = chunk
        recv_bytes_poll(sock, 2, state)
    end
    return table.concat(parts)
end

local function receive_until_close_poll(sock, state)
    local parts = {}
    while true do
        if state.aborted then return nil, "aborted" end
        local data, err = sock:receive(4096)
        if data then
            parts[#parts + 1] = data
        elseif err == "timeout" then
            -- keep polling the abort flag
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
    sock:settimeout(TEMP_HTTP_TIMEOUT)
    local ok, err = sock:connect(TEMP_HOST, TEMP_PORT)
    if not ok then
        ngx.log(ngx.ERR, "temperature endpoint connect failed: " .. tostring(err))
        return nil
    end
    local req = "GET " .. TEMP_PATH .. " HTTP/1.1\r\n"
        .. "Host: " .. TEMP_HOST .. ":" .. TEMP_PORT .. "\r\n"
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
    if ngx.now() - checked_at > TEMP_CACHE_TTL then return nil, nil end
    return max, checked_at
end

-- Stalls the request until every GPU temperature is safely below RESUME_BELOW.
-- If any temperature is above STALL_ABOVE the request is not forwarded to the
-- upstream and we keep polling the monitor (at most once per second, shared
-- across all requests) until everything cools down. If the monitor cannot be
-- reached the proxy conservatively keeps stalling. Returns true when it is
-- safe to proceed, false if the client disconnected while waiting.
local function wait_for_cool_down(state)
    local stalled = false
    while true do
        if state.aborted then return false end

        local max = read_temp_cache()
        if not max then
            local temps = fetch_temperatures()
            if not temps then
                ngx.log(ngx.ERR, "temperature check failed; conservatively stalling request")
                stalled = true
            else
                max = update_temp_cache(temps)
            end
        end

        if max then
            if stalled then
                if max < RESUME_BELOW then
                    ngx.log(ngx.NOTICE, "GPU cooled to " .. max .. " (<" .. RESUME_BELOW .. "), resuming request")
                    return true
                end
            elseif max > STALL_ABOVE then
                ngx.log(ngx.NOTICE, "GPU temperature " .. max .. " > " .. STALL_ABOVE .. ", stalling request")
                stalled = true
            else
                return true
            end
        end

        -- Wait one poll interval, waking periodically to notice client aborts.
        local deadline = ngx.now() + TEMP_POLL_INTERVAL
        while ngx.now() < deadline do
            if state.aborted then return false end
            ngx.sleep(0.2)
        end
    end
end

function _M.completions()
    ngx.req.read_body()
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

    -- Register the abort handler before the cool-down stall so a client
    -- disconnect during the stall (or while waiting for the upstream) is
    -- noticed. The callback runs in a separate light thread, so it must only
    -- set a flag: the cosocket is owned by the handler coroutine and cannot be
    -- closed from here ("socket busy reading"). The handler polls the flag
    -- with short socket timeouts and closes the upstream socket itself.
    local state = { aborted = false }
    local okab, ab_err = ngx.on_abort(function()
        state.aborted = true
        ngx.log(ngx.WARN, "client aborted while waiting for upstream")
    end)
    if not okab then
        ngx.log(ngx.ERR, "failed to register abort handler: " .. tostring(ab_err))
    end

    -- Cool-down guard: while any GPU temperature is above STALL_ABOVE, do not
    -- forward the request to the upstream. Keep polling the monitor (about
    -- once per second) until every temperature drops below RESUME_BELOW.
    if not wait_for_cool_down(state) then
        ngx.log(ngx.INFO, "client aborted while stalling for cool-down")
        return ngx.exit(444)
    end

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
        return ngx.exit(ngx.HTTP_BAD_GATEWAY)
    end

    local status_line, sl_err = recv_line_poll(sock, state)
    if not status_line then
        if state.aborted then
            ngx.log(ngx.INFO, "client aborted, closing upstream connection")
            sock:close()
            return ngx.exit(444)
        end
        ngx.log(ngx.ERR, "failed to read status line from " .. backend .. ": " .. tostring(sl_err))
        sock:close()
        return ngx.exit(ngx.HTTP_BAD_GATEWAY)
    end

    local status = tonumber(status_line:match("^HTTP/%d+%.?%d*%s+(%d+)")) or 500
    local headers = {}
    local content_length = nil
    local chunked = false
    while true do
        if state.aborted then break end
        local line, herr = recv_line_poll(sock, state)
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
        body, err = receive_chunked_poll(sock, state)
    elseif content_length then
        body, err = recv_bytes_poll(sock, content_length, state)
    else
        body, err = receive_until_close_poll(sock, state)
    end

    if state.aborted then
        ngx.log(ngx.INFO, "client aborted, upstream connection closed")
        sock:close()
        return ngx.exit(444)
    end

    sock:close()
    state.sock = nil

    if not body then
        ngx.log(ngx.ERR, "failed to read response body from " .. backend .. ": " .. tostring(err))
        return ngx.exit(ngx.HTTP_BAD_GATEWAY)
    end

    dump(ts, "resp", body)

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

    ngx.header["Content-Type"] = "text/event-stream"
    ngx.header["Cache-Control"] = "no-cache"
    ngx.send_headers()

    if body == "" then
        sse("[DONE]")
        return ngx.exit(ngx.HTTP_OK)
    end

    local okj, jobj = pcall(cjson.decode, body)
    if not okj or type(jobj) ~= "table" then
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

return _M
