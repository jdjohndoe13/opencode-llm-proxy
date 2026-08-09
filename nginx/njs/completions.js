const fs = require('fs');

const SKIP_RESPONSE_HEADERS = ['content-length', 'connection', 'keep-alive', 'transfer-encoding', 'content-encoding'];
const LOG_DIR = '/var/log/nginx/completions';

export default { completions };

const CHUNK_SIZE = 16;

// Rewrites /v1/completions and /v1/chat/completions requests: sets
// top_p=0.95 and stream=false in the JSON request body, then forwards the
// modified body to the backend at the same path. When the client requested
// stream=true, the (always non-streaming) backend response is converted into
// a token-split SSE stream.
function completions(r) {
    const raw = r.requestText;
    const ts = Date.now();
    if (raw === undefined || raw === null || raw === '') {
        r.return(400, 'empty request body');
        return;
    }

    dump(r, ts, 'req', raw);

    let obj;
    try {
        obj = JSON.parse(raw);
    } catch (e) {
        r.return(400, 'invalid JSON in request body: ' + e.message);
        return;
    }

    const wantStream = obj.stream === true;
    obj.top_p = 0.95;
    obj.stream = false;
    delete obj["stream_options"]
    const newBody = JSON.stringify(obj);

    r.subrequest('/_completions' + r.uri, {
        method: r.method,
        body: newBody,
        args: r.variables.args,
    }, function (reply) {
        dump(r, ts, 'resp', reply.responseText);
        copyHeaders(r, reply);
        if (reply.status === 200 && wantStream) {
            streamSse(r, reply, r.uri);
            return;
        }
        r.return(reply.status, reply.responseText);
    });
}

// Turns a non-streaming backend response into a real SSE stream. For
// /v1/chat/completions the single chat.completion object is split into
// chat.completion.chunk events (role, reasoning, content, finish, usage), for
// /v1/completions the text is split into text_completion events. Each event
// is flushed separately (r.send), so the response is chunked and carries no
// Content-Length.
function streamSse(r, reply, uri) {
    const raw = reply.responseText;
    if (raw === undefined || raw === null || raw === '') {
        r.status = 200;
        r.headersOut['Content-Type'] = 'text/event-stream';
        r.headersOut['Cache-Control'] = 'no-cache';
        r.sendHeader();
        r.send('data: [DONE]\n\n');
        r.finish();
        return;
    }

    let obj;
    try {
        obj = JSON.parse(raw);
    } catch (e) {
        r.return(200, 'data: ' + raw + '\n\n' + 'data: [DONE]\n\n');
        return;
    }

    r.status = 200;
    r.headersOut['Content-Type'] = 'text/event-stream';
    r.headersOut['Cache-Control'] = 'no-cache';
    r.sendHeader();

    if (obj.object === 'chat.completion' || uri.indexOf('/chat/') !== -1) {
        emitChatStream(r, obj);
    } else {
        emitTextStream(r, obj);
    }

    r.finish();
}

// Emits a chat.completion response as chat.completion.chunk SSE events,
// mirroring the upstream streaming format: role delta, reasoning deltas,
// content deltas, tool_calls deltas, a finish_reason delta, an (optional)
// usage event, then [DONE]. Tool calls are taken from message.tool_calls if
// present, otherwise parsed out of opencode-style <tool_calls>/<invoke>
// markup that the backend may leave inside reasoning/content.
function emitChatStream(r, obj) {
    const choice = (obj.choices && obj.choices.length) ? obj.choices[0] : null;
    const msg = choice ? choice.message : null;
    let finishReason = choice && choice.finish_reason ? choice.finish_reason : 'stop';
    const base = {
        id: obj.id,
        object: 'chat.completion.chunk',
        created: obj.created,
        model: obj.model,
    };

    const first = {
        id: obj.id,
        object: 'chat.completion.chunk',
        created: obj.created,
        model: obj.model,
        choices: [{ index: 0, delta: { role: 'assistant', content: '' }, logprobs: null, finish_reason: null }],
    };
    if (obj.prompt_token_ids !== undefined) first.prompt_token_ids = obj.prompt_token_ids;
    if (obj.prompt_text !== undefined) first.prompt_text = obj.prompt_text;
    r.send('data: ' + JSON.stringify(first) + '\n\n');

    const calls = [];
    let reasoning = msg && msg.reasoning ? String(msg.reasoning) : '';
    let content = msg && msg.content ? String(msg.content) : '';

    if (msg && Array.isArray(msg.tool_calls)) {
        for (let i = 0; i < msg.tool_calls.length; i++) {
            const tc = msg.tool_calls[i];
            calls.push({
                id: tc.id || null,
                name: tc.function ? tc.function.name : null,
                arguments: tc.function ? tc.function.arguments : '',
            });
        }
    }
    const strippedR = stripToolCalls(reasoning);
    reasoning = strippedR.text;
    for (let i = 0; i < strippedR.calls.length; i++) calls.push(strippedR.calls[i]);
    const strippedC = stripToolCalls(content);
    content = strippedC.text;
    for (let i = 0; i < strippedC.calls.length; i++) calls.push(strippedC.calls[i]);

    if (calls.length > 0) finishReason = 'tool_calls';

    if (reasoning) {
        const parts = splitText(reasoning, CHUNK_SIZE);
        for (let i = 0; i < parts.length; i++) {
            r.send('data: ' + JSON.stringify(chatDelta(base, { reasoning: parts[i] })) + '\n\n');
        }
    }

    if (content) {
        const parts = splitText(content, CHUNK_SIZE);
        for (let i = 0; i < parts.length; i++) {
            r.send('data: ' + JSON.stringify(chatDelta(base, { content: parts[i] })) + '\n\n');
        }
    }

    for (let i = 0; i < calls.length; i++) {
        emitToolCallDeltas(r, base, calls[i], i);
    }

    const done = {
        id: obj.id,
        object: 'chat.completion.chunk',
        created: obj.created,
        model: obj.model,
        choices: [{ index: 0, delta: {}, logprobs: null, finish_reason: finishReason, stop_reason: null, token_ids: null }],
    };
    r.send('data: ' + JSON.stringify(done) + '\n\n');

    if (obj.usage) {
        const usage = {
            id: obj.id,
            object: 'chat.completion.chunk',
            created: obj.created,
            model: obj.model,
            choices: [],
            usage: obj.usage,
        };
        if (obj.system_fingerprint !== undefined) usage.system_fingerprint = obj.system_fingerprint;
        r.send('data: ' + JSON.stringify(usage) + '\n\n');
    }

    r.send('data: [DONE]\n\n');
}

// Emits one tool call as OpenAI-style delta.tool_calls events: a first event
// carrying id/type/function.name, then one event per arguments fragment.
function emitToolCallDeltas(r, base, call, index) {
    const id = call.id || ('call_' + randomHex(24));
    const first = {
        id: base.id,
        object: 'chat.completion.chunk',
        created: base.created,
        model: base.model,
        choices: [{
            index: 0,
            delta: { content: null, tool_calls: [{ id: id, type: 'function', index: index, function: { name: call.name, arguments: '' } }] },
            logprobs: null,
            finish_reason: null,
        }],
    };
    r.send('data: ' + JSON.stringify(first) + '\n\n');

    const args = call.arguments || '';
    const parts = splitText(args, CHUNK_SIZE);
    for (let i = 0; i < parts.length; i++) {
        const d = {
            id: base.id,
            object: 'chat.completion.chunk',
            created: base.created,
            model: base.model,
            choices: [{
                index: 0,
                delta: { content: null, tool_calls: [{ id: null, type: null, index: index, function: { name: null, arguments: parts[i] } }] },
                logprobs: null,
                finish_reason: null,
            }],
        };
        r.send('data: ' + JSON.stringify(d) + '\n\n');
    }
}

// Matches the tag prefix used by the backend's tool-call markup: an optional
// run of ASCII/fullwidth vertical bars around an optional literal "DSML",
// e.g. <tool_calls>, <|tool_calls|>, <\uFF5CDSML\uFF5Ctool_calls>.
const TAG_PREFIX_OPEN = '<\\s*[|\\uFF5C]*\\s*(?:DSML)?\\s*[|\\uFF5C]*\\s*';
const TAG_PREFIX_CLOSE = '<\\/\\s*[|\\uFF5C]*\\s*(?:DSML)?\\s*[|\\uFF5C]*\\s*';

// Parses tool-call markup from a string, e.g.
//   <tool_calls>\n<invoke name="bash">\n<parameter name="command" string="true">ls</parameter>\n</invoke>\n</tool_calls>
// into calls of the form {id: null, name, arguments} where arguments is a JSON
// string of the parameters. Unknown markup is left untouched.
function extractToolCalls(text) {
    const calls = [];
    const re = new RegExp(TAG_PREFIX_OPEN + 'invoke\\s+name="([^"]*)"[^>]*>([\\s\\S]*?)' + TAG_PREFIX_CLOSE + 'invoke[|\\uFF5C]*>', 'g');
    let m;
    while ((m = re.exec(text)) !== null) {
        const params = {};
        const pre = new RegExp(TAG_PREFIX_OPEN + 'parameter\\s+name="([^"]*)"[^>]*>([\\s\\S]*?)' + TAG_PREFIX_CLOSE + 'parameter[|\\uFF5C]*>', 'g');
        let pm;
        while ((pm = pre.exec(m[2])) !== null) {
            params[pm[1]] = pm[2];
        }
        calls.push({ id: null, name: m[1], arguments: JSON.stringify(params) });
    }
    return calls;
}

// Removes tool-call markup from a string and returns {text, calls}: text is
// the input with <tool_calls>...</tool_calls> and bare <invoke>...</invoke>
// blocks removed (trimmed), calls is the list of parsed tool calls.
function stripToolCalls(text) {
    const calls = [];
    let t = text;
    t = t.replace(new RegExp(TAG_PREFIX_OPEN + 'tool_calls[|\\uFF5C]*>[\\s\\S]*?' + TAG_PREFIX_CLOSE + 'tool_calls[|\\uFF5C]*>', 'g'), function (block) {
        const found = extractToolCalls(block);
        for (let i = 0; i < found.length; i++) calls.push(found[i]);
        return '';
    });
    t = t.replace(new RegExp(TAG_PREFIX_OPEN + 'invoke\\s+name="[^"]*"[^>]*>[\\s\\S]*?' + TAG_PREFIX_CLOSE + 'invoke[|\\uFF5C]*>', 'g'), function (block) {
        const found = extractToolCalls(block);
        for (let i = 0; i < found.length; i++) calls.push(found[i]);
        return '';
    });
    return { text: t.replace(/\s+$/g, ''), calls: calls };
}

// Generates a lowercase hex string of the given length.
function randomHex(len) {
    let s = '';
    for (let i = 0; i < len; i++) {
        s += '0123456789abcdef'.charAt(Math.floor(Math.random() * 16));
    }
    return s;
}

// Builds a chat.completion.chunk with a non-empty delta.
function chatDelta(base, delta) {
    return {
        id: base.id,
        object: 'chat.completion.chunk',
        created: base.created,
        model: base.model,
        choices: [{ index: 0, delta: delta, logprobs: null, finish_reason: null, token_ids: null }],
    };
}

// Emits a /v1/completions (text_completion) response as text_completion SSE
// events, chunking choices[0].text and carrying finish_reason on the last
// text event, then [DONE].
function emitTextStream(r, obj) {
    const choice = (obj.choices && obj.choices.length) ? obj.choices[0] : null;
    const text = choice && choice.text ? choice.text : '';
    const finishReason = choice && choice.finish_reason ? choice.finish_reason : 'stop';
    const parts = splitText(text, CHUNK_SIZE);

    for (let i = 0; i < parts.length; i++) {
        const last = i === parts.length - 1;
        const chunk = {
            id: obj.id,
            object: 'text_completion',
            created: obj.created,
            model: obj.model,
            choices: [{
                index: 0,
                text: parts[i],
                logprobs: null,
                finish_reason: last ? finishReason : null,
                stop_reason: null,
                prompt_token_ids: null,
                token_ids: null,
            }],
        };
        if (last && obj.system_fingerprint !== undefined) chunk.system_fingerprint = obj.system_fingerprint;
        r.send('data: ' + JSON.stringify(chunk) + '\n\n');
    }

    r.send('data: [DONE]\n\n');
}

// Partitions a string into substrings of at most maxLen characters, breaking
// on word boundaries when possible. Concatenating the parts reproduces the
// input exactly.
function splitText(s, maxLen) {
    const parts = [];
    let rest = s;
    while (rest.length > maxLen) {
        let cut = rest.lastIndexOf(' ', maxLen);
        if (cut < 1) cut = maxLen;
        parts.push(rest.slice(0, cut));
        rest = rest.slice(cut);
    }
    if (rest.length > 0) parts.push(rest);
    return parts;
}

// Writes a request/response dump to LOG_DIR/<unix-ms>.<kind>.json. Dumping
// must never break the proxy path, so write errors are only logged.
function dump(r, ts, kind, data) {
    if (data === undefined || data === null) {
        return;
    }
    try {
        fs.writeFileSync(LOG_DIR + '/' + ts + '.' + kind + '.json', data);
    } catch (e) {
        r.error('failed to write ' + kind + ' dump: ' + e);
    }
}

// Pass upstream response headers through to the client (Content-Length and
// transfer headers are re-generated by nginx, so they are skipped).
function copyHeaders(r, reply) {
    for (const h in reply.headersOut) {
        if (SKIP_RESPONSE_HEADERS.indexOf(h.toLowerCase()) !== -1) {
            continue;
        }
        r.headersOut[h] = reply.headersOut[h];
    }
}
