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
// content deltas, a finish_reason delta, an (optional) usage event, then
// [DONE].
function emitChatStream(r, obj) {
    const choice = (obj.choices && obj.choices.length) ? obj.choices[0] : null;
    const msg = choice ? choice.message : null;
    const finishReason = choice && choice.finish_reason ? choice.finish_reason : 'stop';
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

    if (msg && msg.reasoning) {
        const parts = splitText(msg.reasoning, CHUNK_SIZE);
        for (let i = 0; i < parts.length; i++) {
            r.send('data: ' + JSON.stringify(chatDelta(base, { reasoning: parts[i] })) + '\n\n');
        }
    }

    if (msg && msg.content) {
        const parts = splitText(msg.content, CHUNK_SIZE);
        for (let i = 0; i < parts.length; i++) {
            r.send('data: ' + JSON.stringify(chatDelta(base, { content: parts[i] })) + '\n\n');
        }
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
