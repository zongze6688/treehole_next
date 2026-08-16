#!/usr/bin/env node
// Reference client for the Danta OpenClaw APP API.
// Exercises: login -> WS auth -> onboard -> chat -> reply -> query.
// Requires Node >= 22 (global fetch + WebSocket). No dependencies.
//
// Usage:
//   BACKEND_URL=http://192.168.84.3:8000 AUTH_URL=http://192.168.84.3:8001 \
//   EMAIL=mvpuserA@test.local PASSWORD=passwordA1 \
//   node openclaw-app-api-client.mjs
//
// Set SKIP_ONBOARD=1 to skip onboard (reuse an existing ready instance).

const BACKEND = process.env.BACKEND_URL || 'http://192.168.84.3:8000';
const AUTH = process.env.AUTH_URL || 'http://192.168.84.3:8001';
const EMAIL = process.env.EMAIL;
const PASSWORD = process.env.PASSWORD;
const SKIP_ONBOARD = process.env.SKIP_ONBOARD === '1';

const WS_URL = BACKEND.replace(/^http/, 'ws') + '/api/claw/ws';

function fail(msg) { console.error('FAIL:', msg); process.exit(1); }

async function login() {
  const res = await fetch(AUTH + '/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: EMAIL, password: PASSWORD }),
  });
  const body = await res.json();
  if (!res.ok || !body.access) fail('login: ' + JSON.stringify(body));
  return body.access;
}

function waitOpen(ws) {
  return new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve);
    ws.addEventListener('error', (e) => reject(new Error('ws error: ' + (e.message || 'unknown'))));
  });
}

class Client {
  constructor(ws) {
    this.ws = ws;
    this.handlers = []; // { predicate, resolve, reject, timer }
    this.seq = 0;
    ws.addEventListener('message', (e) => this.onMessage(JSON.parse(e.data)));
  }
  onMessage(msg) {
    for (let i = 0; i < this.handlers.length; i++) {
      const h = this.handlers[i];
      if (h.predicate(msg)) {
        clearTimeout(h.timer);
        this.handlers.splice(i, 1);
        h.resolve(msg);
        return;
      }
    }
    if (msg.type === 'message' && msg.from === 'openclaw') {
      console.log('  [reply]', msg.content);
    }
  }
  send(obj) { this.ws.send(JSON.stringify(obj)); }
  // Wait for a server frame matching predicate, with timeout.
  waitFor(predicate, label, timeoutMs = 600000) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timeout waiting for ' + label)), timeoutMs);
      this.handlers.push({ predicate, resolve, reject, timer });
    });
  }
  reqId() { return 'req-' + Date.now() + '-' + (++this.seq); }
}

async function main() {
  const token = await login();
  console.log('logged in (access length=%d)', token.length);

  const ws = new WebSocket(WS_URL);
  await waitOpen(ws);
  const c = new Client(ws);
  console.log('ws connected:', WS_URL);

  // 1. auth
  c.send({ type: 'auth', token, version: '1.0' });
  const authOk = await c.waitFor((m) => m.type === 'auth_success', 'auth_success', 30000);
  console.log('auth_success, channel_count=%d', authOk.channel_count);

  // 2. onboard (blocking until ready)
  if (!SKIP_ONBOARD) {
    const rid = c.reqId();
    c.send({ type: 'openclaw.onboard', request_id: rid, payload: { provider: 'fleet' } });
    console.log('onboarding (this blocks for a cold cell)...');
    const st = await c.waitFor((m) => m.type === 'openclaw.onboard.status' && m.request_id === rid, 'onboard.status');
    console.log('onboard.status:', JSON.stringify(st.payload));
    if (st.payload.state !== 'ready') fail('onboard did not reach ready: ' + JSON.stringify(st.payload));
  }

  // 3. instance status
  {
    const rid = c.reqId();
    c.send({ type: 'openclaw.instance.status', request_id: rid, payload: {} });
    const st = await c.waitFor((m) => m.type === 'openclaw.instance.status' && m.request_id === rid, 'instance.status', 15000);
    console.log('instance.status:', JSON.stringify(st.payload));
  }

  // 4. chat (new conversation)
  const rid = c.reqId();
  c.send({
    type: 'openclaw.chat.send',
    request_id: rid,
    payload: { channel_id: 0, session_id: '', content: 'What is the capital of France? Answer in one word.', message_id: 'msg-' + rid, media: null },
  });
  const acc = await c.waitFor((m) => m.type === 'openclaw.chat.accepted' && m.request_id === rid, 'chat.accepted', 30000);
  console.log('chat.accepted:', JSON.stringify(acc.payload));
  const { task_id: taskId, channel_id: channelId } = acc.payload;

  // 5. wait for the model reply (correlated by task_id)
  const reply = await c.waitFor(
    (m) => m.type === 'message' && m.from === 'openclaw' && m.task_id === taskId,
    'model reply (task_id=' + taskId + ')',
    120000,
  );
  console.log('REPLY:', JSON.stringify(reply));

  // 6. query via HTTP
  const hdr = { Authorization: 'Bearer ' + token };
  const inst = await (await fetch(BACKEND + '/api/claw/instance', { headers: hdr })).json();
  const channels = await (await fetch(BACKEND + '/api/claw/channels', { headers: hdr })).json();
  const msgs = await (await fetch(BACKEND + '/api/claw/messages?channel_id=' + channelId, { headers: hdr })).json();
  console.log('instance:', JSON.stringify(inst));
  console.log('channels:', JSON.stringify(channels));
  console.log('messages(%d):', msgs.length);
  for (const m of msgs) console.log('  ', m.from, '->', m.content);

  ws.close();
  console.log('DONE');
}

main().catch((e) => { console.error('FATAL:', e); process.exit(1); });
