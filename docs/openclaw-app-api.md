# Danta OpenClaw — APP-facing API (frontend integration)

This documents the API the Danta APP frontend uses to onboard, chat with, and
manage a user's OpenClaw agent. The backend is `treehole_next`; authentication
is the ordinary APP user JWT issued by `auth_next`.

- Base path: `/api`
- Staging backend: `http://192.168.84.3:8000` (isolated staging; the legacy
  service on 8080 is unrelated and must not be used)
- Auth header (HTTP): `Authorization: Bearer <APP JWT>`

The APP path is **WebSocket-first** (onboard + chat over WS); HTTP is for query
and lifecycle management.

## 1. Authentication

Login via auth_next (not treehole):

```http
POST /api/login            (auth_next, staging http://192.168.84.3:8001)
Content-Type: application/json

{"email": "user@example.com", "password": "..."}
```

Response:

```json
{ "access": "<APP JWT>", "refresh": "<refresh token>", "message": "Login successful" }
```

The `access` token is the APP JWT used for all treehole `/api/claw/*` calls.

## 2. WebSocket channel `/api/claw/ws`

Connect, then authenticate with an `auth` frame before any other frame.

### 2.1 auth

```jsonc
// client -> server
{ "type": "auth", "token": "<APP JWT>", "version": "1.0" }

// server -> client
{ "type": "auth_success", "timestamp": 1750000000000, "channel_count": 0, "version": "1.0" }
```

`channel_count` is the number of existing conversations (0 for a new user).

### 2.2 openclaw.onboard (blocking)

Creates (or reuses) the user's single OpenClaw instance. This call **blocks**
until the instance reaches `ready` or fails — a cold cell (first image pull +
plugin install + gateway boot) can take minutes. Keep the WS connection open
and show a "creating" state.

```jsonc
// client -> server  (request_id doubles as the idempotency key)
{
  "type": "openclaw.onboard",
  "request_id": "<uuid>",
  "payload": { "provider": "fleet", "name": "", "image": "", "metadata": {} }
}

// server -> client (sent only when onboarding has finished)
{
  "type": "openclaw.onboard.status",
  "request_id": "<uuid>",
  "payload": { "state": "ready", "instance_id": 1 }
}
```

`state` is one of: `not_started`, `provisioning`, `starting`, `ready`,
`stopping`, `stopped`, `resetting`, `failed`.

### 2.3 openclaw.instance.status

```jsonc
// request
{ "type": "openclaw.instance.status", "request_id": "<uuid>", "payload": {} }

// response
{ "type": "openclaw.instance.status", "request_id": "<uuid>",
  "payload": { "state": "ready", "instance_id": 1 } }
```

### 2.4 openclaw.chat.send

Send a user message to the agent. The first message should use
`channel_id: 0` (a new conversation is created); reuse the returned
`channel_id` for subsequent messages in the same conversation.

```jsonc
// request
{
  "type": "openclaw.chat.send",
  "request_id": "<uuid>",
  "payload": {
    "channel_id": 0,          // 0 = new conversation
    "session_id": "",         // leave empty
    "content": "What is the capital of France?",
    "message_id": "<uuid>",   // client-generated, unique per message
    "media": null
  }
}

// accepted (message queued / forwarded)
{
  "type": "openclaw.chat.accepted",
  "request_id": "<uuid>",
  "payload": { "status": "queued", "task_id": "task_1_1_1750000000000",
               "session_id": "oc-1-1750000000000", "channel_id": 1 }
}

// model reply arrives later as a "message" frame, correlated by task_id
{
  "type": "message",
  "from": "openclaw",
  "content": "Paris",
  "message_id": "<reply id>",
  "task_id": "task_1_1_1750000000000",
  "channel_id": 1,
  "timestamp": 1750000001000,
  "version": "1.0"
}
```

Correlate the reply to the outgoing message by `task_id` (and
`channel_id`). Do **not** rely on `session_id` in replies — the backend
strips it before forwarding.

### 2.5 errors

```jsonc
{ "type": "openclaw.error", "request_id": "<uuid>", "error_code": "CLAW_001", "message": "..." }
```

Error codes: `AUTH_001` (auth failed), `AUTH_002` (not authed),
`MSG_001` (empty content), `MSG_002` (unknown type), `SYS_001` (internal),
`CLAW_001` (process failed, e.g. instance not ready).

## 3. HTTP endpoints

All require `Authorization: Bearer <APP JWT>`. Lifecycle writes require an
`Idempotency-Key` header.

### 3.1 GET /api/claw/instance

```jsonc
// no instance yet
{ "state": "not_started", "status": "not_started" }
// instance exists
{ "instance_id": 1, "state": "ready", "status": "ready",
  "last_error_code": "", "last_error_message": "" }
```

### 3.2 POST /api/claw/onboard

```http
POST /api/claw/onboard
Idempotency-Key: <uuid>
Content-Type: application/json

{"provider": "fleet"}
```

Blocks until `ready`/failure, like the WS path. The response object contains
the instance (its `state` is the key field).

### 3.3 POST /api/claw/start | stop | restart | reset

Same header + empty/optional body. Each is idempotent under its
`Idempotency-Key`. `reset` destroys the cell + clears messages/session and
returns the instance to `not_started`.

### 3.4 GET /api/claw/channels

Returns the user's conversations (a "channel" = one conversation):

```jsonc
[
  { "instance_id": 1, "user_session_id": 1, "oc_session_id": "", "created_at": "...", "updated_at": "..." }
]
```

Use `user_session_id` as the `channel_id` in chat/messages.

### 3.5 GET /api/claw/messages

Query: `channel_id` (required), `size` (default 30), `offset` (0),
`sort` (`asc`|`desc`, default `desc`).

```jsonc
[
  { "from": "user", "content": "What is the capital of France?",
    "message_id": "...", "task_id": "task_1_1_...", "channel_id": 1,
    "timestamp": 1750000000000, "media": null, "version": "1.0" },
  { "from": "openclaw", "content": "Paris", "message_id": "...",
    "task_id": "task_1_1_...", "channel_id": 1, "timestamp": 1750000001000,
    "media": null, "version": "1.0" }
]
```

## 4. Frontend integration notes

1. **Onboard is blocking and slow.** Show a persistent "creating" state; do not
   drop the WS connection mid-onboard. Poll `openclaw.instance.status` /
   `GET /instance` only after onboard returns.
2. **Idempotency.** Re-sending the same `request_id` (WS) or
   `Idempotency-Key` (HTTP) is safe: no second cell is created.
3. **Correlation.** Track `task_id` -> reply; ignore reply `session_id`.
4. **Ownership.** Never send `user_id` / `instance_id` / `task_id` to
   establish ownership — the backend derives ownership from the authenticated
   session and rejects cross-user access (404 fail-closed).
5. **state machine.** `ready` is required before chat; `stop` preserves
   data, `restart` restores it, `reset` clears it.
