# Danta OpenClaw APP 前端联调 API 文档

版本：MVP / 2026-08-18

本文档描述 Danta APP 前端与 `treehole_next` OpenClaw 控制面之间的接口。
前端只使用 APP 用户 JWT；OpenClaw workload token 由后端内部生成和注入，
不会返回给前端。

## 0. 联调结论与地址

当前已经可以开始前端联调：

- 测试后端：`http://192.168.84.3:8000`
- 测试认证服务：`http://192.168.84.3:8001`
- APP WebSocket：`ws://192.168.84.3:8000/api/claw/ws`
- HTTP Base Path：`/api`
- 当前 staging 初始状态：没有实例，用户首次进入时应为 `not_started`

后端已验证：

- Onboard -> ready；
- 双用户实例隔离；
- Chat 消息持久化和回复关联；
- Stop -> Restart；
- Reset 清理；
- 幂等请求。

最近一次服务器验收为 `37 passed, 0 failed`。首次冷启动可能需要数分钟，
前端必须保持 WebSocket 连接并展示加载状态。

## 1. 用户登录

登录请求发送给 `auth_next`，不是 `treehole_next`：

```http
POST http://192.168.84.3:8001/api/login
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "..."
}
```

响应示例：

```json
{
  "access": "<APP_JWT>",
  "refresh": "<REFRESH_TOKEN>",
  "message": "Login successful"
}
```

后续所有 HTTP 请求使用：

```http
Authorization: Bearer <APP_JWT>
```

WebSocket 连接建立后，通过第一条 `auth` 消息传递同一个 APP JWT。

## 2. 核心状态机

实例状态：

```text
not_started
    -> provisioning
    -> starting
    -> ready

ready -> stopping -> stopped
stopped -> starting -> ready

ready / stopped / failed -> resetting -> not_started
任意生命周期失败 -> failed
```

只有 `ready` 状态允许聊天。

`ready` 同时满足以下三个条件：

1. Fleet 容器正在运行；
2. OpenClaw Gateway 健康；
3. Danta Channel Plugin 已通过鉴权并建立 WebSocket。

## 3. 推荐前端流程

```text
登录
  -> 连接 /api/claw/ws
  -> 发送 auth
  -> 查询 openclaw.instance.status
  -> 如果不是 ready，发送 openclaw.onboard
  -> 收到 onboard.status = ready
  -> 发送 openclaw.chat.send
  -> 监听 message(from=openclaw)
```

前端不需要也不应该直接调用 OpenClaw token provisioning API。

## 4. APP WebSocket API

### 4.1 建立连接

```text
ws://192.168.84.3:8000/api/claw/ws
```

连接成功后，必须先发送 `auth`：

```json
{
  "type": "auth",
  "token": "<APP_JWT>",
  "version": "1.0"
}
```

成功响应：

```json
{
  "type": "auth_success",
  "timestamp": 1787000000000,
  "channel_count": 0,
  "version": "1.0"
}
```

`channel_count` 是当前用户已有会话数量。

后端会定期发送：

```json
{
  "type": "ping",
  "timestamp": 1787000000000,
  "version": "1.0"
}
```

前端应及时回复：

```json
{
  "type": "pong",
  "timestamp": 1787000000000,
  "version": "1.0"
}
```

### 4.2 Onboarding

Onboarding 会执行：

```text
创建或复用实例
  -> 启动 Fleet cell
  -> 等待 Gateway healthy
  -> 等待 Danta Channel authenticated
  -> 返回 ready
```

请求：

```json
{
  "type": "openclaw.onboard",
  "request_id": "onboard-<uuid>",
  "payload": {
    "provider": "fleet",
    "name": "",
    "image": "",
    "metadata": {}
  }
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `type` | string | 是 | 固定为 `openclaw.onboard` |
| `request_id` | string | 是 | 请求 ID，同时作为幂等键 |
| `payload.provider` | string | 否 | MVP 使用 `fleet` |
| `payload.name` | string | 否 | 实例名称，目前可留空 |
| `payload.image` | string | 否 | 使用默认 cell image 时留空 |
| `payload.metadata` | object | 否 | 额外 metadata，通常传 `{}` |

响应：

```json
{
  "type": "openclaw.onboard.status",
  "request_id": "onboard-<uuid>",
  "payload": {
    "state": "ready",
    "instance_id": 1
  }
}
```

注意：

- 这是阻塞式请求，可能持续数分钟；
- 不要因为等待时间较长而关闭 WebSocket；
- `state` 不为 `ready` 时，前端应视为失败；
- 使用同一个 `request_id` 重试不会创建第二个实例；
- 每个用户最多一个 OpenClaw 实例。

### 4.3 查询实例状态

请求：

```json
{
  "type": "openclaw.instance.status",
  "request_id": "status-<uuid>",
  "payload": {}
}
```

响应：

```json
{
  "type": "openclaw.instance.status",
  "request_id": "status-<uuid>",
  "payload": {
    "state": "ready",
    "instance_id": 1
  }
}
```

### 4.4 发送聊天消息

首次聊天创建新会话时，使用 `channel_id: 0`，并将 `session_id` 留空：

```json
{
  "type": "openclaw.chat.send",
  "request_id": "chat-<uuid>",
  "payload": {
    "channel_id": 0,
    "session_id": "",
    "content": "What is the capital of France?",
    "message_id": "message-<uuid>",
    "media": null
  }
}
```

消息发送成功后，先收到 accepted：

```json
{
  "type": "openclaw.chat.accepted",
  "request_id": "chat-<uuid>",
  "payload": {
    "status": "queued",
    "task_id": "task_1_1_1787000000000",
    "session_id": "oc-1-1787000000000",
    "channel_id": 1
  }
}
```

之后异步收到 OpenClaw 回复：

```json
{
  "type": "message",
  "from": "openclaw",
  "content": "Paris",
  "message_id": "reply-<id>",
  "task_id": "task_1_1_1787000000000",
  "channel_id": 1,
  "timestamp": 1787000001000,
  "media": null,
  "version": "1.0"
}
```

前端必须使用 `task_id` 关联请求和回复。

- 不要根据 `task_id` 推断用户身份；
- 不要依赖回复中的 `session_id`；
- 回复转发给 APP 时，后端会隐藏 `session_id`；
- 后续同一会话使用返回的 `channel_id`；
- `message_id` 建议由前端生成并保证唯一。

### 4.5 WebSocket 错误

```json
{
  "type": "openclaw.error",
  "request_id": "chat-<uuid>",
  "error_code": "CLAW_001",
  "message": "OpenClaw 实例未就绪"
}
```

常见错误码：

| 错误码 | 含义 |
|---|---|
| `AUTH_001` | APP JWT 鉴权失败 |
| `AUTH_002` | 尚未完成 WebSocket 鉴权 |
| `MSG_001` | 消息内容为空 |
| `MSG_002` | 未知消息类型或参数格式错误 |
| `SYS_001` | 后端内部错误 |
| `CLAW_001` | OpenClaw 实例未就绪或生命周期操作失败 |
| `OPENCLAW_NOT_CONFIGURED` | 后端没有配置 OpenClaw lifecycle service |

## 5. HTTP 生命周期 API

所有接口都需要 APP JWT。

生命周期写操作必须带：

```http
Idempotency-Key: <unique-key>
```

同一个用户、同一个 key 只能对应同一个操作。前端重试时应复用原 key，
不要每次自动生成新 key。

### 5.1 查询实例

```http
GET http://192.168.84.3:8000/api/claw/instance
Authorization: Bearer <APP_JWT>
```

无实例：

```json
{
  "state": "not_started",
  "status": "not_started"
}
```

已有实例：

```json
{
  "instance_id": 1,
  "state": "ready",
  "status": "ready",
  "last_error_code": "",
  "last_error_message": "",
  "cleanup_error_code": "",
  "cleanup_error_message": ""
}
```

### 5.2 HTTP Onboarding

```http
POST http://192.168.84.3:8000/api/claw/onboard
Authorization: Bearer <APP_JWT>
Idempotency-Key: onboard-<uuid>
Content-Type: application/json

{
  "provider": "fleet",
  "name": "",
  "image": "",
  "metadata": {}
}
```

该接口同样会阻塞到 `ready` 或返回失败。

当前后端返回的是生命周期结果对象，字段名保持 Go JSON 默认形式：

```json
{
  "Instance": {
    "id": 1,
    "user_id": 1,
    "provider": "fleet",
    "state": "ready",
    "created_at": "2026-08-18T03:00:00Z",
    "updated_at": "2026-08-18T03:02:00Z"
  },
  "Operation": {
    "id": 1,
    "operation": "onboard",
    "target_state": "ready",
    "status": "completed"
  },
  "Readiness": {
    "ContainerRunning": true,
    "GatewayHealthy": true,
    "ChannelAuthenticated": true
  },
  "Reused": false
}
```

前端主要读取：

```text
Instance.state
Instance.id
Reused
```

### 5.3 启动、停止、重启、重置

通用格式：

```http
POST /api/claw/{action}
Authorization: Bearer <APP_JWT>
Idempotency-Key: <unique-key>
```

其中 `{action}` 为：

```text
start
stop
restart
reset
```

请求体可以为空。

语义：

| API | 语义 |
|---|---|
| `start` | 启动 stopped/failed 实例并等待 ready |
| `stop` | 停止实例，保留 Agent 数据和会话数据 |
| `restart` | 重启实例并等待 ready |
| `reset` | 销毁 Fleet cell，清除消息和会话，回到 not_started |

示例：

```http
POST /api/claw/stop
Authorization: Bearer <APP_JWT>
Idempotency-Key: stop-<uuid>
```

## 6. HTTP 会话与消息查询

### 6.1 查询会话列表

```http
GET /api/claw/channels
Authorization: Bearer <APP_JWT>
```

响应：

```json
[
  {
    "instance_id": 1,
    "user_session_id": 1,
    "oc_session_id": "",
    "conversation": "",
    "created_at": "2026-08-18T03:00:00Z",
    "updated_at": "2026-08-18T03:00:00Z"
  }
]
```

前端使用 `user_session_id` 作为聊天请求中的 `channel_id`。

后端不会向 APP 暴露 `oc_session_id` 的真实值。

### 6.2 查询会话消息

```http
GET /api/claw/messages?channel_id=1&size=30&offset=0&sort=asc
Authorization: Bearer <APP_JWT>
```

参数：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `channel_id` | 无 | 必填，必须属于当前用户 |
| `size` | `30` | 返回条数 |
| `offset` | `0` | 分页偏移 |
| `sort` | `desc` | `asc` 或 `desc` |

响应：

```json
[
  {
    "from": "user",
    "content": "What is the capital of France?",
    "message_id": "message-1",
    "task_id": "task_1_1_1787000000000",
    "channel_id": 1,
    "timestamp": 1787000000000,
    "media": null,
    "version": "1.0"
  },
  {
    "from": "openclaw",
    "content": "Paris",
    "message_id": "reply-1",
    "task_id": "task_1_1_1787000000000",
    "channel_id": 1,
    "timestamp": 1787000001000,
    "media": null,
    "version": "1.0"
  }
]
```

跨用户访问其他用户的 `channel_id` 会失败关闭，通常返回 HTTP 404。

## 7. 前端实现注意事项

1. Onboarding 必须展示长时间 loading，首次创建可能需要数分钟。
2. WebSocket 连接断开后，需要重新连接并重新发送 `auth`。
3. `request_id` 和 `Idempotency-Key` 应使用稳定、唯一、可重试的值。
4. 只有收到 `state=ready` 后才允许发送聊天消息。
5. 聊天回复使用 `task_id` 关联，不要把用户 ID、实例 ID 当作客户端可信参数。
6. `stop` 不删除数据；`reset` 会删除实例、消息和会话。
7. 前端不要调用 `/api/openclaw/token*`，这些接口只供后端控制面使用。
8. APP 使用 `/api/claw/ws`；`/api/claw/oc` 是 OpenClaw Plugin 到后端的内部
   WebSocket，不是前端接口。

## 8. 参考客户端

仓库中提供了 Node.js 参考客户端：

```text
treehole_next/docs/openclaw-app-api-client.mjs
```

运行示例：

```bash
BACKEND_URL=http://192.168.84.3:8000 \
AUTH_URL=http://192.168.84.3:8001 \
EMAIL=your-email \
PASSWORD=your-password \
node treehole_next/docs/openclaw-app-api-client.mjs
```

跳过 Onboarding、复用已有实例：

```bash
SKIP_ONBOARD=1 \
BACKEND_URL=http://192.168.84.3:8000 \
AUTH_URL=http://192.168.84.3:8001 \
EMAIL=your-email \
PASSWORD=your-password \
node treehole_next/docs/openclaw-app-api-client.mjs
```
