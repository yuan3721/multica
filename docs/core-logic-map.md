# Multica 核心逻辑与代码导航

> 从"用户指派 agent"到"结果回到前端"的每一步，标注真实代码位置，可逐行追踪。
>
> 年轻工程师拿着这份文档，按行号走一遍代码即可理解全部核心链路。

---

## 目录

1. [核心链路总览](#1-核心链路总览)
2. [触发与入队](#2-触发与入队)
3. [唤醒与认领](#3-唤醒与认领)
4. [任务执行](#4-任务执行)
5. [进度回传与终态](#5-进度回传与终态)
6. [实时推送](#6-实时推送)
7. [Daemon 生命周期](#7-daemon-生命周期)
8. [Cloud Runtime 回连](#8-cloud-runtime-回连)
9. [代码索引表](#9-代码索引表)

---

## 1. 核心链路总览

```
用户改 issue assignee
    │
    ▼
[2] WillEnqueueRun 决策 ──────────────────────────────┐
    │ enqueueIssueTask                                 │
    ▼  写 agent_task_queue(runtime_id, status=pending) │
[3] daemonws.Hub.NotifyTaskAvailable                  │ ← 同一张表
    │ WS 唤醒                                          │
    ▼                                                  │
[3] daemon ClaimTaskByRuntime ──────────────────────┐ │
    │ POST /tasks/claim 原子认领                      │ │
    ▼  status = running                              │ │
[4] runTask → Backend.Execute                        │ │
    │ spawn CLI 子进程, drain 事件                    │ │
    ▼                                                │ │
[5] ReportProgress / messages / complete / fail ─────┘ │
    │ 写 DB + events.Bus                                │
    ▼                                                  │
[6] realtime.Hub → /ws 推浏览器 ◄──────────────────────┘
```

---

## 2. 触发与入队

### 2.1 决策：要不要启动一次 run

用户改 issue 的 assignee 或 status 时，handler 先调 `WillEnqueueRun` 判断是否该入队。

**决策规则**（[`issue_trigger.go:97`](../server/internal/service/issue_trigger.go#L97)）：

| 触发来源 | 条件 | 结果 |
|---|---|---|
| 新建 issue / 改 assignee | assignee 是 agent + status ≠ backlog | `RunSourceAssign` |
| status 从 backlog 流出 | assignee 是 agent + 非自环 + 无 pending | `RunSourceStatus` |
| 任何触发 | assignee 是 squad | 走 squad leader 分支 |

关键拦截条件：
- backlog 是停车场，指派进 backlog **不启动** run（L127）
- status 流转路径有**自环检查**——agent 正在跑该 issue 时不重复入队（L134）
- status 路径有 **pending 去重**——已有 pending task 不再入队（L155）

### 2.2 入队：写 task 行

决策通过后，`dispatchIssueRun` 按 assignee 类型分流（[`issue_trigger.go:84`](../server/internal/handler/issue_trigger.go#L84)）：

```go
switch trigger.AssigneeType {
case "agent":  h.TaskService.EnqueueTaskForIssueWithHandoff(ctx, issue, handoffNote)
case "squad":  h.enqueueSquadLeaderTask(ctx, issue, ...)
}
```

核心入队函数 [`enqueueIssueTaskWithCommentPlan`](../server/internal/service/task.go#L1144)，做的事按顺序：

1. **加载 agent**（L1150）——校验未归档、有 runtime_id
2. **归属解析**（L1169）——决定此次 run 的 accountable human（触发者 → owner fallback）
3. **构建 MCP overlay**（L1178）——把 agent 的 connected app 配置注入 task
4. **写 `agent_task_queue`**（L1220+）——一行 INSERT，绑定 `agent_id + runtime_id + issue_id`，status = pending

> **关键设计**：入队只写一行 DB，不选 daemon、不分配 slot。哪个 daemon 先 claim 就归谁，与 daemon 数量无关。

---

## 3. 唤醒与认领

### 3.1 WS 唤醒：best-effort 通知

入队写入 task 行后，server 通过事件总线通知 daemon。

唤醒链路：

```
TaskService 写 task → events.Bus → daemonws.Hub.NotifyTaskAvailable → daemon WS
```

唤醒帧定义在 [`hub.go:386`](../server/internal/daemonws/hub.go#L386)：

```go
func taskAvailableFrame(runtimeID, taskID string) ([]byte, error) {
    return json.Marshal(protocol.Message{
        Type: protocol.EventDaemonTaskAvailable,
        Payload: ...{RuntimeID, TaskID},
    })
}
```

投递逻辑（[`hub.go:330`](../server/internal/daemonws/hub.go#L330)）：按 `runtimeID` 索引连接，有 event-id 做 LRU 去重，send buffer 满则踢掉慢连接。**投递是 best-effort**——丢帧不影响正确性，daemon 还有 HTTP 轮询兜底。

### 3.2 HTTP claim：原子认领

daemon 收到唤醒（或轮询到期）后调 HTTP claim。这是唯一能真正拿到 task 的操作。

**Server 侧 handler**（[`daemon.go:3188`](../server/internal/handler/daemon.go#L3188)）：

```go
func (h *Handler) ClaimTaskByRuntime(w http.ResponseWriter, r *http.Request) {
    runtime, ok := h.requireDaemonRuntimeAccess(w, r, runtimeID)  // 鉴权: 调用方拥有此 runtime
    task, err := h.TaskService.ClaimTaskForRuntime(r.Context(), parseUUID(runtimeID))
    ...
    resp := h.buildClaimedTaskResponse(r, task, runtime, ...)      // 组装 prompt/skills/repos/env
}
```

**Service 侧 claim 事务**（[`task.go:3086`](../server/internal/service/task.go#L3086)）：

```
Begin Tx
  ├─ GetAgentForClaimUpdate        → 加载 agent + 并发上限
  ├─ CountRunningTasks             → running < max_concurrent_tasks?
  ├─ ClaimAgentTask                → SELECT ... FOR UPDATE SKIP LOCKED, 原子认领
  │     status: pending → running
  │     设 prepare_lease 到期时间
  ├─ ReanchorClaimedDirectChatInput → chat 位置修正（如适用）
  └─ dispatchMs / 事件
Commit
```

**`ClaimAgentTask` SQL 的核心**（sqlc 生成）：`FOR UPDATE SKIP LOCKED` 保证多 daemon 并发 claim 同一批 task 时，每个 task 只被一个事务拿到，其余跳过不阻塞。

> **关键设计**：claim 在事务内做容量检查（`running < max_concurrent_tasks`），防止超卖。认领成功后立即返回完整 task payload（prompt、skills、repos、env），daemon 不需要再发额外请求。

### 3.3 prepare-lease：准备阶段保护

claim 成功到 agent 真正 start 之间有慢操作（克隆 repo、装 skills），daemon 起一个续租器（[`daemon.go:6190`](../server/internal/daemon/daemon.go#L6190)）：

```go
func (d *Daemon) startTaskPrepareLeaseExtender(ctx context.Context, task Task, ...) func() {
    // 周期性 POST /tasks/{id}/prepare-lease, 直到 start 或 task 被取消
}
```

对应 server 端点 `POST /runtimes/{runtimeId}/tasks/{taskId}/prepare-lease`（[router.go:733](../server/cmd/server/router.go#L733)），延长 lease 到期时间。

---

## 4. 任务执行

### 4.1 runTask：组装执行环境

[`runTask`](../server/internal/daemon/daemon.go#L6568) 是 daemon 侧最核心的函数，按顺序：

1. **注册 task repos**（L6571）——把 task 的 repo URL 加入 per-workspace 白名单
2. **解析 provider**（L6582）——`task.RuntimeID` → `AgentEntry{Path, Model}`，custom profile 覆盖 Path（L6591）
3. **起 prepare-lease 续租器**（L6604）
4. **解析 skills bundle**（L6607）——[`ensureTaskSkillBundles`](../server/internal/daemon/daemon.go#L5970) 调 `POST .../skill-bundles/resolve` 拉取
5. **构建 ExecOptions**（L6625+）——cwd、model、mcp-config、thinking-level、resumeSessionID、customArgs
6. **构建执行环境**（L7332）——`MULTICA_*` env vars（workspace_id、issue_id、agent_id、repos、token…）
7. **调用 `executeAndDrain`**（详见 4.2）
8. **终态处理**——retry-with-fresh-session 判定（[`daemon.go:7978`](../server/internal/daemon/daemon.go#L7978)）
9. **回报**——`CompleteTask` / `FailTask` + `ReportTaskUsage` + `PinTaskSession`

### 4.2 executeAndDrain：spawn + 事件流

[`executeAndDrain`](../server/internal/daemon/daemon.go#L8135) 是真正 spawn CLI 并消费事件流的地方：

```go
func (d *Daemon) executeAndDrain(ctx, backend, prompt, opts, ...) (agent.Result, int32, error) {
    session, err := backend.Execute(agentCtx, prompt, opts)   // L8144 — spawn CLI 子进程

    // idle watchdog — 监控 lastActivityAt, 超时则 agentCancel()
    go d.runIdleWatchdog(...)                                  // L8206

    for {
        select {
        case msg := <-session.Messages:                        // L8276 — 流式事件
            // text/thinking/tool-use/tool-result → ReportTaskMessages
        case result := <-session.Result:                        // L8443 — 唯一终态
            return result, toolCount, nil
        }
    }
}
```

**关键子机制**：

- **idle watchdog**（L8206 + [`daemon.go:8513`](../server/internal/daemon/daemon.go#L8513)）：
  - 读 `lastActivityAt`（每次收到 Message 时更新）
  - `inFlightTools > 0` 时用更大的 `AgentToolWatchdog` 预算（等工具执行）
  - `inFlightTools == 0` 时用 `AgentIdleWatchdog` 预算（正常静默检测）
  - 超时 → `agentCancel()` 杀子进程，ctx 传播到 drain loop

- **drain deadline**（L8170）：
  - `opts.Timeout > 0`：`drainCtx = WithTimeout(agentCtx, opts.Timeout + 30s)`
  - `opts.Timeout <= 0`：不设 deadline，完全靠 idle watchdog
  - 多出的 30s 给 backend 自己的 timeout Result 留收集窗口

- **launch error 统一处理**（L8147）：
  ```
  backend.Execute 返回 error → agent.ExplainExecError(err)
  → 所有 backend 的 cmd.Start() 失败在此统一处理, 不必每个 backend 各自 wrap
  ```

### 4.3 Backend.Execute：多种 CLI 的统一入口

[`agent.New(type, cfg)`](../server/pkg/agent/agent.go#L366) 按类型选后端：

```go
func New(agentType string, cfg Config) (Backend, error) {
    switch agentType {
    case "claude":    return newClaude(cfg)
    case "codex":     return newCodex(cfg)
    case "copilot":   return newCopilot(cfg)
    case "cursor":    return newCursor(cfg)
    case "dimcli":    return newDimcli(cfg)
    case "openclaw":  return newOpenclaw(cfg)
    ...
    }
}
```

每个后端的 `Execute` 都是：spawn 子进程 → 解析 stdout/stderr 为统一 `Message` → 写 channel。

以 [`claude.go`](../server/pkg/agent/claude.go) 为例，关键参数注入：

| ExecOptions 字段 | CLI flag |
|---|---|
| `Model` | `--model` |
| `McpConfig` | `--mcp-config <path>` |
| `MaxTurns` | `--max-turns` |
| `ResumeSessionID` | `--resume` |
| `ThinkingLevel` | `--effort` |
| `SystemPrompt` | `--system-prompt` |
| `CustomArgs` | 追加到 argv 末尾 |

所有后端的输出归一到 `Session`：

```go
type Session struct {
    Messages <-chan Message  // stream: text/thinking/tool-use/tool-result/status/error/log
    Result   <-chan Result   // final: status/output/sessionID/usage
}
```

---

## 5. 进度回传与终态

### 5.1 流式进度

daemon drain 到每条 Message 后通过 HTTP 回报 server：

| 事件 | 回报端点 | server handler |
|---|---|---|
| text/thinking/tool-use | `POST /tasks/{id}/messages` | [`ReportTaskMessages`](../server/internal/handler/daemon.go#L4463) |
| progress 步数 | `POST /tasks/{id}/progress` | [`ReportTaskProgress`](../server/internal/handler/daemon.go#L3585) |
| 启动标记 | `POST /tasks/{id}/start` | [`StartTask`](../server/internal/handler/daemon.go#L3518) |

每条回报写 DB 后进 `events.Bus`，由 realtime hub 广播到浏览器。

### 5.2 终态

| 结局 | 端点 | handler | 写入 |
|---|---|---|---|
| 完成 | `POST /tasks/{id}/complete` | [`CompleteTask`](../server/internal/handler/daemon.go#L3662) | status=completed + output + branch + session |
| 失败 | `POST /tasks/{id}/fail` | [`FailTask`](../server/internal/handler/daemon.go#L4386) | status=failed + error + reason |
| 用量 | `POST /tasks/{id}/usage` | [`ReportTaskUsage`](../server/internal/handler/daemon.go#L4275) | 写 usage 表 |
| session pin | `POST /tasks/{id}/session` | (daemon client) | 记录 resume 指针 |

**终态重试**：`complete` / `fail` 用 `postJSONWithRetry`（[`client.go:276`](../server/internal/daemon/client.go#L276)），内置指数退避重试 schedule，保证 "must reach server" 的终态不丢。

**fresh-session retry**：agent 返回特定失败模式时，daemon 可能用 `forceFreshSession=true` 重试一次（不 resume 旧 session），决策在 [`shouldRetryWithFreshSession`](../server/internal/daemon/daemon.go#L7978)。

### 5.3 完成后的 issue 状态联动

`CompleteTask` handler 内部会联动 issue 状态（如 autopilot 链路、评论归因），最终通过 `events.Bus` 广播 issue 变更到 /ws。

---

## 6. 实时推送

### 6.1 浏览器侧 /ws

浏览器连接 `WS /ws`（[router.go:660](../server/cmd/server/router.go#L660)），按 workspace + scope 订阅。

连接生命周期（[`realtime/hub.go`](../server/internal/realtime/hub.go)）：
1. **鉴权**——cookie / PAT，校验 workspace membership（[`hub.go:23`](../server/internal/realtime/hub.go#L23) `MembershipChecker`）
2. **scope 授权**——订阅 task/chat scope 时检查资源归属（[`hub.go:40`](../server/internal/realtime/hub.go#L40) `ScopeAuthorizer`）
3. **广播**——`events.Bus` 发的事件按 scope 分发给本地连接

### 6.2 多实例 Redis relay

多 server 实例时，`events.Bus` 的事件通过 Redis Stream relay 跨 Pod 传播：

- 发布：`XADD ws:relay:shard:{hash(scope)%8}`
- 消费：每个 Pod 起所有 8 shard 的阻塞 reader（[`sharded_stream_relay.go`](../server/internal/realtime/sharded_stream_relay.go)）
- 启动从 `(now - 5min)` 回看，补播漏帧
- 下游靠 event-id 去重

### 6.3 前端消费

前端用 React Query 管 server state。WS 帧到达后 patch / invalidate 对应 query key（`wsId` 必须在 key 里），不把 payload mirror 进 Zustand。

---

## 7. Daemon 生命周期

### 7.1 启动序列

[`Daemon.Run`](../server/internal/daemon/daemon.go#L724)：

```
Run(ctx)
  ├─ listenHealth()              → 绑定健康端口, 检测已有 daemon
  ├─ EnsureWorkspacesRootMarker  → 标记 workspaces 目录, 防止 CLI 逃逸
  ├─ resolveAuth()               → 加载 CLI config 的 PAT
  ├─ serveHealth(ctx)            → goroutine, liveness=starting
  ├─ preflightAuth(ctx)          → 续 PAT + 首次 workspace 同步
  │    ├─ token renewal
  │    └─ syncWorkspacesFromAPI
  ├─ registerRuntimesForWorkspace → POST /daemon/register (每 ws)
  ├─ ready = true                → liveness → running
  │
  ├─ go workspaceSyncLoop        → 周期拉 /api/workspaces
  ├─ go taskWakeupLoop           → WS 唤醒监听 (wakeup.go:46)
  ├─ go heartbeatLoop            → WS/HTTP 心跳 (daemon.go:3794)
  ├─ go gcLoop                   → 孤儿+workdir 回收 (gc.go:25)
  ├─ go autoUpdateLoop           → 自更新
  ├─ go tokenRenewalLoop         → PAT 续期 (daemon.go:3468)
  │
  └─ pollLoop(ctx, taskWakeups)  → 主轮询: 每个 runtime 一个 poller
  defer deregisterRuntimes()     → 用 fresh ctx deregister
```

### 7.2 pollLoop —— 每个 runtime 一个 poller

[`pollLoop`](../server/internal/daemon/daemon.go#L4669) 为每个 runtime ID 起一个 goroutine，消费 `taskWakeups` channel：

```go
for {
    select {
    case wakeup := <-taskWakeups:   // WS 唤醒
    case <-ticker:                  // 定时轮询
    }

    tryEnterClaim()                 → 抢 signal slot (MaxConcurrentTasks)
    task = client.ClaimTask(rid)    → HTTP claim
    if task == nil → sleep
    runTask(...)                    → 执行
    exitClaim()                     → 释放 slot
}
```

### 7.3 taskWakeupLoop —— WS 连接管理

[`taskWakeupLoop`](../server/internal/daemon/wakeup.go#L46) → [`runTaskWakeupConnection`](../server/internal/daemon/wakeup.go#L99)：

- 连 `WS /api/daemon/ws`，带 runtime IDs
- 收到 `daemon:task_available` 帧写 `taskWakeups` channel
- WS 断线自动重连，期间 pollLoop 的定时器兜底

### 7.4 心跳

[`heartbeatLoop`](../server/internal/daemon/daemon.go#L3794) → [`runRuntimeHeartbeat`](../server/internal/daemon/daemon.go#L3842)：

- WS 心跳：经 `daemon:heartbeat` / `daemon:heartbeat_ack` 往返
- HTTP fallback：无 WS ack 时打 `POST /daemon/heartbeat`
- ack 里可能带 actions（model-list 请求、local-skill 请求、profile 变更提示）→ `handleHeartbeatActions`

---

## 8. Cloud Runtime 回连

云端 node = EC2 + 内嵌 daemon，回连走完全相同的协议。

```
用户 → POST /api/cloud-runtime/nodes (proxy)
        → Fleet POST /api/v1/nodes
        → Fleet 签发 mcn_ PAT, SSM 注入 EC2

EC2 启动 → multica daemon (mcn_ PAT)
        → POST /api/daemon/register
        → WS /api/daemon/ws
        ── 之后与本地 daemon 同构 ──
        → claim → runTask → Execute → report
```

代理逻辑：[`cloud_runtime.go`](../server/internal/handler/cloud_runtime.go) — server 不转发用户 `mul_` PAT，node 用独立 `mcn_` PAT 回连，blast radius 隔离。

---

## 9. 代码索引表

### 触发与入队

| 逻辑 | 文件:行 |
|---|---|
| 触发决策 | [`service/issue_trigger.go:97`](../server/internal/service/issue_trigger.go#L97) |
| 分流 dispatch | [`handler/issue_trigger.go:84`](../server/internal/handler/issue_trigger.go#L84) |
| 入队核心 | [`service/task.go:1144`](../server/internal/service/task.go#L1144) |
| agent 入队 | [`service/task.go:1091`](../server/internal/service/task.go#L1091) |
| squad leader 入队 | [`service/task.go:1294`](../server/internal/service/task.go#L1294) |

### 认领

| 逻辑 | 文件:行 |
|---|---|
| WS 唤醒投递 | [`daemonws/hub.go:330`](../server/internal/daemonws/hub.go#L330) |
| 唤醒帧定义 | [`daemonws/hub.go:386`](../server/internal/daemonws/hub.go#L386) |
| claim handler | [`handler/daemon.go:3188`](../server/internal/handler/daemon.go#L3188) |
| claim 事务 | [`service/task.go:3086`](../server/internal/service/task.go#L3086) |
| prepare-lease 续租 | [`daemon/daemon.go:6190`](../server/internal/daemon/daemon.go#L6190) |
| daemon 侧 claim | [`daemon/client.go:185`](../server/internal/daemon/client.go#L185) |

### 执行

| 逻辑 | 文件:行 |
|---|---|
| runTask | [`daemon/daemon.go:6568`](../server/internal/daemon/daemon.go#L6568) |
| skills 解析 | [`daemon/daemon.go:5970`](../server/internal/daemon/daemon.go#L5970) |
| executeAndDrain | [`daemon/daemon.go:8135`](../server/internal/daemon/daemon.go#L8135) |
| Backend.Execute 调用 | [`daemon/daemon.go:8144`](../server/internal/daemon/daemon.go#L8144) |
| idle watchdog | [`daemon/daemon.go:8206`](../server/internal/daemon/daemon.go#L8206) + [`daemon.go:8513`](../server/internal/daemon/daemon.go#L8513) |
| fresh-session retry | [`daemon/daemon.go:7978`](../server/internal/daemon/daemon.go#L7978) |
| Backend 接口 | [`agent/agent.go:16`](../server/pkg/agent/agent.go#L16) |
| Backend 工厂 | [`agent/agent.go:366`](../server/pkg/agent/agent.go#L366) |
| ExecOptions | [`agent/agent.go:24`](../server/pkg/agent/agent.go#L24) |
| Session 结构 | [`agent/agent.go:80`](../server/pkg/agent/agent.go#L80) |

### 进度与终态

| 逻辑 | 文件:行 |
|---|---|
| progress handler | [`handler/daemon.go:3585`](../server/internal/handler/daemon.go#L3585) |
| messages handler | [`handler/daemon.go:4463`](../server/internal/handler/daemon.go#L4463) |
| complete handler | [`handler/daemon.go:3662`](../server/internal/handler/daemon.go#L3662) |
| fail handler | [`handler/daemon.go:4386`](../server/internal/handler/daemon.go#L4386) |
| usage handler | [`handler/daemon.go:4275`](../server/internal/handler/daemon.go#L4275) |
| 终态重试 | [`daemon/client.go:560`](../server/internal/daemon/client.go#L560) |
| daemon 侧 complete | [`daemon/client.go:265`](../server/internal/daemon/client.go#L265) |
| daemon 侧 fail | [`daemon/client.go:288`](../server/internal/daemon/client.go#L288) |

### 实时推送

| 逻辑 | 文件:行 |
|---|---|
| /ws 路由 | [`cmd/server/router.go:660`](../server/cmd/server/router.go#L660) |
| realtime Hub | [`realtime/hub.go`](../server/internal/realtime/hub.go) |
| membership 鉴权 | [`realtime/hub.go:23`](../server/internal/realtime/hub.go#L23) |
| scope 授权 | [`realtime/hub.go:40`](../server/internal/realtime/hub.go#L40) |
| 分片 relay | [`realtime/sharded_stream_relay.go`](../server/internal/realtime/sharded_stream_relay.go) |
| relay 消费 | [`realtime/redis_relay.go`](../server/internal/realtime/redis_relay.go) |

### Daemon 生命周期

| 逻辑 | 文件:行 |
|---|---|
| Run 主函数 | [`daemon/daemon.go:724`](../server/internal/daemon/daemon.go#L724) |
| pollLoop | [`daemon/daemon.go:4669`](../server/internal/daemon/daemon.go#L4669) |
| taskWakeupLoop | [`daemon/wakeup.go:46`](../server/internal/daemon/wakeup.go#L46) |
| WS 连接管理 | [`daemon/wakeup.go:99`](../server/internal/daemon/wakeup.go#L99) |
| heartbeatLoop | [`daemon/daemon.go:3794`](../server/internal/daemon/daemon.go#L3794) |
| heartbeat handler | [`handler/daemon.go:1010`](../server/internal/handler/daemon.go#L1010) |
| gcLoop | [`daemon/gc.go:25`](../server/internal/daemon/gc.go#L25) |
| 孤儿回收 | [`daemon/client.go:321`](../server/internal/daemon/client.go#L321) |
| token 续期 | [`daemon/daemon.go:3468`](../server/internal/daemon/daemon.go#L3468) |
| deregister | [`daemon/daemon.go:826`](../server/internal/daemon/daemon.go#L826) |
| register | [`daemon/daemon.go:927`](../server/internal/daemon/daemon.go#L927) |
| api/client | [`daemon/client.go`](../server/internal/daemon/client.go) |

### Cloud Runtime

| 逻辑 | 文件:行 |
|---|---|
| proxy handler | [`handler/cloud_runtime.go`](../server/internal/handler/cloud_runtime.go) |
| fleet client | [`cloudruntime/client.go`](../server/internal/cloudruntime/client.go) |
| 路由注册 | [`cmd/server/router.go:722-747`](../server/cmd/server/router.go#L722) |

### 路由总表

全部 daemon API 端点定义在 [`router.go:722-747`](../server/cmd/server/router.go#L722)：

| 端点 | 用途 |
|---|---|
| `POST /api/daemon/register` | 注册 runtime |
| `POST /api/daemon/deregister` | 注销 runtime |
| `POST /api/daemon/heartbeat` | HTTP 心跳 |
| `GET /api/daemon/ws` | WS 唤醒 + 心跳 |
| `POST .../tasks/claim` | 原子认领 |
| `POST .../prepare-lease` | 准备期续租 |
| `POST .../skill-bundles/resolve` | 解析 skills |
| `GET .../tasks/pending` | 轮询（WS 断线兜底） |
| `POST .../tasks/{id}/start` | 标记 running |
| `POST .../tasks/{id}/progress` | 进度 |
| `POST .../tasks/{id}/messages` | 消息流 |
| `POST .../tasks/{id}/complete` | 完成 |
| `POST .../tasks/{id}/fail` | 失败 |
| `POST .../tasks/{id}/usage` | 用量 |
