# Multica 运行架构总览

> 本文档用 Mermaid 图描述 Multica 的整体运行架构，重点放在**与服务器各端的交互**和 **Agent Runtime 的调用细节**。配置项、部署细节一律略过。
>
> 受众：想快速理解"前端怎么到后端、task 怎么派到 daemon、agent 怎么真正跑起来"的工程师/新同事。
>
> 图中代码引用均指向真实文件，可点击跳转。

---

## 目录

1. [整体运行架构（主图）](#1-整体运行架构主图)
2. [两路 WebSocket 分工](#2-两路-websocket-分工)
3. [Realtime 多实例 Redis Relay](#3-realtime-多实例-redis-relay)
4. [Agent Runtime 单次任务调用时序](#4-agent-runtime-单次任务调用时序)
5. [Daemon 主循环子阶段](#5-daemon-主循环子阶段)
6. [Agent Backend 统一抽象](#6-agent-backend-统一抽象)
7. [Cloud Runtime 开通与回连](#7-cloud-runtime-开通与回连)
8. [关键交互点速查](#8-关键交互点速查)

---

## 1. 整体运行架构（主图）

```mermaid
flowchart TB
  subgraph Clients["客户端"]
    Web["apps/web<br/>Next.js App Router"]
    Desk["apps/desktop<br/>Electron"]
    Mob["apps/mobile<br/>Expo / React Native"]
    CLI["multica CLI<br/>本机 daemon 宿主"]
  end

  subgraph Server["Go Server (Chi)"]
    direction TB
    HTTP["HTTP REST API<br/>/api/issues·agents·tasks·skills·runtimes·integrations·billing"]
    Trig["issueTrigger + TaskService<br/>assignee 变更 → enqueue task"]
    Sched["Scheduler<br/>pg-cron: autopilot·task-usage"]
    Bus["events.Bus<br/>内部事件总线"]
    RTHub["Realtime WS /ws<br/>realtime.Hub → 浏览器订阅"]
    DHub["Daemon WS /api/daemon/ws<br/>daemonws.Hub → 按 runtime 投递"]
    CProxy["Cloud Runtime Proxy<br/>/api/cloud-runtime/*"]
    DB[("PostgreSQL + pgvector")]
    Redis[("Redis<br/>sharded stream relay")]
  end

  subgraph LocalRT["本地 Agent Runtime — multica daemon"]
    direction TB
    Loop["Daemon.Run 主循环<br/>register·heartbeat·wakeup·token续期"]
    Claim["ClaimTask<br/>POST /tasks/claim"]
    RunTask["runTask<br/>组装 prompt / env / skills / repo / local-dir"]
    Exec["agent.Backend.Execute<br/>Backend.Execute(ctx, prompt, opts)"]
    Watch["idle watchdog<br/>+ GC / 孤儿回收"]
    Report["回报 progress·messages<br/>complete·fail·usage·session"]
  end

  subgraph CloudRT["Cloud Runtime Fleet (multica-cloud)"]
    Fleet["Fleet API<br/>/api/v1/nodes<br/>provision·start·stop·delete"]
    Node["云端 node (EC2)<br/>node-scoped mcn_ PAT<br/>内嵌 daemon"]
  end

  subgraph CLIs["AI Coding CLI（子进程）"]
    Claude["claude"]
    Codex["codex"]
    Copilot["copilot"]
    Cursor["cursor"]
    Others["dimcli·hermes·kimi·kiro<br/>openclaw·opencode·pi·qoder·traecli<br/>codebuddy·antigravity"]
  end

  %% 客户端 → server
  Web -->|"HTTPS REST"| HTTP
  Web -->|"WS /ws 订阅 task/chat scope"| RTHub
  Desk -->|"HTTPS REST"| HTTP
  Desk -->|"WS /ws"| RTHub
  Mob -->|"HTTPS REST"| HTTP
  CLI -->|"OAuth/PAT 鉴权"| HTTP
  CLI -.->|"启动/管理"| Loop

  %% server 内部
  HTTP <--> DB
  HTTP <--> Bus
  Sched <--> DB
  RTHub <--> Bus
  RTHub -->|"relay"| Redis
  DHub -->|"relay"| Redis

  %% 派发链
  HTTP -->|"assign agent → trigger"| Trig
  Trig -->|"EnqueueTaskForIssue<br/>写 task(runtime_id)"| Bus
  Bus -->|"NotifyTaskAvailable"| DHub
  Bus -->|"task 事件 patch/invalidate"| RTHub

  %% daemon ↔ server
  Loop -->|"POST /daemon/register"| HTTP
  Loop -.->|"WS 心跳 / 唤醒"| DHub
  Loop -->|"POST /daemon/heartbeat (fallback)"| HTTP
  Claim -->|"claim HTTP"| HTTP
  Report -->|"progress/complete/fail/usage"| HTTP
  HTTP --> Bus

  %% daemon 内部
  Loop --> Claim --> RunTask --> Exec
  Exec -->|"stdout / 事件流"| Watch
  Exec -->|"spawn + --mcp-config"| Claude
  Exec -->|"spawn"| Codex
  Exec -->|"spawn"| Copilot
  Exec -->|"spawn"| Cursor
  Exec -->|"spawn"| Others
  RunTask --> Report

  %% cloud runtime
  HTTP -.->|"用户开通云端 node"| CProxy
  CProxy -->|"转发"| Fleet
  Fleet -->|"起 node"| Node
  Node -->|"同 daemon 协议回连<br/>register/claim/heartbeat/task"| HTTP
  Node -.->|"WS"| DHub
  Node -->|"spawn"| CLIs
```

要点：

- **两种 Agent Runtime 形态**对 server 协议透明：本机 `multica daemon` 与云端 node 都走同一套 register / claim / heartbeat / task，差别只在本机还是 EC2。
- **正确性靠 HTTP claim**，不靠 WS。WS 唤醒只是"可能有活"的提示，真正抢占是 `POST /tasks/claim`，保证多 daemon / 多 server 实例下任务不重复执行。
- **`events.Bus` 是中枢**：task 终态、progress、评论变更都进 Bus，再分发给 daemon WS 投递和 realtime 浏览器广播。

---

## 2. 两路 WebSocket 分工

Multica 有两条独立的 WebSocket 通道，职责完全不重叠：

| | 浏览器侧 `/ws` | Daemon 侧 `/api/daemon/ws` |
|---|---|---|
| 实现 | [`realtime.Hub`](../server/internal/realtime/hub.go) | [`daemonws.Hub`](../server/internal/daemonws/hub.go) |
| 连接方 | Web / Desktop | 本机 daemon / 云端 node daemon |
| 鉴权 | 用户会话 cookie / PAT | daemon PAT + runtime IDs |
| 索引维度 | user × workspace × scope(task/chat) | runtime ID × workspace |
| 主要帧 | task 进度、issue/评论变更、chat 消息 | `daemon:task_available`、`daemon:runtime_profiles_changed`、心跳 |
| 投递方向 | server → 浏览器（订阅广播） | server → daemon（唤醒提示） |

```mermaid
flowchart LR
  subgraph Browser["浏览器"]
    BTab["Web/Desktop tab"]
  end
  subgraph Daemon["本机 daemon"]
    DProc["Daemon.Run"]
  end
  subgraph Server["Server"]
    direction TB
    RTHub["realtime.Hub<br/>/ws"]
    DHub["daemonws.Hub<br/>/api/daemon/ws"]
    Bus["events.Bus"]
    HTTP["HTTP REST + Claim"]
  end

  BTab -->|"WS /ws 订阅 scope"| RTHub
  RTHub -->|"广播 task/issue/chat"| BTab
  DProc -->|"WS 心跳 + runtime 注册"| DHub
  DProc -->|"HTTP claim/progress/complete"| HTTP
  DHub -->|"daemon:task_available 唤醒"| DProc
  Bus --> RTHub
  Bus --> DHub
  HTTP --> Bus
```

核心约束：daemon WS 下发的是**唤醒提示**，daemon 收到后仍要打 HTTP `claim` 才拿得到任务；这样即使 WS 丢帧、多 daemon 抢同一任务，最终只有一个能 claim 成功。

---

## 3. Realtime 多实例 Redis Relay

单机部署时 realtime 直接进程内广播；多实例部署时引入 Redis Stream 分片 relay，保证任一 server 实例发出的帧所有实例的订阅者都能收到。

```mermaid
flowchart TB
  subgraph Pods["Server 实例 ×N"]
    P1["Pod A<br/>realtime.Hub"]
    P2["Pod B<br/>realtime.Hub"]
    P3["Pod C<br/>realtime.Hub"]
  end

  subgraph Redis["Redis"]
    direction LR
    S0["shard 0<br/>ws:relay:shard:0"]
    S1["shard 1<br/>ws:relay:shard:1"]
    S2["...<br/>shard N"]
    S7["shard 7<br/>ws:relay:shard:7"]
  end

  Clients["浏览器订阅者<br/>散布在各 Pod"]

  P1 -->|"XADD (按 hash 选 shard)"| S0
  P1 -->|"XADD"| S3["shard 3"]
  P2 -->|"XADD"| S1
  P3 -->|"XADD"| S7
  P1 -->|"XREADGROUP 阻塞读<br/>所有 shard, RecallGrace 回看"| S0
  P1 --> S1
  P2 --> S0
  P2 --> S7
  P3 --> S1
  P3 --> S3
  P1 -->|"本地 Hub 广播"| Clients
  P2 -->|"本地 Hub 广播"| Clients
  P3 -->|"本地 Hub 广播"| Clients
```

机制要点（见 [`sharded_stream_relay.go`](../server/internal/realtime/sharded_stream_relay.go) / [`redis_relay.go`](../server/internal/realtime/redis_relay.go)）：

- 固定 8 个 shard，key `ws:relay:shard:N`；发布按 `hash(scopeID) % shards` 选 shard。
- 每个 Pod 起**所有 shard 的阻塞 reader** —— 任意 Pod 发的帧，所有 Pod 都能读到并广播给本地订阅者。
- reader 启动从 `(now - ReplayGrace)` 回看，保证 Pod 重启期间漏掉的帧能补播；stream 有 `MAXLEN` 上界，下游消费必须幂等。
- 本地 Hub 端有 event-id 去重，同一帧不会被重复推给同一订阅者。

---

## 4. Agent Runtime 单次任务调用时序

这是"用户给 issue 指派 agent → agent 真的跑起来 → 结果回到前端"的完整时序，最值得反复对照。

```mermaid
sequenceDiagram
  autonumber
  participant U as 用户 Web/Desktop
  participant S as Server HTTP API
  participant T as issueTrigger + TaskService
  participant E as events.Bus
  participant DW as daemonws.Hub
  participant RT as Realtime /ws → 浏览器
  participant D as 本机 Daemon
  participant B as agent.Backend
  participant CLI as AI CLI 子进程

  U->>S: 改 issue: assignee=agent, status=启动
  S->>T: issueTrigger.dispatchIssueRun
  T->>T: EnqueueTaskForIssue (写 task, 绑定 runtime_id)
  T->>E: task.assigned
  E->>DW: NotifyTaskAvailable(runtimeID, taskID)
  DW->>D: daemon:task_available (WS 唤醒)

  D->>S: POST /api/daemon/runtimes/{rid}/tasks/claim
  S-->>D: 200 Task{prompt, repo, skills, model, mcp, local_dir}
  D->>S: POST /tasks/{id}/prepare-lease (抢占期续租)
  D->>S: POST /tasks/{id}/start (标记 running)
  D->>D: runTask: 解析 repo 克隆 / skills bundle / local-dir / env
  D->>B: Backend.Execute(ctx, prompt, ExecOptions)
  B->>CLI: spawn claude/codex/copilot/cursor/... --mcp-config ...

  loop 流式产出（并行心跳）
    CLI-->>B: text / thinking / tool-use / tool-result 事件
    B-->>D: Message 流（channel）
    D->>S: POST /tasks/{id}/progress {step,total}
    D->>S: POST /tasks/{id}/messages
    S->>E: task.progress / task.message
    E->>RT: 广播到 workspace scope 订阅者
    RT-->>U: WS 推送 → React Query patch 缓存
  end

  D->>D: idle watchdog: 语义停活跃度判定，kill 长静默

  alt 正常完成
    D->>S: POST /tasks/{id}/complete {output, branch, session, workdir}
    D->>S: POST /tasks/{id}/usage {per-model tokens}
    D->>S: POST /tasks/{id}/session (pin resume 指针)
  else 失败/超时/取消
    D->>S: POST /tasks/{id}/fail {err, failureReason}
  end
  S->>E: task.terminal
  E->>RT: 广播 issue 状态/评论变更
  RT-->>U: WS 推送 → 前端 invalidate/patch
```

关键点：

- **唤醒 → claim 是两步**：WS 唤醒只让 daemon 去轮询，`claim` 才原子地把 task 标给这个 runtime。
- **prepare-lease** 是 claim 之后、start 之前的抢占保护期，runTask 启动慢（克隆 repo、装 skills）时靠它续命，防止被另一个 daemon 抢走。
- **流式事件双向出**：CLI 的每条事件既走 daemon 回报（`progress`/`messages`）进 `events.Bus`，又由 realtime `/ws` 推给浏览器，所以前端能看到 agent 在干啥。
- **resume 指针**：`POST /tasks/{id}/session` 在完成时 pin 住 session，下次同 issue 再触发可 `resumeSessionID` 续跑。

---

## 5. Daemon 主循环子阶段

`multica daemon start` 后 [`Daemon.Run`](../server/internal/daemon/daemon.go#L724) 是一个长生命周期循环，由若干子循环并发构成。

```mermaid
flowchart TB
  Start([multica daemon start]) --> Preflight["preflightAuth<br/>续期 PAT + 首次 workspace 同步"]
  Preflight --> Register["registerRuntimesForWorkspace<br/>POST /daemon/register (每 ws)"]
  Register --> Ready([d.ready = true])

  Ready --> LoopA["workspaceSyncLoop<br/>周期拉 /api/workspaces, 新 ws 触发 register"]
  Ready --> LoopB["heartbeatLoop<br/>WS 心跳 + HTTP fallback, 续租 runtime 在线"]
  Ready --> LoopC["tokenRenewalLoop<br/>PAT 到期前续期"]
  Ready --> LoopD["wakeup / reconcile<br/>runTaskWakeupConnection 监听 WS 唤醒"]
  Ready --> LoopE["taskRunner 池<br/>MaxConcurrentTasks 个 slot"]
  Ready --> LoopF["GC / 孤儿回收<br/>recover-orphans, workdir 清理"]
  Ready --> LoopG["auto_update / model-list 报告"]

  LoopD -->|"task_available"| LoopE
  LoopE --> runTask["runTask<br/>claim → prepare-lease → start → Execute → report"]

  subgraph Fallback["WS 不可用时的降级"]
    LoopB -.->|"心跳无 ack"| HBhttp["HTTP POST /daemon/heartbeat"]
    LoopD -.->|"WS 断线"| Poll["定时 POST /tasks/pending 轮询"]
  end
```

设计取舍：

- **WS 优先、HTTP 兜底**：心跳和任务唤醒都优先走 WS，丢 ack 才回退 HTTP，降低 server 负载又不丢正确性。
- **task slot 池**：`MaxConcurrentTasks` 控制并发，每个 slot 独立跑一次 `runTask`；claim 失败（被抢/无任务）就回睡眠等下一轮唤醒。
- **孤儿回收**：daemon 重启或崩溃前正在跑的 task，靠 `recover-orphans` 把它们标记回 pending，避免永久卡死。

---

## 6. Agent Backend 统一抽象

[`server/pkg/agent`](../server/pkg/agent/agent.go) 把 16 种编码 CLI 收敛成一个 `Backend` 接口，`runTask` 只对接这个接口 —— 加新 CLI 不动调度链。

```mermaid
classDiagram
  class Backend {
    <<interface>>
    +Execute(ctx, prompt, opts ExecOptions) Session
  }
  class ExecOptions {
    +Cwd string
    +Model string
    +SystemPrompt string
    +ThreadName string
    +MaxTurns int
    +Timeout Duration
    +SemanticInactivityTimeout Duration
    +ResumeSessionID string
    +ExtraArgs []string
    +CustomArgs []string
    +McpConfig json.RawMessage
    +ThinkingLevel string
    +OpenclawMode string
  }
  class Session {
    +Messages ~chan~Message
    +Result ~chan~Result
  }
  class Message {
    +Type MessageType
    +Content string
    +Tool string
    +CallID string
    +Input map
    +Output string
    +Status string
    +SessionID string
  }
  class Result {
    +Status string
    +Output string
    +Error string
    +DurationMs int64
    +SessionID string
    +Usage map~string,TokenUsage~
  }

  Backend <|.. Claude
  Backend <|.. Codex
  Backend <|.. Copilot
  Backend <|.. Cursor
  Backend <|.. Hermes
  Backend <|.. Dimcli
  Backend <|.. Openclaw
  Backend <|.. Opencode
  Backend <|.. Others["Kimi·Kiro·Pi·Qoder·Traecli·Codebuddy·Antigravity"]

  Backend ..> ExecOptions
  Backend ..> Session
  Session ..> Message
  Session ..> Result
```

- `agent.New(type)` 按 `SupportedTypes` 白名单选具体后端；白名单与 DB `runtime_profile.protocol_family` CHECK 约束同源。
- 自定义 runtime profile 只覆盖 `ExecutablePath + CustomArgs`，protocol_family 仍决定走哪个 backend 的参数注入逻辑。
- 所有后端的输出都归一成 `Message` 事件流 + `Result` 终态，daemon 侧的进度回报 / usage 统计因此与具体 CLI 解耦。

---

## 7. Cloud Runtime 开通与回连

云端 node 不是另一套协议：server 只代理 fleet 管理 node 生命周期，node 起来后自带 daemon 用 node-scoped `mcn_` PAT 回连，对 server 完全透明。

```mermaid
sequenceDiagram
  autonumber
  participant U as 用户
  participant S as Server /api/cloud-runtime/*
  participant F as Fleet API (multica-cloud)
  participant EC2 as 云端 node / EC2
  participant S2 as Server (同上, 回连侧)
  participant DW as daemonws.Hub

  U->>S: POST /cloud-runtime/nodes (开通)
  S->>F: POST /api/v1/nodes (转发, 带 X-User-ID)
  F->>F: 签发 node-scoped mcn_ PAT
  F->>EC2: SSM bootstrap 注入 mcn_ PAT + daemon
  F-->>S: node 元信息
  S-->>U: node 列表 / 状态

  U->>S: POST /cloud-runtime/nodes/start
  S->>F: POST /api/v1/nodes/start
  F->>EC2: 启动实例

  EC2->>S2: POST /api/daemon/register (mcn_ PAT)
  EC2->>S2: WS /api/daemon/ws (注册 runtime)
  S2->>DW: 索引到该 node 的 runtime

  Note over EC2,S2: 之后与本地 daemon 完全同构:<br/>claim → runTask → Execute → report

  DW->>EC2: daemon:task_available 唤醒
  EC2->>S2: POST /tasks/claim
  EC2->>EC2: spawn AI CLI 子进程
  EC2->>S2: progress / complete / usage
```

安全取舍（见 [`cloud_runtime.go`](../server/internal/handler/cloud_runtime.go) / [`cloudruntime/client.go`](../server/internal/cloudruntime/client.go)）：

- server **不转发**用户的 `mul_` PAT 给 fleet，node 用自己独立的 `mcn_` PAT 回连，单 node 被攻破不波及用户长期凭证。
- server 对 fleet 的调用全程带 `X-User-ID` / `X-Request-ID`，并有 op 维度的请求指标（provision / terminate / status / gateway）。
- node 回连后走**完全相同的 daemon 协议**，所以前文时序图对云端 node 同样成立。

---

## 8. 关键交互点速查

| 交互 | 端点 / 通道 | 方向 | 用途 |
|---|---|---|---|
| 浏览器订阅 | `WS /ws` | server → 浏览器 | task/issue/chat 实时推送 |
| Daemon 注册 | `POST /api/daemon/register` | daemon → server | 上报 runtime 列表/模型/skills |
| Daemon 心跳 | `POST /api/daemon/heartbeat` + `WS` | daemon ↔ server | 续租在线态, 兜底降级 |
| 任务唤醒 | `daemon:task_available` (WS) | server → daemon | "可能有活"的提示 |
| 任务抢占 | `POST .../tasks/claim` | daemon → server | 原子认领, 唯一执行者 |
| 进度回报 | `POST .../tasks/progress` `/messages` | daemon → server | 流式状态回到 events.Bus |
| 终态回报 | `POST .../tasks/complete` `/fail` `/usage` | daemon → server | 结束 + token 统计 |
| Cloud 开通 | `POST /api/cloud-runtime/nodes` | server → fleet | 起 EC2 node |
| Cloud 回连 | `POST /api/daemon/register` (mcn_ PAT) | node → server | 回连成普通 daemon |

一句话总结架构：**前端走 REST + `/ws` 看结果，daemon 走 REST + `/api/daemon/ws` 干活，两者由 `events.Bus` + Redis relay 串起来；agent 真正执行是 daemon 用统一 `Backend` 接口 spawn 本机 / 云端 CLI 子进程，云端 node 只是把同一套 daemon 协议搬到了 EC2。**
