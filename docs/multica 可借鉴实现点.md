# Multica 可借鉴实现点

> 从 Multica 源码中提炼的工程实践，按主题分组。每条标注真实代码位置，可直接对照。
> 适用场景：任何需要"调度远端执行单元 + 实时回传 + 多种 CLI 适配"的系统。

---

## 目录

1. [分布式任务派发](#1-分布式任务派发)
2. [实时通信](#2-实时通信)
3. [Agent 执行抽象](#3-agent-执行抽象)
4. [生命周期与韧性](#4-生命周期与韧性)

---

## 1. 分布式任务派发

### 1.1 WS 唤醒 + HTTP claim 双轨

**场景**：多个执行单元（daemon / 云 node）竞争同一批任务，需要既低延迟又不重复执行。

**Multica 做法**：WS 只发"有活"提示，真正认领走 HTTP `POST claim` 原子抢占。hub 注释直言消息是 best-effort hint。

```go
// daemonws/hub.go:137 — "Messages are best-effort wakeup hints; the daemon
// still uses HTTP claim for correctness."
func (h *Hub) NotifyTaskAvailable(runtimeID, taskID string) { ... }

// daemon.go:2652 — 收到唤醒后才来 claim；claim 失败就回睡眠
task, err := d.client.ClaimTask(pollerCtx, rid)
```

**借鉴点**：把"通知"和"认领"拆成两个通道。通知可以丢、可以重复、可以广播；认领必须原子、必须 HTTP、必须幂等。这样 WS 断线不影响正确性，只是延迟变高。

### 1.2 prepare-lease 抢占保护期

**场景**：claim 成功后到 agent 真正 start 之间有慢启动（克隆 repo、装 skills bundle），期间任务可能被另一个执行单元抢走。

**Multica 做法**：claim 后立刻起一个 lease 续期器，在 start 之前周期性 `POST prepare-lease` 续命。

```go
// daemon.go:3504
stopPrepareLease := d.startTaskPrepareLeaseExtender(ctx, task, taskLog)
defer stopPrepareLease()
```

**借鉴点**：两阶段认领（lease → start）中间的慢操作需要租约保护，否则第一个执行单元还在准备、第二个已经把 task 抢走，造成重复执行或资源浪费。

### 1.3 孤儿 task 回收

**场景**：执行单元崩溃重启，之前 claim 了但没跑完的 task 会永久卡在 running。

**Multica 做法**：daemon 启动时主动调 `recover-orphans`，把本 runtime 名下的非终态 task 标回 pending。

```go
// client.go:321
func (c *Client) RecoverOrphans(ctx context.Context, runtimeID string) error {
    return c.postJSON(ctx, fmt.Sprintf("/api/daemon/runtimes/%s/recover-orphans", runtimeID), ...)
}
```

**借鉴点**：任何有"认领"语义的系统都需要孤儿回收。执行单元重启是必然事件，不回收就会出现幽灵任务。回收应按 runtime 维度，只动自己名下的，避免误改别人的。

---

## 2. 实时通信

### 2.1 两路 WS 职责隔离

**场景**：浏览器要看结果，执行单元要拿活，两者订阅维度、鉴权方式、投递方向都不同。

**Multica 做法**：两条完全独立的 WS 通道，互不耦合。

| | `/ws` (realtime.Hub) | `/api/daemon/ws` (daemonws.Hub) |
|---|---|---|
| 连接方 | 浏览器 | daemon / 云 node |
| 索引维度 | user × workspace × scope | runtime ID × workspace |
| 鉴权 | 会话 cookie / PAT | daemon PAT + runtime IDs |
| 投递方向 | server → 浏览器（广播） | server → daemon（唤醒） |

**借鉴点**：不要用一条 WS 干两件事。不同消费者的鉴权模型、订阅粒度、投递语义差异大，混在一起会迫使鉴权逻辑互相妥协（比如 hub 注释提到 cookie CSWSH 风险不适用于 daemon 侧）。

### 2.2 Redis Stream 分片 relay（多实例广播）

**场景**：多 server 实例部署，任一实例发出的帧所有实例的订阅者都要收到；pub/sub 丢消息不可接受。

**Multica 做法**：固定 8 个 Redis Stream shard，发布按 `hash(scopeID) % shards` 选 shard；每个 Pod 起所有 shard 的阻塞 reader，启动时回看 `ReplayGrace` 窗口补播漏帧。

```go
// sharded_stream_relay.go
const (
    defaultShardedRelayShards       = 8
    defaultShardedRelayStreamMaxLen = 100000
    defaultShardedRelayReplayGrace  = 5 * time.Minute
)
```

**借鉴点**：多实例实时广播用 Redis Stream + 固定分片，比 pub/sub 可靠（有 MAXLEN 上界、有回看窗口）、比每 scope 一条 stream 开销低（8 条 vs 数千条）。前提是下游消费必须幂等——靠 event-id 去重（见下条）。

### 2.3 event-id 有界 LRU 去重

**场景**：relay 回看补播 + 多路径投递同一帧，订阅端会收到重复事件。

**Multica 做法**：每个连接维护一个 128 容量的 LRU 去重集，event-id 命中就跳过。

```go
// daemonws/hub.go:97
const eventDedupCapacity = 128

func (c *client) markSeen(eventID string) bool {
    if eventID == "" { return true }       // 空 id 不去重，直接放行
    // ... LRU lookup, capacity eviction
}
```

**借鉴点**：分布式去重不靠全局锁，靠端侧有界缓存 + 幂等消费。128 条足够覆盖 relay 重播窗口，超出容量的旧 id 自然被淘汰——因为旧帧不会再被补播。空 event-id 是逃生口：需要强投递时跳过去重。

### 2.4 WS 优先 + HTTP 兜底降级

**场景**：WS 可能断线、丢 ack，但心跳和任务唤醒不能因此停摆。

**Multica 做法**：心跳和唤醒都先走 WS；WS 心跳无 ack 时 daemon 用 fallback 计时器自动切 HTTP；唤醒通道断了改轮询 `POST /tasks/pending`。

```go
// daemon.go:803-806 — WS 和 HTTP 循环并行跑, 互为降级
go d.taskWakeupLoop(ctx, taskWakeups)   // WS 唤醒
go d.heartbeatLoop(ctx)                  // WS 心跳 + HTTP fallback
```

**借鉴点**：实时通道和兜底通道不是二选一，而是**同时运行**。WS 正常时 HTTP 循环空转（成本低），WS 断线时 HTTP 自然接管。不要做"WS 断了才启动 HTTP"的切换逻辑——切换窗口会丢事件。

---

## 3. Agent 执行抽象

### 3.1 Backend 接口归一

**场景**：16 种编码 CLI（claude / codex / copilot / cursor …），参数、协议、输出格式各异，但调度链不该为每种 CLI 写分支。

**Multica 做法**：一个 `Backend` 接口，`Execute` 返回 `Session{Messages channel, Result channel}`；`agent.New(type)` 按类型选后端。

```go
// agent.go:16
type Backend interface {
    Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error)
}

// Session：事件流 + 终态，所有后端输出归一成这两种 channel
type Session struct {
    Messages <-chan Message  // text/thinking/tool-use/tool-result/...
    Result   <-chan Result   // 唯一终态
}
```

**借鉴点**：异构执行器的适配层应该收口成"流式事件 + 单终态"两个 channel。上游调度只读 channel，不关心后端是 spawn 子进程还是 RPC。加新后端只实现接口，调度链 / 进度回报 / usage 统计全部复用。

### 3.2 白名单与 DB 约束同源

**场景**：运行时支持哪些 CLI 类型，代码白名单和数据库 CHECK 约束必须一致，否则用户能存进 DB 但代码不认。

**Multica 做法**：`SupportedTypes` 切片与 migration 的 `protocol_family` CHECK 约束手工同步，注释明列每次迁移 widening 记录。

```go
// agent.go:152 — "It MUST stay in lockstep with the runtime_profile.protocol_family
// CHECK constraint (migration 120, widened by migration 134 to add qoder
// and migration 136 to add traecli)"
var SupportedTypes = []string{"claude", "codebuddy", "codex", ...}
```

**借鉴点**：枚举值跨代码和 schema 两处定义时，用注释记录同步关系 + 每次变更的迁移号。比代码生成轻量，比"相信不会漂移"可靠。加类型时两边同时改，注释更新迁移历史。

### 3.3 custom runtime 只换二进制，不换后端

**场景**：用户想用自己的 claude 二进制路径或自定义参数，但不应让调度链为此分支。

**Multica 做法**：custom profile 只覆盖 `ExecutablePath + fixedArgs`，`protocol_family` 仍决定走哪个 backend 的参数注入逻辑。后端完整复用。

```go
// daemon.go:3491 — custom profile 覆盖 path, 但 provider/backend 不变
if customSpec, isCustom := d.customProfileLaunchForRuntime(task.RuntimeID); isCustom {
    entry.Path = customSpec.path           // 只换二进制
    profileFixedArgs = customSpec.fixedArgs // 只加参数
    ok = true
}
```

**借鉴点**：可配置点应该分层——"换执行器"和"换协议"是两个维度。protocol_family 决定参数注入策略（怎么传 mcp-config、怎么传 thinking-level），profile 决定具体调哪个二进制。两维正交，用户可自由组合而不触发代码分支。

---

## 4. 生命周期与韧性

### 4.1 idle watchdog 语义停活跃度（非墙钟超时）

**场景**：agent 跑很久但一直在产出事件，不该因为墙钟超时被杀；但卡死不产出时必须 kill。

**Multica 做法**：`Timeout <= 0` 时**不设 deadline**，完全靠 `SemanticInactivityTimeout` 判断"多久没新事件"才 kill。

```go
// agent.go:72 — 零超时 = 无 deadline, 只靠 inactivity watchdog
func runContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
    if timeout > 0 {
        return context.WithTimeout(ctx, timeout)  // 硬墙钟
    }
    return context.WithCancel(ctx)                 // 只靠 watchdog
}
```

**借鉴点**：长任务超时策略要看"活跃度"而非"总时长"。一个跑了 2 小时但每秒都在输出的事件流不该被杀；一个跑了 5 分钟但 4 分钟没动静的要杀。把 `timeout` 和 `inactivityTimeout` 拆成两个独立参数，前者可选、后者必选。

### 4.2 token 续期独立循环

**场景**：daemon 是长生命周期进程，PAT 90 天过期会让所有在跑 task 突然 401。

**Multica 做法**：独立的 `tokenRenewalLoop`，到期前自动续期，不依赖任何 task 循环触发。

```go
// daemon.go:806 + 1661
go d.tokenRenewalLoop(ctx)
```

**借鉴点**：凭证续期是**基础设施关注点**，不该耦合到业务循环。独立 goroutine + 独立周期，业务循环只管调 API，401 时由续期循环兜底。续期失败要冒泡成"re-login required"，不要默默重试掩盖问题。

### 4.3 shutdown 优雅 deregister

**场景**：daemon 被 kill 时，server 侧 runtime 还显示在线，要等 150s 心跳过期才标 offline。

**Multica 做法**：`Run` defer 里用**独立 context**（因为主 ctx 已 cancel）调 deregister，5s 超时。

```go
// daemon.go:796
defer d.deregisterRuntimes()

// daemon.go:835 — 注意用 context.Background(), 不是已取消的 ctx
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
d.client.Deregister(ctx, runtimeIDs)
```

**借鉴点**：优雅退出要在 defer 里用 **fresh context**，不能用已 cancel 的主 ctx——否则 deregister 请求还没发出就被 ctx 拒绝。加短超时防止 kill -9 场景卡住。server 侧虽然有心跳过期兜底，但主动 deregister 能把 offline 延迟从 150s 降到 0。

### 4.4 node-scoped 凭证隔离

**场景**：云端 node 需要回连 server，但不应持有用户长期凭证——单 node 被攻破会波及用户所有数据。

**Multica 做法**：fleet 签发 node-scoped `mcn_` PAT，server **不转发**用户 `mul_` PAT 给 fleet。

```go
// cloud_runtime.go — 注释: "We no longer forward the caller's mul_ PAT —
// Fleet doesn't need it, and propagating a long-lived user PAT into a remote
// machine widened the blast radius of any node compromise."
```

**借鉴点**：远端执行单元的凭证要**最小化 blast radius**。node 用独立 PAT 回连，该 PAT 只能回连、不能调用户 API；用户撤销 node 时该 PAT 失效，不影响用户其他凭证。宁可多一套 PAT 签发逻辑，也不要把用户主凭证扩散到不可控的远端机器上。

---

## 速查表

| # | 实践 | 核心思想 | 代码位置 |
|---|---|---|---|
| 1.1 | WS 唤醒 + HTTP claim | 通知可丢，认领必原子 | `daemonws/hub.go:137`, `daemon.go:2652` |
| 1.2 | prepare-lease | 慢启动阶段租约保护 | `daemon.go:3504` |
| 1.3 | 孤儿回收 | 重启后回收非终态 task | `client.go:321` |
| 2.1 | 两路 WS 隔离 | 按消费者拆通道 | `realtime/hub.go`, `daemonws/hub.go` |
| 2.2 | Stream 分片 relay | 多实例可靠广播 | `sharded_stream_relay.go` |
| 2.3 | event-id LRU 去重 | 端侧幂等消费 | `daemonws/hub.go:99` |
| 2.4 | WS 优先 HTTP 兜底 | 降级不切换, 同时跑 | `daemon.go:803` |
| 3.1 | Backend 接口归一 | 流式事件 + 单终态 | `agent.go:16` |
| 3.2 | 白名单同源 | 枚举跨代码/schema 同步 | `agent.go:152` |
| 3.3 | custom 只换二进制 | 配置维度正交 | `daemon.go:3491` |
| 4.1 | idle watchdog | 活跃度超时非墙钟 | `agent.go:72` |
| 4.2 | token 续期独立循环 | 凭证管理解耦业务 | `daemon.go:806` |
| 4.3 | shutdown fresh ctx | 退出时用新 context | `daemon.go:796` |
| 4.4 | node-scoped PAT | 凭证 blast radius 最小 | `cloud_runtime.go` |
