# dimcli.go 核心代码解析

## 文件概述

`dimcli.go` 实现了一个 ACP (Agent Client Protocol) 后端，通过启动 `dim` CLI 进程并通过 stdin/stdout 进行 JSON-RPC 2.0 通信来驱动 AI Agent。

## 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│                     multica-agent-sdk                       │
│                          │                                  │
│                          ▼                                  │
│              ┌──────────────────────┐                       │
│              │   dimcliBackend      │                       │
│              │   Execute()          │                       │
│              └──────────┬───────────┘                       │
│                         │                                   │
│                         ▼                                   │
│    ┌────────────────────────────────────────────┐           │
│    │            dim acp 进程                     │           │
│    │  ┌─────────────────────────────────────┐    │           │
│    │  │         ACP JSON-RPC 2.0            │    │           │
│    │  │  stdin ◄──────────────────── stdout │    │           │
│    │  └─────────────────────────────────────┘    │           │
│    └─────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

## 核心数据结构

### dimcliBlockedArgs (第 21-24 行)

```go
var dimcliBlockedArgs = map[string]blockedArgMode{
    "acp":         blockedStandalone,  // 禁止覆盖协议子命令
    "--approvals": blockedWithValue,   // 禁止覆盖自动审批参数
}
```

**作用**：保护关键参数不被用户自定义 `custom_args` 覆盖。`acp` 是 ACP 传输协议子命令，`--approvals` 用于自动审批工具调用。

### dimcliMessageStream (第 50-77 行)

```go
type dimcliMessageStream struct {
    ch     chan Message    // 消息缓冲通道
    mu     sync.Mutex     // 保护 closed 标志
    closed bool           // 是否已关闭
}
```

**作用**：序列化消息发送，防止并发写入和重复关闭 channel。

### dimcliBackend 结构体 (第 41-43 行)

```go
type dimcliBackend struct {
    cfg Config  // 配置：可执行文件路径、日志、环境变量等
}
```

**作用**：实现 `Backend` 接口的执行器。

---

## 核心流程：Execute() 方法 (第 79-418 行)

### 第一步：准备执行环境 (第 80-113 行)

```go
// 1. 确定 dim 可执行文件路径
execPath := b.cfg.ExecutablePath  // 默认 "dim"
exec.LookPath(execPath)           // 验证可执行文件存在

// 2. 构建 MCP 服务器配置
mcpServers, err := buildACPMcpServers(opts.McpConfig, b.cfg.Logger)

// 3. 创建超时上下文
runCtx, cancel := runContext(ctx, timeout)

// 4. 构建命令参数
dimcliArgs := []string{
    "acp",           // ACP 协议子命令
    "--approvals", "auto",  // 自动审批所有操作
    // + 用户自定义参数（已过滤 blocked args）
}

// 5. 配置进程
cmd := exec.CommandContext(runCtx, execPath, dimcliArgs...)
cmd.Dir = opts.Cwd      // 工作目录
cmd.Env = buildEnv()    // 环境变量
```

**核心点**：
- 注入 `--approvals auto` 实现无交互的 yolo 模式
- 过滤用户自定义参数中的危险字段

### 第二步：建立进程管道 (第 115-148 行)

```go
// 创建三个管道
stdout, _ := cmd.StdoutPipe()  // 读取 dim 输出
stdin, _  := cmd.StdinPipe()   // 写入 dim 输入
stderr, _ := cmd.StderrPipe()  // 捕获错误流

// 启动进程
cmd.Start()

// 异步复制 stderr 到日志
go func() {
    io.Copy(stderrSink, stderr)
}()
```

**核心点**：
- stdin/stdout 用于 JSON-RPC 通信
- stderr 单独捕获用于错误诊断

### 第三步：初始化 ACP 连接 (第 222-235 行)

```go
// 发送 initialize 请求
initResult, err := c.request(runCtx, "initialize", map[string]any{
    "protocolVersion": 1,
    "clientInfo": map[string]any{
        "name":    "multica-agent-sdk",
        "version": "0.2.0",
    },
    "clientCapabilities": map[string]any{},
})
```

**核心点**：
- 遵循 ACP JSON-RPC 2.0 协议
- 获取服务端能力（是否支持 MCP HTTP 等）

### 第四步：创建或恢复会话 (第 247-294 行)

```go
// 情况 A：恢复已有会话
if opts.ResumeSessionID != "" {
    c.request(runCtx, "session/load", map[string]any{
        "cwd":        cwd,
        "sessionId":  opts.ResumeSessionID,
        "mcpServers": mcpServers,
    })
}
// 情况 B：创建新会话
else {
    result, err := c.request(runCtx, "session/new", map[string]any{
        "cwd":        cwd,
        "mcpServers": mcpServers,
    })
    sessionID = extractACPSessionID(result)
}
```

**核心点**：
- 支持会话恢复（断点续传）
- 从响应中提取 session ID 和当前模型

### 第五步：设置模型（如需要）(第 299-311 行)

```go
// 尝试设置模型（dim 可能不支持，静默继续）
if opts.Model != "" {
    c.request(runCtx, "session/set_model", map[string]any{
        "sessionId": sessionID,
        "modelId":   opts.Model,
    })
}
```

### 第六步：发送 Prompt 并流式处理响应 (第 313-357 行)

```go
// 构造 prompt（可选添加系统提示）
userText := opts.SystemPrompt + "\n\n---\n\n" + prompt

// 发送 prompt
streamingCurrentTurn.Store(true)  // 开启流式门
c.request(runCtx, "session/prompt", map[string]any{
    "sessionId": sessionID,
    "prompt": []map[string]any{
        {"type": "text", "text": userText},
    },
})

// 异步接收消息
onMessage: func(msg Message) {
    if msg.Type == MessageText {
        output.WriteString(msg.Content)  // 累积输出
    }
    msgStream.send(msg)  // 发送到消息流
}
```

**核心点**：
- `streamingCurrentTurn` 门控确保只在当前轮次处理消息
- 消息实时发送到 channel，消费者可实时看到

### 第七步：优雅关闭与资源清理 (第 362-384 行)

```go
// 1. 关闭 stdin（让 dim 知道不再发送）
stdin.Close()

// 2. 取消上下文
cancel()

// 3. 等待 stdout reader 完成（有 2 秒宽限期）
drainCtx, _ := context.WithTimeout(context.Background(), dimcliReaderDrainGrace)
select {
case <-readerDone:
case <-drainCtx.Done():
}

// 4. 关闭消息流
streamingCurrentTurn.Store(false)  // 先关闸门
msgStream.close()                   // 再关闭 channel
```

**核心点**：
- 给 dim 进程留出时间flush剩余输出
- 门控先关闭，防止关闭后的 late message 触发 panic

### 第八步：错误提升与结果返回 (第 390-418 行)

```go
// 检查 stderr 或输出中的终端错误
finalStatus, finalError = promoteACPResultOnProviderError(...)

// 构建结果
resCh <- Result{
    Status:     finalStatus,      // completed / failed / timeout / aborted
    Output:     finalOutput,      // 累积的文本输出
    Error:      finalError,       // 错误信息
    DurationMs: duration,         // 执行时长
    SessionID:  sessionID,        // 会话 ID（可用于恢复）
    Usage:      usageMap,         // Token 使用统计
}
```

**返回结构**：
```go
&Session{
    Messages: msgStream.ch,  // 实时消息流
    Result:   resCh,         // 最终结果
}
```

---

## 消息流架构

```
dim 进程 stdout
       │
       ▼
┌──────────────────┐
│  bufio.Scanner   │  逐行读取
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   handleLine()   │  JSON-RPC 解析
└────────┬─────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
 onMessage  onPromptDone
    │         │
    ▼         ▼
msgStream   promptDone
 .send()     channel
    │
    ▼
消费者 ← chan Message (256 buffer)
```

---

## ACP 通信机制详解

### 角色分配：谁是 Client，谁是 Server？

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              本地机器                                    │
│                                                                         │
│  ┌──────────────────────────┐          ┌──────────────────────────────┐ │
│  │  multica 后端 (Go SDK)   │          │      dim 进程                │ │
│  │                          │          │                              │ │
│  │  hermesClient 结构体     │          │  实现 ACP Server             │ │
│  │  - request() 发送请求    │  stdin   │  - 接收 JSON-RPC 请求        │ │
│  │  - handleLine() 解析响应 │ ───────► │  - 返回响应                  │ │
│  │  - pending[] 等待响应    │          │  - 主动发送 notifications    │ │
│  │                          │          │                              │ │
│  │  ← ACP Client (发起方) ← │ stdout   │  ← ACP Server (响应方) ←     │ │
│  └──────────────────────────┘          └──────────────────────────────┘ │
│                                                                         │
│  守护进程 (multica-daemon)：管理 runtime、任务调度、WebSocket 上报        │
└─────────────────────────────────────────────────────────────────────────┘
```

| 角色 | 组件 | 职责 |
|------|------|------|
| **ACP Client** | `hermesClient` (Go SDK) | 主动发起请求（initialize, session/new, session/prompt） |
| **ACP Server** | `dim acp` 进程 | 被动响应请求，**主动推送** notifications（工具调用、token 统计） |
| **守护进程** | `multica-daemon` | 管理本地 runtime、调度任务、通过 WebSocket 与云端通信 |

### ACP JSON-RPC 2.0 通信流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         完整 ACP 对话流程                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1️⃣  Client ──► Server: initialize                                      │
│      {"jsonrpc":"2.0","id":1,"method":"initialize","params":{...}}     │
│                         │                                               │
│                         ▼                                               │
│      ◄── Server: Response                                               │
│      {"jsonrpc":"2.0","id":1,"result":{"capabilities":{...}}}          │
│                                                                         │
│  2️⃣  Client ──► Server: session/new                                     │
│      {"jsonrpc":"2.0","id":2,"method":"session/new","params":{...}}    │
│                         │                                               │
│                         ▼                                               │
│      ◄── Server: Response (带 sessionId)                                │
│      {"jsonrpc":"2.0","id":2,"result":{"sessionId":"xxx","modelId":...}}
│                                                                         │
│  3️⃣  Client ──► Server: session/prompt                                  │
│      {"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{...}}  │
│                         │                                               │
│                         ▼                                               │
│      ◄── Server: Notification (流式 token)                              │
│      {"jsonrpc":"2.0","method":"text_delta","params":{"delta":"Hello"}}│
│                                                                         │
│      ◄── Server: Notification (工具调用)                                 │
│      {"jsonrpc":"2.0","method":"tool_call","params":{...}}             │
│                                                                         │
│      ◄── Server: Response (最终结果)                                     │
│      {"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn",...}}   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 三种消息类型

| 类型 | 格式 | 方向 | 示例 |
|------|------|------|------|
| **Request** | `{"jsonrpc":"2.0","id":N,"method":"xxx","params":{}}` | Client → Server | `initialize`, `session/new` |
| **Response** | `{"jsonrpc":"2.0","id":N,"result":{}}` 或 `{"error":{}}` | Server → Client | 对请求的响应 |
| **Notification** | `{"jsonrpc":"2.0","method":"xxx","params":{}}` (无 id) | Server → Client | `text_delta`, `tool_call` |

### hermesClient 请求-响应配对机制

```go
// 第 502-540 行：request() 方法
func (c *hermesClient) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
    c.mu.Lock()
    id := c.nextID          // 分配唯一 ID
    c.nextID++
    pr := &pendingRPC{ch: make(chan rpcResult, 1), method: method}
    c.pending[id] = pr      // 注册到 pending map
    c.mu.Unlock()

    // 发送请求
    msg := map[string]any{
        "jsonrpc": "2.0",
        "id":      id,
        "method":  method,
        "params":  params,
    }
    data, _ := json.Marshal(msg)
    data = append(data, '\n')
    c.writeLine(data)       // 写入 stdin

    // 阻塞等待响应
    select {
    case res := <-pr.ch:    // 等待对应 ID 的响应
        return res.result, res.err
    case <-ctx.Done():      // 或超时/取消
        return nil, ctx.Err()
    }
}
```

```
pending[] map 机制：

    发送请求时                          收到响应时
    ─────────                          ─────────
    id=5 → pending[5] = pr             id=5 匹配
         │                                   │
         ▼                                   ▼
    ┌─────────┐                        ┌─────────┐
    │  wait   │ ◄── pr.ch <- result    │ resolve │
    └─────────┘                        └─────────┘
         │                                   │
         └────────────► request() 返回 ◄─────┘
```

### 双向通信：Server 主动请求 Client

```
Server (dim) ──────────────────────► Client (SDK)
      │
      │  session/request_permission
      │  (需要用户审批的工具调用)
      │
      ▼
Client 回复：
{"jsonrpc":"2.0","id":X,"result":{"outcome":{"outcome":"selected","optionId":"approve_for_session"}}}
```

在 `dimcli.go` 中，通过 `--approvals auto` 参数让 dim 自动审批，无需用户交互。

---

## 守护进程 (multica-daemon) 角色

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           系统架构全貌                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────┐       │
│   │                     云端服务器 (cloud)                       │       │
│   │                                                             │       │
│   │   - Web 服务/API                                            │       │
│   │   - 数据库                                                  │       │
│   │   - 任务调度                                                │       │
│   └──────────────────────────┬──────────────────────────────────┘       │
│                              │ WebSocket                                │
│                              ▼                                          │
│   ┌─────────────────────────────────────────────────────────────┐       │
│   │              multica-daemon (本地守护进程)                   │       │
│   │                                                             │       │
│   │   - 启动/停止 agent 进程                                    │       │
│   │   - 管理 runtime 和工作目录                                 │       │
│   │   - 接收云端任务并分配给 agent 进程                         │       │
│   │   - 通过 WebSocket 上报 agent 执行结果到云端                │       │
│   │   - 本地 HTTP 健康检查端点 (:18792/health)                  │       │
│   └──────────────────────────┬──────────────────────────────────┘       │
│                              │                                          │
│              ┌───────────────┼───────────────┐                         │
│              │               │               │                         │
│              ▼               ▼               ▼                         │
│   ┌──────────────────┐ ┌──────────────┐ ┌──────────────┐               │
│   │    dim 进程      │ │ hermes 进程  │ │其他 CLI Agent│               │
│   │                  │ │              │ │              │               │
│   │ ACP Client ←──→  │ │  ACP Client  │ │              │               │
│   │   (stdin/stdout) │ │              │ │              │               │
│   └──────────────────┘ └──────────────┘ └──────────────┘               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 守护进程职责

| 职责 | 说明 |
|------|------|
| **进程管理** | spawn/kill agent CLI 进程（dim/hermes 等） |
| **任务分发** | 从云端 WebSocket 接收任务，调度到对应的 agent 后端 |
| **状态上报** | 通过 WebSocket 将 agent 的执行进度/结果上报给云端 |
| **健康检查** | 提供 `localhost:18792/health` HTTP 端点 |
| **仓库管理** | 预下载 Git 仓库到本地缓存 (`/repo/checkout`) |
| **认证管理** | 处理 daemon token、PAT 等认证信息 |

### 数据流向

```
云端任务创建
    │
    ▼ WebSocket
daemon receives task
    │
    ▼
daemon selects backend (dimcli/hermes/etc.)
    │
    ▼ exec.CommandContext (spawn 进程)
agent CLI process starts
    │
    ├──► stdin: JSON-RPC requests (initialize, session/new, session/prompt)
    │
    ◄── stdout: JSON-RPC responses + notifications (tool_call, text_delta)
    │
    ▼
daemon streams messages back via WebSocket
    │
    ▼
cloud UI receives real-time updates
```

---

## 关键设计模式

| 模式 | 位置 | 说明 |
|------|------|------|
| **门控模式** | `streamingCurrentTurn` | 防止历史 replay 污染当前输出 |
| **序列化发送** | `dimcliMessageStream` | mutex + channel 确保线程安全 |
| **优雅关闭** | drain grace period | 给进程留时间 flush 输出 |
| **错误提升** | `promoteACPResultOnProviderError` | 把 provider 错误转为我方错误 |
| **静默降级** | set_model 失败 | 继续执行而非失败 |
