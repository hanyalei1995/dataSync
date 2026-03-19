# SSE 同步进度设计

## 目标

在任务详情页实时展示同步进度：已同步行数、剩余行数、总行数、百分比进度条。

## 后端设计

### ProgressEvent 结构

```go
type ProgressEvent struct {
    Phase      string  // "connecting" | "structure" | "data" | "done" | "failed"
    Message    string  // 阶段描述文字
    RowsSynced int64
    TotalRows  int64
    Percent    float64 // 0-100
    ErrorMsg   string  // 仅 failed 时有值
}
```

### Executor 变更

- 新增 `progress sync.Map` 存 `map[taskID] chan ProgressEvent`
- `Run()` 开始时创建带缓冲的 channel（容量 64），结束时关闭
- 阶段推进点：
  - 解析/连接阶段 → 发 `connecting`
  - `SyncStructure` 前后 → 发 `structure`
  - `SyncData` 的 `OnProgress` 回调 → 发 `data`（含行数）
  - goroutine 结束 → 发 `done` 或 `failed`
- 新增 `Subscribe(taskID) (<-chan ProgressEvent, bool)` 方法供 handler 使用

### 新 SSE 端点

`GET /api/tasks/:id/progress/stream`

- 设置响应头：`Content-Type: text/event-stream`、`Cache-Control: no-cache`、`X-Accel-Buffering: no`
- Subscribe channel，循环读取事件写入 `data: {json}\n\n`
- 任务未运行时立即发 `done` 事件结束流
- 监听 `c.Request.Context().Done()` 处理客户端断开

## 前端设计

### 进度面板（任务详情页）

任务状态为 `running` 时显示：

```
[ 数据同步中... ]
[████████████░░░░░░░░] 62%
已同步: 6,200 行 | 剩余: 3,800 行 | 总计: 10,000 行
```

结构同步时（无行数）显示 indeterminate 动画进度条 + "结构同步中…"

### EventSource 生命周期

1. 页面加载时：若 `task.Status == "running"` 则立即 `new EventSource(...)`
2. 点击"立即运行"按钮后：submit 成功后自动连接 SSE
3. 收到 `done` / `failed` 事件：关闭 EventSource，1 秒后刷新页面
4. CDC 实时模式：不显示进度条，仅显示"实时监听中"徽章

## 不在本次范围内

- 进度持久化到数据库
- 多客户端广播（单 channel 单消费者即可）
