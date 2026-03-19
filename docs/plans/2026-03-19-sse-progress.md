# SSE 同步进度 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在任务详情页通过 SSE 实时展示同步进度（已同步行数、剩余行数、总行数、百分比）。

**Architecture:** 在 `Executor` 中用 `sync.Map` 维护每个运行任务的 `chan ProgressEvent`，`engine.SyncData` 已有 `OnProgress` 回调，只需接入。新增 SSE 端点 `/api/tasks/:id/progress/stream`，任务详情页用 `EventSource` 订阅并渲染进度条。

**Tech Stack:** Go (net/http SSE), JavaScript EventSource API, Tailwind CSS progress bar

---

### Task 1: 定义 ProgressEvent 并在 Executor 加入进度广播

**Files:**
- Modify: `internal/service/executor.go`

**Step 1: 在文件顶部添加 ProgressEvent 结构体和 progress map**

在 `Executor` 结构体的 `running sync.Map` 下面加一行，并在文件中添加类型定义：

```go
// ProgressEvent holds a single progress update for a running task.
type ProgressEvent struct {
    Phase      string  `json:"phase"`    // connecting | structure | data | done | failed
    Message    string  `json:"message"`
    RowsSynced int64   `json:"rows_synced"`
    TotalRows  int64   `json:"total_rows"`
    Percent    float64 `json:"percent"`
    ErrorMsg   string  `json:"error_msg,omitempty"`
}
```

在 `Executor` 结构体中添加：
```go
progress sync.Map // map[uint]chan ProgressEvent
```

**Step 2: 添加 Subscribe / emit 辅助方法（在 Stop 方法之后）**

```go
// Subscribe returns the progress channel for a running task.
// Returns nil, false if the task is not running.
func (e *Executor) Subscribe(taskID uint) (<-chan ProgressEvent, bool) {
    val, ok := e.progress.Load(taskID)
    if !ok {
        return nil, false
    }
    return val.(chan ProgressEvent), true
}

// emit sends a progress event, dropping it if the channel is full.
func (e *Executor) emit(taskID uint, ev ProgressEvent) {
    val, ok := e.progress.Load(taskID)
    if !ok {
        return
    }
    ch := val.(chan ProgressEvent)
    select {
    case ch <- ev:
    default: // drop if consumer is slow
    }
}
```

**Step 3: 在 Run() 开始时创建 channel，在 goroutine 结束时关闭**

在 `Run()` 中，`e.running.Store(taskID, cancel)` 之前插入：
```go
progressCh := make(chan ProgressEvent, 64)
e.progress.Store(taskID, progressCh)
```

在 goroutine 的 `defer` 块中（`defer e.running.Delete(taskID)` 旁边）添加：
```go
defer func() {
    e.progress.Delete(taskID)
    close(progressCh)
}()
```

对于 realtime/CDC 路径，在 `e.running.Store(taskID, ...)` 之前同样创建 channel，在结束时关闭（CDC 路径没有 goroutine，需要在 Stop() 里处理关闭）。

**Step 4: 在 Run() goroutine 的各阶段发送事件**

在 `e.running.Store` / goroutine 启动之前（连接阶段），发送 connecting 事件（此时 channel 已创建）：
```go
e.emit(taskID, ProgressEvent{Phase: "connecting", Message: "正在建立数据库连接..."})
```

在 goroutine 内部，`switch task.SyncType` 各分支中：

```go
case "structure":
    e.emit(taskID, ProgressEvent{Phase: "structure", Message: "正在同步表结构..."})
    syncErr = engine.SyncStructure(...)

case "data":
    e.emit(taskID, ProgressEvent{Phase: "data", Message: "正在同步数据..."})
    opts := engine.DataSyncOptions{
        // ... 现有字段 ...
        OnProgress: func(synced, total int64) {
            pct := float64(0)
            if total > 0 {
                pct = float64(synced) / float64(total) * 100
            }
            e.emit(taskID, ProgressEvent{
                Phase:      "data",
                Message:    "正在同步数据...",
                RowsSynced: synced,
                TotalRows:  total,
                Percent:    pct,
            })
        },
    }
    // ...

case "both":
    e.emit(taskID, ProgressEvent{Phase: "structure", Message: "正在同步表结构..."})
    syncErr = engine.SyncStructure(...)
    if syncErr == nil {
        e.emit(taskID, ProgressEvent{Phase: "data", Message: "正在同步数据..."})
        opts := engine.DataSyncOptions{
            // ... 现有字段 ...
            OnProgress: func(synced, total int64) {
                pct := float64(0)
                if total > 0 {
                    pct = float64(synced) / float64(total) * 100
                }
                e.emit(taskID, ProgressEvent{
                    Phase:      "data",
                    Message:    "正在同步数据...",
                    RowsSynced: synced,
                    TotalRows:  total,
                    Percent:    pct,
                })
            },
        }
        // ...
    }
```

在 goroutine 末尾，更新 log / status 之后发终止事件：
```go
if syncErr != nil {
    e.emit(taskID, ProgressEvent{Phase: "failed", Message: "同步失败", ErrorMsg: syncErr.Error()})
} else {
    e.emit(taskID, ProgressEvent{Phase: "done", Message: "同步完成", Percent: 100,
        RowsSynced: rowsSynced, TotalRows: rowsSynced})
}
```

**Step 5: 在 Stop() 中关闭 CDC 任务的 progress channel**

在 `e.running.Delete(taskID)` 之后：
```go
if val, ok := e.progress.Load(taskID); ok {
    ch := val.(chan ProgressEvent)
    e.progress.Delete(taskID)
    e.emit(taskID, ProgressEvent{Phase: "done", Message: "任务已停止"})
    // channel will drain naturally; close after emit
    close(ch)
}
```

**Step 6: 编译验证**

```bash
go build ./...
```
Expected: 无报错

**Step 7: Commit**

```bash
git add internal/service/executor.go
git commit -m "feat: add SSE progress event broadcasting to Executor"
```

---

### Task 2: 新增 SSE 进度流端点

**Files:**
- Modify: `internal/handler/task.go`
- Modify: `main.go`

**Step 1: 在 task.go 中添加 ProgressStream handler**

在 `Stop` 方法之后添加：

```go
func (h *TaskHandler) ProgressStream(c *gin.Context) {
    id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
    taskID := uint(id)

    c.Header("Content-Type", "text/event-stream")
    c.Header("Cache-Control", "no-cache")
    c.Header("Connection", "keep-alive")
    c.Header("X-Accel-Buffering", "no")

    ch, ok := h.Executor.Subscribe(taskID)
    if !ok {
        // Task not running — send done immediately
        fmt.Fprintf(c.Writer, "data: {\"phase\":\"done\",\"message\":\"任务未运行\"}\n\n")
        c.Writer.Flush()
        return
    }

    ctx := c.Request.Context()
    flusher, _ := c.Writer.(http.Flusher)

    for {
        select {
        case ev, open := <-ch:
            if !open {
                return
            }
            data, _ := json.Marshal(ev)
            fmt.Fprintf(c.Writer, "data: %s\n\n", data)
            if flusher != nil {
                flusher.Flush()
            }
            if ev.Phase == "done" || ev.Phase == "failed" {
                return
            }
        case <-ctx.Done():
            return
        }
    }
}
```

需要在 import 中添加 `"encoding/json"` 和 `"fmt"`（如果还没有的话，`encoding/json` 已存在，`fmt` 需要检查）。

**Step 2: 在 main.go 注册路由**

在 `api` 路由组中（`api.PUT("/tasks/:id/mappings", ...)` 下面）添加：

```go
api.GET("/tasks/:id/progress/stream", taskHandler.ProgressStream)
```

**Step 3: 编译验证**

```bash
go build ./...
```
Expected: 无报错

**Step 4: 快速手动测试（可选）**

启动服务后，在另一个终端运行一个耗时任务，然后：
```bash
curl -N -H "Cookie: <auth_cookie>" http://localhost:9090/api/tasks/1/progress/stream
```
Expected: 收到 `data: {...}` 行流式输出

**Step 5: Commit**

```bash
git add internal/handler/task.go main.go
git commit -m "feat: add SSE progress stream endpoint GET /api/tasks/:id/progress/stream"
```

---

### Task 3: 任务详情页添加进度面板

**Files:**
- Modify: `templates/task/detail.html`

**Step 1: 在 Task Info Card 下方插入进度面板 HTML**

在 `<!-- Field Mappings -->` 注释之前插入：

```html
<!-- Progress Panel (shown when running) -->
<div id="progress-panel" class="bg-white rounded-xl shadow p-6 mb-6 hidden">
    <h2 class="text-lg font-bold text-gray-800 mb-4">同步进度</h2>
    <div class="space-y-3">
        <div class="flex justify-between items-center text-sm text-gray-600">
            <span id="progress-phase-label">准备中...</span>
            <span id="progress-percent-label" class="font-semibold text-indigo-600">0%</span>
        </div>
        <!-- Progress bar -->
        <div class="w-full bg-gray-200 rounded-full h-4 overflow-hidden">
            <div id="progress-bar-fill"
                 class="h-4 rounded-full transition-all duration-300 bg-indigo-500"
                 style="width: 0%"></div>
        </div>
        <!-- Indeterminate bar (structure sync) -->
        <div id="progress-bar-indeterminate" class="hidden w-full bg-gray-200 rounded-full h-4 overflow-hidden">
            <div class="h-4 rounded-full bg-indigo-400 animate-pulse" style="width: 100%"></div>
        </div>
        <!-- Row counts -->
        <div id="progress-row-stats" class="hidden flex justify-between text-sm text-gray-600 pt-1">
            <span>已同步：<strong id="stat-synced" class="text-gray-900">0</strong> 行</span>
            <span>剩余：<strong id="stat-remaining" class="text-gray-900">0</strong> 行</span>
            <span>总计：<strong id="stat-total" class="text-gray-900">0</strong> 行</span>
        </div>
        <!-- Error message -->
        <div id="progress-error" class="hidden p-2 bg-red-50 border border-red-200 text-red-700 rounded text-sm"></div>
    </div>
</div>
```

**Step 2: 在页面底部 `</body>` 之前的 `<script>` 块末尾添加 SSE 逻辑**

```javascript
// ── SSE Progress ──────────────────────────────────────────
var taskStatus = "{{.task.Status}}";
var progressES = null;

function fmtNum(n) {
    return Number(n).toLocaleString();
}

function updateProgress(ev) {
    var panel = document.getElementById('progress-panel');
    var barFill = document.getElementById('progress-bar-fill');
    var barIndet = document.getElementById('progress-bar-indeterminate');
    var phaseLabel = document.getElementById('progress-phase-label');
    var pctLabel = document.getElementById('progress-percent-label');
    var rowStats = document.getElementById('progress-row-stats');
    var errDiv = document.getElementById('progress-error');

    panel.classList.remove('hidden');

    if (ev.phase === 'data') {
        barFill.parentElement.classList.remove('hidden');
        barIndet.classList.add('hidden');
        var pct = ev.percent || 0;
        barFill.style.width = pct.toFixed(1) + '%';
        pctLabel.textContent = pct.toFixed(1) + '%';
        if (ev.total_rows > 0) {
            rowStats.classList.remove('hidden');
            document.getElementById('stat-synced').textContent = fmtNum(ev.rows_synced);
            document.getElementById('stat-remaining').textContent = fmtNum(ev.total_rows - ev.rows_synced);
            document.getElementById('stat-total').textContent = fmtNum(ev.total_rows);
        }
    } else if (ev.phase === 'structure' || ev.phase === 'connecting') {
        barFill.parentElement.classList.add('hidden');
        barIndet.classList.remove('hidden');
        rowStats.classList.add('hidden');
        pctLabel.textContent = '';
    } else if (ev.phase === 'done') {
        barFill.parentElement.classList.remove('hidden');
        barIndet.classList.add('hidden');
        barFill.style.width = '100%';
        barFill.classList.remove('bg-indigo-500');
        barFill.classList.add('bg-green-500');
        pctLabel.textContent = '100%';
    } else if (ev.phase === 'failed') {
        barFill.parentElement.classList.add('hidden');
        barIndet.classList.add('hidden');
        pctLabel.textContent = '';
        errDiv.classList.remove('hidden');
        errDiv.textContent = ev.error_msg || '同步失败';
    }

    phaseLabel.textContent = ev.message || '';
}

function connectProgressStream() {
    if (progressES) return;
    progressES = new EventSource('/api/tasks/' + taskID + '/progress/stream');
    progressES.onmessage = function(e) {
        var ev = JSON.parse(e.data);
        updateProgress(ev);
        if (ev.phase === 'done' || ev.phase === 'failed') {
            progressES.close();
            progressES = null;
            // Reload after short delay to refresh status + logs
            setTimeout(function() { window.location.reload(); }, 1500);
        }
    };
    progressES.onerror = function() {
        progressES.close();
        progressES = null;
    };
}

// Auto-connect if task is already running on page load
if (taskStatus === 'running') {
    connectProgressStream();
}

// Connect SSE after clicking 立即运行
var runForm = document.querySelector('form[action$="/run"]');
if (runForm) {
    runForm.addEventListener('submit', function() {
        setTimeout(connectProgressStream, 500);
    });
}
// ── End SSE Progress ──────────────────────────────────────
```

**Step 3: 编译验证**

```bash
go build ./...
```
Expected: 无报错（模板错误只在运行时暴露）

**Step 4: 启动服务并手动验证**

```bash
./datasync
```

打开任务详情页，点击"立即运行"：
- Expected: 进度面板出现，`connecting → structure/data` 阶段依次更新
- 数据同步任务 Expected: 进度条随 batch 推进，行数数字实时更新
- 完成后 Expected: 进度条变绿，1.5 秒后页面刷新

**Step 5: Commit**

```bash
git add templates/task/detail.html
git commit -m "feat: add SSE progress panel to task detail page"
```

---

### Task 4: 修复 Stop() 的 channel 关闭竞态

**Files:**
- Modify: `internal/service/executor.go`

**背景:** Stop() 需要先 emit done 事件再 close channel，且不能 double-close。

**Step 1: 重写 Stop() 中的 progress 清理逻辑**

确保 Stop() 里的处理顺序为：
1. Load channel
2. emit done/stop 事件
3. Delete from map
4. close channel

```go
// 在 e.running.Delete(taskID) 之后添加:
if val, ok := e.progress.LoadAndDelete(taskID); ok {
    ch := val.(chan ProgressEvent)
    select {
    case ch <- ProgressEvent{Phase: "done", Message: "任务已手动停止"}:
    default:
    }
    close(ch)
}
```

同时确保 Run() goroutine 的 defer 里也用 `LoadAndDelete` 而非 `Delete`+`close(progressCh)` 直接引用，避免 Stop() 和 goroutine 同时 close：

在 goroutine 的 defer 中改为：
```go
defer func() {
    e.running.Delete(taskID)
    // Close progress channel only if not already closed by Stop()
    if _, loaded := e.progress.LoadAndDelete(taskID); loaded {
        close(progressCh)
    }
}()
```

**Step 2: 编译并运行测试**

```bash
go build ./...
go test ./internal/service/... -v
```
Expected: 编译通过，测试通过

**Step 3: Commit**

```bash
git add internal/service/executor.go
git commit -m "fix: prevent double-close of progress channel in Stop()"
```
