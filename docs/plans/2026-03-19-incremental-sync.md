# 增量同步与自定义过滤条件 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在任务配置中支持自定义 WHERE 条件和自动水位线增量同步（时间戳/自增 ID 两种模式）。

**Architecture:** SyncTask 模型新增 4 个字段存储过滤配置和水位值；engine.DataSyncOptions 新增 WhereClause 参数影响 COUNT 和 SELECT 查询；Executor 在运行前组装 WHERE 子句，同步成功后回写水位值。

**Tech Stack:** Go, GORM AutoMigrate (SQLite), Go html/template

---

### Task 1: SyncTask 模型新增过滤字段

**Files:**
- Modify: `internal/model/models.go`

GORM AutoMigrate 在启动时自动执行，无需手写 migration SQL。

**Step 1: 在 SyncTask 结构体末尾新增 4 个字段**

找到 `SyncTask` 结构体，在 `CreatedAt` 字段之前添加：

```go
FilterCondition    string `gorm:"type:text" json:"filter_condition"`
WatermarkColumn    string `gorm:"size:200" json:"watermark_column"`
WatermarkType      string `gorm:"size:20" json:"watermark_type"`
LastWatermarkValue string `gorm:"size:500" json:"last_watermark_value"`
```

完整结构体应为：

```go
type SyncTask struct {
    ID           uint      `gorm:"primaryKey" json:"id"`
    Name         string    `gorm:"size:200" json:"name"`
    SourceDSID   *uint     `json:"source_ds_id"`
    TargetDSID   *uint     `json:"target_ds_id"`
    SourceConfig string    `gorm:"type:text" json:"source_config"`
    TargetConfig string    `gorm:"type:text" json:"target_config"`
    SourceTable  string    `gorm:"size:200" json:"source_table"`
    TargetTable  string    `gorm:"size:200" json:"target_table"`
    SyncType     string    `gorm:"size:20" json:"sync_type"`
    SyncMode     string    `gorm:"size:20" json:"sync_mode"`
    CronExpr     string    `gorm:"size:100" json:"cron_expr"`
    Status       string    `gorm:"size:20;default:idle" json:"status"`
    FilterCondition    string `gorm:"type:text" json:"filter_condition"`
    WatermarkColumn    string `gorm:"size:200" json:"watermark_column"`
    WatermarkType      string `gorm:"size:20" json:"watermark_type"`
    LastWatermarkValue string `gorm:"size:500" json:"last_watermark_value"`
    CreatedBy    uint      `json:"created_by"`
    CreatedAt    time.Time `json:"created_at"`
}
```

**Step 2: 编译验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

Expected: 无报错

**Step 3: 验证 AutoMigrate 会添加列**

GORM AutoMigrate 在 `database.Init()` 中自动调用，启动服务后会自动为 SQLite 添加新列。无需手动迁移。

**Step 4: Commit**

```bash
git add internal/model/models.go
git commit -m "feat: add filter_condition and watermark fields to SyncTask"
```

---

### Task 2: Engine 层 — DataSyncOptions 支持 WhereClause

**Files:**
- Modify: `internal/engine/data.go`

**Step 1: 在 DataSyncOptions 添加 WhereClause 字段**

在 `OnProgress func(synced, total int64)` 行之后添加：

```go
WhereClause string // SQL WHERE 条件（不含 WHERE 关键字），由调用方组装
```

**Step 2: 修改 SyncResult 添加 MaxWatermarkValue**

```go
type SyncResult struct {
    RowsSynced        int64
    Duration          time.Duration
    Error             error
    MaxWatermarkValue string // id 水位线模式：本次同步源表最大水位列值
}
```

**Step 3: 修改 SyncData 中的 COUNT 查询**

找到：
```go
countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(opts.SourceDBType, opts.SourceTable))
```

改为：
```go
countSQL := buildCountSQL(opts.SourceDBType, opts.SourceTable, opts.WhereClause)
```

在文件中添加 `buildCountSQL` 函数（放在 `buildBatchSelectSQL` 附近）：

```go
// buildCountSQL builds a COUNT query with optional WHERE clause.
func buildCountSQL(dbType, table, whereClause string) string {
    q := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(dbType, table))
    if whereClause != "" {
        q += " WHERE " + whereClause
    }
    return q
}
```

**Step 4: 修改 buildBatchSelectSQL 支持 WhereClause**

当前签名：
```go
func buildBatchSelectSQL(dbType, table string, columns []string, offset, limit int64) string
```

改为：
```go
func buildBatchSelectSQL(dbType, table string, columns []string, offset, limit int64, whereClause string) string
```

函数内部，在 `tbl := quoteIdentifier(dbType, table)` 之后、switch 之前，构建 where 片段：

```go
whereFragment := ""
if whereClause != "" {
    whereFragment = " WHERE " + whereClause
}
```

然后修改各 case 中的 SQL，在表名后 LIMIT/OFFSET 前加入 `whereFragment`：

```go
case "oracle":
    return fmt.Sprintf(
        "SELECT %s FROM (SELECT a.*, ROWNUM rn FROM (SELECT %s FROM %s%s) a WHERE ROWNUM <= %d) WHERE rn > %d",
        colList, colList, tbl, whereFragment, offset+limit, offset,
    )
default:
    return fmt.Sprintf("SELECT %s FROM %s%s LIMIT %d OFFSET %d", colList, tbl, whereFragment, limit, offset)
```

**Step 5: 更新 SyncData 中调用 buildBatchSelectSQL 的地方**

找到：
```go
selectSQL := buildBatchSelectSQL(opts.SourceDBType, opts.SourceTable, sourceCols, offset, int64(opts.BatchSize))
```

改为：
```go
selectSQL := buildBatchSelectSQL(opts.SourceDBType, opts.SourceTable, sourceCols, offset, int64(opts.BatchSize), opts.WhereClause)
```

**Step 6: 编译验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

Expected: 无报错

**Step 7: 运行已有测试**

```bash
cd /Users/mac/Documents/dataSync && go test ./internal/engine/... -v
```

Expected: 所有测试通过

**Step 8: Commit**

```bash
git add internal/engine/data.go
git commit -m "feat: add WhereClause support to DataSyncOptions and SyncData"
```

---

### Task 3: Executor 层 — buildWhereClause + 水位线回写

**Files:**
- Modify: `internal/service/executor.go`

**Step 1: 添加 import**

确认 import 块中有 `"strings"` 和 `"time"`（已有 `"time"`，需确认 `"strings"`；如没有则添加）。

**Step 2: 添加 buildWhereClause 函数**

在 `resolveDataSource` 函数之后添加：

```go
// buildWhereClause assembles the SQL WHERE clause (without the WHERE keyword)
// from the task's FilterCondition and watermark settings.
func buildWhereClause(task *model.SyncTask, sourceDBType string) string {
    var parts []string

    if task.FilterCondition != "" {
        parts = append(parts, task.FilterCondition)
    }

    if task.WatermarkColumn != "" && task.WatermarkType != "" && task.LastWatermarkValue != "" {
        col := quoteWatermarkCol(task.WatermarkColumn, sourceDBType)
        switch task.WatermarkType {
        case "id":
            parts = append(parts, fmt.Sprintf("%s > %s", col, task.LastWatermarkValue))
        case "timestamp":
            parts = append(parts, fmt.Sprintf("%s > '%s'", col, task.LastWatermarkValue))
        }
    }

    return strings.Join(parts, " AND ")
}

// quoteWatermarkCol quotes a column name for the given DB type.
func quoteWatermarkCol(col, dbType string) string {
    switch strings.ToLower(dbType) {
    case "mysql":
        return "`" + col + "`"
    case "postgresql":
        return `"` + col + `"`
    default:
        return col
    }
}
```

Note: `engine` package 的 `quoteIdentifier` 未导出，所以这里在 service 层自己实现一个轻量版本。

**Step 3: 在 Run() goroutine 中为 "data" 和 "both" 的 opts 添加 WhereClause**

找到 `case "data":` 中的 opts 构建，在 `OnProgress: makeOnProgress(),` 之后添加：

```go
WhereClause: buildWhereClause(task, sourceDS.DBType),
```

找到 `case "both":` 中的 opts 构建，同样添加：

```go
WhereClause: buildWhereClause(task, sourceDS.DBType),
```

**Step 4: 在同步成功后回写水位值**

找到 goroutine 末尾这段代码：

```go
e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).Update("status", taskStatus)
```

在这行**之后**添加水位线回写逻辑：

```go
// Update watermark after successful sync
if syncErr == nil && task.WatermarkColumn != "" && task.WatermarkType != "" {
    var newWatermark string
    switch task.WatermarkType {
    case "timestamp":
        // Use sync start time to avoid missing rows written during sync
        newWatermark = now.UTC().Format(time.RFC3339)
    case "id":
        // Query max watermark column value from source table
        col := quoteWatermarkCol(task.WatermarkColumn, sourceDS.DBType)
        tbl := task.SourceTable
        maxSQL := fmt.Sprintf("SELECT COALESCE(MAX(%s), '') FROM %s", col, tbl)
        if wc := buildWhereClause(&model.SyncTask{FilterCondition: task.FilterCondition}, sourceDS.DBType); wc != "" {
            maxSQL += " WHERE " + wc
        }
        var maxVal string
        if err := sourceDB.QueryRowContext(ctx, maxSQL).Scan(&maxVal); err == nil && maxVal != "" {
            newWatermark = maxVal
        }
    }
    if newWatermark != "" {
        e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).
            Update("last_watermark_value", newWatermark)
    }
}
```

注意：这里 `now` 是已有变量（`now := time.Now()` 在 goroutine 外、创建 syncLog 时定义），需确认变量名一致。如变量名不同请对应调整。

**Step 5: 编译验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

Expected: 无报错

**Step 6: Commit**

```bash
git add internal/service/executor.go
git commit -m "feat: build WHERE clause from filter+watermark and update watermark after sync"
```

---

### Task 4: Handler 层 — Create/Update 解析新字段

**Files:**
- Modify: `internal/handler/task.go`

**Step 1: 在 Create() 的 task 初始化中添加新字段**

找到 Create() 中的：
```go
task := &model.SyncTask{
    Name:        c.PostForm("name"),
    ...
    Status:      "idle",
}
```

在 `Status: "idle",` 之后添加：

```go
FilterCondition: c.PostForm("filter_condition"),
WatermarkColumn: c.PostForm("watermark_column"),
WatermarkType:   c.PostForm("watermark_type"),
```

注意：`LastWatermarkValue` 不从表单读取（只读，由系统自动维护）。

**Step 2: 在 Update() 的字段赋值中添加新字段**

找到 Update() 中：
```go
task.Name = c.PostForm("name")
task.SourceTable = c.PostForm("source_table")
...
task.CronExpr = c.PostForm("cron_expr")
```

在末尾添加：

```go
task.FilterCondition = c.PostForm("filter_condition")
task.WatermarkColumn = c.PostForm("watermark_column")
task.WatermarkType = c.PostForm("watermark_type")
// LastWatermarkValue: allow manual reset via hidden field (empty = reset)
if c.PostForm("reset_watermark") == "1" {
    task.LastWatermarkValue = ""
}
```

**Step 3: 编译验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

Expected: 无报错

**Step 4: Commit**

```bash
git add internal/handler/task.go
git commit -m "feat: parse filter_condition and watermark fields in task create/update"
```

---

### Task 5: 任务表单 UI — 高级过滤区块

**Files:**
- Modify: `templates/task/form.html`

**Step 1: 在同步类型/模式字段之后、提交按钮之前插入高级过滤区块**

找到：
```html
<div class="pt-4">
    <button type="submit" ...>保存</button>
</div>
```

在这段之前插入：

```html
<!-- Advanced Filter Section -->
<div class="border border-gray-200 rounded-lg p-4 space-y-4">
    <button type="button" id="toggle-advanced"
        class="flex items-center justify-between w-full text-sm font-semibold text-gray-700">
        <span>高级过滤（可选）</span>
        <span id="toggle-icon" class="text-gray-400 transition-transform">▶</span>
    </button>

    <div id="advanced-filter-body" class="hidden space-y-4">
        <!-- Watermark -->
        <div class="space-y-3">
            <label class="flex items-center gap-2 text-sm font-medium text-gray-700">
                <input type="checkbox" id="enable-watermark" class="h-4 w-4"
                    {{if and .task .task.WatermarkColumn}}checked{{end}}>
                启用增量同步（水位线）
            </label>

            <div id="watermark-fields" class="{{if not (and .task .task.WatermarkColumn)}}hidden{{end}} grid grid-cols-2 gap-3 pl-6">
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">水位线字段名</label>
                    <input type="text" name="watermark_column"
                        value="{{if .task}}{{.task.WatermarkColumn}}{{end}}"
                        placeholder="如: updated_at 或 id"
                        class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500">
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">水位线类型</label>
                    <select name="watermark_type"
                        class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500">
                        <option value="">请选择</option>
                        <option value="timestamp" {{if and .task (eq .task.WatermarkType "timestamp")}}selected{{end}}>时间戳字段</option>
                        <option value="id" {{if and .task (eq .task.WatermarkType "id")}}selected{{end}}>自增 ID 字段</option>
                    </select>
                </div>
                {{if and .task .task.LastWatermarkValue}}
                <div class="col-span-2">
                    <label class="block text-sm font-medium text-gray-500 mb-1">当前水位值（只读）</label>
                    <div class="flex items-center gap-3">
                        <span class="text-sm text-gray-700 font-mono bg-gray-50 border border-gray-200 rounded px-3 py-1.5">{{.task.LastWatermarkValue}}</span>
                        <label class="flex items-center gap-1 text-xs text-red-600 cursor-pointer">
                            <input type="checkbox" name="reset_watermark" value="1" class="h-3 w-3">
                            重置水位（下次全量同步）
                        </label>
                    </div>
                </div>
                {{end}}
            </div>
        </div>

        <!-- Filter Condition -->
        <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
                自定义过滤条件（WHERE 子句）
            </label>
            <textarea name="filter_condition" rows="2"
                placeholder="直接写条件表达式，不需要 WHERE 关键字，例如：status = 'active' AND region = 'CN'"
                class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm font-mono focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500">{{if .task}}{{.task.FilterCondition}}{{end}}</textarea>
            <p class="text-xs text-gray-400 mt-1">此条件在每次同步时追加到查询，与水位线条件用 AND 合并</p>
        </div>
    </div>
</div>
```

**Step 2: 在现有 `<script>` 块末尾添加高级过滤的 JS 交互**

在 `</script>` 之前添加：

```javascript
// Toggle advanced filter panel
var toggleBtn = document.getElementById('toggle-advanced');
var filterBody = document.getElementById('advanced-filter-body');
var toggleIcon = document.getElementById('toggle-icon');

// Auto-expand if has values
{{if and .task (or .task.FilterCondition .task.WatermarkColumn)}}
filterBody.classList.remove('hidden');
toggleIcon.style.transform = 'rotate(90deg)';
{{end}}

if (toggleBtn) {
    toggleBtn.addEventListener('click', function() {
        var hidden = filterBody.classList.contains('hidden');
        filterBody.classList.toggle('hidden', !hidden);
        toggleIcon.style.transform = hidden ? 'rotate(90deg)' : '';
    });
}

// Toggle watermark fields
var enableWm = document.getElementById('enable-watermark');
var wmFields = document.getElementById('watermark-fields');
if (enableWm) {
    enableWm.addEventListener('change', function() {
        wmFields.classList.toggle('hidden', !this.checked);
        if (!this.checked) {
            // Clear watermark fields when disabled
            var wmCol = document.querySelector('input[name="watermark_column"]');
            var wmType = document.querySelector('select[name="watermark_type"]');
            if (wmCol) wmCol.value = '';
            if (wmType) wmType.value = '';
        }
    });
}
```

注意：Go 模板中 `or` 不是内置函数，用 `{{if .task}}{{if or ...}}` 这种写法会报错。改用两次独立判断：

```
{{if .task}}{{if .task.FilterCondition}}
filterBody.classList.remove('hidden');
toggleIcon.style.transform = 'rotate(90deg)';
{{else if .task.WatermarkColumn}}
filterBody.classList.remove('hidden');
toggleIcon.style.transform = 'rotate(90deg)';
{{end}}{{end}}
```

**Step 3: 编译验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

Expected: 无报错（模板语法错误只在运行时出现，需手动验证）

**Step 4: 启动服务，打开新建任务页面，手动验证**

```bash
./datasync
```

- 高级过滤区块默认折叠 ✓
- 勾选"启用增量同步"后展示水位线字段 ✓
- 编辑已有水位线任务时自动展开 ✓

**Step 5: Commit**

```bash
git add templates/task/form.html
git commit -m "feat: add advanced filter section to task form (watermark + WHERE condition)"
```

---

### Task 6: 任务详情页 — 展示过滤条件和水位线

**Files:**
- Modify: `templates/task/detail.html`

**Step 1: 在任务信息卡片的 grid 中添加两个字段**

找到任务信息卡片中的 grid，在最后一个 `<div>` （状态那一格）之后添加：

```html
{{if .task.FilterCondition}}
<div class="col-span-2 md:col-span-3">
    <span class="text-sm text-gray-500">过滤条件</span>
    <p class="font-mono text-sm text-gray-900 bg-gray-50 rounded px-2 py-1 mt-0.5">{{.task.FilterCondition}}</p>
</div>
{{end}}
{{if .task.WatermarkColumn}}
<div>
    <span class="text-sm text-gray-500">水位线字段</span>
    <p class="font-medium text-gray-900">{{.task.WatermarkColumn}} ({{.task.WatermarkType}})</p>
</div>
<div>
    <span class="text-sm text-gray-500">当前水位值</span>
    <p class="font-mono text-sm text-gray-900">
        {{if .task.LastWatermarkValue}}{{.task.LastWatermarkValue}}{{else}}<span class="text-gray-400">未同步过</span>{{end}}
    </p>
</div>
{{end}}
```

**Step 2: 编译验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

Expected: 无报错

**Step 3: 手动验证**

创建一个带水位线的任务，运行一次，查看详情页水位值是否更新。

**Step 4: Commit**

```bash
git add templates/task/detail.html
git commit -m "feat: show filter condition and watermark info in task detail"
```

---

### Task 7: 运行全量测试

**Step 1: 运行所有测试**

```bash
cd /Users/mac/Documents/dataSync && go test ./... -v
```

Expected: 所有测试通过，无编译错误

**Step 2: 如有失败，修复后重新运行确认**
