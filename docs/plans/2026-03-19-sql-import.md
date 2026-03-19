# SQL 导入功能实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在现有同步任务中新增 `sql_import` 同步类型，在源数据源上执行自定义 SQL，将结果分批写入目标表。

**Architecture:** 新增 `SyncTask.SourceSQL` 字段存储 SQL；`DataSyncOptions` 新增 `SourceSQL` 字段；`SyncData` 检测到 `SourceSQL` 非空时走 SQL 导入路径，通过 `RawDB()` 拿到底层 `*sql.DB` 执行分页查询（`SELECT * FROM (...) _t LIMIT ? OFFSET ?`），结果写入目标表。表单 JS 动态切换源表/SQL 编辑区。

**Tech Stack:** Go `database/sql`, Gin, Go html/template, Tailwind CSS

---

### Task 1: 模型 — 添加 SourceSQL 字段

**Files:**
- Modify: `internal/model/models.go:42`（在 `CheckpointOffset` 后插入）

**Step 1: 在 SyncTask 结构体中添加字段**

在 `CheckpointOffset int64` 那行后面加：

```go
SourceSQL          string    `gorm:"type:text" json:"source_sql"`
```

**Step 2: 验证编译**

```bash
go build ./...
```
Expected: 无输出（编译通过）

**Step 3: Commit**

```bash
git add internal/model/models.go
git commit -m "feat: add SourceSQL field to SyncTask model"
```

---

### Task 2: 引擎 — SyncData 支持 SQL 导入路径

**Files:**
- Modify: `internal/engine/data.go`

**Context:**
- `DataSyncOptions` 在文件顶部约第 14 行定义
- `SyncData` 函数约从第 40 行开始
- `connector.SQLConnector` 有 `RawDB() *sql.DB` 方法
- 需要导入 `"database/sql"` 包

**Step 1: DataSyncOptions 新增 SourceSQL 字段**

在 `DataSyncOptions` 结构体的 `OnCheckpoint` 字段后添加：

```go
SourceSQL    string // 非空时走 SQL 导入路径，忽略 SourceTable
```

**Step 2: SyncData 中添加 SQL 导入分支**

在 `SyncData` 函数开头，`Apply defaults` 块之后、`if opts.Concurrency > 1` 判断之前，插入：

```go
// SQL 导入路径：SourceSQL 非空时分页执行自定义 SQL
if opts.SourceSQL != "" {
    return syncDataFromSQL(ctx, opts)
}
```

**Step 3: 实现 syncDataFromSQL 函数**

在文件末尾追加（在最后一个函数之后）：

```go
// syncDataFromSQL executes a user-provided SQL on the source (SQL databases only),
// paginates results via LIMIT/OFFSET subquery, and writes each batch to the target.
func syncDataFromSQL(ctx context.Context, opts DataSyncOptions) (*SyncResult, error) {
	start := time.Now()

	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	if opts.WriteStrategy == "" {
		opts.WriteStrategy = "insert"
	}

	// SQL 导入只支持 SQL 类型的数据源
	type rawDBer interface {
		RawDB() *sql.DB
	}
	rdb, ok := opts.Source.(rawDBer)
	if !ok {
		return nil, fmt.Errorf("sql_import 仅支持 SQL 类数据源（MySQL/PG/Oracle/ClickHouse/Doris）")
	}
	db := rdb.RawDB()

	// 用子查询估算总行数
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) _datasync_count", opts.SourceSQL)
	var totalRows int64
	if err := db.QueryRowContext(ctx, countSQL).Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("count sql rows: %w", err)
	}

	// 获取目标表主键（upsert 需要）
	var pkColumns []string
	if opts.WriteStrategy == "upsert" {
		schema, err := opts.Target.GetSchema(ctx, opts.TargetTable)
		if err != nil {
			return nil, fmt.Errorf("get target pk: %w", err)
		}
		for _, col := range schema.Columns {
			if col.IsPrimary {
				pkColumns = append(pkColumns, col.Name)
			}
		}
		if len(pkColumns) == 0 {
			return nil, fmt.Errorf("upsert requires primary key on target table %s", opts.TargetTable)
		}
	}

	writeStrategy := connector.StrategyInsert
	if opts.WriteStrategy == "upsert" {
		writeStrategy = connector.StrategyUpsert
	}

	var rowsSynced int64
	for offset := opts.StartOffset; offset < totalRows; offset += int64(opts.BatchSize) {
		select {
		case <-ctx.Done():
			return &SyncResult{RowsSynced: rowsSynced, Duration: time.Since(start), Error: ctx.Err()}, ctx.Err()
		default:
		}

		batchSQL := fmt.Sprintf("SELECT * FROM (%s) _datasync_batch LIMIT %d OFFSET %d",
			opts.SourceSQL, opts.BatchSize, offset)
		sqlRows, err := db.QueryContext(ctx, batchSQL)
		if err != nil {
			return nil, fmt.Errorf("query batch at offset %d: %w", offset, err)
		}

		cols, err := sqlRows.Columns()
		if err != nil {
			sqlRows.Close()
			return nil, fmt.Errorf("get columns: %w", err)
		}

		var rows []connector.Row
		for sqlRows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := sqlRows.Scan(ptrs...); err != nil {
				sqlRows.Close()
				return nil, fmt.Errorf("scan row: %w", err)
			}
			row := make(connector.Row, len(cols))
			for i, col := range cols {
				row[col] = vals[i]
			}
			rows = append(rows, row)
		}
		sqlRows.Close()
		if err := sqlRows.Err(); err != nil {
			return nil, fmt.Errorf("rows error: %w", err)
		}

		if len(rows) == 0 {
			break
		}

		if err := opts.Target.WriteBatch(ctx, connector.WriteOptions{
			Table:    opts.TargetTable,
			Columns:  cols,
			Rows:     rows,
			Strategy: writeStrategy,
			PKCols:   pkColumns,
		}); err != nil {
			return nil, fmt.Errorf("write batch at offset %d: %w", offset, err)
		}

		rowsSynced += int64(len(rows))

		if opts.OnCheckpoint != nil {
			opts.OnCheckpoint(offset + int64(len(rows)))
		}
		if opts.OnProgress != nil {
			opts.OnProgress(rowsSynced, totalRows)
		}
	}

	return &SyncResult{RowsSynced: rowsSynced, Duration: time.Since(start)}, nil
}
```

**Step 4: 在文件顶部 import 中确认有 `"database/sql"`**

`internal/engine/data.go` 的 import 块中加入（若没有）：

```go
"database/sql"
```

**Step 5: 验证编译**

```bash
go build ./...
```
Expected: 无输出

**Step 6: 验证测试**

```bash
go test ./internal/engine/...
```
Expected: PASS

**Step 7: Commit**

```bash
git add internal/engine/data.go
git commit -m "feat: syncDataFromSQL — paginated SQL import via LIMIT/OFFSET subquery"
```

---

### Task 3: Executor — 处理 sql_import 同步类型

**Files:**
- Modify: `internal/service/executor.go`

**Context:**
- switch task.SyncType 约在第 212 行
- `case "both":` 约在第 244 行
- `default:` 约在第 277 行
- 在 `default:` 之前插入新 case

**Step 1: 在 switch 中插入 sql_import case**

在 `default: syncErr = fmt.Errorf("未知同步类型: %s", task.SyncType)` 之前插入：

```go
case "sql_import":
    e.emit(taskID, ProgressEvent{Phase: "data", Message: "正在执行 SQL 导入..."})
    opts := engine.DataSyncOptions{
        Source:      sourceDB,
        Target:      targetDB,
        SourceSQL:   task.SourceSQL,
        TargetTable: task.TargetTable,
        BatchSize:   1000,
        OnProgress:  makeOnProgress(),
        Concurrency: 1,
        StartOffset: task.CheckpointOffset,
        OnCheckpoint: func(offset int64) {
            e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).
                Update("checkpoint_offset", offset)
        },
    }
    if task.SyncMode == "upsert" {
        opts.WriteStrategy = "upsert"
    } else {
        opts.WriteStrategy = "insert"
    }
    var result *engine.SyncResult
    result, syncErr = engine.SyncData(ctx, opts)
    if result != nil {
        rowsSynced = result.RowsSynced
    }
```

**Step 2: 验证编译**

```bash
go build ./...
```
Expected: 无输出

**Step 3: Commit**

```bash
git add internal/service/executor.go
git commit -m "feat: executor handles sql_import sync type"
```

---

### Task 4: Handler — 解析 source_sql 表单字段

**Files:**
- Modify: `internal/handler/task.go`

**Context:**
- Create handler 约第 56-63 行赋值 task 字段（`task.Name = c.PostForm("name")` 等）
- Update handler 约第 160 行有同样的赋值块
- 两处都需要添加 `task.SourceSQL = c.PostForm("source_sql")`

**Step 1: Create handler 添加 SourceSQL 解析**

在 Create handler 的字段赋值块（`task.SyncType = c.PostForm("sync_type")` 附近）添加：

```go
task.SourceSQL = c.PostForm("source_sql")
```

**Step 2: Update handler 同样添加**

找到 Update handler 中对应的字段赋值块，同样添加：

```go
task.SourceSQL = c.PostForm("source_sql")
```

**Step 3: 验证编译**

```bash
go build ./...
```
Expected: 无输出

**Step 4: Commit**

```bash
git add internal/handler/task.go
git commit -m "feat: parse source_sql from task form in create and update handlers"
```

---

### Task 5: 表单 UI — SQL 导入类型 + 动态字段切换

**Files:**
- Modify: `templates/task/form.html`

**Context:**
- 同步类型 `<select name="sync_type">` 约第 235 行
- 源表 `<select name="source_table">` 约第 121 行，外层包了一个 `<div>`
- 同步模式 `<select name="sync_mode">` 约第 222 行
- JS 部分在文件末尾 `<script>` 标签中

**Step 1: sync_type select 新增 sql_import 选项**

在 `<option value="both" ...>全部同步</option>` 后追加：

```html
<option value="sql_import" {{if and .task (eq .task.SyncType "sql_import")}}selected{{end}}>SQL 导入</option>
```

**Step 2: 源表 div 加 id，方便 JS 控制显隐**

找到源表选择器的外层 `<div>`（`<label>源表</label>` 的父级），加上 `id="source_table_section"`：

```html
<div id="source_table_section">
    <label class="block text-sm font-medium text-gray-700 mb-1">源表</label>
    <select name="source_table" id="source_table" ...>
        ...
    </select>
</div>
```

**Step 3: 在源表 div 下方插入 SQL 编辑区（默认隐藏）**

紧接在 `id="source_table_section"` div 关闭标签之后插入：

```html
<div id="source_sql_section" class="hidden">
    <label class="block text-sm font-medium text-gray-700 mb-1">
        源 SQL
        <span class="text-xs text-gray-400 ml-1">（查询结果将分批写入目标表）</span>
    </label>
    <textarea name="source_sql" id="source_sql" rows="6"
        placeholder="SELECT id, name, amount FROM orders WHERE status = 'paid'"
        class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm font-mono focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500">{{if and .task .task.SourceSQL}}{{.task.SourceSQL}}{{end}}</textarea>
    <p class="text-xs text-gray-400 mt-1">系统会自动分页执行：SELECT * FROM (your SQL) _t LIMIT ? OFFSET ?</p>
</div>
```

**Step 4: sync_mode select 加 id**

找到 `<select name="sync_mode">`，加上 `id="sync_mode_select"`（原本已有 `id="sync_mode"`，确认有即可，无需修改）。

**Step 5: JS — updateSyncType 函数控制显隐**

在 `<script>` 中，`// Toggle cron section` 那行上方插入：

```javascript
// Toggle fields when sync_type changes
function updateSyncType() {
    var t = document.querySelector('select[name="sync_type"]').value;
    var isSQLImport = (t === 'sql_import');
    document.getElementById('source_table_section').classList.toggle('hidden', isSQLImport);
    document.getElementById('source_sql_section').classList.toggle('hidden', !isSQLImport);

    // sql_import 不支持实时模式
    var modeSelect = document.getElementById('sync_mode');
    var realtimeOpt = modeSelect.querySelector('option[value="realtime"]');
    if (isSQLImport) {
        if (modeSelect.value === 'realtime') modeSelect.value = 'manual';
        if (realtimeOpt) realtimeOpt.disabled = true;
    } else {
        if (realtimeOpt) realtimeOpt.disabled = false;
    }
}
document.querySelector('select[name="sync_type"]').addEventListener('change', updateSyncType);
// 页面加载时执行一次（编辑模式恢复状态）
updateSyncType();
```

**Step 6: 验证编译**

```bash
go build ./...
```
Expected: 无输出

**Step 7: 手动验证**

```bash
./datasync
```

- 打开 http://localhost:9090/tasks/new
- 同步类型选「SQL 导入」→ 源表隐藏，SQL 编辑区出现，实时模式不可选
- 切回「数据同步」→ 恢复源表，实时模式可选

**Step 8: Commit**

```bash
git add templates/task/form.html
git commit -m "feat: sql_import option in task form with dynamic source-table/sql-editor toggle"
```

---

### Task 6: 任务详情页显示 SourceSQL

**Files:**
- Modify: `templates/task/detail.html`

**Context:**
- 详情页在任务信息卡片中展示 SourceTable 等字段
- 需要在 SourceTable 旁边条件渲染 SourceSQL

**Step 1: 找到 SourceTable 显示位置**

在 `detail.html` 中找到显示源表的行，通常类似：

```html
<span>{{.task.SourceTable}}</span>
```

**Step 2: 在其旁边加条件渲染**

将该区域改为：

```html
{{if eq .task.SyncType "sql_import"}}
<div>
    <span class="text-xs text-gray-500">源 SQL</span>
    <pre class="mt-1 text-xs font-mono bg-gray-50 border border-gray-200 rounded p-2 whitespace-pre-wrap break-all">{{.task.SourceSQL}}</pre>
</div>
{{else}}
<div>
    <span class="text-xs text-gray-500">源表</span>
    <p class="font-medium text-gray-800">{{.task.SourceTable}}</p>
</div>
{{end}}
```

**Step 3: 验证编译**

```bash
go build ./...
```
Expected: 无输出

**Step 4: Commit**

```bash
git add templates/task/detail.html
git commit -m "feat: show source SQL in task detail page for sql_import tasks"
```
