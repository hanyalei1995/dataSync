# 数据质量校验 + 性能优化 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 连接池复用 + 并发分片大表同步 + 同步后数据质量校验（行数对比 + 抽样比对）。

**Architecture:** 三阶段实施：(1) ConnPool 缓存 `*sql.DB` 供 Executor 复用；(2) DataSyncOptions 新增 Concurrency，SyncData 内部自动选择串行/并发分片路径；(3) engine/quality.go 实现质量校验，Executor 同步后自动调用并写回 SyncLog。

**Tech Stack:** Go, database/sql, sync.WaitGroup/atomic, Go html/template, GORM AutoMigrate

---

### Task 1: D2 — 连接池 ConnPool

**Files:**
- Create: `internal/service/connpool.go`
- Modify: `internal/service/executor.go`
- Modify: `main.go`

**Step 1: 创建 connpool.go**

```go
package service

import (
	"crypto/md5"
	"database/sql"
	"datasync/internal/engine"
	"datasync/internal/model"
	"fmt"
	"sync"
	"time"
)

// ConnPool caches *sql.DB instances to avoid reconnecting on every task run.
type ConnPool struct {
	mu    sync.Mutex
	conns map[string]*sql.DB
}

func NewConnPool() *ConnPool {
	return &ConnPool{conns: make(map[string]*sql.DB)}
}

// Get returns a cached connection for the datasource, creating one if needed.
// The caller must NOT close the returned *sql.DB.
func (p *ConnPool) Get(ds model.DataSource) (*sql.DB, error) {
	key := dsKey(ds)
	p.mu.Lock()
	defer p.mu.Unlock()

	if db, ok := p.conns[key]; ok {
		if err := db.Ping(); err == nil {
			return db, nil
		}
		// Stale connection — close and reconnect
		db.Close()
		delete(p.conns, key)
	}

	db, err := engine.Connect(ds)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(3)
	db.SetConnMaxLifetime(30 * time.Minute)
	p.conns[key] = db
	return db, nil
}

// CloseAll closes all cached connections. Call on app shutdown.
func (p *ConnPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, db := range p.conns {
		db.Close()
		delete(p.conns, k)
	}
}

func dsKey(ds model.DataSource) string {
	if ds.ID != 0 {
		return fmt.Sprintf("id:%d", ds.ID)
	}
	h := md5.Sum([]byte(fmt.Sprintf("%s:%s:%d:%s:%s:%s",
		ds.DBType, ds.Host, ds.Port, ds.Username, ds.Password, ds.DatabaseName)))
	return fmt.Sprintf("hash:%x", h)
}
```

**Step 2: 在 Executor 结构体中添加 Pool 字段**

在 `internal/service/executor.go` 的 `Executor` 结构体中加入：
```go
Pool *ConnPool
```

**Step 3: 在 executor.go 的 Run() 中用 Pool 取连接**

找到：
```go
sourceDB, err := engine.Connect(sourceDS)
if err != nil {
    return fmt.Errorf("连接源数据库失败: %w", err)
}
targetDB, err := engine.Connect(targetDS)
if err != nil {
    sourceDB.Close()
    return fmt.Errorf("连接目标数据库失败: %w", err)
}
```

替换为：
```go
sourceDB, err := e.Pool.Get(sourceDS)
if err != nil {
    return fmt.Errorf("连接源数据库失败: %w", err)
}
targetDB, err := e.Pool.Get(targetDS)
if err != nil {
    return fmt.Errorf("连接目标数据库失败: %w", err)
}
```

**Step 4: 删除 goroutine 中的 defer Close()**

找到 goroutine 开头的两行：
```go
defer sourceDB.Close()
defer targetDB.Close()
```
删除这两行（连接由 Pool 管理，不在此关闭）。

**Step 5: 同样修改 CDC 分支中的 sourceDB.Close() / targetDB.Close() 调用**

在 CDC 错误分支中找到：
```go
sourceDB.Close()
targetDB.Close()
return fmt.Errorf("启动CDC监听失败: %w", err)
```
删除两个 Close() 调用（pool 管理生命周期）。

**Step 6: main.go 初始化 Pool 并在退出时关闭**

在 `main.go` 中，找到：
```go
executor := &service.Executor{
    DB:      db,
    DSSvc:   dsSvc,
    TaskSvc: taskSvc,
}
```
替换为：
```go
pool := service.NewConnPool()
executor := &service.Executor{
    DB:      db,
    DSSvc:   dsSvc,
    TaskSvc: taskSvc,
    Pool:    pool,
}
```

在 `main.go` 的 shutdown 段（`scheduler.Stop()` 之后）添加：
```go
pool.CloseAll()
```

**Step 7: 编译验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

Expected: 无报错

**Step 8: Commit**

```bash
git add internal/service/connpool.go internal/service/executor.go main.go
git commit -m "feat: add connection pool to reuse *sql.DB across task runs"
```

---

### Task 2: D1 — SyncTask 新增 Concurrency 字段 + 表单 UI

**Files:**
- Modify: `internal/model/models.go`
- Modify: `internal/handler/task.go`
- Modify: `templates/task/form.html`

**Step 1: 在 SyncTask 结构体添加 Concurrency 字段**

在 `LastWatermarkValue` 字段之后添加：
```go
Concurrency int `gorm:"default:1" json:"concurrency"` // 并发分片数，1=串行
```

**Step 2: handler Create() 中解析 concurrency**

在 `internal/handler/task.go` 的 `Create()` task 初始化里，在 `Status: "idle",` 之前添加：
```go
Concurrency: func() int {
    n, _ := strconv.Atoi(c.PostForm("concurrency"))
    if n < 1 { n = 1 }
    if n > 8 { n = 8 }
    return n
}(),
```

**Step 3: handler Update() 中解析 concurrency**

在 `task.FilterCondition = ...` 之前添加：
```go
if n, err := strconv.Atoi(c.PostForm("concurrency")); err == nil {
    if n < 1 { n = 1 }
    if n > 8 { n = 8 }
    task.Concurrency = n
}
```

**Step 4: 表单 UI 在「同步设置」grid 下方添加并发数输入**

在 `templates/task/form.html` 中，找到 `</div>` 关闭「同步设置」`grid grid-cols-2` 那个 div 之后，在 cron_section div 之前插入：

```html
<div>
    <label class="block text-sm font-medium text-gray-700 mb-1">并发分片数
        <span class="text-xs text-gray-400 ml-1">（1=串行，2-8=并发，大表提速）</span>
    </label>
    <input type="number" name="concurrency" min="1" max="8"
        value="{{if and .task .task.Concurrency}}{{.task.Concurrency}}{{else}}1{{end}}"
        class="w-32 border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500">
</div>
```

**Step 5: 编译验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

**Step 6: Commit**

```bash
git add internal/model/models.go internal/handler/task.go templates/task/form.html
git commit -m "feat: add concurrency field to SyncTask and form UI"
```

---

### Task 3: D1 — Engine 并发分片同步

**Files:**
- Modify: `internal/engine/data.go`

**Step 1: 在 DataSyncOptions 添加 Concurrency 字段**

在 `WhereClause string` 之后添加：
```go
Concurrency int // 0 or 1 = serial; >1 = parallel shards
```

**Step 2: 在 SyncData 末尾分支到并发路径**

在 `SyncData` 函数体最开头（opts defaults 之后、count 查询之前），插入：

```go
// Use parallel sharding if Concurrency > 1
if opts.Concurrency > 1 {
    return syncDataParallel(ctx, opts)
}
```

**Step 3: 添加 detectIntPK 辅助函数**

在文件末尾添加：

```go
// detectIntPK returns the name of a single integer primary key column, or "".
func detectIntPK(db *sql.DB, dbType, table string) string {
    schema, err := ReadTableSchema(db, dbType, table)
    if err != nil {
        return ""
    }
    for _, col := range schema.Columns {
        if !col.IsPrimary {
            continue
        }
        t := strings.ToLower(col.Type)
        if strings.Contains(t, "int") || strings.Contains(t, "serial") || strings.Contains(t, "number") {
            return col.Name
        }
    }
    return ""
}
```

**Step 4: 添加 syncDataParallel 函数**

```go
// syncDataParallel runs SyncData using N concurrent goroutines, each handling a shard.
func syncDataParallel(ctx context.Context, opts DataSyncOptions) (*SyncResult, error) {
    start := time.Now()
    n := opts.Concurrency

    pkCol := detectIntPK(opts.SourceDB, opts.SourceDBType, opts.SourceTable)

    // --- Range-based sharding (integer PK) ---
    if pkCol != "" {
        quotedPK := quoteIdentifier(opts.SourceDBType, pkCol)
        var minVal, maxVal int64

        baseWhere := ""
        if opts.WhereClause != "" {
            baseWhere = " WHERE " + opts.WhereClause
        }

        row := opts.SourceDB.QueryRowContext(ctx,
            fmt.Sprintf("SELECT COALESCE(MIN(%s),0), COALESCE(MAX(%s),0) FROM %s%s",
                quotedPK, quotedPK,
                quoteIdentifier(opts.SourceDBType, opts.SourceTable), baseWhere))
        if err := row.Scan(&minVal, &maxVal); err != nil || maxVal == 0 {
            // fallback to serial
            opts.Concurrency = 1
            return SyncData(ctx, opts)
        }

        step := (maxVal - minVal + int64(n)) / int64(n)
        shards := make([]string, 0, n)
        for i := 0; i < n; i++ {
            lo := minVal + int64(i)*step
            hi := lo + step
            var clause string
            if i == n-1 {
                clause = fmt.Sprintf("%s >= %d", quotedPK, lo)
            } else {
                clause = fmt.Sprintf("%s >= %d AND %s < %d", quotedPK, lo, quotedPK, hi)
            }
            if opts.WhereClause != "" {
                clause = opts.WhereClause + " AND " + clause
            }
            shards = append(shards, clause)
        }

        return runShards(ctx, opts, shards, start)
    }

    // --- Offset-based sharding (no int PK) ---
    countSQL := buildCountSQL(opts.SourceDBType, opts.SourceTable, opts.WhereClause)
    var total int64
    if err := opts.SourceDB.QueryRowContext(ctx, countSQL).Scan(&total); err != nil || total == 0 {
        opts.Concurrency = 1
        return SyncData(ctx, opts)
    }

    perShard := (total + int64(n) - 1) / int64(n)
    shardOpts := make([]DataSyncOptions, n)
    for i := 0; i < n; i++ {
        o := opts
        o.Concurrency = 1
        o.OnProgress = nil // aggregate below
        // Each shard uses a sub-slice via LIMIT/OFFSET baked into BatchSize + offset
        // Simpler: just run n independent serial syncs with offset ranges via WhereClause
        // We can't easily inject OFFSET into WhereClause; instead run serial with offset params
        // For offset sharding, fall back to serial for simplicity
        _ = perShard
        shardOpts[i] = o
    }
    // Offset sharding without PK is complex to implement correctly with the current
    // batch loop; fall back to serial to avoid duplicate rows.
    opts.Concurrency = 1
    return SyncData(ctx, opts)
}

// runShards executes each WHERE-clause shard concurrently and merges results.
func runShards(ctx context.Context, opts DataSyncOptions, shards []string, start time.Time) (*SyncResult, error) {
    type shardResult struct {
        rows int64
        err  error
    }
    results := make([]shardResult, len(shards))
    var wg sync.WaitGroup
    var totalSynced int64

    for i, clause := range shards {
        wg.Add(1)
        go func(idx int, whereClause string) {
            defer wg.Done()
            shardOpts := opts
            shardOpts.Concurrency = 1
            shardOpts.WhereClause = whereClause
            shardOpts.OnProgress = nil // individual shard progress not reported
            res, err := SyncData(ctx, shardOpts)
            if err != nil {
                results[idx] = shardResult{err: err}
                return
            }
            results[idx] = shardResult{rows: res.RowsSynced}
            atomic.AddInt64(&totalSynced, res.RowsSynced)
            if opts.OnProgress != nil {
                opts.OnProgress(atomic.LoadInt64(&totalSynced), 0)
            }
        }(i, clause)
    }
    wg.Wait()

    for _, r := range results {
        if r.err != nil {
            return &SyncResult{RowsSynced: totalSynced, Duration: time.Since(start)}, r.err
        }
    }
    return &SyncResult{RowsSynced: totalSynced, Duration: time.Since(start)}, nil
}
```

在文件顶部 import 中补充 `"sync/atomic"` 和 `"sync"`（检查是否已有，若无则添加）。

**Step 5: 编译验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

Expected: 无报错

**Step 6: 运行现有测试**

```bash
go test ./internal/engine/... -v
```

Expected: 所有测试通过

**Step 7: Commit**

```bash
git add internal/engine/data.go
git commit -m "feat: add parallel shard sync to SyncData (range-based on int PK)"
```

---

### Task 4: D1 — Executor 传递 Concurrency

**Files:**
- Modify: `internal/service/executor.go`

**Step 1: 在 "data" case 的 opts 中添加 Concurrency**

找到 `case "data":` 中的 opts，在 `WhereClause: buildWhereClause(task, sourceDS.DBType),` 之后添加：
```go
Concurrency: task.Concurrency,
```

**Step 2: 在 "both" case 的 opts 中同样添加**

找到 `case "both":` 中的 opts，同样添加 `Concurrency: task.Concurrency,`。

**Step 3: 编译 + 测试**

```bash
go build ./... && go test ./...
```

Expected: 无报错，测试全部通过

**Step 4: Commit**

```bash
git add internal/service/executor.go
git commit -m "feat: pass task.Concurrency to DataSyncOptions for parallel sharding"
```

---

### Task 5: B3 — SyncLog 新增质量字段 + SyncTask EnableQualityCheck

**Files:**
- Modify: `internal/model/models.go`

**Step 1: SyncLog 添加 5 个质量字段**

在 `ErrorMsg` 字段之后添加：
```go
SourceRows    int64  `gorm:"default:0" json:"source_rows"`
TargetRows    int64  `gorm:"default:0" json:"target_rows"`
SampleTotal   int    `gorm:"default:0" json:"sample_total"`
SampleMatched int    `gorm:"default:0" json:"sample_matched"`
QualityStatus string `gorm:"size:20" json:"quality_status"` // passed|warning|failed|""
```

**Step 2: SyncTask 添加 EnableQualityCheck 字段**

在 `Concurrency` 字段之后添加：
```go
EnableQualityCheck bool `gorm:"default:true" json:"enable_quality_check"`
```

**Step 3: 编译验证（AutoMigrate 在启动时自动加列）**

```bash
go build ./...
```

**Step 4: Commit**

```bash
git add internal/model/models.go
git commit -m "feat: add quality check fields to SyncLog and EnableQualityCheck to SyncTask"
```

---

### Task 6: B3 — Engine 质量校验

**Files:**
- Create: `internal/engine/quality.go`

**Step 1: 创建 quality.go**

```go
package engine

import (
	"context"
	"database/sql"
	"datasync/internal/model"
	"fmt"
	"math"
	"strings"
)

// QualityCheckOptions configures a data quality check.
type QualityCheckOptions struct {
	SourceDB     *sql.DB
	TargetDB     *sql.DB
	SourceDBType string
	TargetDBType string
	SourceTable  string
	TargetTable  string
	WhereClause  string // applied to source COUNT
	Mappings     []model.FieldMapping
	SampleSize   int // default 50
}

// QualityCheckResult holds the outcome of a quality check.
type QualityCheckResult struct {
	SourceRows    int64
	TargetRows    int64
	SampleTotal   int
	SampleMatched int
	Status        string // "passed" | "warning" | "failed"
}

// CheckQuality runs row-count comparison and random-sample field comparison.
func CheckQuality(ctx context.Context, opts QualityCheckOptions) (*QualityCheckResult, error) {
	if opts.SampleSize <= 0 {
		opts.SampleSize = 50
	}

	res := &QualityCheckResult{}

	// 1. Count source rows (with filter)
	countSQL := buildCountSQL(opts.SourceDBType, opts.SourceTable, opts.WhereClause)
	if err := opts.SourceDB.QueryRowContext(ctx, countSQL).Scan(&res.SourceRows); err != nil {
		return nil, fmt.Errorf("count source rows: %w", err)
	}

	// 2. Count target rows (total, no WHERE)
	tgtCountSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(opts.TargetDBType, opts.TargetTable))
	if err := opts.TargetDB.QueryRowContext(ctx, tgtCountSQL).Scan(&res.TargetRows); err != nil {
		return nil, fmt.Errorf("count target rows: %w", err)
	}

	// 3. Sample comparison (requires int PK)
	pkCol := detectIntPK(opts.SourceDB, opts.SourceDBType, opts.SourceTable)
	if pkCol != "" {
		matched, total, err := sampleCompare(ctx, opts, pkCol)
		if err == nil {
			res.SampleTotal = total
			res.SampleMatched = matched
		}
		// If sample fails, we still report row counts
	}

	// 4. Determine overall status
	res.Status = determineQualityStatus(res)
	return res, nil
}

// sampleCompare picks random PKs from source, fetches same rows from target, compares.
func sampleCompare(ctx context.Context, opts QualityCheckOptions, pkCol string) (matched, total int, err error) {
	quotedPK := quoteIdentifier(opts.SourceDBType, pkCol)
	tgtQuotedPK := quoteIdentifier(opts.TargetDBType, pkCol)

	// Random sample of PKs from source
	var sampleSQL string
	switch strings.ToLower(opts.SourceDBType) {
	case "postgresql":
		sampleSQL = fmt.Sprintf("SELECT %s FROM %s ORDER BY RANDOM() LIMIT %d",
			quotedPK, quoteIdentifier(opts.SourceDBType, opts.SourceTable), opts.SampleSize)
	default: // mysql, oracle
		sampleSQL = fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT %d",
			quotedPK, quoteIdentifier(opts.SourceDBType, opts.SourceTable), opts.SampleSize)
	}

	rows, err := opts.SourceDB.QueryContext(ctx, sampleSQL)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var pkVals []interface{}
	for rows.Next() {
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			continue
		}
		pkVals = append(pkVals, v)
	}
	if len(pkVals) == 0 {
		return 0, 0, nil
	}

	// Build column list from mappings
	sourceCols, _ := columnsFromMappings(opts.Mappings)
	if len(sourceCols) == 0 {
		cols, err := getSourceColumns(opts.SourceDB, opts.SourceDBType, opts.SourceTable)
		if err != nil {
			return 0, 0, err
		}
		sourceCols = cols
	}

	// Fetch source rows for sampled PKs
	placeholders := make([]string, len(pkVals))
	for i := range pkVals {
		placeholders[i] = "?"
		if strings.ToLower(opts.SourceDBType) == "postgresql" {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
	}
	srcFetchSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
		buildColList(opts.SourceDBType, sourceCols),
		quoteIdentifier(opts.SourceDBType, opts.SourceTable),
		quotedPK,
		strings.Join(placeholders, ","))

	srcRows, err := opts.SourceDB.QueryContext(ctx, srcFetchSQL, pkVals...)
	if err != nil {
		return 0, 0, err
	}
	srcData, err := scanRows(srcRows, len(sourceCols))
	srcRows.Close()
	if err != nil {
		return 0, 0, err
	}

	// Fetch target rows for same PKs
	tgtPlaceholders := make([]string, len(pkVals))
	for i := range pkVals {
		tgtPlaceholders[i] = "?"
		if strings.ToLower(opts.TargetDBType) == "postgresql" {
			tgtPlaceholders[i] = fmt.Sprintf("$%d", i+1)
		}
	}
	_, targetCols := columnsFromMappings(opts.Mappings)
	if len(targetCols) == 0 {
		targetCols = sourceCols
	}
	tgtFetchSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
		buildColList(opts.TargetDBType, targetCols),
		quoteIdentifier(opts.TargetDBType, opts.TargetTable),
		tgtQuotedPK,
		strings.Join(tgtPlaceholders, ","))

	tgtRows, err := opts.TargetDB.QueryContext(ctx, tgtFetchSQL, pkVals...)
	if err != nil {
		return 0, 0, err
	}
	tgtData, err := scanRows(tgtRows, len(targetCols))
	tgtRows.Close()
	if err != nil {
		return 0, 0, err
	}

	// Build target lookup by PK index (first col assumed to be PK or find by name)
	pkIdx := 0
	for i, c := range sourceCols {
		if strings.EqualFold(c, pkCol) {
			pkIdx = i
			break
		}
	}
	tgtMap := make(map[interface{}][]interface{})
	for _, row := range tgtData {
		if len(row) > pkIdx {
			tgtMap[fmt.Sprintf("%v", row[pkIdx])] = row
		}
	}

	total = len(srcData)
	for _, srcRow := range srcData {
		if len(srcRow) <= pkIdx {
			continue
		}
		pk := fmt.Sprintf("%v", srcRow[pkIdx])
		tgtRow, ok := tgtMap[pk]
		if !ok {
			continue
		}
		if rowsMatch(srcRow, tgtRow) {
			matched++
		}
	}
	return matched, total, nil
}

func buildColList(dbType string, cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quoteIdentifier(dbType, c)
	}
	return strings.Join(quoted, ", ")
}

// rowsMatch compares two rows field by field, tolerating float precision differences.
func rowsMatch(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		av := fmt.Sprintf("%v", a[i])
		bv := fmt.Sprintf("%v", b[i])
		if av == bv {
			continue
		}
		// Tolerate small float differences
		var af, bf float64
		if _, err := fmt.Sscanf(av, "%f", &af); err == nil {
			if _, err := fmt.Sscanf(bv, "%f", &bf); err == nil {
				if math.Abs(af-bf) < 0.0001 {
					continue
				}
			}
		}
		return false
	}
	return true
}

func determineQualityStatus(r *QualityCheckResult) string {
	// Row count check
	if r.SourceRows > 0 {
		diff := float64(r.SourceRows-r.TargetRows) / float64(r.SourceRows)
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.01 { // >1% difference
			return "failed"
		}
		if diff > 0 {
			return "warning"
		}
	}
	// Sample check
	if r.SampleTotal > 0 {
		ratio := float64(r.SampleMatched) / float64(r.SampleTotal)
		if ratio < 0.98 {
			return "failed"
		}
	}
	return "passed"
}
```

**Step 2: 编译验证**

```bash
go build ./...
```

Expected: 无报错

**Step 3: Commit**

```bash
git add internal/engine/quality.go
git commit -m "feat: add CheckQuality engine for row count + sample comparison"
```

---

### Task 7: B3 — Executor 同步后触发质量校验

**Files:**
- Modify: `internal/service/executor.go`

**Step 1: 在同步成功后运行质量校验**

在 `executor.go` 中，找到水位线回写代码块末尾（`if newWatermark != ""` 块结束之后），添加：

```go
// Run quality check after successful sync
if syncErr == nil && task.EnableQualityCheck && task.SyncType != "structure" {
    qOpts := engine.QualityCheckOptions{
        SourceDB:     sourceDB,
        TargetDB:     targetDB,
        SourceDBType: sourceDS.DBType,
        TargetDBType: targetDS.DBType,
        SourceTable:  task.SourceTable,
        TargetTable:  task.TargetTable,
        WhereClause:  buildWhereClause(task, sourceDS.DBType),
        Mappings:     mappings,
        SampleSize:   50,
    }
    if qRes, qErr := engine.CheckQuality(ctx, qOpts); qErr == nil {
        e.DB.Model(&model.SyncLog{}).Where("id = ?", syncLog.ID).Updates(map[string]interface{}{
            "source_rows":    qRes.SourceRows,
            "target_rows":    qRes.TargetRows,
            "sample_total":   qRes.SampleTotal,
            "sample_matched": qRes.SampleMatched,
            "quality_status": qRes.Status,
        })
    }
}
```

确认 import 中有 `"datasync/internal/engine"`（已有）。

**Step 2: 编译验证**

```bash
go build ./...
```

**Step 3: Commit**

```bash
git add internal/service/executor.go
git commit -m "feat: run quality check after successful data sync and store in SyncLog"
```

---

### Task 8: B3 — 手动触发质量校验 API

**Files:**
- Modify: `internal/handler/task.go`
- Modify: `main.go`

**Step 1: 在 task.go 添加 Verify handler**

在文件末尾添加：

```go
// Verify runs a data quality check on the task's last sync log and returns results.
func (h *TaskHandler) Verify(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	taskID := uint(id)

	task, err := h.TaskService.GetByID(taskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
		return
	}

	sourceDS, err := h.DataSourceService.resolveForVerify(task)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	targetDS, err := h.DataSourceService.resolveTargetForVerify(task)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	sourceDB, err := h.Executor.Pool.Get(sourceDS)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "连接源库失败: " + err.Error()})
		return
	}
	targetDB, err := h.Executor.Pool.Get(targetDS)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "连接目标库失败: " + err.Error()})
		return
	}

	mappings, _ := h.TaskService.GetMappings(taskID)
	qOpts := engine.QualityCheckOptions{
		SourceDB:     sourceDB,
		TargetDB:     targetDB,
		SourceDBType: sourceDS.DBType,
		TargetDBType: targetDS.DBType,
		SourceTable:  task.SourceTable,
		TargetTable:  task.TargetTable,
		Mappings:     mappings,
		SampleSize:   50,
	}

	qRes, err := engine.CheckQuality(c.Request.Context(), qOpts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Update the latest SyncLog for this task
	var latestLog model.SyncLog
	if h.TaskService.DB.Where("task_id = ?", taskID).Order("id desc").First(&latestLog).Error == nil {
		h.TaskService.DB.Model(&latestLog).Updates(map[string]interface{}{
			"source_rows":    qRes.SourceRows,
			"target_rows":    qRes.TargetRows,
			"sample_total":   qRes.SampleTotal,
			"sample_matched": qRes.SampleMatched,
			"quality_status": qRes.Status,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"source_rows":    qRes.SourceRows,
		"target_rows":    qRes.TargetRows,
		"sample_total":   qRes.SampleTotal,
		"sample_matched": qRes.SampleMatched,
		"status":         qRes.Status,
	})
}
```

注意：`h.TaskService.DB` 需要在 `TaskService` 中暴露 `DB` 字段，或者通过已有方法访问。检查 `internal/service/task.go` 中 `TaskService` 的结构体，确认 `DB *gorm.DB` 字段是否导出。若未导出需添加。

另外 `resolveForVerify` / `resolveTargetForVerify` 是辅助方法，实际上可以直接复用 executor 中的 `resolveDataSource` 逻辑。更简单的做法是在 handler 里直接调用：

```go
// 替代方案：直接使用 DataSourceService 查询
func resolveDS(h *TaskHandler, dsID *uint, config string) (model.DataSource, error) {
    if dsID != nil {
        ds, err := h.DataSourceService.GetByID(*dsID)
        if err != nil { return model.DataSource{}, err }
        return *ds, nil
    }
    if config != "" {
        var ds model.DataSource
        if err := json.Unmarshal([]byte(config), &ds); err != nil {
            return model.DataSource{}, err
        }
        return ds, nil
    }
    return model.DataSource{}, fmt.Errorf("未配置数据源")
}
```

在 Verify() 中用这个替代上面两个假方法。

**Step 2: Verify handler 还需要 engine import**

确认 `internal/handler/task.go` import 中添加：
```go
"datasync/internal/engine"
"datasync/internal/model"
"encoding/json"
```
（其中 json 和 model 已有，engine 可能需要添加）

**Step 3: main.go 注册路由**

在 `api.GET("/tasks/:id/progress/stream", taskHandler.ProgressStream)` 之后添加：
```go
api.POST("/tasks/:id/verify", taskHandler.Verify)
```

**Step 4: 编译验证**

```bash
go build ./...
```

**Step 5: Commit**

```bash
git add internal/handler/task.go main.go
git commit -m "feat: add POST /api/tasks/:id/verify handler for manual quality check"
```

---

### Task 9: B3 — 日志列表质量徽章 UI

**Files:**
- Modify: `templates/log/list.html`

**Step 1: 在日志表格 thead 添加「质量」列**

找到：
```html
<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">错误信息</th>
```
之后添加：
```html
<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">数据质量</th>
```

**Step 2: 在每行 tbody 添加质量徽章单元格**

找到：
```html
<td class="px-6 py-4 text-sm text-gray-600 max-w-xs truncate" title="{{.ErrorMsg}}">{{.ErrorMsg}}</td>
```
之后添加：
```html
<td class="px-6 py-4 text-sm">
    {{if eq .QualityStatus "passed"}}
    <span class="px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">✓ 通过</span>
    {{else if eq .QualityStatus "warning"}}
    <span class="px-2 py-1 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">⚠ 行数差异</span>
    {{else if eq .QualityStatus "failed"}}
    <span class="px-2 py-1 rounded-full text-xs font-medium bg-red-100 text-red-800">✗ 不一致</span>
    {{else}}
    <span class="text-gray-300 text-xs">—</span>
    {{end}}
</td>
```

**Step 3: 修改 colspan（暂无数据那行）**

找到 `colspan="7"` 改为 `colspan="8"`。

**Step 4: 确认 LogHandler 查询返回了新字段**

查看 `internal/handler/log.go`，确认 `SyncLog` 查询 `SELECT *` 或明确包含新字段。GORM 默认 `SELECT *` 会包含新列。

**Step 5: SyncLog 的 LogRow 视图模型是否需要更新**

检查 log handler 是否使用了 DTO 结构体（如 `LogRow`）。若有，需在 DTO 中添加 `QualityStatus string`。若直接用 `model.SyncLog`，则不需要改动。

**Step 6: 编译验证**

```bash
go build ./...
```

**Step 7: Commit**

```bash
git add templates/log/list.html
git commit -m "feat: add quality badge column to sync log list"
```

---

### Task 10: B3 — 任务详情「数据校验」卡片 + 全量测试

**Files:**
- Modify: `templates/task/detail.html`
- Modify: `internal/handler/task.go` (传递 quality 数据到模板)

**Step 1: Detail handler 传递最近一次日志的质量数据**

在 `internal/handler/task.go` 的 `Detail()` 中，找到：
```go
logs, _ := h.TaskService.GetLogs(task.ID, 10)
```
之后添加：
```go
// Latest log for quality card
var latestLog *model.SyncLog
if len(logs) > 0 {
    latestLog = &logs[0]
}
```

在 `c.HTML(...)` 的 gin.H map 中添加：
```go
"latestLog": latestLog,
```

**Step 2: 在 detail.html 进度面板之后添加「数据校验」卡片**

找到 `<!-- Field Mappings -->` 注释之前，插入：

```html
<!-- Data Quality Card -->
<div class="bg-white rounded-xl shadow p-6 mb-6">
    <div class="flex justify-between items-center mb-4">
        <h2 class="text-lg font-bold text-gray-800">数据校验</h2>
        <button id="verify-btn"
            class="bg-indigo-600 text-white px-4 py-1.5 rounded-lg text-sm hover:bg-indigo-700 transition">
            立即校验
        </button>
    </div>

    <div id="quality-result">
    {{if .latestLog}}
        {{if .latestLog.QualityStatus}}
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-3">
            <div class="text-center p-3 bg-gray-50 rounded-lg">
                <div class="text-xs text-gray-500 mb-1">源表行数</div>
                <div class="text-lg font-bold text-gray-900">{{.latestLog.SourceRows}}</div>
            </div>
            <div class="text-center p-3 bg-gray-50 rounded-lg">
                <div class="text-xs text-gray-500 mb-1">目标表行数</div>
                <div class="text-lg font-bold text-gray-900">{{.latestLog.TargetRows}}</div>
            </div>
            <div class="text-center p-3 bg-gray-50 rounded-lg">
                <div class="text-xs text-gray-500 mb-1">抽样匹配</div>
                <div class="text-lg font-bold text-gray-900">{{.latestLog.SampleMatched}}/{{.latestLog.SampleTotal}}</div>
            </div>
            <div class="text-center p-3 rounded-lg
                {{if eq .latestLog.QualityStatus "passed"}}bg-green-50
                {{else if eq .latestLog.QualityStatus "warning"}}bg-yellow-50
                {{else}}bg-red-50{{end}}">
                <div class="text-xs text-gray-500 mb-1">校验结果</div>
                <div class="text-lg font-bold
                    {{if eq .latestLog.QualityStatus "passed"}}text-green-700
                    {{else if eq .latestLog.QualityStatus "warning"}}text-yellow-700
                    {{else}}text-red-700{{end}}">
                    {{if eq .latestLog.QualityStatus "passed"}}✓ 通过
                    {{else if eq .latestLog.QualityStatus "warning"}}⚠ 差异
                    {{else}}✗ 不一致{{end}}
                </div>
            </div>
        </div>
        {{else}}
        <p class="text-sm text-gray-400">尚无校验记录，点击「立即校验」运行</p>
        {{end}}
    {{else}}
    <p class="text-sm text-gray-400">尚无同步记录</p>
    {{end}}
    </div>
</div>
```

**Step 3: 在 detail.html `<script>` 块中添加「立即校验」JS**

在 `// ── SSE Progress ──` 注释之前插入：

```javascript
// ── Quality Verify ──────────────────────────────────────
var verifyBtn = document.getElementById('verify-btn');
if (verifyBtn) {
    verifyBtn.addEventListener('click', function() {
        verifyBtn.disabled = true;
        verifyBtn.textContent = '校验中...';
        fetch('/api/tasks/' + taskID + '/verify', { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    document.getElementById('quality-result').innerHTML =
                        '<p class="text-sm text-red-600">' + data.error + '</p>';
                    return;
                }
                var statusClass = data.status === 'passed' ? 'text-green-700' :
                                  data.status === 'warning' ? 'text-yellow-700' : 'text-red-700';
                var statusText = data.status === 'passed' ? '✓ 通过' :
                                 data.status === 'warning' ? '⚠ 差异' : '✗ 不一致';
                document.getElementById('quality-result').innerHTML =
                    '<div class="grid grid-cols-2 md:grid-cols-4 gap-4">' +
                    '<div class="text-center p-3 bg-gray-50 rounded-lg"><div class="text-xs text-gray-500 mb-1">源表行数</div><div class="text-lg font-bold">' + data.source_rows + '</div></div>' +
                    '<div class="text-center p-3 bg-gray-50 rounded-lg"><div class="text-xs text-gray-500 mb-1">目标表行数</div><div class="text-lg font-bold">' + data.target_rows + '</div></div>' +
                    '<div class="text-center p-3 bg-gray-50 rounded-lg"><div class="text-xs text-gray-500 mb-1">抽样匹配</div><div class="text-lg font-bold">' + data.sample_matched + '/' + data.sample_total + '</div></div>' +
                    '<div class="text-center p-3 rounded-lg"><div class="text-xs text-gray-500 mb-1">校验结果</div><div class="text-lg font-bold ' + statusClass + '">' + statusText + '</div></div>' +
                    '</div>';
            })
            .catch(function() {
                document.getElementById('quality-result').innerHTML =
                    '<p class="text-sm text-red-600">请求失败</p>';
            })
            .finally(function() {
                verifyBtn.disabled = false;
                verifyBtn.textContent = '立即校验';
            });
    });
}
// ── End Quality Verify ────────────────────────────────────
```

**Step 4: 编译验证**

```bash
go build ./...
```

**Step 5: 运行全量测试**

```bash
go test ./... -v
```

Expected: 所有测试通过

**Step 6: Commit**

```bash
git add templates/task/detail.html internal/handler/task.go
git commit -m "feat: add data quality card to task detail with manual verify trigger"
```

---

Plan complete and saved to `docs/plans/2026-03-19-quality-and-perf.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

**Which approach?**
