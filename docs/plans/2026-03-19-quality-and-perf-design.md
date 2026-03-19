# 数据质量校验 + 性能优化 设计文档

## 目标

在现有同步基础上，新增三项能力：
1. **D2 连接池**：复用数据库连接，避免每次任务运行重新握手
2. **D1 并发分片同步**：大表按主键范围或偏移分片并发同步，提升吞吐
3. **B3 数据质量校验**：同步后自动行数对比 + 抽样数据比对，可手动触发

实现顺序：D2 → D1 → B3（连接池是并发分片的基础设施）

---

## D2 连接池

### 问题
`engine.Connect()` 每次调用都新建连接，任务结束后立即关闭。cron 任务频繁运行时浪费握手时间。

### 设计
新增 `internal/service/connpool.go`，内部用 `sync.Map` 缓存 `*sql.DB`，key 为数据源唯一标识（已保存数据源用 ID，临时连接用连接字符串 hash）。

连接池参数：
- `MaxOpenConns`: 10
- `MaxIdleConns`: 3
- `ConnMaxLifetime`: 30分钟

`Executor.Run()` 改为从池里取连接，goroutine 结束后**不 Close()**。应用退出时池统一关闭。

### 影响范围
- 新增 `internal/service/connpool.go`
- 修改 `internal/service/executor.go`（取连接方式）
- `main.go` 退出时调用 `pool.CloseAll()`

---

## D1 并发分片同步

### 数据模型
`SyncTask` 新增字段：
```go
Concurrency int `gorm:"default:1"` // 并发数，1=不分片，最大8
```

### 分片策略（自动选择）

| 条件 | 策略 |
|------|------|
| 源表有整型主键 | 范围分片：`WHERE pk >= x AND pk < y`，按 MIN/MAX 均分 N 段 |
| 无整型主键 | 偏移分片：总行数除以 N，每段独立 OFFSET/LIMIT |

### 执行流程
1. `Concurrency <= 1` 时走原有串行逻辑，不变
2. `Concurrency > 1` 时：
   - 查主键信息，决定分片策略
   - 计算 N 个分片范围
   - 启动 N 个 goroutine，共享连接池中的连接
   - `rowsSynced` 原子累加，统一上报进度
3. 与水位线/自定义 WHERE 兼容：分片条件与现有 WhereClause 用 AND 合并

### UI
任务表单「同步设置」区域新增「并发数」输入框（1-8，默认1）

---

## B3 数据质量校验

### 数据模型
`SyncLog` 新增字段：
```go
SourceRows    int64  `gorm:"default:0"`  // 源表行数（带 WHERE）
TargetRows    int64  `gorm:"default:0"`  // 目标表行数（同步后）
SampleTotal   int    `gorm:"default:0"`  // 抽样总行数
SampleMatched int    `gorm:"default:0"`  // 抽样匹配行数
QualityStatus string `gorm:"size:20"`   // passed | warning | failed | ""
```

### 校验逻辑

**行数校验**：
- 源表：`SELECT COUNT(*) FROM source WHERE <whereClause>`
- 目标表：`SELECT COUNT(*) FROM target`（不加 WHERE，校验目标表总量）
- `SourceRows == TargetRows` → passed；差值 < 1% → warning；否则 → failed

**抽样对比**（需要有主键）：
- 源表随机取 50 行主键：`SELECT pk FROM source ORDER BY RAND() LIMIT 50`（MySQL）/ `TABLESAMPLE`（PG）
- 按这些主键从目标表查对应行
- 逐字段比对（忽略浮点精度差异 < 0.0001）
- `SampleMatched / SampleTotal < 0.98` → failed

**最终 QualityStatus**：行数和抽样都 passed → passed；任一 warning → warning；任一 failed → failed

### 触发方式
- **自动**：每次同步完成后自动执行（可在任务配置中关闭，新增 `EnableQualityCheck bool` 字段）
- **手动**：`POST /api/tasks/:id/verify` → 查最近一条 SyncLog，重新跑校验，更新结果

### UI
- **同步日志列表**：每行末尾显示质量徽章（✅ passed / ⚠️ warning / ❌ failed）
- **任务详情「数据校验」卡片**：
  - 显示最近一次校验的行数对比（源 / 目标 / 差值）
  - 抽样结果（50 行中 X 行一致）
  - 「立即校验」按钮（手动触发，结果实时刷新）

---

## 不在本次范围
- 字段级数据分布统计（min/max/null率）
- 跨库 JOIN 校验
- 校验历史趋势图表
