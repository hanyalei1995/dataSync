# Connector 接口重构 + 新数据源 + 断点续传 设计文档

**Goal:** 将所有数据源统一为 `Connector` 接口，支持 ClickHouse、Doris、MongoDB、CSV/Excel，并实现断点续传。

**Architecture:** 定义统一 `Connector` 接口替换原有 `*sql.DB` 直接传递；引擎层 `SyncData` 面向接口编程；各数据源实现各自的 Connector；`SyncTask` 增加 checkpoint 字段支持断点续传。

**Tech Stack:** Go, `clickhouse-go/v2`, `go-sql-driver/mysql`(Doris), `mongo-driver/v2`, `encoding/csv`, `excelize/v2`

---

## Connector 接口

```go
// internal/connector/connector.go

type Row = map[string]interface{}

type ReadOptions struct {
    Table   string
    Columns []string
    Where   string
    Offset  int64
    Limit   int64
}

type WriteOptions struct {
    Table    string
    Columns  []string
    Rows     []Row
    Strategy string   // "insert" | "upsert"
    PKCols   []string
}

type Schema struct {
    Columns []ColumnInfo
}

type ColumnInfo struct {
    Name      string
    Type      string
    Nullable  bool
    IsPrimary bool
}

type Connector interface {
    Ping(ctx context.Context) error
    ListTables(ctx context.Context) ([]string, error)
    GetSchema(ctx context.Context, table string) (*Schema, error)
    CountRows(ctx context.Context, table, where string) (int64, error)
    ReadBatch(ctx context.Context, opts ReadOptions) ([]Row, error)
    WriteBatch(ctx context.Context, opts WriteOptions) error
    Close() error
}
```

## Connector 实现

### SQLConnector (`internal/connector/sql.go`)
- 支持：MySQL、PostgreSQL、Oracle、ClickHouse、Doris
- 底层：`database/sql` + 各驱动
- ClickHouse：使用 `clickhouse-go/v2` 驱动，DSN: `clickhouse://host:port/db?username=&password=`
- Doris：MySQL 协议，DSN 同 MySQL
- WriteBatch upsert：MySQL=INSERT...ON DUPLICATE KEY UPDATE，PG=ON CONFLICT DO UPDATE，ClickHouse=ReplacingMergeTree（仅 insert），Doris=INSERT INTO...ON DUPLICATE KEY UPDATE

### MongoConnector (`internal/connector/mongo.go`)
- 支持：MongoDB 作为源和目标
- 底层：`go.mongodb.org/mongo-driver/v2`
- ListTables → 列出 collections
- GetSchema → 采样10条文档，推断顶层字段类型
- ReadBatch → Find with skip+limit，文档转 Row（顶层字段直接映射，嵌套字段用 `parent.child` key）
- WriteBatch insert → InsertMany；upsert → BulkWrite with ReplaceOne(upsert=true)

### FileConnector (`internal/connector/file.go`)
- 支持：CSV、Excel（.xlsx）作为源和目标
- 文件路径模式：直接读写服务器本地路径
- 上传模式：POST /api/upload 保存到临时目录，返回路径，再作为 FileConnector 路径
- ListTables：CSV返回文件名，Excel返回 sheet 名列表
- GetSchema：读第一行（header）推断列名，类型统一为 string
- ReadBatch：按行 offset/limit 读取
- WriteBatch：追加写入（insert）或全量覆盖（upsert 不适用文件，统一用 insert）

## 引擎层重构

### `internal/engine/data.go`
```go
// DataSyncOptions 改为使用 Connector
type DataSyncOptions struct {
    Source       connector.Connector
    Target       connector.Connector
    SourceTable  string
    TargetTable  string
    Mappings     []model.FieldMapping
    BatchSize    int
    WriteStrategy string
    OnProgress   func(synced, total int64)
    WhereClause  string
    Concurrency  int
    StartOffset  int64  // 断点续传起始位置
    OnCheckpoint func(offset int64)  // 每批完成后回调保存 checkpoint
}
```

### `internal/engine/schema.go`
- `SyncStructure` 改为接收 `connector.Connector` 源和目标
- 调用 `src.GetSchema()` 获取结构，调用 `dst.WriteBatch(DDL rows)` 或直接用 dst 的特殊建表方法

## 断点续传

### Model 变更
```go
// SyncTask 新增字段
CheckpointOffset int64 `gorm:"default:0"`
```

### 逻辑
1. 任务启动时：`opts.StartOffset = task.CheckpointOffset`
2. 每批写入完成后：`opts.OnCheckpoint(offset)` → `db.Update("checkpoint_offset", offset)`
3. 任务成功完成后：`db.Update("checkpoint_offset", 0)` 清零
4. 任务失败/中断：保留 checkpoint，下次从该 offset 继续

## UI 变更

### 数据源表单
根据 `db_type` 动态显示字段（JS 控制显示/隐藏）：

| 字段 | MySQL/PG/Oracle/CH/Doris | MongoDB | CSV/Excel |
|---|---|---|---|
| Host | ✓ | ✓ | - |
| Port | ✓ | ✓ | - |
| Username | ✓ | ✓ | - |
| Password | ✓ | ✓ | - |
| Database | ✓ | ✓ | - |
| Auth DB | - | ✓ | - |
| 文件路径 | - | - | ✓ |
| 上传按钮 | - | - | ✓ |

### 文件上传
- `POST /api/upload` 接收文件，保存到 `./uploads/` 目录，返回文件路径
- DataSource 的 `host` 字段存储文件路径（复用现有字段）

## 实现顺序

1. 定义 Connector 接口 + SQLConnector（迁移现有逻辑）
2. 重构 engine/data.go 使用 Connector
3. 重构 service/executor.go 创建 Connector
4. 实现 ClickHouse / Doris（SQLConnector 扩展）
5. 实现 MongoConnector
6. 实现 FileConnector + 文件上传 API
7. 断点续传
8. UI 更新（数据源表单 + 上传）
