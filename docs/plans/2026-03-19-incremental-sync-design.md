# 增量同步与自定义过滤条件 设计文档

## 目标

在现有全量同步基础上，支持两种数据过滤能力：
1. **自动水位线增量同步**：系统记录上次同步位置，下次仅同步新增/变更数据（支持时间戳字段和自增 ID 字段两种模式）
2. **自定义 WHERE 条件**：用户手写 SQL 条件，只同步满足条件的结果集

两者可单独使用，也可组合（AND 连接）。

## 数据模型

`SyncTask` 新增 4 个字段：

```go
FilterCondition    string `gorm:"type:text"`  // 自定义 WHERE 子句（不含 WHERE 关键字）
WatermarkColumn    string `gorm:"size:200"`   // 水位线字段名，如 updated_at / id
WatermarkType      string `gorm:"size:20"`    // "timestamp" | "id" | ""（空=不启用）
LastWatermarkValue string `gorm:"size:500"`   // 上次水位值：时间存 RFC3339，ID 存数字字符串
```

### WHERE 组合规则

| FilterCondition | WatermarkColumn | 实际 WHERE |
|---|---|---|
| 有 | 无 | `WHERE <FilterCondition>` |
| 无 | 有 | `WHERE col > lastValue` |
| 有 | 有 | `WHERE <FilterCondition> AND col > lastValue` |
| 无 | 无 | 无（全量，原有行为） |

### 水位值更新策略

- **timestamp 类型**：同步成功后，将本次同步**开始时间**（`syncStart`）写入 `LastWatermarkValue`（RFC3339 格式）。用开始时间而非结束时间，避免同步期间写入的新数据被漏掉。
- **id 类型**：同步成功后，将 `SyncResult.MaxWatermarkValue`（本批扫描到的最大 ID）写入 `LastWatermarkValue`。

## Engine 层

### DataSyncOptions 新增字段

```go
WhereClause string // 完整过滤条件，由 Executor 组装，不含 WHERE 关键字
```

### SyncData 修改点

1. COUNT 查询加 WHERE：`SELECT COUNT(*) FROM table WHERE <clause>`
2. 分批 SELECT 加 WHERE：`SELECT cols FROM table WHERE <clause> LIMIT x OFFSET y`
3. 对 id 水位线：分批扫描时追踪 `maxWatermarkValue`（读每行的水位列值，取最大）

### SyncResult 新增字段

```go
MaxWatermarkValue string // id 模式下返回本次同步扫描到的最大水位列值
```

## Executor 层

### buildWhereClause 函数

```go
func buildWhereClause(task *model.SyncTask, dbType string) string {
    var parts []string
    if task.FilterCondition != "" {
        parts = append(parts, task.FilterCondition)
    }
    if task.WatermarkColumn != "" && task.WatermarkType != "" && task.LastWatermarkValue != "" {
        col := quoteIdentifier(dbType, task.WatermarkColumn)
        switch task.WatermarkType {
        case "id":
            parts = append(parts, fmt.Sprintf("%s > %s", col, task.LastWatermarkValue))
        case "timestamp":
            parts = append(parts, fmt.Sprintf("%s > '%s'", col, task.LastWatermarkValue))
        }
    }
    return strings.Join(parts, " AND ")
}
```

### 同步后回写水位值

```go
// 在 goroutine 末尾同步成功后：
if syncErr == nil && task.WatermarkColumn != "" {
    newVal := ""
    switch task.WatermarkType {
    case "timestamp":
        newVal = syncStart.UTC().Format(time.RFC3339)
    case "id":
        if result != nil && result.MaxWatermarkValue != "" {
            newVal = result.MaxWatermarkValue
        }
    }
    if newVal != "" {
        e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).
            Update("last_watermark_value", newVal)
    }
}
```

## UI

### 任务表单（新增「高级过滤」区块）

折叠面板，位于同步类型/模式字段之后：

```
▶ 高级过滤（可选）

  [ ] 启用增量同步（水位线）
      水位线字段: [__________]
      水位线类型: [时间戳 ▼]  // 时间戳 | 自增ID
      当前水位值: 2024-03-01T10:00:00Z（只读标签，编辑模式显示）

  自定义过滤条件（WHERE 子句）:
  [textarea: status = 'active' AND region = 'CN']
  提示文字：直接写条件表达式，不需要 WHERE 关键字
```

### 任务详情页

在「任务信息」卡片新增两行：
- 过滤条件：`status = 'active'`（若有）
- 当前水位值：`2024-03-01T10:00:00Z`（若有）

## 不在本次范围

- 水位线列的自动发现（需要连接数据库探测）
- 多水位线列支持
- 水位线回滚/重置 UI（可手动编辑任务清空）
