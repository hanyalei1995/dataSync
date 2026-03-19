# SQL 导入设计文档

**日期：** 2026-03-19

## 目标

在现有同步任务体系中新增 `sql_import` 同步类型，允许用户在源数据源上执行自定义 SQL，将查询结果分批写入目标数据源的指定表。

## 核心决策

| 项目 | 决策 |
|------|------|
| 入口 | 现有「新建同步任务」表单，新增 `sql_import` 同步类型 |
| 分页方式 | 自动包装：`SELECT * FROM (user_sql) _t LIMIT ? OFFSET ?` |
| 总行数估算 | `SELECT COUNT(*) FROM (user_sql) _t`，用于进度上报 |
| 并发分片 | 不支持（自定义 SQL 无法安全范围分片） |
| 支持的同步模式 | 手动、定时（不含实时） |
| BatchSize | 复用现有字段，用户可在表单配置 |

## 数据模型变更

`SyncTask` 新增字段：

```go
SourceSQL string `gorm:"type:text" json:"source_sql"`
```

## 执行逻辑

`DataSyncOptions` 新增 `SourceSQL string` 字段。

`engine/data.go` 中 `SyncData` 判断：若 `opts.SourceSQL != ""`，走 SQL 导入路径：

1. `SELECT COUNT(*) FROM (opts.SourceSQL) _t` 获取 totalRows
2. 循环 `SELECT * FROM (opts.SourceSQL) _t LIMIT batchSize OFFSET offset`
3. 结果通过 `Target.WriteBatch` 写入目标表
4. 每批后调用 `OnProgress` 和 `OnCheckpoint`，支持断点续传

## 界面变更

- `templates/task/form.html`：同步类型下拉新增「SQL 导入」
- 选中 `sql_import` 时：
  - 隐藏源表下拉
  - 显示 SQL 编辑区（`<textarea>`，等宽字体）
- 同步模式选中 `sql_import` 时，JS 动态隐藏「实时」选项

## 不在范围内

- SQL 语法校验
- 并发分片
- 实时模式
