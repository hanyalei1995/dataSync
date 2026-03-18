# 数据库表同步工具 - 设计文档

## 概述

一个基于 Go 的数据库表同步工具，支持跨数据源的表结构和数据同步，提供 Web 管理界面。

## 架构方案

单体应用架构：单个 Go 二进制，内嵌 HTML 模板，SQLite 存储元数据，内置调度器。

```
┌─────────────────────────────────────────────┐
│                  浏览器                       │
│         Go html/template + HTMX              │
└──────────────────┬──────────────────────────┘
                   │ HTTP
┌──────────────────▼──────────────────────────┐
│              Go Web Server (Gin)             │
├──────────┬───────────┬───────────┬──────────┤
│ 数据源管理 │ 同步任务管理 │ 调度管理   │ 用户认证  │
├──────────┴───────────┴───────────┴──────────┤
│               同步引擎                        │
│  ┌─────────┐ ┌──────────┐ ┌──────────────┐  │
│  │结构同步   │ │数据全量同步│ │实时监听(CDC) │  │
│  └─────────┘ └──────────┘ └──────────────┘  │
├─────────────────────────────────────────────┤
│            数据库驱动层                       │
│    MySQL    PostgreSQL    Oracle             │
├─────────────────────────────────────────────┤
│          元数据存储 (SQLite)                   │
│  数据源配置 | 任务定义 | 字段映射 | 执行日志    │
└─────────────────────────────────────────────┘
```

## 核心功能

- **数据源管理**：CRUD 数据源连接，支持连接测试
- **同步任务管理**：创建任务、选择源/目标、配置字段映射
- **同步引擎**：结构同步、全量数据同步、增量 CDC 同步
- **调度管理**：cron 定时任务、手动触发
- **实时监听**：MySQL binlog、PostgreSQL WAL
- **用户认证**：简单登录（用户名密码 + JWT session）

## 支持的数据库

- MySQL
- PostgreSQL
- Oracle

## 数据模型

### 用户表 `users`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| username | VARCHAR | 用户名，唯一 |
| password_hash | VARCHAR | bcrypt 加密密码 |
| created_at | DATETIME | 创建时间 |

### 数据源表 `datasources`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| name | VARCHAR | 数据源名称 |
| db_type | VARCHAR | mysql/postgresql/oracle |
| host | VARCHAR | 主机地址 |
| port | INTEGER | 端口 |
| username | VARCHAR | 数据库用户名 |
| password | VARCHAR | 加密存储的密码 |
| database_name | VARCHAR | 数据库名/SID |
| extra_params | TEXT | 额外连接参数(JSON) |
| created_by | INTEGER FK | 创建人 |
| created_at | DATETIME | 创建时间 |

### 同步任务表 `sync_tasks`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| name | VARCHAR | 任务名称 |
| source_ds_id | INTEGER FK | 源数据源（可空，支持临时连接） |
| target_ds_id | INTEGER FK | 目标数据源（可空） |
| source_config | TEXT | 临时连接信息(JSON)，ds_id 为空时使用 |
| target_config | TEXT | 临时连接信息(JSON) |
| source_table | VARCHAR | 源表名 |
| target_table | VARCHAR | 目标表名 |
| sync_type | VARCHAR | structure/data/both |
| sync_mode | VARCHAR | manual/cron/realtime |
| cron_expr | VARCHAR | cron 表达式（定时模式时使用） |
| status | VARCHAR | idle/running/error |
| created_by | INTEGER FK | 创建人 |
| created_at | DATETIME | 创建时间 |

### 字段映射表 `field_mappings`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| task_id | INTEGER FK | 所属任务 |
| source_field | VARCHAR | 源字段名 |
| target_field | VARCHAR | 目标字段名 |
| enabled | BOOLEAN | 是否启用该字段同步 |

### 执行日志表 `sync_logs`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增主键 |
| task_id | INTEGER FK | 所属任务 |
| start_time | DATETIME | 开始时间 |
| end_time | DATETIME | 结束时间 |
| status | VARCHAR | success/failed/running |
| rows_synced | INTEGER | 同步行数 |
| error_msg | TEXT | 错误信息 |

## 页面设计

| 页面 | 路径 | 功能 |
|------|------|------|
| 登录页 | `/login` | 用户名密码登录 |
| 仪表盘 | `/` | 任务概览、最近执行状态 |
| 数据源管理 | `/datasources` | 数据源列表、新增/编辑/删除、连接测试 |
| 同步任务列表 | `/tasks` | 任务列表、状态筛选、手动触发 |
| 创建/编辑任务 | `/tasks/new` `/tasks/:id/edit` | 选择源/目标数据源、选表、字段映射配置 |
| 任务详情 | `/tasks/:id` | 任务配置详情、执行日志列表 |
| 执行日志 | `/logs` | 全局日志查看、按任务/状态筛选 |

交互方式：HTMX 局部刷新，创建任务时选择数据源后动态加载表列表和字段列表。

## API 接口

```
# 认证
POST   /api/login              登录获取 token
POST   /api/logout             登出

# 数据源
GET    /api/datasources        数据源列表
POST   /api/datasources        新增数据源
PUT    /api/datasources/:id    编辑数据源
DELETE /api/datasources/:id    删除数据源
POST   /api/datasources/test   测试连接（支持已保存和临时连接）
GET    /api/datasources/:id/tables          获取表列表
GET    /api/datasources/:id/tables/:t/columns  获取字段列表

# 同步任务
GET    /api/tasks              任务列表
POST   /api/tasks              创建任务
PUT    /api/tasks/:id          编辑任务
DELETE /api/tasks/:id          删除任务
POST   /api/tasks/:id/run      手动触发执行
POST   /api/tasks/:id/stop     停止任务

# 字段映射
GET    /api/tasks/:id/mappings     获取字段映射
PUT    /api/tasks/:id/mappings     更新字段映射

# 执行日志
GET    /api/logs                全局日志列表
GET    /api/tasks/:id/logs      任务执行日志
```

## 同步引擎设计

### 结构同步

- 读取源表 DDL，转换为目标数据库方言（MySQL → PG、Oracle 等类型映射）
- 目标表不存在则创建，已存在则对比差异生成 ALTER 语句
- 类型映射表内置，覆盖常见类型（VARCHAR、INT、DECIMAL、TIMESTAMP 等）

### 全量数据同步

- 分批读取源表数据（默认每批 1000 行）
- 根据字段映射转换后批量写入目标表
- 支持两种写入策略：INSERT（跳过冲突）/ UPSERT（有则更新）
- 大表同步显示进度（已同步行数/总行数）

### 实时监听 (CDC)

- **MySQL**：使用 `go-mysql` 库监听 binlog，解析 row event
- **PostgreSQL**：使用 `pglogrepl` 库监听 logical replication
- 监听到变更后实时写入目标表，维护位点信息（断点续传）

### 调度

- `robfig/cron/v3` 管理定时任务
- 服务启动时从数据库加载所有 cron 任务并注册
- 任务变更时动态更新调度器

## 技术选型

| 组件 | 选择 | 说明 |
|------|------|------|
| Web 框架 | Gin | 轻量成熟 |
| 模板引擎 | html/template + HTMX | 无需前端构建 |
| ORM | GORM | 支持多数据库驱动 |
| 元数据存储 | SQLite | 零依赖部署 |
| 定时调度 | robfig/cron/v3 | Go 标准 cron 库 |
| MySQL CDC | go-mysql | binlog 解析 |
| PG CDC | pglogrepl | logical replication |
| Oracle 驱动 | godror | Oracle 官方 Go 驱动 |
| 认证 | JWT + bcrypt | 简单登录 |
| CSS | Tailwind CDN | 快速美观，无需构建 |
