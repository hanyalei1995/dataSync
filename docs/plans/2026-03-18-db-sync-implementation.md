# DataSync - 数据库表同步工具实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 构建一个 Go 单体应用，支持 MySQL/PostgreSQL/Oracle 跨数据源的表结构和数据同步，提供 Web 管理界面。

**Architecture:** 单个 Go 二进制，Gin 提供 HTTP 服务，html/template + HTMX 渲染页面，GORM 操作数据库，SQLite 存储元数据。同步引擎支持结构同步、全量数据同步和 CDC 实时监听。robfig/cron 管理定时调度。

**Tech Stack:** Go 1.22+, Gin, GORM, SQLite, HTMX, Tailwind CSS CDN, go-mysql, pglogrepl, godror, robfig/cron/v3, JWT, bcrypt

**Design doc:** `docs/plans/2026-03-18-db-sync-design.md`

---

## Task 1: 项目脚手架和目录结构

**Files:**
- Create: `go.mod`
- Create: `main.go`
- Create: `internal/config/config.go`
- Create: `internal/model/models.go`
- Create: `internal/database/sqlite.go`

**Step 1: 初始化 Go module**

```bash
cd /Users/mac/Documents/data
go mod init datasync
```

**Step 2: 创建目录结构**

```bash
mkdir -p internal/{config,model,database,handler,service,engine,middleware,cdc}
mkdir -p templates/{layout,datasource,task,log,auth}
mkdir -p static
```

**Step 3: 编写配置模块**

Create `internal/config/config.go`:
```go
package config

type Config struct {
	Port       int    `json:"port"`
	DBPath     string `json:"db_path"`
	JWTSecret  string `json:"jwt_secret"`
	AdminUser  string `json:"admin_user"`
	AdminPass  string `json:"admin_pass"`
}

func Default() *Config {
	return &Config{
		Port:      8080,
		DBPath:    "datasync.db",
		JWTSecret: "change-me-in-production",
		AdminUser: "admin",
		AdminPass: "admin123",
	}
}
```

**Step 4: 编写数据模型**

Create `internal/model/models.go` — 定义 User, DataSource, SyncTask, FieldMapping, SyncLog 五个 GORM model，与设计文档数据模型一一对应。

```go
package model

import "time"

type User struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	Username     string    `gorm:"uniqueIndex;size:100" json:"username"`
	PasswordHash string    `gorm:"size:255" json:"-"`
	CreatedAt    time.Time `json:"created_at"`
}

type DataSource struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	Name         string    `gorm:"size:100" json:"name"`
	DBType       string    `gorm:"size:20" json:"db_type"`
	Host         string    `gorm:"size:255" json:"host"`
	Port         int       `json:"port"`
	Username     string    `gorm:"size:100" json:"username"`
	Password     string    `gorm:"size:255" json:"password"`
	DatabaseName string    `gorm:"size:100" json:"database_name"`
	ExtraParams  string    `gorm:"type:text" json:"extra_params"`
	CreatedBy    uint      `json:"created_by"`
	CreatedAt    time.Time `json:"created_at"`
}

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
	CreatedBy    uint      `json:"created_by"`
	CreatedAt    time.Time `json:"created_at"`
}

type FieldMapping struct {
	ID          uint   `gorm:"primaryKey" json:"id"`
	TaskID      uint   `gorm:"index" json:"task_id"`
	SourceField string `gorm:"size:200" json:"source_field"`
	TargetField string `gorm:"size:200" json:"target_field"`
	Enabled     bool   `gorm:"default:true" json:"enabled"`
}

type SyncLog struct {
	ID        uint       `gorm:"primaryKey" json:"id"`
	TaskID    uint       `gorm:"index" json:"task_id"`
	StartTime time.Time  `json:"start_time"`
	EndTime   *time.Time `json:"end_time"`
	Status    string     `gorm:"size:20" json:"status"`
	RowsSynced int64     `json:"rows_synced"`
	ErrorMsg  string     `gorm:"type:text" json:"error_msg"`
}
```

**Step 5: 编写 SQLite 初始化**

Create `internal/database/sqlite.go`:
```go
package database

import (
	"datasync/internal/config"
	"datasync/internal/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func Init(cfg *config.Config) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(cfg.DBPath), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	err = db.AutoMigrate(
		&model.User{},
		&model.DataSource{},
		&model.SyncTask{},
		&model.FieldMapping{},
		&model.SyncLog{},
	)
	return db, err
}
```

**Step 6: 编写 main.go 入口**

Create `main.go` — 加载配置、初始化数据库、启动 Gin server。

```go
package main

import (
	"datasync/internal/config"
	"datasync/internal/database"
	"fmt"
	"log"
)

func main() {
	cfg := config.Default()
	db, err := database.Init(cfg)
	if err != nil {
		log.Fatal("failed to init database:", err)
	}
	_ = db
	fmt.Printf("DataSync server starting on :%d\n", cfg.Port)
}
```

**Step 7: 安装依赖并验证编译**

```bash
go get gorm.io/gorm gorm.io/driver/sqlite github.com/gin-gonic/gin
go build ./...
```

**Step 8: Commit**

```bash
git init
git add .
git commit -m "feat: project scaffold with config, models, and SQLite init"
```

---

## Task 2: 用户认证 — JWT 中间件和登录接口

**Files:**
- Create: `internal/middleware/auth.go`
- Create: `internal/handler/auth.go`
- Create: `internal/service/user.go`
- Modify: `main.go`

**Step 1: 编写用户 service**

Create `internal/service/user.go`:
```go
package service

import (
	"datasync/internal/model"
	"errors"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type UserService struct {
	DB *gorm.DB
}

func (s *UserService) EnsureAdmin(username, password string) error {
	var user model.User
	err := s.DB.Where("username = ?", username).First(&user).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		hash, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		return s.DB.Create(&model.User{
			Username:     username,
			PasswordHash: string(hash),
		}).Error
	}
	return err
}

func (s *UserService) Authenticate(username, password string) (*model.User, error) {
	var user model.User
	if err := s.DB.Where("username = ?", username).First(&user).Error; err != nil {
		return nil, errors.New("invalid credentials")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, errors.New("invalid credentials")
	}
	return &user, nil
}
```

**Step 2: 编写 JWT 中间件**

Create `internal/middleware/auth.go` — 解析 JWT token，验证用户身份，注入 user_id 到 Gin context。使用 cookie 存储 token。

```go
package middleware

import (
	"net/http"
	"time"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

func AuthMiddleware(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenStr, err := c.Cookie("token")
		if err != nil {
			c.Redirect(http.StatusFound, "/login")
			c.Abort()
			return
		}
		claims := jwt.MapClaims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
			return []byte(secret), nil
		})
		if err != nil || !token.Valid {
			c.Redirect(http.StatusFound, "/login")
			c.Abort()
			return
		}
		c.Set("user_id", uint(claims["user_id"].(float64)))
		c.Set("username", claims["username"].(string))
		c.Next()
	}
}

func GenerateToken(secret string, userID uint, username string) (string, error) {
	claims := jwt.MapClaims{
		"user_id":  userID,
		"username": username,
		"exp":      time.Now().Add(24 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}
```

**Step 3: 编写登录 handler**

Create `internal/handler/auth.go` — 处理登录页面渲染和登录 POST 请求。

**Step 4: 创建登录页模板**

Create `templates/layout/base.html` — 基础布局模板，引入 HTMX CDN 和 Tailwind CDN。
Create `templates/auth/login.html` — 登录表单页面。

**Step 5: 在 main.go 中注册路由**

修改 `main.go`：初始化 UserService，创建默认管理员，配置 Gin 路由（公开路由 `/login`，受保护路由组使用 auth middleware）。

**Step 6: 安装依赖并验证**

```bash
go get github.com/golang-jwt/jwt/v5 golang.org/x/crypto
go build ./...
go run main.go
# 浏览器访问 http://localhost:8080/login 验证页面渲染
```

**Step 7: Commit**

```bash
git add .
git commit -m "feat: user authentication with JWT, login page, and auth middleware"
```

---

## Task 3: 数据源管理 — CRUD 和连接测试

**Files:**
- Create: `internal/handler/datasource.go`
- Create: `internal/service/datasource.go`
- Create: `internal/engine/connector.go`
- Create: `templates/datasource/list.html`
- Create: `templates/datasource/form.html`
- Modify: `main.go`

**Step 1: 编写数据库连接器**

Create `internal/engine/connector.go` — 根据 db_type 创建 `*sql.DB` 连接。封装 `Connect(ds model.DataSource) (*sql.DB, error)` 和 `TestConnection(ds model.DataSource) error`。支持 MySQL（`go-sql-driver/mysql`）、PostgreSQL（`lib/pq`）、Oracle（`godror`）三种驱动。

```go
package engine

import (
	"database/sql"
	"datasync/internal/model"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/sijms/go-ora/v2"
)

func BuildDSN(ds model.DataSource) string {
	switch ds.DBType {
	case "mysql":
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
			ds.Username, ds.Password, ds.Host, ds.Port, ds.DatabaseName)
	case "postgresql":
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
			ds.Username, ds.Password, ds.Host, ds.Port, ds.DatabaseName)
	case "oracle":
		return fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
			ds.Username, ds.Password, ds.Host, ds.Port, ds.DatabaseName)
	}
	return ""
}

func DriverName(dbType string) string {
	switch dbType {
	case "mysql":
		return "mysql"
	case "postgresql":
		return "pgx"
	case "oracle":
		return "oracle"
	}
	return ""
}

func Connect(ds model.DataSource) (*sql.DB, error) {
	return sql.Open(DriverName(ds.DBType), BuildDSN(ds))
}

func TestConnection(ds model.DataSource) error {
	db, err := Connect(ds)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Ping()
}
```

**Step 2: 编写数据源 service**

Create `internal/service/datasource.go` — CRUD 操作 + 获取表列表 `GetTables(ds) ([]string, error)` + 获取字段列表 `GetColumns(ds, table) ([]Column, error)`。GetTables/GetColumns 通过 `information_schema` 查询（MySQL/PG），或 `ALL_TABLES`/`ALL_TAB_COLUMNS`（Oracle）。

**Step 3: 编写数据源 handler**

Create `internal/handler/datasource.go` — 处理列表页、新增/编辑表单、删除、连接测试的 HTTP handler。API 接口返回 JSON，页面请求返回 HTML。

**Step 4: 创建数据源页面模板**

Create `templates/datasource/list.html` — 数据源列表页，表格展示，操作按钮（编辑/删除/测试连接）。
Create `templates/datasource/form.html` — 新增/编辑表单，db_type 下拉选择，连接测试按钮（HTMX POST）。

**Step 5: 注册路由并验证**

修改 `main.go`：注册数据源相关路由。

```bash
go get github.com/go-sql-driver/mysql github.com/jackc/pgx/v5 github.com/sijms/go-ora/v2
go build ./...
go run main.go
# 验证数据源 CRUD 和连接测试
```

**Step 6: Commit**

```bash
git add .
git commit -m "feat: datasource management with CRUD, connection test, and multi-db connector"
```

---

## Task 4: 同步任务管理 — CRUD 和字段映射

**Files:**
- Create: `internal/handler/task.go`
- Create: `internal/service/task.go`
- Create: `templates/task/list.html`
- Create: `templates/task/form.html`
- Create: `templates/task/detail.html`
- Modify: `main.go`

**Step 1: 编写任务 service**

Create `internal/service/task.go` — SyncTask CRUD、FieldMapping CRUD。创建任务时支持选择已有数据源或填写临时连接信息。`AutoMapFields(taskID)` 方法：读取源/目标表字段，按同名匹配自动生成 field_mappings。

**Step 2: 编写任务 handler**

Create `internal/handler/task.go` — 任务列表、创建/编辑、详情、手动触发、字段映射配置。创建任务表单中，选择数据源后通过 HTMX 动态加载表列表（`hx-get="/api/datasources/:id/tables"`），选择表后加载字段列表。

**Step 3: 创建任务页面模板**

- `templates/task/list.html` — 任务列表，状态标签（idle/running/error），操作按钮（运行/编辑/删除）
- `templates/task/form.html` — 创建/编辑表单，源/目标数据源选择（下拉或手动输入切换），表选择，同步类型/模式选择，cron 表达式输入
- `templates/task/detail.html` — 任务详情 + 字段映射配置表格（可勾选启用/禁用，编辑目标字段名）+ 执行日志列表

**Step 4: 注册路由并验证**

```bash
go build ./...
go run main.go
# 验证任务 CRUD 和字段映射配置
```

**Step 5: Commit**

```bash
git add .
git commit -m "feat: sync task management with CRUD, field mapping, and dynamic table/column loading"
```

---

## Task 5: 同步引擎 — 结构同步

**Files:**
- Create: `internal/engine/schema.go`
- Create: `internal/engine/typemap.go`
- Create: `internal/engine/schema_test.go`

**Step 1: 编写类型映射表**

Create `internal/engine/typemap.go` — 定义 MySQL ↔ PostgreSQL ↔ Oracle 之间的数据类型映射表。例如 MySQL `INT` → PG `INTEGER` → Oracle `NUMBER(10)`。覆盖常见类型：VARCHAR, INT, BIGINT, DECIMAL, TEXT, BOOLEAN, TIMESTAMP, DATE, BLOB。

```go
package engine

var TypeMapping = map[string]map[string]string{
	"mysql_to_postgresql": {
		"INT":       "INTEGER",
		"BIGINT":    "BIGINT",
		"VARCHAR":   "VARCHAR",
		"TEXT":      "TEXT",
		"DATETIME":  "TIMESTAMP",
		"TINYINT":   "SMALLINT",
		"DOUBLE":    "DOUBLE PRECISION",
		"FLOAT":     "REAL",
		"DECIMAL":   "NUMERIC",
		"BLOB":      "BYTEA",
		"BOOLEAN":   "BOOLEAN",
		"DATE":      "DATE",
		"TIMESTAMP": "TIMESTAMP",
	},
	// ... postgresql_to_mysql, mysql_to_oracle, etc.
}

func MapType(fromDB, toDB, colType string) string {
	key := fromDB + "_to_" + toDB
	if m, ok := TypeMapping[key]; ok {
		if mapped, ok := m[colType]; ok {
			return mapped
		}
	}
	return colType // fallback: keep original
}
```

**Step 2: 编写结构同步引擎**

Create `internal/engine/schema.go`:
- `ReadTableSchema(db *sql.DB, dbType, table string) (*TableSchema, error)` — 读取表结构（字段名、类型、是否可空、默认值、主键）
- `GenerateCreateSQL(schema *TableSchema, targetDBType string, fieldMappings []model.FieldMapping) string` — 生成目标库建表 SQL
- `CompareSchema(source, target *TableSchema) []SchemaDiff` — 对比差异
- `GenerateAlterSQL(diffs []SchemaDiff, targetDBType string) []string` — 生成 ALTER 语句
- `SyncStructure(sourceDB, targetDB *sql.DB, sourceType, targetType, sourceTable, targetTable string, mappings []model.FieldMapping) error` — 结构同步主流程

**Step 3: 编写类型映射测试**

Create `internal/engine/schema_test.go`:
```go
func TestMapType(t *testing.T) {
	assert.Equal(t, "INTEGER", MapType("mysql", "postgresql", "INT"))
	assert.Equal(t, "BYTEA", MapType("mysql", "postgresql", "BLOB"))
	assert.Equal(t, "VARCHAR", MapType("mysql", "postgresql", "VARCHAR"))
}
```

**Step 4: 验证**

```bash
go test ./internal/engine/... -v
```

**Step 5: Commit**

```bash
git add .
git commit -m "feat: schema sync engine with cross-database type mapping"
```

---

## Task 6: 同步引擎 — 全量数据同步

**Files:**
- Create: `internal/engine/data.go`
- Create: `internal/engine/data_test.go`
- Modify: `internal/model/models.go` (add SyncLog helpers)

**Step 1: 编写全量数据同步引擎**

Create `internal/engine/data.go`:
- `SyncData(ctx context.Context, sourceDB, targetDB *sql.DB, opts DataSyncOptions) (*SyncResult, error)` — 主流程
- `DataSyncOptions` struct：source/target table, field mappings, batch size (default 1000), write strategy (insert/upsert)
- 分批 SELECT 源表 → 根据字段映射转换 → 批量 INSERT/UPSERT 目标表
- 通过 `context.Context` 支持取消
- 返回 `SyncResult`：rows_synced, duration, error

分批读取使用 `LIMIT ? OFFSET ?`（MySQL/PG）或 `ROWNUM`（Oracle）。

批量写入根据目标数据库使用不同语法：
- MySQL: `INSERT ... ON DUPLICATE KEY UPDATE`
- PG: `INSERT ... ON CONFLICT DO UPDATE`
- Oracle: `MERGE INTO`

**Step 2: 编写进度回调**

在 `DataSyncOptions` 中添加 `OnProgress func(synced, total int64)` 回调，用于前端显示进度。

**Step 3: 验证编译**

```bash
go build ./...
```

**Step 4: Commit**

```bash
git add .
git commit -m "feat: full data sync engine with batch processing and upsert support"
```

---

## Task 7: 同步任务执行器和日志

**Files:**
- Create: `internal/service/executor.go`
- Create: `internal/handler/log.go`
- Create: `templates/log/list.html`
- Modify: `internal/handler/task.go` (add run/stop handler)
- Modify: `main.go`

**Step 1: 编写任务执行器**

Create `internal/service/executor.go`:
- `Executor` struct：持有 db, 活跃任务 map（`sync.Map`，key=taskID, value=cancel func）
- `Run(taskID uint) error` — 加载任务配置 → 建立源/目标连接 → 创建 SyncLog(running) → 根据 sync_type 调用结构同步/数据同步 → 更新 SyncLog(success/failed)
- `Stop(taskID uint) error` — 调用 cancel func 取消正在运行的任务
- 任务状态管理：运行前检查是否已在运行，更新 task.status

**Step 2: 编写日志 handler 和页面**

Create `internal/handler/log.go` — 全局日志列表、按任务筛选。
Create `templates/log/list.html` — 日志列表页，显示任务名、开始/结束时间、状态、同步行数、错误信息。

**Step 3: 在 task handler 中添加 run/stop**

修改 `internal/handler/task.go`：添加 `POST /api/tasks/:id/run` 和 `POST /api/tasks/:id/stop`。

**Step 4: 验证**

```bash
go build ./...
go run main.go
# 创建数据源和任务，手动触发执行
```

**Step 5: Commit**

```bash
git add .
git commit -m "feat: task executor with run/stop, sync logging, and log viewer"
```

---

## Task 8: 定时调度

**Files:**
- Create: `internal/service/scheduler.go`
- Modify: `main.go`
- Modify: `internal/handler/task.go`

**Step 1: 编写调度器**

Create `internal/service/scheduler.go`:
```go
package service

import (
	"datasync/internal/model"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
	"sync"
)

type Scheduler struct {
	cron     *cron.Cron
	executor *Executor
	db       *gorm.DB
	entries  map[uint]cron.EntryID // taskID -> cron entryID
	mu       sync.Mutex
}

func NewScheduler(db *gorm.DB, executor *Executor) *Scheduler {
	return &Scheduler{
		cron:     cron.New(),
		executor: executor,
		db:       db,
		entries:  make(map[uint]cron.EntryID),
	}
}

func (s *Scheduler) Start() error {
	// 从数据库加载所有 sync_mode=cron 的任务并注册
	var tasks []model.SyncTask
	s.db.Where("sync_mode = ?", "cron").Find(&tasks)
	for _, t := range tasks {
		s.AddTask(t)
	}
	s.cron.Start()
	return nil
}

func (s *Scheduler) AddTask(task model.SyncTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entryID, err := s.cron.AddFunc(task.CronExpr, func() {
		s.executor.Run(task.ID)
	})
	if err != nil {
		return err
	}
	s.entries[task.ID] = entryID
	return nil
}

func (s *Scheduler) RemoveTask(taskID uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entryID, ok := s.entries[taskID]; ok {
		s.cron.Remove(entryID)
		delete(s.entries, taskID)
	}
}

func (s *Scheduler) Stop() {
	s.cron.Stop()
}
```

**Step 2: 在 main.go 中启动调度器**

修改 `main.go`：初始化 Scheduler，调用 `Start()`，defer `Stop()`。

**Step 3: 任务 CRUD 时更新调度器**

修改 `internal/handler/task.go`：创建/编辑/删除 cron 任务时同步更新 Scheduler（AddTask/RemoveTask）。

**Step 4: 验证**

```bash
go get github.com/robfig/cron/v3
go build ./...
```

**Step 5: Commit**

```bash
git add .
git commit -m "feat: cron scheduler for periodic sync tasks"
```

---

## Task 9: CDC 实时监听 — MySQL Binlog

**Files:**
- Create: `internal/cdc/mysql.go`
- Create: `internal/cdc/manager.go`
- Modify: `internal/service/executor.go`

**Step 1: 编写 CDC Manager**

Create `internal/cdc/manager.go`:
```go
package cdc

import (
	"context"
	"sync"
)

type CDCListener interface {
	Start(ctx context.Context) error
	Stop() error
}

type Manager struct {
	listeners map[uint]CDCListener // taskID -> listener
	cancels   map[uint]context.CancelFunc
	mu        sync.Mutex
}
```

**Step 2: 编写 MySQL binlog 监听**

Create `internal/cdc/mysql.go`:
- 使用 `go-mysql-org/go-mysql` 库的 `canal` 包
- `MySQLListener` struct：实现 `CDCListener` 接口
- 监听指定表的 row event（INSERT/UPDATE/DELETE）
- 将变更实时写入目标表
- 维护 binlog position（存入 SQLite），支持断点续传

**Step 3: 在 executor 中集成 CDC**

修改 `internal/service/executor.go`：当 sync_mode=realtime 时，启动对应的 CDC listener 而非全量同步。

**Step 4: 验证编译**

```bash
go get github.com/go-mysql-org/go-mysql
go build ./...
```

**Step 5: Commit**

```bash
git add .
git commit -m "feat: MySQL CDC listener with binlog replication"
```

---

## Task 10: CDC 实时监听 — PostgreSQL WAL

**Files:**
- Create: `internal/cdc/postgresql.go`
- Modify: `internal/cdc/manager.go`

**Step 1: 编写 PostgreSQL logical replication 监听**

Create `internal/cdc/postgresql.go`:
- 使用 `jackc/pglogrepl` 库
- `PGListener` struct：实现 `CDCListener` 接口
- 创建 logical replication slot，监听 WAL 变更
- 解析 pgoutput 消息（INSERT/UPDATE/DELETE）
- 将变更实时写入目标表
- 维护 LSN 位点，支持断点续传

**Step 2: 在 Manager 中注册 PG listener**

修改 `internal/cdc/manager.go`：根据源数据库类型选择 MySQL 或 PG listener。

**Step 3: 验证编译**

```bash
go get github.com/jackc/pglogrepl github.com/jackc/pgx/v5
go build ./...
```

**Step 4: Commit**

```bash
git add .
git commit -m "feat: PostgreSQL CDC listener with logical replication"
```

---

## Task 11: 仪表盘页面

**Files:**
- Create: `internal/handler/dashboard.go`
- Create: `templates/dashboard/index.html`
- Modify: `main.go`

**Step 1: 编写仪表盘 handler**

Create `internal/handler/dashboard.go` — 查询统计数据：数据源总数、任务总数、运行中任务数、最近10条执行日志。

**Step 2: 创建仪表盘模板**

Create `templates/dashboard/index.html`:
- 顶部统计卡片（数据源数、任务数、运行中、今日执行次数）
- 最近执行日志表格
- 任务状态分布

使用 Tailwind CSS 样式，HTMX 定时刷新运行状态（`hx-trigger="every 5s"`）。

**Step 3: 注册路由并验证**

```bash
go build ./...
go run main.go
# 访问 http://localhost:8080/ 验证仪表盘
```

**Step 4: Commit**

```bash
git add .
git commit -m "feat: dashboard with task overview and recent sync logs"
```

---

## Task 12: 页面美化和导航整合

**Files:**
- Modify: `templates/layout/base.html`
- Create: `templates/layout/nav.html`
- Modify: all page templates

**Step 1: 完善基础布局**

修改 `templates/layout/base.html`：
- 左侧导航栏（仪表盘、数据源管理、同步任务、执行日志）
- 顶部 header（应用名称、当前用户、登出按钮）
- 主内容区域
- Tailwind CSS 统一风格

**Step 2: 统一所有页面样式**

确保所有页面继承 base layout，风格统一：
- 表格统一样式
- 表单统一样式
- 状态标签颜色（idle=灰色, running=蓝色, success=绿色, error=红色）
- 按钮样式统一

**Step 3: 验证所有页面**

```bash
go run main.go
# 逐页验证：登录 → 仪表盘 → 数据源 → 任务 → 日志
```

**Step 4: Commit**

```bash
git add .
git commit -m "feat: unified layout with navigation, consistent styling across all pages"
```

---

## Task 13: 端到端测试和收尾

**Files:**
- Create: `internal/engine/connector_test.go`
- Create: `internal/service/executor_test.go`
- Modify: `main.go` (graceful shutdown)

**Step 1: 添加关键测试**

- `connector_test.go` — 测试 DSN 构建
- `executor_test.go` — 测试任务执行流程（使用 SQLite 作为源和目标做集成测试）
- `schema_test.go` — 补充结构同步测试

**Step 2: 添加优雅停机**

修改 `main.go`：监听 OS signal，优雅关闭 HTTP server、scheduler、CDC listeners。

**Step 3: 全量编译和测试**

```bash
go build ./...
go test ./... -v
go run main.go
```

**Step 4: Commit**

```bash
git add .
git commit -m "feat: add tests, graceful shutdown, and final polish"
```

---

## 项目目录结构总览

```
datasync/
├── main.go
├── go.mod
├── go.sum
├── datasync.db              (运行时生成)
├── docs/plans/
│   ├── 2026-03-18-db-sync-design.md
│   └── 2026-03-18-db-sync-implementation.md
├── internal/
│   ├── config/
│   │   └── config.go
│   ├── model/
│   │   └── models.go
│   ├── database/
│   │   └── sqlite.go
│   ├── middleware/
│   │   └── auth.go
│   ├── handler/
│   │   ├── auth.go
│   │   ├── dashboard.go
│   │   ├── datasource.go
│   │   ├── task.go
│   │   └── log.go
│   ├── service/
│   │   ├── user.go
│   │   ├── datasource.go
│   │   ├── task.go
│   │   ├── executor.go
│   │   └── scheduler.go
│   ├── engine/
│   │   ├── connector.go
│   │   ├── typemap.go
│   │   ├── schema.go
│   │   ├── data.go
│   │   ├── schema_test.go
│   │   ├── data_test.go
│   │   └── connector_test.go
│   └── cdc/
│       ├── manager.go
│       ├── mysql.go
│       └── postgresql.go
├── templates/
│   ├── layout/
│   │   ├── base.html
│   │   └── nav.html
│   ├── auth/
│   │   └── login.html
│   ├── dashboard/
│   │   └── index.html
│   ├── datasource/
│   │   ├── list.html
│   │   └── form.html
│   ├── task/
│   │   ├── list.html
│   │   ├── form.html
│   │   └── detail.html
│   └── log/
│       └── list.html
└── static/
```
