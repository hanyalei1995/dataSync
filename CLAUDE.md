# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
go build -o datasync .

# Run (starts on :9090, default admin/admin123)
./datasync

# Run all tests
go test ./...

# Run tests for a specific package
go test ./internal/engine/...

# Run a single test
go test ./internal/engine/ -run TestMapType
```

## Architecture

DataSync is a single-binary Go web application for synchronizing tables across databases (MySQL, PostgreSQL, Oracle). It uses:
- **Gin** for HTTP routing
- **Go `html/template` + HTMX** for server-rendered UI (no frontend build step)
- **GORM + SQLite** (`datasync.db`) for metadata persistence
- **`database/sql`** for direct connections to source/target databases

### Request flow

`Browser → Gin router → handler → service → engine/cdc`

All routes except `/login` are protected by JWT cookie auth (`internal/middleware/auth.go`). The JWT secret and admin credentials are configured in `internal/config/config.go` (hardcoded defaults — no env var support yet).

### Package responsibilities

| Package | Role |
|---|---|
| `internal/config` | App config struct with hardcoded defaults |
| `internal/database` | SQLite init and GORM auto-migration |
| `internal/model` | GORM model definitions (`DataSource`, `SyncTask`, `FieldMapping`, `SyncLog`, `User`) |
| `internal/engine` | Core sync logic: `connector.go` builds DSNs and opens `*sql.DB`; `schema.go` reads/generates DDL and diffs schemas; `data.go` does batched full-load sync; `typemap.go` has cross-dialect type mappings |
| `internal/service` | Business logic: `executor.go` runs sync tasks in goroutines; `scheduler.go` wraps `robfig/cron/v3`; `datasource.go`/`task.go`/`user.go` are CRUD services |
| `internal/cdc` | CDC listeners: `mysql.go` uses `go-mysql` for binlog; `postgresql.go` uses `pglogrepl` for logical replication; `manager.go` tracks running listeners |
| `internal/handler` | Gin HTTP handlers (thin layer delegating to services) |
| `templates/` | Go HTML templates, loaded at startup via `templates/**/*.html` glob |

### Sync task model

A `SyncTask` has:
- **`sync_type`**: `structure` | `data` | `both`
- **`sync_mode`**: `manual` | `cron` | `realtime`
- **`source_ds_id` / `target_ds_id`**: FK to saved `DataSource`, OR
- **`source_config` / `target_config`**: inline JSON `DataSource` for ad-hoc connections

`Executor.Run()` resolves the data source, opens connections, then dispatches:
- `realtime` mode → starts a CDC listener via `cdc.Manager` (long-running goroutine)
- all other modes → runs `engine.SyncStructure` / `engine.SyncData` in a goroutine, records a `SyncLog`

### CDC requirements

- **MySQL**: source must have binlog enabled in row format
- **PostgreSQL**: source must have `wal_level = logical`; a replication slot named `datasync_task_<id>` is created automatically
