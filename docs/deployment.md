# DataSync 部署指南

## 编译

需要 Go 1.16+（使用 `embed` 包），所有模板已嵌入二进制，**发布只需单个可执行文件**。

### 本机编译

```bash
go build -o datasync .
```

### 交叉编译

| 平台 | 命令 |
|------|------|
| Linux x86_64 | `GOOS=linux GOARCH=amd64 go build -o datasync-linux .` |
| Linux ARM64 | `GOOS=linux GOARCH=arm64 go build -o datasync-linux-arm64 .` |
| macOS x86_64 | `GOOS=darwin GOARCH=amd64 go build -o datasync-darwin .` |
| macOS ARM64 | `GOOS=darwin GOARCH=arm64 go build -o datasync-darwin-arm64 .` |
| Windows x86_64 | `GOOS=windows GOARCH=amd64 go build -o datasync.exe .` |

> **注意**：依赖 `mattn/go-sqlite3`（CGO），交叉编译需要目标平台的 C 编译器。
> 推荐在目标平台上直接编译，或使用 Docker 构建。

### 使用 Docker 交叉编译（推荐）

```bash
# 编译 Linux amd64
docker run --rm -v $(pwd):/app -w /app \
  golang:1.21 \
  go build -o datasync-linux .
```

---

## 运行

```bash
# 直接运行（监听 :9090，默认账号 admin/admin123）
./datasync

# Linux 后台运行
nohup ./datasync > datasync.log 2>&1 &

# 查看日志 / 停止
tail -f datasync.log
kill $(pgrep datasync)
```

运行后会在当前目录自动创建 `datasync.db`（SQLite 数据文件）。

---

## 配置

当前版本使用硬编码默认值，如需修改请编辑 `internal/config/config.go`：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `Port` | `9090` | HTTP 监听端口 |
| `DBPath` | `datasync.db` | SQLite 数据库路径 |
| `JWTSecret` | `change-me-in-production` | JWT 签名密钥，**生产环境必须修改** |
| `AdminUser` | `admin` | 初始管理员账号 |
| `AdminPass` | `admin123` | 初始管理员密码，**生产环境必须修改** |

---

## Linux 系统服务（systemd）

创建 `/etc/systemd/system/datasync.service`：

```ini
[Unit]
Description=DataSync
After=network.target

[Service]
ExecStart=/opt/datasync/datasync
WorkingDirectory=/opt/datasync
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
# 部署并启动
sudo mkdir -p /opt/datasync
sudo cp datasync /opt/datasync/

sudo systemctl daemon-reload
sudo systemctl enable datasync
sudo systemctl start datasync
sudo systemctl status datasync
```

---

## Windows 服务

```powershell
# 使用 NSSM 注册为 Windows 服务
nssm install DataSync "C:\datasync\datasync.exe"
nssm set DataSync AppDirectory "C:\datasync"
nssm start DataSync
```

或直接运行：

```powershell
.\datasync.exe
```

---

## 数据备份

数据库为单文件 SQLite，备份只需复制：

```bash
# 备份
cp datasync.db datasync.db.bak

# 或热备份（运行中也可用）
sqlite3 datasync.db ".backup datasync.db.bak"
```

---

## CDC 前置条件

使用实时同步（CDC）模式时，源数据库需额外配置：

**MySQL**

```sql
-- my.cnf 需开启
log_bin = ON
binlog_format = ROW
binlog_row_image = FULL
```

**PostgreSQL**

```sql
-- postgresql.conf 需设置
wal_level = logical

-- 授予复制权限
ALTER ROLE your_user REPLICATION;
```

复制槽由 DataSync 自动创建，名称格式为 `datasync_task_<id>`。
