# Connector 接口重构 + 新数据源 + 断点续传 实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将所有数据源统一为 `Connector` 接口，新增 ClickHouse、Doris、MongoDB、CSV/Excel 支持，并实现断点续传。

**Architecture:** 新建 `internal/connector` 包定义统一接口，SQLConnector 迁移现有 MySQL/PG/Oracle 逻辑并扩展 ClickHouse/Doris，MongoConnector 和 FileConnector 新建；engine/data.go 面向 Connector 接口重构；SyncTask 加 checkpoint_offset 字段支持断点续传。

**Tech Stack:** Go, `github.com/ClickHouse/clickhouse-go/v2`, `github.com/xuri/excelize/v2`, `go.mongodb.org/mongo-driver/v2`（已在 go.mod 中）

---

### Task 1: 定义 Connector 接口

**Files:**
- Create: `internal/connector/connector.go`

**Step 1: 创建接口文件**

```go
package connector

import "context"

// Row 代表一行数据，key 为列名。
type Row = map[string]interface{}

// ReadOptions 控制批量读取行为。
type ReadOptions struct {
	Table   string
	Columns []string // 空表示 SELECT *
	Where   string   // 不含 WHERE 关键字
	Offset  int64
	Limit   int64
}

// WriteOptions 控制批量写入行为。
type WriteOptions struct {
	Table    string
	Columns  []string
	Rows     []Row
	Strategy string   // "insert" | "upsert"
	PKCols   []string // upsert 时需要
}

// Schema 描述一张表/集合的字段信息。
type Schema struct {
	Columns []ColumnInfo
}

// ColumnInfo 描述单个字段。
type ColumnInfo struct {
	Name      string
	Type      string
	Nullable  bool
	IsPrimary bool
}

// Connector 是所有数据源的统一接口。
type Connector interface {
	// Ping 验证连接是否可用。
	Ping(ctx context.Context) error
	// ListTables 列出所有可用表/集合/sheet。
	ListTables(ctx context.Context) ([]string, error)
	// GetSchema 返回指定表的字段信息。
	GetSchema(ctx context.Context, table string) (*Schema, error)
	// CountRows 统计满足条件的行数。
	CountRows(ctx context.Context, table, where string) (int64, error)
	// ReadBatch 批量读取数据行。
	ReadBatch(ctx context.Context, opts ReadOptions) ([]Row, error)
	// WriteBatch 批量写入数据行。
	WriteBatch(ctx context.Context, opts WriteOptions) error
	// Close 释放底层资源。
	Close() error
	// DBType 返回数据源类型标识，如 "mysql"、"mongodb"、"csv"。
	DBType() string
}
```

**Step 2: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./internal/connector/...
```

Expected: 无错误

**Step 3: Commit**

```bash
git add internal/connector/connector.go
git commit -m "feat: define unified Connector interface"
```

---

### Task 2: 实现 SQLConnector（MySQL / PG / Oracle / ClickHouse / Doris）

**Files:**
- Create: `internal/connector/sql.go`

**Step 1: 添加 ClickHouse 依赖**

```bash
cd /Users/mac/Documents/dataSync
go get github.com/ClickHouse/clickhouse-go/v2
```

**Step 2: 创建 sql.go**

```go
package connector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/sijms/go-ora/v2"
)

// SQLConnector 实现基于 database/sql 的 Connector。
// 支持 MySQL、PostgreSQL、Oracle、ClickHouse、Doris。
type SQLConnector struct {
	db     *sql.DB
	dbType string // "mysql"|"postgresql"|"oracle"|"clickhouse"|"doris"
}

// NewSQLConnector 创建并返回一个 SQLConnector，内部会 Ping 验证连接。
func NewSQLConnector(dbType, dsn string) (*SQLConnector, error) {
	driver := sqlDriverName(dbType)
	if driver == "" {
		return nil, fmt.Errorf("unsupported sql db type: %s", dbType)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(3)
	return &SQLConnector{db: db, dbType: strings.ToLower(dbType)}, nil
}

// WrapSQLDB 将已有 *sql.DB 包装为 SQLConnector（ConnPool 复用场景）。
func WrapSQLDB(db *sql.DB, dbType string) *SQLConnector {
	return &SQLConnector{db: db, dbType: strings.ToLower(dbType)}
}

func sqlDriverName(dbType string) string {
	switch strings.ToLower(dbType) {
	case "mysql", "doris":
		return "mysql"
	case "postgresql":
		return "pgx"
	case "oracle":
		return "oracle"
	case "clickhouse":
		return "clickhouse"
	}
	return ""
}

// BuildDSN 根据 DataSource 字段组装 DSN 字符串。
func BuildDSN(dbType, host string, port int, user, pass, dbName, extra string) string {
	switch strings.ToLower(dbType) {
	case "mysql", "doris":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", user, pass, host, port, dbName)
		if extra != "" {
			dsn += "&" + extra
		}
		return dsn
	case "postgresql":
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, pass, host, port, dbName)
	case "oracle":
		return fmt.Sprintf("oracle://%s:%s@%s:%d/%s", user, pass, host, port, dbName)
	case "clickhouse":
		return fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s", user, pass, host, port, dbName)
	}
	return ""
}

func (c *SQLConnector) DBType() string { return c.dbType }

func (c *SQLConnector) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *SQLConnector) Close() error {
	return c.db.Close()
}

// RawDB 暴露底层 *sql.DB，供 CDC 等需要直接操作的场景使用。
func (c *SQLConnector) RawDB() *sql.DB { return c.db }

func (c *SQLConnector) ListTables(ctx context.Context) ([]string, error) {
	var query string
	switch c.dbType {
	case "mysql", "doris":
		query = "SHOW TABLES"
	case "postgresql":
		query = "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
	case "oracle":
		query = "SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME"
	case "clickhouse":
		query = "SHOW TABLES"
	default:
		return nil, fmt.Errorf("ListTables: unsupported type %s", c.dbType)
	}
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (c *SQLConnector) GetSchema(ctx context.Context, table string) (*Schema, error) {
	// 复用 engine/schema.go 的查询逻辑，但返回 connector.Schema
	cols, err := c.readColumns(ctx, table)
	if err != nil {
		return nil, err
	}
	return &Schema{Columns: cols}, nil
}

func (c *SQLConnector) readColumns(ctx context.Context, table string) ([]ColumnInfo, error) {
	switch c.dbType {
	case "mysql", "doris":
		return c.readMySQLColumns(ctx, table)
	case "postgresql":
		return c.readPGColumns(ctx, table)
	case "oracle":
		return c.readOracleColumns(ctx, table)
	case "clickhouse":
		return c.readClickHouseColumns(ctx, table)
	}
	return nil, fmt.Errorf("GetSchema: unsupported type %s", c.dbType)
}

func (c *SQLConnector) readMySQLColumns(ctx context.Context, table string) ([]ColumnInfo, error) {
	q := `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE,
		CASE WHEN COLUMN_KEY='PRI' THEN 1 ELSE 0 END
		FROM information_schema.COLUMNS WHERE TABLE_NAME=? ORDER BY ORDINAL_POSITION`
	rows, err := c.db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanColumnInfoRows(rows)
}

func (c *SQLConnector) readPGColumns(ctx context.Context, table string) ([]ColumnInfo, error) {
	q := `SELECT c.column_name, c.udt_name,
		CASE WHEN c.is_nullable='YES' THEN true ELSE false END,
		CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END
		FROM information_schema.columns c
		LEFT JOIN (
			SELECT ku.column_name FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage ku
			ON tc.constraint_name=ku.constraint_name
			WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_name=$1
		) pk ON c.column_name=pk.column_name
		WHERE c.table_name=$1 ORDER BY c.ordinal_position`
	rows, err := c.db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanColumnInfoRows(rows)
}

func (c *SQLConnector) readOracleColumns(ctx context.Context, table string) ([]ColumnInfo, error) {
	q := `SELECT c.COLUMN_NAME, c.DATA_TYPE,
		CASE WHEN c.NULLABLE='Y' THEN 1 ELSE 0 END,
		CASE WHEN p.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END
		FROM USER_TAB_COLUMNS c
		LEFT JOIN (
			SELECT cc.COLUMN_NAME FROM USER_CONSTRAINTS con
			JOIN USER_CONS_COLUMNS cc ON con.CONSTRAINT_NAME=cc.CONSTRAINT_NAME
			WHERE con.CONSTRAINT_TYPE='P' AND con.TABLE_NAME=:1
		) p ON c.COLUMN_NAME=p.COLUMN_NAME
		WHERE c.TABLE_NAME=:1 ORDER BY c.COLUMN_ID`
	rows, err := c.db.QueryContext(ctx, q, strings.ToUpper(table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanColumnInfoRows(rows)
}

func (c *SQLConnector) readClickHouseColumns(ctx context.Context, table string) ([]ColumnInfo, error) {
	q := `SELECT name, type, 0, 0 FROM system.columns WHERE table=? ORDER BY position`
	rows, err := c.db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanColumnInfoRows(rows)
}

func scanColumnInfoRows(rows *sql.Rows) ([]ColumnInfo, error) {
	var cols []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable, isPrimary int
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &isPrimary); err != nil {
			return nil, err
		}
		col.Nullable = nullable != 0
		col.IsPrimary = isPrimary != 0
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func (c *SQLConnector) CountRows(ctx context.Context, table, where string) (int64, error) {
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", c.quoteIdent(table))
	if where != "" {
		q += " WHERE " + where
	}
	var n int64
	return n, c.db.QueryRowContext(ctx, q).Scan(&n)
}

func (c *SQLConnector) ReadBatch(ctx context.Context, opts ReadOptions) ([]Row, error) {
	cols := opts.Columns
	colList := "*"
	if len(cols) > 0 {
		quoted := make([]string, len(cols))
		for i, col := range cols {
			quoted[i] = c.quoteIdent(col)
		}
		colList = strings.Join(quoted, ", ")
	}

	whereClause := ""
	if opts.Where != "" {
		whereClause = " WHERE " + opts.Where
	}

	var query string
	tbl := c.quoteIdent(opts.Table)
	switch c.dbType {
	case "oracle":
		query = fmt.Sprintf(
			"SELECT %s FROM (SELECT a.*, ROWNUM rn FROM (SELECT %s FROM %s%s) a WHERE ROWNUM <= %d) WHERE rn > %d",
			colList, colList, tbl, whereClause, opts.Offset+opts.Limit, opts.Offset)
	default:
		query = fmt.Sprintf("SELECT %s FROM %s%s LIMIT %d OFFSET %d",
			colList, tbl, whereClause, opts.Limit, opts.Offset)
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ReadBatch query: %w", err)
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []Row
	for rows.Next() {
		vals := make([]interface{}, len(colNames))
		ptrs := make([]interface{}, len(colNames))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(Row, len(colNames))
		for i, name := range colNames {
			row[name] = vals[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (c *SQLConnector) WriteBatch(ctx context.Context, opts WriteOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	cols := opts.Columns
	if len(cols) == 0 {
		// 从第一行推断列名
		for k := range opts.Rows[0] {
			cols = append(cols, k)
		}
	}

	quotedCols := make([]string, len(cols))
	for i, c2 := range cols {
		quotedCols[i] = c.quoteIdent(c2)
	}

	tbl := c.quoteIdent(opts.Table)

	if opts.Strategy == "upsert" && c.dbType != "clickhouse" {
		return c.upsertBatch(ctx, tbl, cols, quotedCols, opts)
	}
	return c.insertBatch(ctx, tbl, cols, quotedCols, opts.Rows)
}

func (c *SQLConnector) insertBatch(ctx context.Context, tbl string, cols, quotedCols []string, rows []Row) error {
	placeholders := make([]string, len(cols))
	for i := range cols {
		if c.dbType == "oracle" {
			placeholders[i] = fmt.Sprintf(":%d", i+1)
		} else if c.dbType == "postgresql" {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		} else {
			placeholders[i] = "?"
		}
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tbl, strings.Join(quotedCols, ","), strings.Join(placeholders, ","))

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, row := range rows {
		vals := make([]interface{}, len(cols))
		for i, col := range cols {
			vals[i] = row[col]
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert row: %w", err)
		}
	}
	return tx.Commit()
}

func (c *SQLConnector) upsertBatch(ctx context.Context, tbl string, cols, quotedCols []string, opts WriteOptions) error {
	switch c.dbType {
	case "mysql", "doris":
		updates := make([]string, 0, len(cols))
		for i, col := range quotedCols {
			updates = append(updates, fmt.Sprintf("%s=VALUES(%s)", col, quotedCols[i]))
		}
		placeholders := strings.Repeat("?,", len(cols))
		placeholders = placeholders[:len(placeholders)-1]
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			tbl, strings.Join(quotedCols, ","), placeholders, strings.Join(updates, ","))
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			tx.Rollback()
			return err
		}
		defer stmt.Close()
		for _, row := range opts.Rows {
			vals := make([]interface{}, len(cols))
			for i, col := range cols {
				vals[i] = row[col]
			}
			if _, err := stmt.ExecContext(ctx, vals...); err != nil {
				tx.Rollback()
				return err
			}
		}
		return tx.Commit()

	case "postgresql":
		pkSet := make(map[string]bool)
		for _, pk := range opts.PKCols {
			pkSet[pk] = true
		}
		updates := make([]string, 0)
		for _, col := range quotedCols {
			raw := strings.Trim(col, `"`)
			if !pkSet[raw] {
				updates = append(updates, fmt.Sprintf("%s=EXCLUDED.%s", col, col))
			}
		}
		conflictCols := make([]string, len(opts.PKCols))
		for i, pk := range opts.PKCols {
			conflictCols[i] = c.quoteIdent(pk)
		}
		placeholders := make([]string, len(cols))
		for i := range cols {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			tbl, strings.Join(quotedCols, ","), strings.Join(placeholders, ","),
			strings.Join(conflictCols, ","), strings.Join(updates, ","))
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			tx.Rollback()
			return err
		}
		defer stmt.Close()
		for _, row := range opts.Rows {
			vals := make([]interface{}, len(cols))
			for i, col := range cols {
				vals[i] = row[col]
			}
			if _, err := stmt.ExecContext(ctx, vals...); err != nil {
				tx.Rollback()
				return err
			}
		}
		return tx.Commit()

	default:
		// Oracle upsert: MERGE INTO
		pkSet := make(map[string]bool)
		for _, pk := range opts.PKCols {
			pkSet[pk] = true
		}
		onClauses := make([]string, len(opts.PKCols))
		for i, pk := range opts.PKCols {
			onClauses[i] = fmt.Sprintf("t.%s = s.%s", c.quoteIdent(pk), c.quoteIdent(pk))
		}
		updateCols := make([]string, 0)
		for _, col := range cols {
			if !pkSet[col] {
				updateCols = append(updateCols, fmt.Sprintf("t.%s = s.%s", c.quoteIdent(col), c.quoteIdent(col)))
			}
		}
		srcCols := make([]string, len(cols))
		bindParams := make([]string, len(cols))
		for i, col := range cols {
			srcCols[i] = fmt.Sprintf(":%d %s", i+1, c.quoteIdent(col))
			bindParams[i] = fmt.Sprintf(":%d", i+1)
		}
		query := fmt.Sprintf(`MERGE INTO %s t USING (SELECT %s FROM DUAL) s ON (%s)
			WHEN MATCHED THEN UPDATE SET %s
			WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`,
			tbl, strings.Join(srcCols, ","), strings.Join(onClauses, " AND "),
			strings.Join(updateCols, ","),
			strings.Join(quotedCols, ","), strings.Join(bindParams, ","))
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			tx.Rollback()
			return err
		}
		defer stmt.Close()
		for _, row := range opts.Rows {
			vals := make([]interface{}, len(cols))
			for i, col := range cols {
				vals[i] = row[col]
			}
			if _, err := stmt.ExecContext(ctx, vals...); err != nil {
				tx.Rollback()
				return err
			}
		}
		return tx.Commit()
	}
}

func (c *SQLConnector) quoteIdent(name string) string {
	switch c.dbType {
	case "mysql", "doris":
		return "`" + name + "`"
	case "postgresql", "oracle":
		return `"` + name + `"`
	case "clickhouse":
		return "`" + name + "`"
	default:
		return name
	}
}
```

**Step 3: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./internal/connector/...
```

Expected: 无错误

**Step 4: Commit**

```bash
git add internal/connector/sql.go go.mod go.sum
git commit -m "feat: implement SQLConnector for MySQL/PG/Oracle/ClickHouse/Doris"
```

---

### Task 3: 实现 MongoConnector

**Files:**
- Create: `internal/connector/mongo.go`

**Step 1: 确认 mongo-driver 依赖**

```bash
cd /Users/mac/Documents/dataSync
go get go.mongodb.org/mongo-driver/v2/mongo
```

**Step 2: 创建 mongo.go**

```go
package connector

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoConnector 实现基于 MongoDB 的 Connector。
// 文档顶层字段直接映射为列，嵌套字段用 "parent.child" 表示（读时展平）。
type MongoConnector struct {
	client   *mongo.Client
	database string
}

// NewMongoConnector 建立 MongoDB 连接。
// uri 格式: mongodb://user:pass@host:port/authdb
func NewMongoConnector(uri, database string) (*MongoConnector, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return &MongoConnector{client: client, database: database}, nil
}

func (c *MongoConnector) DBType() string { return "mongodb" }

func (c *MongoConnector) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, nil)
}

func (c *MongoConnector) Close() error {
	return c.client.Disconnect(context.Background())
}

func (c *MongoConnector) ListTables(ctx context.Context) ([]string, error) {
	return c.client.Database(c.database).ListCollectionNames(ctx, bson.D{})
}

// GetSchema 采样 20 条文档推断字段类型。
func (c *MongoConnector) GetSchema(ctx context.Context, table string) (*Schema, error) {
	coll := c.client.Database(c.database).Collection(table)
	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetLimit(20))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	fieldTypes := make(map[string]string)
	fieldOrder := []string{}

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		flatRow := flattenBSONDoc(doc, "")
		for k, v := range flatRow {
			if _, exists := fieldTypes[k]; !exists {
				fieldOrder = append(fieldOrder, k)
				fieldTypes[k] = inferBSONType(v)
			}
		}
	}

	cols := make([]ColumnInfo, 0, len(fieldOrder))
	for _, name := range fieldOrder {
		isPK := name == "_id"
		cols = append(cols, ColumnInfo{
			Name:      name,
			Type:      fieldTypes[name],
			Nullable:  !isPK,
			IsPrimary: isPK,
		})
	}
	return &Schema{Columns: cols}, cursor.Err()
}

func (c *MongoConnector) CountRows(ctx context.Context, table, where string) (int64, error) {
	filter, err := parseMongoFilter(where)
	if err != nil {
		return 0, err
	}
	return c.client.Database(c.database).Collection(table).CountDocuments(ctx, filter)
}

func (c *MongoConnector) ReadBatch(ctx context.Context, opts ReadOptions) ([]Row, error) {
	filter, err := parseMongoFilter(opts.Where)
	if err != nil {
		return nil, err
	}

	findOpts := options.Find().SetSkip(opts.Offset).SetLimit(opts.Limit)
	if len(opts.Columns) > 0 {
		proj := bson.D{}
		for _, col := range opts.Columns {
			proj = append(proj, bson.E{Key: col, Value: 1})
		}
		findOpts.SetProjection(proj)
	}

	cursor, err := c.client.Database(c.database).Collection(opts.Table).Find(ctx, filter, findOpts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var rows []Row
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		rows = append(rows, flattenBSONDoc(doc, ""))
	}
	return rows, cursor.Err()
}

func (c *MongoConnector) WriteBatch(ctx context.Context, opts WriteOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	coll := c.client.Database(c.database).Collection(opts.Table)

	if opts.Strategy == "upsert" && len(opts.PKCols) > 0 {
		models := make([]mongo.WriteModel, len(opts.Rows))
		for i, row := range opts.Rows {
			filter := bson.D{}
			for _, pk := range opts.PKCols {
				filter = append(filter, bson.E{Key: pk, Value: row[pk]})
			}
			doc := rowToBSONDoc(row)
			models[i] = mongo.NewReplaceOneModel().
				SetFilter(filter).SetReplacement(doc).SetUpsert(true)
		}
		_, err := coll.BulkWrite(ctx, models)
		return err
	}

	docs := make([]interface{}, len(opts.Rows))
	for i, row := range opts.Rows {
		docs[i] = rowToBSONDoc(row)
	}
	_, err := coll.InsertMany(ctx, docs)
	return err
}

// flattenBSONDoc 将 bson.D 展平为 map，嵌套文档用 "parent.child" 键。
func flattenBSONDoc(doc bson.D, prefix string) Row {
	result := make(Row)
	for _, elem := range doc {
		key := elem.Key
		if prefix != "" {
			key = prefix + "." + key
		}
		switch v := elem.Value.(type) {
		case bson.D:
			for k2, v2 := range flattenBSONDoc(v, key) {
				result[k2] = v2
			}
		default:
			result[key] = v
		}
	}
	return result
}

func rowToBSONDoc(row Row) bson.D {
	doc := make(bson.D, 0, len(row))
	for k, v := range row {
		doc = append(doc, bson.E{Key: k, Value: v})
	}
	return doc
}

func inferBSONType(v interface{}) string {
	switch v.(type) {
	case int32, int64:
		return "int"
	case float64:
		return "double"
	case bool:
		return "bool"
	default:
		return "string"
	}
}

// parseMongoFilter 将简单 JSON 字符串解析为 bson.D filter；空字符串返回空 filter。
func parseMongoFilter(where string) (bson.D, error) {
	if strings.TrimSpace(where) == "" {
		return bson.D{}, nil
	}
	var filter bson.D
	if err := bson.UnmarshalExtJSON([]byte(where), true, &filter); err != nil {
		return nil, fmt.Errorf("invalid mongo filter: %w", err)
	}
	return filter, nil
}
```

**Step 3: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./internal/connector/...
```

**Step 4: Commit**

```bash
git add internal/connector/mongo.go go.mod go.sum
git commit -m "feat: implement MongoConnector for MongoDB source/target"
```

---

### Task 4: 实现 FileConnector（CSV / Excel）

**Files:**
- Create: `internal/connector/file.go`

**Step 1: 添加 excelize 依赖**

```bash
cd /Users/mac/Documents/dataSync
go get github.com/xuri/excelize/v2
```

**Step 2: 创建 file.go**

```go
package connector

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/xuri/excelize/v2"
)

// FileConnector 实现基于本地 CSV / Excel 文件的 Connector。
// table 字段：CSV 时忽略（文件就是表），Excel 时为 sheet 名。
// Host 字段存储文件路径。
type FileConnector struct {
	filePath string
	fileType string // "csv" | "excel"
	// 缓存的全部数据（文件较小，全量载入内存）
	headers []string
	data    [][]string
}

// NewFileConnector 按文件扩展名自动识别 CSV 或 Excel。
func NewFileConnector(filePath string) (*FileConnector, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	var fileType string
	switch ext {
	case ".csv":
		fileType = "csv"
	case ".xlsx", ".xls":
		fileType = "excel"
	default:
		return nil, fmt.Errorf("unsupported file type: %s", ext)
	}
	return &FileConnector{filePath: filePath, fileType: fileType}, nil
}

func (c *FileConnector) DBType() string { return c.fileType }

func (c *FileConnector) Ping(_ context.Context) error {
	_, err := os.Stat(c.filePath)
	return err
}

func (c *FileConnector) Close() error { return nil }

func (c *FileConnector) ListTables(_ context.Context) ([]string, error) {
	if c.fileType == "csv" {
		return []string{filepath.Base(c.filePath)}, nil
	}
	f, err := excelize.OpenFile(c.filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.GetSheetList(), nil
}

func (c *FileConnector) GetSchema(_ context.Context, table string) (*Schema, error) {
	if err := c.loadData(table); err != nil {
		return nil, err
	}
	cols := make([]ColumnInfo, len(c.headers))
	for i, h := range c.headers {
		cols[i] = ColumnInfo{Name: h, Type: "string", Nullable: true}
	}
	return &Schema{Columns: cols}, nil
}

func (c *FileConnector) CountRows(_ context.Context, table, _ string) (int64, error) {
	if err := c.loadData(table); err != nil {
		return 0, err
	}
	return int64(len(c.data)), nil
}

func (c *FileConnector) ReadBatch(_ context.Context, opts ReadOptions) ([]Row, error) {
	if err := c.loadData(opts.Table); err != nil {
		return nil, err
	}
	start := opts.Offset
	end := opts.Offset + opts.Limit
	if start >= int64(len(c.data)) {
		return nil, nil
	}
	if end > int64(len(c.data)) {
		end = int64(len(c.data))
	}
	slice := c.data[start:end]
	rows := make([]Row, len(slice))
	for i, record := range slice {
		row := make(Row, len(c.headers))
		for j, h := range c.headers {
			if j < len(record) {
				row[h] = record[j]
			}
		}
		rows[i] = row
	}
	return rows, nil
}

func (c *FileConnector) WriteBatch(_ context.Context, opts WriteOptions) error {
	cols := opts.Columns
	if len(cols) == 0 && len(opts.Rows) > 0 {
		for k := range opts.Rows[0] {
			cols = append(cols, k)
		}
	}

	if c.fileType == "csv" {
		return c.writeCSV(opts.Table, cols, opts.Rows)
	}
	return c.writeExcel(opts.Table, cols, opts.Rows)
}

func (c *FileConnector) writeCSV(_ string, cols []string, rows []Row) error {
	// 追加模式：若文件不存在则创建并写 header
	fileExists := true
	if _, err := os.Stat(c.filePath); os.IsNotExist(err) {
		fileExists = false
	}
	f, err := os.OpenFile(c.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if !fileExists {
		if err := w.Write(cols); err != nil {
			return err
		}
	}
	for _, row := range rows {
		record := make([]string, len(cols))
		for i, col := range cols {
			record[i] = fmt.Sprintf("%v", row[col])
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func (c *FileConnector) writeExcel(sheet string, cols []string, rows []Row) error {
	var f *excelize.File
	if _, err := os.Stat(c.filePath); os.IsNotExist(err) {
		f = excelize.NewFile()
		f.SetSheetName("Sheet1", sheet)
	} else {
		var err error
		f, err = excelize.OpenFile(c.filePath)
		if err != nil {
			return err
		}
	}
	defer f.Close()

	// 找到当前最后一行
	existingRows, _ := f.GetRows(sheet)
	startRow := len(existingRows) + 1
	if startRow == 1 {
		// 写 header
		for j, col := range cols {
			cell, _ := excelize.CoordinatesToCellName(j+1, 1)
			f.SetCellValue(sheet, cell, col)
		}
		startRow = 2
	}
	for i, row := range rows {
		for j, col := range cols {
			cell, _ := excelize.CoordinatesToCellName(j+1, startRow+i)
			f.SetCellValue(sheet, cell, row[col])
		}
	}
	return f.SaveAs(c.filePath)
}

func (c *FileConnector) loadData(table string) error {
	if c.headers != nil {
		return nil // 已缓存
	}
	if c.fileType == "csv" {
		return c.loadCSV()
	}
	return c.loadExcel(table)
}

func (c *FileConnector) loadCSV() error {
	f, err := os.Open(c.filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	c.headers = records[0]
	c.data = records[1:]
	return nil
}

func (c *FileConnector) loadExcel(sheet string) error {
	f, err := excelize.OpenFile(c.filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	if sheet == "" {
		sheets := f.GetSheetList()
		if len(sheets) == 0 {
			return fmt.Errorf("excel file has no sheets")
		}
		sheet = sheets[0]
	}
	rows, err := f.GetRows(sheet)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	c.headers = rows[0]
	c.data = rows[1:]
	return nil
}
```

**Step 3: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./internal/connector/...
```

**Step 4: Commit**

```bash
git add internal/connector/file.go go.mod go.sum
git commit -m "feat: implement FileConnector for CSV/Excel source/target"
```

---

### Task 5: 新建 ConnectorFactory + 更新 ConnPool

**Files:**
- Create: `internal/connector/factory.go`
- Modify: `internal/service/connpool.go`

**Step 1: 创建 factory.go**

```go
package connector

import (
	"datasync/internal/model"
	"fmt"
	"strings"
)

// FromDataSource 根据 DataSource 创建对应的 Connector。
func FromDataSource(ds model.DataSource) (Connector, error) {
	switch strings.ToLower(ds.DBType) {
	case "mysql", "postgresql", "oracle", "clickhouse", "doris":
		dsn := BuildDSN(ds.DBType, ds.Host, ds.Port, ds.Username, ds.Password, ds.DatabaseName, ds.ExtraParams)
		return NewSQLConnector(ds.DBType, dsn)
	case "mongodb":
		uri := buildMongoURI(ds)
		return NewMongoConnector(uri, ds.DatabaseName)
	case "csv", "excel":
		return NewFileConnector(ds.Host) // Host 字段存文件路径
	default:
		return nil, fmt.Errorf("unsupported db_type: %s", ds.DBType)
	}
}

func buildMongoURI(ds model.DataSource) string {
	authDB := ds.ExtraParams // ExtraParams 存 authDB
	if authDB == "" {
		authDB = ds.DatabaseName
	}
	if ds.Username != "" {
		return fmt.Sprintf("mongodb://%s:%s@%s:%d/%s",
			ds.Username, ds.Password, ds.Host, ds.Port, authDB)
	}
	return fmt.Sprintf("mongodb://%s:%d", ds.Host, ds.Port)
}
```

**Step 2: 更新 connpool.go — 缓存 Connector 而非 \*sql.DB**

用以下内容完整替换 `internal/service/connpool.go`：

```go
package service

import (
	"crypto/md5"
	"datasync/internal/connector"
	"datasync/internal/model"
	"fmt"
	"sync"
)

// ConnPool 缓存 Connector 实例，避免每次任务都重新建连。
type ConnPool struct {
	mu    sync.Mutex
	conns map[string]connector.Connector
}

func NewConnPool() *ConnPool {
	return &ConnPool{conns: make(map[string]connector.Connector)}
}

// Get 返回缓存的 Connector，不存在或 Ping 失败则重新创建。
func (p *ConnPool) Get(ds model.DataSource) (connector.Connector, error) {
	key := dsKey(ds)
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[key]; ok {
		if err := conn.Ping(nil); err == nil {
			return conn, nil
		}
		conn.Close()
		delete(p.conns, key)
	}

	conn, err := connector.FromDataSource(ds)
	if err != nil {
		return nil, err
	}
	p.conns[key] = conn
	return conn, nil
}

// CloseAll 关闭所有缓存连接，应用关闭时调用。
func (p *ConnPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, conn := range p.conns {
		conn.Close()
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

注意：`conn.Ping(nil)` 中 nil 需要替换为 `context.Background()`，记得加 import "context"。

**Step 3: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

**Step 4: Commit**

```bash
git add internal/connector/factory.go internal/service/connpool.go
git commit -m "feat: add ConnectorFactory and refactor ConnPool to cache Connector"
```

---

### Task 6: 重构 engine/data.go 使用 Connector 接口

**Files:**
- Modify: `internal/engine/data.go`

**Step 1: 替换 DataSyncOptions 的 SourceDB/TargetDB 为 Connector**

将文件开头的 `DataSyncOptions` 改为：

```go
type DataSyncOptions struct {
	Source        connector.Connector
	Target        connector.Connector
	SourceTable   string
	TargetTable   string
	Mappings      []model.FieldMapping
	BatchSize     int
	WriteStrategy string
	OnProgress    func(synced, total int64)
	WhereClause   string
	Concurrency   int
	StartOffset   int64           // 断点续传起始 offset
	OnCheckpoint  func(int64)     // 每批完成后保存 checkpoint
}
```

**Step 2: 更新 SyncData 主循环**

用 Connector 接口方法替换所有直接 SQL 操作：

```go
func SyncData(ctx context.Context, opts DataSyncOptions) (*SyncResult, error) {
	start := time.Now()
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	if opts.WriteStrategy == "" {
		opts.WriteStrategy = "insert"
	}
	if opts.Concurrency > 1 {
		return syncDataParallel(ctx, opts)
	}

	totalRows, err := opts.Source.CountRows(ctx, opts.SourceTable, opts.WhereClause)
	if err != nil {
		return nil, fmt.Errorf("count source rows: %w", err)
	}

	sourceCols, targetCols := columnsFromMappings(opts.Mappings)
	if len(sourceCols) == 0 {
		schema, err := opts.Source.GetSchema(ctx, opts.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("get source schema: %w", err)
		}
		for _, col := range schema.Columns {
			sourceCols = append(sourceCols, col.Name)
			targetCols = append(targetCols, col.Name)
		}
	}

	var pkCols []string
	if opts.WriteStrategy == "upsert" {
		schema, err := opts.Target.GetSchema(ctx, opts.TargetTable)
		if err != nil {
			return nil, fmt.Errorf("get target schema for upsert: %w", err)
		}
		for _, col := range schema.Columns {
			if col.IsPrimary {
				pkCols = append(pkCols, col.Name)
			}
		}
		if len(pkCols) == 0 {
			return nil, fmt.Errorf("upsert requires primary key on target table %s", opts.TargetTable)
		}
	}

	var rowsSynced int64
	for offset := opts.StartOffset; offset < totalRows; offset += int64(opts.BatchSize) {
		select {
		case <-ctx.Done():
			return &SyncResult{RowsSynced: rowsSynced, Duration: time.Since(start)}, ctx.Err()
		default:
		}

		rows, err := opts.Source.ReadBatch(ctx, connector.ReadOptions{
			Table:   opts.SourceTable,
			Columns: sourceCols,
			Where:   opts.WhereClause,
			Offset:  offset,
			Limit:   int64(opts.BatchSize),
		})
		if err != nil {
			return nil, fmt.Errorf("read batch at offset %d: %w", offset, err)
		}
		if len(rows) == 0 {
			break
		}

		// 重新映射列名（source→target）
		if len(opts.Mappings) > 0 {
			rows = remapColumns(rows, sourceCols, targetCols)
		}

		if err := opts.Target.WriteBatch(ctx, connector.WriteOptions{
			Table:    opts.TargetTable,
			Columns:  targetCols,
			Rows:     rows,
			Strategy: opts.WriteStrategy,
			PKCols:   pkCols,
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

// remapColumns 将 source 列名替换为 target 列名。
func remapColumns(rows []connector.Row, srcCols, tgtCols []string) []connector.Row {
	result := make([]connector.Row, len(rows))
	for i, row := range rows {
		newRow := make(connector.Row, len(row))
		for j, src := range srcCols {
			if j < len(tgtCols) {
				newRow[tgtCols[j]] = row[src]
			}
		}
		result[i] = newRow
	}
	return result
}
```

**Step 3: 更新 syncDataParallel 和 detectIntPK**

`detectIntPK` 改为从 `opts.Source.GetSchema()` 读取，不再接收 `*sql.DB`：

```go
func detectIntPKFromConnector(ctx context.Context, src connector.Connector, table string) string {
	schema, err := src.GetSchema(ctx, table)
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

在 `syncDataParallel` 中用 `detectIntPKFromConnector(ctx, opts.Source, opts.SourceTable)` 替换原调用，MIN/MAX 查询改用 `opts.Source.ReadBatch` 或通过 SQL Connector 的 RawDB()（仅 SQL 类型支持分片）。

**Step 4: 删除不再需要的函数**

删除 `buildCountSQL`、`buildBatchSelectSQL`、`writeBatch`、`scanRows`、`getSourceColumns`、`getPrimaryKeyColumns`、`detectIntPK` 等已被 Connector 接口取代的函数。

**Step 5: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

**Step 6: Commit**

```bash
git add internal/engine/data.go
git commit -m "refactor: engine/data.go uses Connector interface instead of *sql.DB"
```

---

### Task 7: 重构 engine/schema.go 和 engine/quality.go

**Files:**
- Modify: `internal/engine/schema.go`
- Modify: `internal/engine/quality.go`

**Step 1: 更新 SyncStructure 签名**

`schema.go` 的 `SyncStructure` 目前接收 `*sql.DB`，改为接收 `connector.Connector`：

```go
// SyncStructure 同步表结构：读取源 schema，在目标库创建/修改表。
func SyncStructure(src, dst connector.Connector, srcTable, dstTable string, mappings []model.FieldMapping) error {
	schema, err := src.GetSchema(context.Background(), srcTable)
	if err != nil {
		return fmt.Errorf("read source schema: %w", err)
	}
	// 转换为 TableSchema 并生成 DDL（复用现有 generateCreateDDL / diffAndAlter 逻辑）
	// ...
}
```

**Step 2: 更新 QualityCheckOptions**

`quality.go` 中把 `SourceDB *sql.DB` 和 `TargetDB *sql.DB` 改为：

```go
type QualityCheckOptions struct {
	Source      connector.Connector
	Target      connector.Connector
	SourceTable string
	TargetTable string
	WhereClause string
	Mappings    []model.FieldMapping
	SampleSize  int
}
```

内部 CountRows / ReadBatch 调用改为调用 Connector 接口方法。

**Step 3: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

**Step 4: Commit**

```bash
git add internal/engine/schema.go internal/engine/quality.go
git commit -m "refactor: schema.go and quality.go use Connector interface"
```

---

### Task 8: 更新 executor.go 使用新接口

**Files:**
- Modify: `internal/service/executor.go`

**Step 1: 替换 *sql.DB 为 connector.Connector**

将 `executor.go` 中所有 `*sql.DB` 的使用替换为 `connector.Connector`：

```go
// Run 中替换：
sourceConn, err := e.Pool.Get(sourceDS)  // 返回 connector.Connector
targetConn, err := e.Pool.Get(targetDS)

// CDC 路径：从 sourceConn 提取 RawDB（仅 SQL 类型）
if sqlConn, ok := sourceConn.(*connector.SQLConnector); ok {
    // CDC 需要直接 *sql.DB，通过 RawDB() 获取
    _ = sqlConn.RawDB()
}

// 数据同步路径：
opts := engine.DataSyncOptions{
    Source:       sourceConn,
    Target:       targetConn,
    SourceTable:  task.SourceTable,
    TargetTable:  task.TargetTable,
    Mappings:     mappings,
    BatchSize:    1000,
    OnProgress:   makeOnProgress(),
    WhereClause:  buildWhereClause(task, sourceDS.DBType),
    Concurrency:  task.Concurrency,
    StartOffset:  task.CheckpointOffset,
    OnCheckpoint: func(offset int64) {
        e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).
            Update("checkpoint_offset", offset)
    },
}
```

**Step 2: 任务完成后清零 checkpoint**

在同步成功后加：
```go
e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).Update("checkpoint_offset", 0)
```

**Step 3: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

**Step 4: Commit**

```bash
git add internal/service/executor.go
git commit -m "refactor: executor.go uses Connector interface and checkpoint resume"
```

---

### Task 9: 更新 Model — 新增 CheckpointOffset，DataSource 支持新类型

**Files:**
- Modify: `internal/model/models.go`
- Modify: `internal/database/sqlite.go`（AutoMigrate 会自动处理）

**Step 1: 更新 SyncTask**

在 `SyncTask` 结构体加字段：

```go
CheckpointOffset int64 `gorm:"default:0" json:"checkpoint_offset"`
```

**Step 2: 验证 AutoMigrate**

`database/sqlite.go` 中已有 `AutoMigrate(&model.SyncTask{}, ...)` 会自动加列，无需手动改。

**Step 3: 构建并运行**

```bash
cd /Users/mac/Documents/dataSync && go build ./... && go test ./...
```

**Step 4: Commit**

```bash
git add internal/model/models.go
git commit -m "feat: add checkpoint_offset to SyncTask for resume support"
```

---

### Task 10: 更新 handler/datasource.go + 文件上传 API

**Files:**
- Modify: `internal/handler/datasource.go`
- Modify: `main.go`

**Step 1: 在 datasource handler 中加 TestConn 对新类型的支持**

`TestConn` 中调用 `connector.FromDataSource(ds)` + `Ping`，统一替换原有的 `engine.TestConnection(ds)`：

```go
func (h *DataSourceHandler) TestConn(c *gin.Context) {
    // 解析 form 参数 → model.DataSource
    // ...
    conn, err := connector.FromDataSource(ds)
    if err != nil {
        c.JSON(400, gin.H{"error": err.Error()})
        return
    }
    defer conn.Close()
    if err := conn.Ping(c.Request.Context()); err != nil {
        c.JSON(200, gin.H{"success": false, "error": err.Error()})
        return
    }
    c.JSON(200, gin.H{"success": true})
}
```

**Step 2: 在 handler/datasource.go 中加文件上传接口**

```go
// Upload 接收上传文件，保存到 ./uploads/ 目录，返回文件路径。
func (h *DataSourceHandler) Upload(c *gin.Context) {
    file, header, err := c.Request.FormFile("file")
    if err != nil {
        c.JSON(400, gin.H{"error": err.Error()})
        return
    }
    defer file.Close()

    os.MkdirAll("./uploads", 0755)
    dst := "./uploads/" + header.Filename
    out, err := os.Create(dst)
    if err != nil {
        c.JSON(500, gin.H{"error": err.Error()})
        return
    }
    defer out.Close()
    io.Copy(out, file)
    c.JSON(200, gin.H{"path": dst})
}
```

**Step 3: 在 main.go 注册上传路由**

```go
api.POST("/upload", dsHandler.Upload)
```

**Step 4: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

**Step 5: Commit**

```bash
git add internal/handler/datasource.go main.go
git commit -m "feat: add file upload API and update TestConn to use Connector interface"
```

---

### Task 11: 更新 UI — 数据源表单支持新类型

**Files:**
- Modify: `templates/datasource/form.html`

**Step 1: 在 db_type select 中加新选项**

```html
<option value="clickhouse">ClickHouse</option>
<option value="doris">Doris</option>
<option value="mongodb">MongoDB</option>
<option value="csv">CSV 文件</option>
<option value="excel">Excel 文件</option>
```

**Step 2: 用 JS 根据类型动态显示/隐藏字段**

```html
<script>
function updateFields() {
    var t = document.getElementById('db_type').value;
    var isFile = (t === 'csv' || t === 'excel');
    var isMongo = (t === 'mongodb');

    // 文件类型：显示文件路径+上传，隐藏 host/port/user/pass/db
    document.getElementById('field-host-port').style.display = isFile ? 'none' : '';
    document.getElementById('field-auth').style.display = isFile ? 'none' : '';
    document.getElementById('field-dbname').style.display = isFile ? 'none' : '';
    document.getElementById('field-file').style.display = isFile ? '' : 'none';

    // MongoDB：显示 authDB（ExtraParams 字段复用）
    document.getElementById('field-extra').querySelector('label').textContent =
        isMongo ? 'Auth Database' : '额外参数';
}
document.getElementById('db_type').addEventListener('change', updateFields);
updateFields();
</script>
```

**Step 3: 加文件路径 + 上传 UI**

```html
<div id="field-file" style="display:none">
    <label class="block text-sm font-medium text-gray-700 mb-1">文件路径</label>
    <input type="text" name="host" id="file-path" placeholder="/data/users.csv 或点击上传"
        class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm">
    <input type="file" id="file-upload" accept=".csv,.xlsx,.xls" class="mt-2">
    <button type="button" onclick="uploadFile()"
        class="mt-1 text-sm text-indigo-600 hover:underline">上传文件</button>
</div>
<script>
function uploadFile() {
    var input = document.getElementById('file-upload');
    if (!input.files[0]) return;
    var fd = new FormData();
    fd.append('file', input.files[0]);
    fetch('/api/upload', {method:'POST', body:fd})
        .then(r => r.json())
        .then(d => {
            if (d.path) document.getElementById('file-path').value = d.path;
        });
}
</script>
```

**Step 4: 构建验证**

```bash
cd /Users/mac/Documents/dataSync && go build ./...
```

**Step 5: Commit**

```bash
git add templates/datasource/form.html
git commit -m "feat: datasource form supports ClickHouse/Doris/MongoDB/CSV/Excel"
```

---

### Task 12: 集成测试 + 修复

**Files:**
- Modify: 根据编译/测试报错修复各文件

**Step 1: 运行全量测试**

```bash
cd /Users/mac/Documents/dataSync && go test ./... -v 2>&1 | tail -50
```

**Step 2: 修复测试中引用旧 API 的地方**

`engine/data_test.go`、`engine/connector_test.go`、`engine/schema_test.go` 中凡是使用 `*sql.DB` 的地方，改为创建 `connector.WrapSQLDB(db, "sqlite")` 或 `connector.SQLConnector`。

**Step 3: 最终构建**

```bash
cd /Users/mac/Documents/dataSync && go build -o datasync . && echo "BUILD OK"
```

Expected: `BUILD OK`

**Step 4: Final Commit**

```bash
git add -A
git commit -m "feat: complete Connector refactor with ClickHouse/Doris/MongoDB/CSV/Excel + checkpoint resume"
```
