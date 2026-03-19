package connector

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/sijms/go-ora/v2"
)

// SQLConnector 实现基于 database/sql 的 Connector，支持 MySQL/PG/Oracle/ClickHouse/Doris。
type SQLConnector struct {
	db     *sql.DB
	dbType string
}

// NewSQLConnector 创建 SQLConnector。
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

// BuildDSN 根据字段组装 DSN。
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

func (c *SQLConnector) DBType() string { return c.dbType }

func (c *SQLConnector) Ping(ctx context.Context) error { return c.db.PingContext(ctx) }

func (c *SQLConnector) Close() error { return c.db.Close() }

// RawDB 返回底层 *sql.DB，供 CDC 等特殊场景使用。
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
	cols, err := c.readColumns(ctx, table)
	if err != nil {
		return nil, err
	}
	return &Schema{TableName: table, Columns: cols}, nil
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
		FROM information_schema.COLUMNS WHERE TABLE_NAME=? AND TABLE_SCHEMA=DATABASE() ORDER BY ORDINAL_POSITION`
	rows, err := c.db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanColumnInfoRows(rows)
}

func (c *SQLConnector) readPGColumns(ctx context.Context, table string) ([]ColumnInfo, error) {
	q := `SELECT c.column_name, c.udt_name,
		CASE WHEN c.is_nullable='YES' THEN 1 ELSE 0 END,
		CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END
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
	q := "SELECT name, type, 0, 0 FROM system.columns WHERE table=? ORDER BY position"
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
	colList := "*"
	if len(opts.Columns) > 0 {
		quoted := make([]string, len(opts.Columns))
		for i, col := range opts.Columns {
			quoted[i] = c.quoteIdent(col)
		}
		colList = strings.Join(quoted, ", ")
	}
	whereClause := ""
	if opts.Where != "" {
		whereClause = " WHERE " + opts.Where
	}
	tbl := c.quoteIdent(opts.Table)

	var query string
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
		return nil, fmt.Errorf("ReadBatch: %w", err)
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []Row
	for rows.Next() {
		vals := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
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
		for k := range opts.Rows[0] {
			cols = append(cols, k)
		}
		sort.Strings(cols) // map 迭代顺序不稳定，排序保证 vals 与列名对齐
	}
	quotedCols := make([]string, len(cols))
	for i, col := range cols {
		quotedCols[i] = c.quoteIdent(col)
	}
	tbl := c.quoteIdent(opts.Table)

	if opts.Strategy == StrategyUpsert && c.dbType != "clickhouse" {
		return c.upsertBatch(ctx, tbl, cols, quotedCols, opts)
	}
	return c.insertBatch(ctx, tbl, cols, quotedCols, opts.Rows)
}

func (c *SQLConnector) insertBatch(ctx context.Context, tbl string, cols, quotedCols []string, rows []Row) error {
	placeholders := make([]string, len(cols))
	for i := range cols {
		switch c.dbType {
		case "oracle":
			placeholders[i] = fmt.Sprintf(":%d", i+1)
		case "postgresql":
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		default:
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
		vals := make([]any, len(cols))
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
		updates := make([]string, len(quotedCols))
		for i, col := range quotedCols {
			updates[i] = fmt.Sprintf("%s=VALUES(%s)", col, col)
		}
		ph := strings.Repeat("?,", len(cols))
		ph = ph[:len(ph)-1]
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			tbl, strings.Join(quotedCols, ","), ph, strings.Join(updates, ","))
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
			vals := make([]any, len(cols))
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
		pkSet := make(map[string]bool, len(opts.PKCols))
		for _, pk := range opts.PKCols {
			pkSet[pk] = true
		}
		updates := make([]string, 0, len(cols))
		for _, col := range cols {
			if !pkSet[col] {
				qc := c.quoteIdent(col)
				updates = append(updates, fmt.Sprintf("%s=EXCLUDED.%s", qc, qc))
			}
		}
		conflictCols := make([]string, len(opts.PKCols))
		for i, pk := range opts.PKCols {
			conflictCols[i] = c.quoteIdent(pk)
		}
		ph := make([]string, len(cols))
		for i := range cols {
			ph[i] = fmt.Sprintf("$%d", i+1)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			tbl, strings.Join(quotedCols, ","), strings.Join(ph, ","),
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
			vals := make([]any, len(cols))
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
		// Oracle: MERGE INTO
		pkSet := make(map[string]bool, len(opts.PKCols))
		for _, pk := range opts.PKCols {
			pkSet[pk] = true
		}
		onClauses := make([]string, len(opts.PKCols))
		for i, pk := range opts.PKCols {
			qpk := c.quoteIdent(pk)
			onClauses[i] = fmt.Sprintf("t.%s=s.%s", qpk, qpk)
		}
		updateCols := make([]string, 0)
		for _, col := range cols {
			if !pkSet[col] {
				qc := c.quoteIdent(col)
				updateCols = append(updateCols, fmt.Sprintf("t.%s=s.%s", qc, qc))
			}
		}
		srcCols := make([]string, len(cols))
		bindParams := make([]string, len(cols))
		for i, col := range cols {
			srcCols[i] = fmt.Sprintf(":%d %s", i+1, c.quoteIdent(col))
			bindParams[i] = fmt.Sprintf(":%d", i+1)
		}
		query := fmt.Sprintf(
			`MERGE INTO %s t USING (SELECT %s FROM DUAL) s ON (%s)
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
			vals := make([]any, len(cols))
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
	case "postgresql", "oracle":
		return `"` + name + `"`
	default: // mysql, doris, clickhouse
		return "`" + name + "`"
	}
}
