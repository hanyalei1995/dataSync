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
	Ping(ctx context.Context) error
	ListTables(ctx context.Context) ([]string, error)
	GetSchema(ctx context.Context, table string) (*Schema, error)
	CountRows(ctx context.Context, table, where string) (int64, error)
	ReadBatch(ctx context.Context, opts ReadOptions) ([]Row, error)
	WriteBatch(ctx context.Context, opts WriteOptions) error
	Close() error
	DBType() string
}
