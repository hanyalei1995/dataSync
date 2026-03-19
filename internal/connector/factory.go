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
		// Host 字段复用为文件路径；使用显式类型避免因路径无扩展名导致报错
		return NewFileConnectorWithType(ds.Host, strings.ToLower(ds.DBType))
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
