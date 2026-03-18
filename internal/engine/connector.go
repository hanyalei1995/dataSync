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
