package engine

import (
	"datasync/internal/model"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildDSN_MySQL(t *testing.T) {
	ds := model.DataSource{
		DBType: "mysql", Host: "localhost", Port: 3306,
		Username: "root", Password: "pass", DatabaseName: "testdb",
	}
	dsn := BuildDSN(ds)
	assert.Equal(t, "root:pass@tcp(localhost:3306)/testdb?parseTime=true", dsn)
}

func TestBuildDSN_PostgreSQL(t *testing.T) {
	ds := model.DataSource{
		DBType: "postgresql", Host: "localhost", Port: 5432,
		Username: "postgres", Password: "pass", DatabaseName: "testdb",
	}
	dsn := BuildDSN(ds)
	assert.Equal(t, "postgres://postgres:pass@localhost:5432/testdb?sslmode=disable", dsn)
}

func TestBuildDSN_Oracle(t *testing.T) {
	ds := model.DataSource{
		DBType: "oracle", Host: "localhost", Port: 1521,
		Username: "sys", Password: "pass", DatabaseName: "orcl",
	}
	dsn := BuildDSN(ds)
	assert.Equal(t, "oracle://sys:pass@localhost:1521/orcl", dsn)
}

func TestBuildDSN_Unknown(t *testing.T) {
	ds := model.DataSource{
		DBType: "unknown", Host: "localhost", Port: 1234,
		Username: "user", Password: "pass", DatabaseName: "db",
	}
	dsn := BuildDSN(ds)
	assert.Equal(t, "", dsn)
}

func TestDriverName(t *testing.T) {
	assert.Equal(t, "mysql", DriverName("mysql"))
	assert.Equal(t, "pgx", DriverName("postgresql"))
	assert.Equal(t, "oracle", DriverName("oracle"))
	assert.Equal(t, "", DriverName("unknown"))
}
