package engine

import (
	"testing"

	"datasync/internal/model"

	"github.com/stretchr/testify/assert"
)

func TestMapType(t *testing.T) {
	// Test MySQL to PostgreSQL mappings
	assert.Equal(t, "INTEGER", MapType("mysql", "postgresql", "INT"))
	assert.Equal(t, "BYTEA", MapType("mysql", "postgresql", "BLOB"))
	assert.Equal(t, "TIMESTAMP", MapType("mysql", "postgresql", "DATETIME"))
	assert.Equal(t, "BOOLEAN", MapType("mysql", "postgresql", "TINYINT(1)"))
	assert.Equal(t, "SMALLINT", MapType("mysql", "postgresql", "TINYINT"))
	assert.Equal(t, "DOUBLE PRECISION", MapType("mysql", "postgresql", "DOUBLE"))

	// Test MySQL to Oracle mappings
	assert.Equal(t, "NUMBER(10)", MapType("mysql", "oracle", "INT"))
	assert.Equal(t, "CLOB", MapType("mysql", "oracle", "TEXT"))
	assert.Equal(t, "VARCHAR2", MapType("mysql", "oracle", "VARCHAR"))
	assert.Equal(t, "NUMBER(19)", MapType("mysql", "oracle", "BIGINT"))
	assert.Equal(t, "NUMBER(1)", MapType("mysql", "oracle", "BOOLEAN"))

	// Test PostgreSQL to MySQL mappings
	assert.Equal(t, "INT", MapType("postgresql", "mysql", "INTEGER"))
	assert.Equal(t, "DOUBLE", MapType("postgresql", "mysql", "DOUBLE PRECISION"))
	assert.Equal(t, "BLOB", MapType("postgresql", "mysql", "BYTEA"))
	assert.Equal(t, "TINYINT(1)", MapType("postgresql", "mysql", "BOOLEAN"))
	assert.Equal(t, "INT AUTO_INCREMENT", MapType("postgresql", "mysql", "SERIAL"))

	// Test PostgreSQL to Oracle mappings
	assert.Equal(t, "NUMBER(10)", MapType("postgresql", "oracle", "INTEGER"))
	assert.Equal(t, "VARCHAR2", MapType("postgresql", "oracle", "VARCHAR"))
	assert.Equal(t, "CLOB", MapType("postgresql", "oracle", "TEXT"))
	assert.Equal(t, "NUMBER(1)", MapType("postgresql", "oracle", "BOOLEAN"))

	// Test Oracle to MySQL mappings
	assert.Equal(t, "DECIMAL", MapType("oracle", "mysql", "NUMBER"))
	assert.Equal(t, "VARCHAR", MapType("oracle", "mysql", "VARCHAR2"))
	assert.Equal(t, "TEXT", MapType("oracle", "mysql", "CLOB"))
	assert.Equal(t, "TINYINT(1)", MapType("oracle", "mysql", "NUMBER(1)"))

	// Test Oracle to PostgreSQL mappings
	assert.Equal(t, "NUMERIC", MapType("oracle", "postgresql", "NUMBER"))
	assert.Equal(t, "VARCHAR", MapType("oracle", "postgresql", "VARCHAR2"))
	assert.Equal(t, "TEXT", MapType("oracle", "postgresql", "CLOB"))
	assert.Equal(t, "BOOLEAN", MapType("oracle", "postgresql", "NUMBER(1)"))
	assert.Equal(t, "BYTEA", MapType("oracle", "postgresql", "BLOB"))

	// Test fallback (unknown type returns original uppercased)
	assert.Equal(t, "CUSTOMTYPE", MapType("mysql", "postgresql", "CUSTOMTYPE"))
	assert.Equal(t, "CUSTOMTYPE", MapType("mysql", "postgresql", "customtype"))

	// Test case insensitivity of input
	assert.Equal(t, "INTEGER", MapType("mysql", "postgresql", "int"))
	assert.Equal(t, "BYTEA", MapType("mysql", "postgresql", "blob"))

	// Test size preservation for VARCHAR
	assert.Equal(t, "VARCHAR(100)", MapType("postgresql", "mysql", "VARCHAR(100)"))
	assert.Equal(t, "VARCHAR2(200)", MapType("mysql", "oracle", "VARCHAR(200)"))
}

func TestGenerateCreateSQL(t *testing.T) {
	schema := &TableSchema{
		TableName:    "users",
		SourceDBType: "mysql",
		Columns: []ColumnSchema{
			{Name: "id", Type: "INT", IsPrimary: true},
			{Name: "name", Type: "VARCHAR(100)", Nullable: false},
			{Name: "email", Type: "VARCHAR(200)", Nullable: true},
		},
	}

	sql := GenerateCreateSQL(schema, "postgresql", "users", nil)
	assert.Contains(t, sql, "CREATE TABLE")
	assert.Contains(t, sql, "users")
	assert.Contains(t, sql, "PRIMARY KEY")
	assert.Contains(t, sql, "INTEGER")      // INT → INTEGER
	assert.Contains(t, sql, "VARCHAR(100)") // preserved size
	assert.Contains(t, sql, "NOT NULL")     // name is not nullable
}

func TestGenerateCreateSQLWithMappings(t *testing.T) {
	schema := &TableSchema{
		TableName:    "users",
		SourceDBType: "mysql",
		Columns: []ColumnSchema{
			{Name: "id", Type: "INT", IsPrimary: true},
			{Name: "name", Type: "VARCHAR(100)", Nullable: false},
			{Name: "email", Type: "VARCHAR(200)", Nullable: true},
			{Name: "secret", Type: "TEXT", Nullable: true},
		},
	}

	mappings := []model.FieldMapping{
		{SourceField: "name", TargetField: "full_name", Enabled: true},
		{SourceField: "secret", TargetField: "", Enabled: false}, // disabled
	}

	sql := GenerateCreateSQL(schema, "postgresql", "pg_users", mappings)
	assert.Contains(t, sql, "full_name")
	assert.NotContains(t, sql, "secret")
	assert.Contains(t, sql, "pg_users")
}

func TestCompareSchema(t *testing.T) {
	source := &TableSchema{
		TableName: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: "INT", IsPrimary: true},
			{Name: "name", Type: "VARCHAR(100)"},
			{Name: "age", Type: "INT"},    // new column
			{Name: "email", Type: "TEXT"}, // type changed from VARCHAR(200)
		},
	}

	target := &TableSchema{
		TableName: "users",
		Columns: []ColumnSchema{
			{Name: "id", Type: "INT", IsPrimary: true},
			{Name: "name", Type: "VARCHAR(100)"},
			{Name: "email", Type: "VARCHAR(200)"},
			{Name: "phone", Type: "VARCHAR(50)"}, // to be dropped
		},
	}

	diffs := CompareSchema(source, target)
	assert.Len(t, diffs, 3)

	actions := make(map[string][]string)
	for _, d := range diffs {
		actions[d.Action] = append(actions[d.Action], d.Column.Name)
	}

	assert.Contains(t, actions["add"], "age")
	assert.Contains(t, actions["modify"], "email")
	assert.Contains(t, actions["drop"], "phone")
}

func TestGenerateAlterSQL(t *testing.T) {
	diffs := []SchemaDiff{
		{Action: "add", Column: ColumnSchema{Name: "age", Type: "INTEGER", Nullable: true}},
		{Action: "modify", Column: ColumnSchema{Name: "email", Type: "TEXT"}},
		{Action: "drop", Column: ColumnSchema{Name: "phone", Type: "VARCHAR(50)"}},
	}

	// PostgreSQL
	stmts := GenerateAlterSQL(diffs, "postgresql", "users")
	assert.Len(t, stmts, 3)
	assert.Contains(t, stmts[0], "ADD COLUMN")
	assert.Contains(t, stmts[1], "ALTER COLUMN")
	assert.Contains(t, stmts[1], "TYPE TEXT")
	assert.Contains(t, stmts[2], "DROP COLUMN")

	// MySQL
	stmts = GenerateAlterSQL(diffs, "mysql", "users")
	assert.Len(t, stmts, 3)
	assert.Contains(t, stmts[0], "ADD COLUMN")
	assert.Contains(t, stmts[1], "MODIFY COLUMN")
	assert.Contains(t, stmts[2], "DROP COLUMN")

	// Oracle
	stmts = GenerateAlterSQL(diffs, "oracle", "users")
	assert.Len(t, stmts, 3)
	assert.Contains(t, stmts[0], "ADD (")
	assert.Contains(t, stmts[1], "MODIFY (")
	assert.Contains(t, stmts[2], "DROP (")
}

func TestMapTypePreservesSize(t *testing.T) {
	// DECIMAL with precision should be preserved
	assert.Equal(t, "DECIMAL(10,2)", MapType("postgresql", "mysql", "DECIMAL(10,2)"))

	// NUMBER with size in oracle→mysql mapping should still map correctly
	assert.Equal(t, "TINYINT(1)", MapType("oracle", "mysql", "NUMBER(1)"))
	assert.Equal(t, "BOOLEAN", MapType("oracle", "postgresql", "NUMBER(1)"))
}
