package engine

import (
	"datasync/internal/model"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildBatchSelectSQL_MySQL(t *testing.T) {
	sql := buildBatchSelectSQL("mysql", "users", []string{"id", "name"}, 0, 100)
	assert.Contains(t, sql, "SELECT")
	assert.Contains(t, sql, "`id`")
	assert.Contains(t, sql, "`name`")
	assert.Contains(t, sql, "`users`")
	assert.Contains(t, sql, "LIMIT 100 OFFSET 0")
}

func TestBuildBatchSelectSQL_PostgreSQL(t *testing.T) {
	sql := buildBatchSelectSQL("postgresql", "users", []string{"id", "name"}, 50, 100)
	assert.Contains(t, sql, "SELECT")
	assert.Contains(t, sql, "\"id\"")
	assert.Contains(t, sql, "\"name\"")
	assert.Contains(t, sql, "\"users\"")
	assert.Contains(t, sql, "LIMIT 100 OFFSET 50")
}

func TestBuildBatchSelectSQL_Oracle(t *testing.T) {
	sql := buildBatchSelectSQL("oracle", "users", []string{"id", "name"}, 0, 100)
	assert.Contains(t, sql, "SELECT")
	assert.Contains(t, sql, "ROWNUM")
	assert.Contains(t, sql, "\"ID\"")
	assert.Contains(t, sql, "\"NAME\"")
}

func TestBuildInsertSQL_MySQL(t *testing.T) {
	sql := buildInsertSQL("mysql", "users", []string{"id", "name", "email"}, 2)
	assert.Contains(t, sql, "INSERT IGNORE INTO")
	assert.Contains(t, sql, "`users`")
	assert.Contains(t, sql, "`id`")
	assert.Contains(t, sql, "?")
	// 2 rows x 3 cols = should have two value groups
	assert.Equal(t, 2, strings.Count(sql, "(?, ?, ?)"))
}

func TestBuildInsertSQL_PostgreSQL(t *testing.T) {
	sql := buildInsertSQL("postgresql", "users", []string{"id", "name"}, 1)
	assert.Contains(t, sql, "INSERT INTO")
	assert.Contains(t, sql, "\"users\"")
	assert.Contains(t, sql, "ON CONFLICT DO NOTHING")
	assert.Contains(t, sql, "$1")
	assert.Contains(t, sql, "$2")
}

func TestBuildInsertSQL_Oracle_SingleRow(t *testing.T) {
	sql := buildInsertSQL("oracle", "users", []string{"id", "name"}, 1)
	assert.Contains(t, sql, "INSERT INTO")
	assert.Contains(t, sql, "\"USERS\"")
	assert.Contains(t, sql, ":1")
	assert.Contains(t, sql, ":2")
	assert.NotContains(t, sql, "INSERT ALL")
}

func TestBuildInsertSQL_Oracle_MultiRow(t *testing.T) {
	sql := buildInsertSQL("oracle", "users", []string{"id", "name"}, 3)
	assert.Contains(t, sql, "INSERT ALL")
	assert.Contains(t, sql, "SELECT 1 FROM DUAL")
	assert.Contains(t, sql, "\"USERS\"")
}

func TestBuildUpsertSQL_MySQL(t *testing.T) {
	sql := buildUpsertSQL("mysql", "users", []string{"id", "name", "email"}, []string{"id"}, 1)
	assert.Contains(t, sql, "INSERT INTO")
	assert.Contains(t, sql, "ON DUPLICATE KEY UPDATE")
	assert.Contains(t, sql, "`name` = VALUES(`name`)")
	assert.Contains(t, sql, "`email` = VALUES(`email`)")
	// PK column should not appear in UPDATE clause
	assert.NotContains(t, sql, "`id` = VALUES(`id`)")
}

func TestBuildUpsertSQL_PostgreSQL(t *testing.T) {
	sql := buildUpsertSQL("postgresql", "users", []string{"id", "name", "email"}, []string{"id"}, 1)
	assert.Contains(t, sql, "INSERT INTO")
	assert.Contains(t, sql, "ON CONFLICT")
	assert.Contains(t, sql, "DO UPDATE SET")
	assert.Contains(t, sql, "\"name\" = EXCLUDED.\"name\"")
	assert.Contains(t, sql, "\"email\" = EXCLUDED.\"email\"")
}

func TestBuildUpsertSQL_Oracle(t *testing.T) {
	sql := buildUpsertSQL("oracle", "users", []string{"id", "name", "email"}, []string{"id"}, 1)
	assert.Contains(t, sql, "MERGE INTO")
	assert.Contains(t, sql, "USING")
	assert.Contains(t, sql, "WHEN MATCHED THEN UPDATE SET")
	assert.Contains(t, sql, "WHEN NOT MATCHED THEN INSERT")
	assert.Contains(t, sql, "FROM DUAL")
}

func TestBuildPlaceholderRow_MySQL(t *testing.T) {
	row := buildPlaceholderRow("mysql", 3, 0)
	assert.Equal(t, "?, ?, ?", row)
}

func TestBuildPlaceholderRow_PostgreSQL(t *testing.T) {
	row := buildPlaceholderRow("postgresql", 3, 0)
	assert.Equal(t, "$1, $2, $3", row)

	row2 := buildPlaceholderRow("postgresql", 2, 3)
	assert.Equal(t, "$4, $5", row2)
}

func TestBuildPlaceholderRow_Oracle(t *testing.T) {
	row := buildPlaceholderRow("oracle", 3, 0)
	assert.Equal(t, ":1, :2, :3", row)
}

func TestColumnsFromMappings(t *testing.T) {
	mappings := []model.FieldMapping{
		{SourceField: "id", TargetField: "id", Enabled: true},
		{SourceField: "name", TargetField: "user_name", Enabled: true},
		{SourceField: "deleted", TargetField: "", Enabled: false},
		{SourceField: "email", TargetField: "", Enabled: true},
	}

	srcCols, tgtCols := columnsFromMappings(mappings)

	// Only enabled mappings should be returned
	assert.Equal(t, 3, len(srcCols))
	assert.Equal(t, 3, len(tgtCols))

	assert.Equal(t, []string{"id", "name", "email"}, srcCols)
	// Empty target should fall back to source field name
	assert.Equal(t, []string{"id", "user_name", "email"}, tgtCols)
}

func TestColumnsFromMappings_Empty(t *testing.T) {
	srcCols, tgtCols := columnsFromMappings(nil)
	assert.Nil(t, srcCols)
	assert.Nil(t, tgtCols)
}
