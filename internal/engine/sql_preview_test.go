package engine

import (
	"context"
	"datasync/internal/connector"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestBuildPreviewPageSQL_OracleUsesRownumWrapper(t *testing.T) {
	got := buildPreviewPageSQL("oracle", "select id, name from orders order by id", 50, 100)

	assert.Contains(t, got, "ROWNUM datasync_rn")
	assert.Contains(t, got, "WHERE ROWNUM <= 150")
	assert.Contains(t, got, "WHERE datasync_rn > 100")
}

func TestPreviewSQL_ReturnsColumnsRowsAndTotal(t *testing.T) {
	gdb, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	require.NoError(t, err)

	sqlDB, err := gdb.DB()
	require.NoError(t, err)

	require.NoError(t, gdb.Exec("create table orders (id integer primary key, shop_type text, amount integer)").Error)
	require.NoError(t, gdb.Exec("insert into orders (id, shop_type, amount) values (1, 'Shopee', 10), (2, 'Shopee', 20), (3, 'Lazada', 30)").Error)

	source := connector.WrapSQLDB(sqlDB, "mysql")

	result, err := PreviewSQL(context.Background(), source, "select id, shop_type, amount from orders where shop_type = ? order by id", []any{"Shopee"}, 2, 1)

	require.NoError(t, err)
	assert.Equal(t, []string{"id", "shop_type", "amount"}, result.Columns)
	assert.Equal(t, int64(2), result.Total)
	assert.Equal(t, 2, result.Page)
	assert.Equal(t, 1, result.PageSize)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(2), result.Rows[0]["id"])
	assert.Equal(t, "Shopee", result.Rows[0]["shop_type"])
	assert.Equal(t, int64(20), result.Rows[0]["amount"])
}

func TestClampPreviewPage_MinIsOne(t *testing.T) {
	assert.Equal(t, 1, clampPreviewPage(0))
	assert.Equal(t, 1, clampPreviewPage(-5))
	assert.Equal(t, 3, clampPreviewPage(3))
}

func TestClampPreviewPageSize_ClampsToRange(t *testing.T) {
	assert.Equal(t, 50, clampPreviewPageSize(0))
	assert.Equal(t, 50, clampPreviewPageSize(-1))
	assert.Equal(t, 200, clampPreviewPageSize(500))
	assert.Equal(t, 100, clampPreviewPageSize(100))
}

func TestPreviewSQL_ReturnsEmptyResultWhenNoRowsMatch(t *testing.T) {
	gdb, err := gorm.Open(sqlite.Open("file::memory:?cache=shared&id=preview_empty_task10"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, gdb.Exec("create table if not exists empty_orders (id integer primary key)").Error)

	source := connector.WrapSQLDB(sqlDB, "mysql")

	result, err := PreviewSQL(context.Background(), source, "select id from empty_orders", nil, 1, 50)

	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Total)
	assert.Empty(t, result.Rows)
	assert.Equal(t, 1, result.Page)
}
