package service

import (
	"context"
	"datasync/internal/connector"
	"datasync/internal/model"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openSQLResultsTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	return db
}

func TestSQLResultsService_PreviewRejectsIneligibleTasks(t *testing.T) {
	db := openSQLResultsTestDB(t)
	var err error
	require.NoError(t, db.AutoMigrate(&model.SyncTask{}, &model.DataSource{}))

	task := model.SyncTask{Name: "普通同步", SyncType: "data", SyncMode: "manual"}
	require.NoError(t, db.Create(&task).Error)

	svc := &SQLResultsService{
		TaskSvc: &TaskService{DB: db},
		DSSvc:   &DataSourceService{DB: db},
		Pool:    NewConnPool(),
	}

	_, err = svc.Preview(context.Background(), task.ID, nil, 1, 50)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "结果页")
}

func TestSQLResultsService_PreviewReturnsLiveQueryResult(t *testing.T) {
	db := openSQLResultsTestDB(t)
	var err error
	require.NoError(t, db.AutoMigrate(&model.SyncTask{}, &model.DataSource{}))
	require.NoError(t, db.Exec("create table orders (id integer primary key, shop_type text, amount integer)").Error)
	require.NoError(t, db.Exec("insert into orders (id, shop_type, amount) values (1, 'Shopee', 10), (2, 'Shopee', 20), (3, 'Lazada', 30)").Error)

	sourceDS := model.DataSource{Name: "源库", DBType: "mysql", Host: "src", DatabaseName: "preview"}
	targetDS := model.DataSource{Name: "导出", DBType: "excel", Host: t.TempDir()}
	require.NoError(t, db.Create(&sourceDS).Error)
	require.NoError(t, db.Create(&targetDS).Error)

	task := model.SyncTask{
		Name:       "订单预览",
		SyncType:   "sql_import",
		SyncMode:   "manual",
		SourceDSID: &sourceDS.ID,
		TargetDSID: &targetDS.ID,
		SourceSQL:  "select id, shop_type, amount from orders where shop_type = {{shop_type}} order by id",
		SQLParams:  `[{"name":"shop_type","label":"店铺","input_type":"text","required":true,"default_value":"Shopee","order":1}]`,
	}
	require.NoError(t, db.Create(&task).Error)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	pool := NewConnPool()
	pool.conns[dsKey(sourceDS)] = connector.WrapSQLDB(sqlDB, "mysql")

	svc := &SQLResultsService{
		TaskSvc: &TaskService{DB: db},
		DSSvc:   &DataSourceService{DB: db},
		Pool:    pool,
	}

	result, err := svc.Preview(context.Background(), task.ID, map[string]string{"shop_type": "Shopee"}, 1, 1)

	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Total)
	assert.Equal(t, []string{"id", "shop_type", "amount"}, result.Columns)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, int64(1), result.Rows[0]["id"])
	assert.Equal(t, "Shopee", result.Rows[0]["shop_type"])
}

func TestSQLResultsService_ExportWorksWithNoTargetDatasource(t *testing.T) {
	db := openSQLResultsTestDB(t)
	require.NoError(t, db.AutoMigrate(&model.SyncTask{}, &model.DataSource{}))
	require.NoError(t, db.Exec("create table orders2 (id integer primary key, name text)").Error)
	require.NoError(t, db.Exec("insert into orders2 values (1, 'A'), (2, 'B')").Error)

	sourceDS := model.DataSource{Name: "源库", DBType: "mysql", Host: "src", DatabaseName: "db"}
	require.NoError(t, db.Create(&sourceDS).Error)

	// No TargetDSID, no TargetConfig — sql_import with auto-generated output
	task := model.SyncTask{
		Name:       "无目标导出",
		SyncType:   "sql_import",
		SyncMode:   "manual",
		SourceDSID: &sourceDS.ID,
		SourceSQL:  "select id, name from orders2 order by id",
	}
	require.NoError(t, db.Create(&task).Error)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	pool := NewConnPool()
	pool.conns[dsKey(sourceDS)] = connector.WrapSQLDB(sqlDB, "mysql")

	svc := &SQLResultsService{
		TaskSvc: &TaskService{DB: db},
		DSSvc:   &DataSourceService{DB: db},
		Pool:    pool,
	}

	filePath, err := svc.Export(context.Background(), task.ID, nil, "csv")

	require.NoError(t, err)
	assert.Equal(t, ".csv", filepath.Ext(filePath))
}

func TestSQLResultsService_ExportWritesPreviewFile(t *testing.T) {
	db := openSQLResultsTestDB(t)
	var err error
	require.NoError(t, db.AutoMigrate(&model.SyncTask{}, &model.DataSource{}))
	require.NoError(t, db.Exec("create table orders (id integer primary key, shop_type text)").Error)
	require.NoError(t, db.Exec("insert into orders (id, shop_type) values (1, 'Shopee'), (2, 'Shopee')").Error)

	sourceDS := model.DataSource{Name: "源库", DBType: "mysql", Host: "src", DatabaseName: "preview"}
	targetDS := model.DataSource{Name: "导出目录", DBType: "csv", Host: t.TempDir()}
	require.NoError(t, db.Create(&sourceDS).Error)
	require.NoError(t, db.Create(&targetDS).Error)

	task := model.SyncTask{
		Name:       "订单预览",
		SyncType:   "sql_import",
		SyncMode:   "manual",
		SourceDSID: &sourceDS.ID,
		TargetDSID: &targetDS.ID,
		SourceSQL:  "select id, shop_type from orders where shop_type = {{shop_type}} order by id",
		SQLParams:  `[{"name":"shop_type","label":"店铺","input_type":"text","required":true,"default_value":"Shopee","order":1}]`,
	}
	require.NoError(t, db.Create(&task).Error)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	pool := NewConnPool()
	pool.conns[dsKey(sourceDS)] = connector.WrapSQLDB(sqlDB, "mysql")

	svc := &SQLResultsService{
		TaskSvc: &TaskService{DB: db},
		DSSvc:   &DataSourceService{DB: db},
		Pool:    pool,
	}

	filePath, err := svc.Export(context.Background(), task.ID, map[string]string{"shop_type": "Shopee"}, "excel")

	require.NoError(t, err)
	assert.Equal(t, ".xlsx", filepath.Ext(filePath))
	assert.Contains(t, filepath.Base(filePath), "preview")
}
