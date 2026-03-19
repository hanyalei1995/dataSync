package database

import (
	"datasync/internal/config"
	"datasync/internal/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func Init(cfg *config.Config) (*gorm.DB, error) {
	dsn := cfg.DBPath + "?_journal_mode=WAL&_busy_timeout=5000"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// SQLite only supports one writer at a time; limit the pool to 1 connection
	// to prevent "attempt to write a readonly database" under concurrent requests.
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(1)

	err = db.AutoMigrate(
		&model.User{},
		&model.DataSource{},
		&model.SyncTask{},
		&model.FieldMapping{},
		&model.SyncLog{},
	)
	return db, err
}
