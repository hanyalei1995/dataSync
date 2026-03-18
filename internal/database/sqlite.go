package database

import (
	"datasync/internal/config"
	"datasync/internal/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func Init(cfg *config.Config) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(cfg.DBPath), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	err = db.AutoMigrate(
		&model.User{},
		&model.DataSource{},
		&model.SyncTask{},
		&model.FieldMapping{},
		&model.SyncLog{},
	)
	return db, err
}
