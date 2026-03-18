package model

import "time"

type User struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	Username     string    `gorm:"uniqueIndex;size:100" json:"username"`
	PasswordHash string    `gorm:"size:255" json:"-"`
	CreatedAt    time.Time `json:"created_at"`
}

type DataSource struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	Name         string    `gorm:"size:100" json:"name"`
	DBType       string    `gorm:"size:20" json:"db_type"`
	Host         string    `gorm:"size:255" json:"host"`
	Port         int       `json:"port"`
	Username     string    `gorm:"size:100" json:"username"`
	Password     string    `gorm:"size:255" json:"password"`
	DatabaseName string    `gorm:"size:100" json:"database_name"`
	ExtraParams  string    `gorm:"type:text" json:"extra_params"`
	CreatedBy    uint      `json:"created_by"`
	CreatedAt    time.Time `json:"created_at"`
}

type SyncTask struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	Name         string    `gorm:"size:200" json:"name"`
	SourceDSID   *uint     `json:"source_ds_id"`
	TargetDSID   *uint     `json:"target_ds_id"`
	SourceConfig string    `gorm:"type:text" json:"source_config"`
	TargetConfig string    `gorm:"type:text" json:"target_config"`
	SourceTable  string    `gorm:"size:200" json:"source_table"`
	TargetTable  string    `gorm:"size:200" json:"target_table"`
	SyncType     string    `gorm:"size:20" json:"sync_type"`
	SyncMode     string    `gorm:"size:20" json:"sync_mode"`
	CronExpr     string    `gorm:"size:100" json:"cron_expr"`
	Status       string    `gorm:"size:20;default:idle" json:"status"`
	CreatedBy    uint      `json:"created_by"`
	CreatedAt    time.Time `json:"created_at"`
}

type FieldMapping struct {
	ID          uint   `gorm:"primaryKey" json:"id"`
	TaskID      uint   `gorm:"index" json:"task_id"`
	SourceField string `gorm:"size:200" json:"source_field"`
	TargetField string `gorm:"size:200" json:"target_field"`
	Enabled     bool   `gorm:"default:true" json:"enabled"`
}

type SyncLog struct {
	ID         uint       `gorm:"primaryKey" json:"id"`
	TaskID     uint       `gorm:"index" json:"task_id"`
	StartTime  time.Time  `json:"start_time"`
	EndTime    *time.Time `json:"end_time"`
	Status     string     `gorm:"size:20" json:"status"`
	RowsSynced int64      `json:"rows_synced"`
	ErrorMsg   string     `gorm:"type:text" json:"error_msg"`
}
