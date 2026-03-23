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
	ID                 uint      `gorm:"primaryKey" json:"id"`
	Name               string    `gorm:"size:200" json:"name"`
	SourceDSID         *uint     `json:"source_ds_id"`
	TargetDSID         *uint     `json:"target_ds_id"`
	SourceConfig       string    `gorm:"type:text" json:"source_config"`
	TargetConfig       string    `gorm:"type:text" json:"target_config"`
	SourceTable        string    `gorm:"size:200" json:"source_table"`
	TargetTable        string    `gorm:"size:200" json:"target_table"`
	SyncType           string    `gorm:"size:20" json:"sync_type"`
	SyncMode           string    `gorm:"size:20" json:"sync_mode"`
	CronExpr           string    `gorm:"size:100" json:"cron_expr"`
	Status             string    `gorm:"size:20;default:idle" json:"status"`
	FilterCondition    string    `gorm:"type:text" json:"filter_condition"`
	SourceSQL          string    `gorm:"type:text" json:"source_sql"`
	SQLParams          string    `gorm:"type:text" json:"sql_params"`
	WatermarkColumn    string    `gorm:"size:200" json:"watermark_column"`
	WatermarkType      string    `gorm:"size:20" json:"watermark_type"`
	LastWatermarkValue string    `gorm:"size:500" json:"last_watermark_value"`
	Concurrency        int       `gorm:"default:1" json:"concurrency"`
	EnableQualityCheck bool      `gorm:"default:true" json:"enable_quality_check"`
	CheckpointOffset   int64     `gorm:"default:0" json:"checkpoint_offset"`
	CreatedBy          uint      `json:"created_by"`
	CreatedAt          time.Time `json:"created_at"`
}

type SQLParamOption struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type SQLParamDefinition struct {
	Name         string           `json:"name"`
	Label        string           `json:"label"`
	InputType    string           `json:"input_type"`
	Required     bool             `json:"required"`
	DefaultValue string           `json:"default_value"`
	HelpText     string           `json:"help_text"`
	Options      []SQLParamOption `json:"options"`
	Source       string           `json:"source"`
	Order        int              `json:"order"`
}

type FieldMapping struct {
	ID          uint   `gorm:"primaryKey" json:"id"`
	TaskID      uint   `gorm:"index" json:"task_id"`
	SourceField string `gorm:"size:200" json:"source_field"`
	TargetField string `gorm:"size:200" json:"target_field"`
	Enabled     bool   `gorm:"default:true" json:"enabled"`
}

type SyncLog struct {
	ID            uint       `gorm:"primaryKey" json:"id"`
	TaskID        uint       `gorm:"index" json:"task_id"`
	StartTime     time.Time  `json:"start_time"`
	EndTime       *time.Time `json:"end_time"`
	Status        string     `gorm:"size:20" json:"status"`
	RowsSynced    int64      `json:"rows_synced"`
	ErrorMsg      string     `gorm:"type:text" json:"error_msg"`
	SourceRows    int64      `gorm:"default:0" json:"source_rows"`
	TargetRows    int64      `gorm:"default:0" json:"target_rows"`
	SampleTotal   int        `gorm:"default:0" json:"sample_total"`
	SampleMatched int        `gorm:"default:0" json:"sample_matched"`
	QualityStatus string     `gorm:"size:20" json:"quality_status"` // passed|warning|failed|""
	FilePath      string     `gorm:"type:text" json:"file_path"`    // non-empty for file-export tasks
	TriggeredBy   string     `gorm:"size:100" json:"triggered_by"`  // username or "scheduler"
}
