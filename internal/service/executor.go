package service

import (
	"context"
	"datasync/internal/cdc"
	"datasync/internal/engine"
	"datasync/internal/model"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gorm.io/gorm"
)

// Executor manages running sync tasks.
type Executor struct {
	DB         *gorm.DB
	DSSvc      *DataSourceService
	TaskSvc    *TaskService
	CDCManager *cdc.Manager
	running    sync.Map // map[uint]context.CancelFunc
}

// IsRunning returns true if the given task is currently executing.
func (e *Executor) IsRunning(taskID uint) bool {
	_, ok := e.running.Load(taskID)
	return ok
}

// Run starts a sync task asynchronously.
func (e *Executor) Run(taskID uint) error {
	if e.IsRunning(taskID) {
		return fmt.Errorf("任务 %d 正在运行中", taskID)
	}

	task, err := e.TaskSvc.GetByID(taskID)
	if err != nil {
		return fmt.Errorf("加载任务失败: %w", err)
	}

	// Resolve source and target datasources
	sourceDS, err := e.resolveDataSource(task.SourceDSID, task.SourceConfig)
	if err != nil {
		return fmt.Errorf("解析源数据源失败: %w", err)
	}
	targetDS, err := e.resolveDataSource(task.TargetDSID, task.TargetConfig)
	if err != nil {
		return fmt.Errorf("解析目标数据源失败: %w", err)
	}

	// Connect to source and target
	sourceDB, err := engine.Connect(sourceDS)
	if err != nil {
		return fmt.Errorf("连接源数据库失败: %w", err)
	}
	targetDB, err := engine.Connect(targetDS)
	if err != nil {
		sourceDB.Close()
		return fmt.Errorf("连接目标数据库失败: %w", err)
	}

	// Handle realtime CDC mode
	if task.SyncMode == "realtime" && (sourceDS.DBType == "mysql" || sourceDS.DBType == "postgresql") {
		mappings, _ := e.TaskSvc.GetMappings(taskID)
		colMap := make(map[string]string)
		for _, m := range mappings {
			if m.Enabled {
				colMap[m.SourceField] = m.TargetField
			}
		}

		var listener cdc.CDCListener

		switch sourceDS.DBType {
		case "mysql":
			listener = &cdc.MySQLListener{
				SourceHost:     sourceDS.Host,
				SourcePort:     uint16(sourceDS.Port),
				SourceUser:     sourceDS.Username,
				SourcePassword: sourceDS.Password,
				SourceDB:       sourceDS.DatabaseName,
				SourceTable:    task.SourceTable,
				TargetDB:       targetDB,
				TargetDBType:   targetDS.DBType,
				TargetTable:    task.TargetTable,
				ColumnMappings: colMap,
			}
		case "postgresql":
			listener = &cdc.PGListener{
				SourceHost:     sourceDS.Host,
				SourcePort:     uint16(sourceDS.Port),
				SourceUser:     sourceDS.Username,
				SourcePassword: sourceDS.Password,
				SourceDB:       sourceDS.DatabaseName,
				SourceTable:    task.SourceTable,
				TargetDB:       targetDB,
				TargetDBType:   targetDS.DBType,
				TargetTable:    task.TargetTable,
				ColumnMappings: colMap,
				SlotName:       fmt.Sprintf("datasync_task_%d", taskID),
			}
		}

		if e.CDCManager == nil {
			e.CDCManager = cdc.NewManager()
		}

		if err := e.CDCManager.StartListener(taskID, listener); err != nil {
			sourceDB.Close()
			targetDB.Close()
			return fmt.Errorf("启动CDC监听失败: %w", err)
		}

		e.running.Store(taskID, context.CancelFunc(func() {}))
		e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).Update("status", "running")
		return nil
	}

	// Create sync log
	now := time.Now()
	syncLog := model.SyncLog{
		TaskID:    taskID,
		StartTime: now,
		Status:    "running",
	}
	if err := e.DB.Create(&syncLog).Error; err != nil {
		sourceDB.Close()
		targetDB.Close()
		return fmt.Errorf("创建日志记录失败: %w", err)
	}

	// Update task status to running
	e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).Update("status", "running")

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	e.running.Store(taskID, cancel)

	// Run sync in goroutine
	go func() {
		defer sourceDB.Close()
		defer targetDB.Close()
		defer e.running.Delete(taskID)

		var syncErr error
		var rowsSynced int64

		mappings, _ := e.TaskSvc.GetMappings(taskID)

		switch task.SyncType {
		case "structure":
			syncErr = engine.SyncStructure(sourceDB, targetDB, sourceDS.DBType, targetDS.DBType, task.SourceTable, task.TargetTable, mappings)
		case "data":
			opts := engine.DataSyncOptions{
				SourceDB:     sourceDB,
				TargetDB:     targetDB,
				SourceDBType: sourceDS.DBType,
				TargetDBType: targetDS.DBType,
				SourceTable:  task.SourceTable,
				TargetTable:  task.TargetTable,
				Mappings:     mappings,
				BatchSize:    1000,
			}
			if task.SyncMode == "upsert" {
				opts.WriteStrategy = "upsert"
			} else {
				opts.WriteStrategy = "insert"
			}
			var result *engine.SyncResult
			result, syncErr = engine.SyncData(ctx, opts)
			if result != nil {
				rowsSynced = result.RowsSynced
			}
		case "both":
			syncErr = engine.SyncStructure(sourceDB, targetDB, sourceDS.DBType, targetDS.DBType, task.SourceTable, task.TargetTable, mappings)
			if syncErr == nil {
				opts := engine.DataSyncOptions{
					SourceDB:     sourceDB,
					TargetDB:     targetDB,
					SourceDBType: sourceDS.DBType,
					TargetDBType: targetDS.DBType,
					SourceTable:  task.SourceTable,
					TargetTable:  task.TargetTable,
					Mappings:     mappings,
					BatchSize:    1000,
				}
				if task.SyncMode == "upsert" {
					opts.WriteStrategy = "upsert"
				} else {
					opts.WriteStrategy = "insert"
				}
				var result *engine.SyncResult
				result, syncErr = engine.SyncData(ctx, opts)
				if result != nil {
					rowsSynced = result.RowsSynced
				}
			}
		default:
			syncErr = fmt.Errorf("未知同步类型: %s", task.SyncType)
		}

		// Update sync log
		endTime := time.Now()
		logUpdate := map[string]interface{}{
			"end_time":    endTime,
			"rows_synced": rowsSynced,
		}
		taskStatus := "idle"
		if syncErr != nil {
			logUpdate["status"] = "failed"
			logUpdate["error_msg"] = syncErr.Error()
			taskStatus = "error"
		} else {
			logUpdate["status"] = "success"
		}
		e.DB.Model(&model.SyncLog{}).Where("id = ?", syncLog.ID).Updates(logUpdate)
		e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).Update("status", taskStatus)
	}()

	return nil
}

// Stop cancels a running sync task.
func (e *Executor) Stop(taskID uint) error {
	val, ok := e.running.Load(taskID)
	if !ok {
		return fmt.Errorf("任务 %d 未在运行", taskID)
	}
	cancel := val.(context.CancelFunc)
	cancel()

	// Also stop CDC listener if running
	if e.CDCManager != nil {
		_ = e.CDCManager.StopListener(taskID)
	}

	e.running.Delete(taskID)
	e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).Update("status", "idle")
	return nil
}

// resolveDataSource loads a DataSource from DB by ID or parses a JSON config string.
func (e *Executor) resolveDataSource(dsID *uint, configJSON string) (model.DataSource, error) {
	if dsID != nil {
		ds, err := e.DSSvc.GetByID(*dsID)
		if err != nil {
			return model.DataSource{}, err
		}
		return *ds, nil
	}
	if configJSON != "" {
		var ds model.DataSource
		if err := json.Unmarshal([]byte(configJSON), &ds); err != nil {
			return model.DataSource{}, fmt.Errorf("解析数据源配置JSON失败: %w", err)
		}
		return ds, nil
	}
	return model.DataSource{}, fmt.Errorf("未指定数据源")
}
