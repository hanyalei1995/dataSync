package service

import (
	"context"
	"database/sql"
	"datasync/internal/cdc"
	"datasync/internal/connector"
	"datasync/internal/engine"
	"datasync/internal/model"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
)

// ProgressEvent holds a single progress update for a running task.
type ProgressEvent struct {
	Phase      string  `json:"phase"`    // connecting | structure | data | done | failed
	Message    string  `json:"message"`
	RowsSynced int64   `json:"rows_synced"`
	TotalRows  int64   `json:"total_rows"`
	Percent    float64 `json:"percent"`
	ErrorMsg   string  `json:"error_msg,omitempty"`
}

// Executor manages running sync tasks.
type Executor struct {
	DB           *gorm.DB
	DSSvc        *DataSourceService
	TaskSvc      *TaskService
	CDCManager   *cdc.Manager
	Pool         *ConnPool
	running      sync.Map // map[uint]context.CancelFunc
	progress     sync.Map // map[uint]chan ProgressEvent
	lastProgress sync.Map // map[uint]ProgressEvent — latest snapshot for polling
}

// GetProgress returns the most recent progress snapshot for a task.
func (e *Executor) GetProgress(taskID uint) (ProgressEvent, bool) {
	val, ok := e.lastProgress.Load(taskID)
	if !ok {
		return ProgressEvent{}, false
	}
	return val.(ProgressEvent), true
}

// IsRunning returns true if the given task is currently executing.
func (e *Executor) IsRunning(taskID uint) bool {
	_, ok := e.running.Load(taskID)
	return ok
}

// connRawDB attempts to extract the underlying *sql.DB from a Connector.
// Returns nil if the connector does not expose a raw DB.
func connRawDB(c connector.Connector) *sql.DB {
	type rawDBer interface {
		RawDB() *sql.DB
	}
	if r, ok := c.(rawDBer); ok {
		return r.RawDB()
	}
	return nil
}

// Run starts a sync task asynchronously.
func (e *Executor) Run(taskID uint) error {
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
	sourceDB, err := e.Pool.Get(sourceDS)
	if err != nil {
		return fmt.Errorf("连接源数据库失败: %w", err)
	}
	targetDB, err := e.Pool.Get(targetDS)
	if err != nil {
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

		rawTargetDB := connRawDB(targetDB)
		switch sourceDS.DBType {
		case "mysql":
			listener = &cdc.MySQLListener{
				SourceHost:     sourceDS.Host,
				SourcePort:     uint16(sourceDS.Port),
				SourceUser:     sourceDS.Username,
				SourcePassword: sourceDS.Password,
				SourceDB:       sourceDS.DatabaseName,
				SourceTable:    task.SourceTable,
				TargetDB:       rawTargetDB,
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
				TargetDB:       rawTargetDB,
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
			return fmt.Errorf("启动CDC监听失败: %w", err)
		}

		progressCh := make(chan ProgressEvent, 64)
		e.progress.Store(taskID, progressCh)
		// Store a no-op cancel func so IsRunning() works; actual cancellation goes
		// through CDCManager.StopListener, called by Stop().
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
		return fmt.Errorf("创建日志记录失败: %w", err)
	}

	// Update task status to running
	e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).Update("status", "running")

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	progressCh := make(chan ProgressEvent, 64)
	e.progress.Store(taskID, progressCh)
	if _, loaded := e.running.LoadOrStore(taskID, cancel); loaded {
		e.progress.Delete(taskID)
		cancel()
		return fmt.Errorf("任务 %d 正在运行中", taskID)
	}

	// Run sync in goroutine
	go func() {
		defer e.running.Delete(taskID)
		defer e.lastProgress.Delete(taskID)
		defer func() {
			if _, loaded := e.progress.LoadAndDelete(taskID); loaded {
				close(progressCh)
			}
		}()

		e.emit(taskID, ProgressEvent{Phase: "connecting", Message: "正在建立数据库连接..."})

		makeOnProgress := func() func(int64, int64) {
			return func(synced, total int64) {
				pct := float64(0)
				if total > 0 {
					pct = float64(synced) / float64(total) * 100
				}
				e.emit(taskID, ProgressEvent{
					Phase:      "data",
					Message:    "正在同步数据...",
					RowsSynced: synced,
					TotalRows:  total,
					Percent:    pct,
				})
			}
		}

		var syncErr error
		var rowsSynced int64

		mappings, _ := e.TaskSvc.GetMappings(taskID)

		switch task.SyncType {
		case "structure":
			e.emit(taskID, ProgressEvent{Phase: "structure", Message: "正在同步表结构..."})
			syncErr = engine.SyncStructure(sourceDB, targetDB, task.SourceTable, task.TargetTable, mappings)
		case "data":
			e.emit(taskID, ProgressEvent{Phase: "data", Message: "正在同步数据..."})
			opts := engine.DataSyncOptions{
				Source:      sourceDB,
				Target:      targetDB,
				SourceTable: task.SourceTable,
				TargetTable: task.TargetTable,
				Mappings:    mappings,
				BatchSize:   1000,
				OnProgress:  makeOnProgress(),
				WhereClause: buildWhereClause(task, sourceDS.DBType),
				Concurrency: task.Concurrency,
				StartOffset: task.CheckpointOffset,
				OnCheckpoint: func(offset int64) {
					e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).
						Update("checkpoint_offset", offset)
				},
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
			e.emit(taskID, ProgressEvent{Phase: "structure", Message: "正在同步表结构..."})
			syncErr = engine.SyncStructure(sourceDB, targetDB, task.SourceTable, task.TargetTable, mappings)
			if syncErr == nil {
				e.emit(taskID, ProgressEvent{Phase: "data", Message: "正在同步数据..."})
				opts := engine.DataSyncOptions{
					Source:      sourceDB,
					Target:      targetDB,
					SourceTable: task.SourceTable,
					TargetTable: task.TargetTable,
					Mappings:    mappings,
					BatchSize:   1000,
					OnProgress:  makeOnProgress(),
					WhereClause: buildWhereClause(task, sourceDS.DBType),
					Concurrency: task.Concurrency,
					StartOffset: task.CheckpointOffset,
					OnCheckpoint: func(offset int64) {
						e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).
							Update("checkpoint_offset", offset)
					},
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
		case "sql_import":
			if task.SourceSQL == "" {
				syncErr = fmt.Errorf("sql_import 任务的源 SQL 不能为空")
				break
			}
			e.emit(taskID, ProgressEvent{Phase: "data", Message: "正在执行 SQL 导入..."})
			opts := engine.DataSyncOptions{
				Source:      sourceDB,
				Target:      targetDB,
				SourceSQL:   task.SourceSQL,
				TargetTable: task.TargetTable,
				BatchSize:   1000,
				OnProgress:  makeOnProgress(),
				Concurrency: 1,
				StartOffset: task.CheckpointOffset,
				OnCheckpoint: func(offset int64) {
					e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).
						Update("checkpoint_offset", offset)
				},
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

		// 同步成功后清零断点，下次从头开始
		if syncErr == nil {
			e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).
				Update("checkpoint_offset", 0)
		}

		// Update watermark after successful sync
		if syncErr == nil && task.WatermarkColumn != "" && task.WatermarkType != "" {
			var newWatermark string
			switch task.WatermarkType {
			case "timestamp":
				newWatermark = now.UTC().Format(time.RFC3339)
			case "id":
				col := quoteWatermarkCol(task.WatermarkColumn, sourceDS.DBType)
				maxSQL := fmt.Sprintf("SELECT COALESCE(MAX(%s), '') FROM %s", col, task.SourceTable)
				if fc := task.FilterCondition; fc != "" {
					maxSQL += " WHERE " + fc
				}
				if rawSrc := connRawDB(sourceDB); rawSrc != nil {
					var maxVal string
					if err := rawSrc.QueryRowContext(ctx, maxSQL).Scan(&maxVal); err == nil && maxVal != "" {
						newWatermark = maxVal
					}
				}
			}
			if newWatermark != "" {
				e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).
					Update("last_watermark_value", newWatermark)
			}
		}

		// Run quality check after successful data sync
		if syncErr == nil && task.EnableQualityCheck && task.SyncType != "structure" {
			qOpts := engine.QualityCheckOptions{
				Source:      sourceDB,
				Target:      targetDB,
				SourceTable: task.SourceTable,
				TargetTable: task.TargetTable,
				WhereClause: buildWhereClause(task, sourceDS.DBType),
				Mappings:    mappings,
				SampleSize:  50,
			}
			if qRes, qErr := engine.CheckQuality(ctx, qOpts); qErr == nil {
				e.DB.Model(&model.SyncLog{}).Where("id = ?", syncLog.ID).Updates(map[string]interface{}{
					"source_rows":    qRes.SourceRows,
					"target_rows":    qRes.TargetRows,
					"sample_total":   qRes.SampleTotal,
					"sample_matched": qRes.SampleMatched,
					"quality_status": qRes.Status,
				})
			}
		}

		if syncErr != nil {
			e.emit(taskID, ProgressEvent{Phase: "failed", Message: "同步失败", ErrorMsg: syncErr.Error()})
		} else {
			e.emit(taskID, ProgressEvent{
				Phase:      "done",
				Message:    "同步完成",
				Percent:    100,
				RowsSynced: rowsSynced,
				TotalRows:  rowsSynced,
			})
		}
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
	e.lastProgress.Delete(taskID)

	// Close progress channel. LoadAndDelete is atomic with the goroutine's defer
	// (which also uses LoadAndDelete), so exactly one caller closes the channel.
	// For non-CDC tasks, if the goroutine already emitted a terminal event before
	// Stop() wins the race, there may be two terminal events in the buffer — the
	// SSE handler exits on the first, so the second is harmless.
	if val, ok := e.progress.LoadAndDelete(taskID); ok {
		ch := val.(chan ProgressEvent)
		select {
		case ch <- ProgressEvent{Phase: "done", Message: "任务已手动停止"}:
		default:
		}
		close(ch)
	}

	e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).Update("status", "idle")
	return nil
}

// ForceReset clears all in-memory executor state for a task and resets its DB status to idle.
// Use this to recover tasks stuck in "running" state with no active goroutine.
func (e *Executor) ForceReset(taskID uint) {
	if val, ok := e.running.LoadAndDelete(taskID); ok {
		cancel := val.(context.CancelFunc)
		cancel()
	}
	if e.CDCManager != nil {
		_ = e.CDCManager.StopListener(taskID)
	}
	if val, ok := e.progress.LoadAndDelete(taskID); ok {
		ch := val.(chan ProgressEvent)
		close(ch)
	}
	e.lastProgress.Delete(taskID)
	e.DB.Model(&model.SyncTask{}).Where("id = ?", taskID).Update("status", "idle")
}

// Subscribe returns the progress channel for a running task.
func (e *Executor) Subscribe(taskID uint) (<-chan ProgressEvent, bool) {
	val, ok := e.progress.Load(taskID)
	if !ok {
		return nil, false
	}
	return val.(chan ProgressEvent), true
}

// emit sends a progress event, dropping it if the channel is full.
// It also caches the event as the latest snapshot for polling consumers.
func (e *Executor) emit(taskID uint, ev ProgressEvent) {
	e.lastProgress.Store(taskID, ev)
	val, ok := e.progress.Load(taskID)
	if !ok {
		return
	}
	ch := val.(chan ProgressEvent)
	select {
	case ch <- ev:
	default:
	}
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

// buildWhereClause assembles the SQL WHERE clause (without the WHERE keyword)
// from the task's FilterCondition and watermark settings.
func buildWhereClause(task *model.SyncTask, sourceDBType string) string {
	var parts []string

	if task.FilterCondition != "" {
		parts = append(parts, task.FilterCondition)
	}

	if task.WatermarkColumn != "" && task.WatermarkType != "" && task.LastWatermarkValue != "" {
		col := quoteWatermarkCol(task.WatermarkColumn, sourceDBType)
		switch task.WatermarkType {
		case "id":
			parts = append(parts, fmt.Sprintf("%s > %s", col, task.LastWatermarkValue))
		case "timestamp":
			parts = append(parts, fmt.Sprintf("%s > '%s'", col, task.LastWatermarkValue))
		}
	}

	return strings.Join(parts, " AND ")
}

// quoteWatermarkCol quotes a column name for the given DB type.
func quoteWatermarkCol(col, dbType string) string {
	switch strings.ToLower(dbType) {
	case "mysql":
		return "`" + col + "`"
	case "postgresql":
		return `"` + col + `"`
	default:
		return col
	}
}
