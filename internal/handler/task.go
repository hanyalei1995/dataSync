package handler

import (
	"datasync/internal/engine"
	"datasync/internal/model"
	"datasync/internal/service"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gin-gonic/gin"
)

type TaskHandler struct {
	TaskService       *service.TaskService
	DataSourceService *service.DataSourceService
	Executor          *service.Executor
	Scheduler         *service.Scheduler
}

func (h *TaskHandler) List(c *gin.Context) {
	tasks, err := h.TaskService.List()
	if err != nil {
		c.HTML(http.StatusInternalServerError, "task_list", gin.H{"error": err.Error()})
		return
	}

	// Load datasource names for display
	dsList, _ := h.DataSourceService.List()
	dsMap := make(map[uint]string)
	for _, ds := range dsList {
		dsMap[ds.ID] = ds.Name
	}

	username, _ := c.Get("username")
	c.HTML(http.StatusOK, "task_list", gin.H{
		"tasks":    tasks,
		"dsMap":    dsMap,
		"username": username,
	})
}

func (h *TaskHandler) CreateForm(c *gin.Context) {
	dsList, _ := h.DataSourceService.List()
	username, _ := c.Get("username")
	c.HTML(http.StatusOK, "task_form", gin.H{
		"datasources": dsList,
		"username":    username,
	})
}

func (h *TaskHandler) Create(c *gin.Context) {
	task := &model.SyncTask{
		Name:        c.PostForm("name"),
		SourceTable: c.PostForm("source_table"),
		TargetTable: c.PostForm("target_table"),
		SyncType:    c.PostForm("sync_type"),
		SyncMode:    c.PostForm("sync_mode"),
		CronExpr:        c.PostForm("cron_expr"),
		FilterCondition: c.PostForm("filter_condition"),
		WatermarkColumn: c.PostForm("watermark_column"),
		WatermarkType:   c.PostForm("watermark_type"),
		Concurrency: func() int {
			n, _ := strconv.Atoi(c.PostForm("concurrency"))
			if n < 1 {
				n = 1
			}
			if n > 8 {
				n = 8
			}
			return n
		}(),
		EnableQualityCheck: c.PostForm("enable_quality_check") == "true",
		Status:             "idle",
	}

	if uid, exists := c.Get("userID"); exists {
		task.CreatedBy = uid.(uint)
	}

	// Source
	sourceMode := c.PostForm("source_mode")
	if sourceMode == "existing" {
		id, _ := strconv.ParseUint(c.PostForm("source_ds_id"), 10, 64)
		uid := uint(id)
		task.SourceDSID = &uid
	} else {
		cfg := map[string]interface{}{
			"db_type":       c.PostForm("source_db_type"),
			"host":          c.PostForm("source_host"),
			"port":          c.PostForm("source_port"),
			"username":      c.PostForm("source_username"),
			"password":      c.PostForm("source_password"),
			"database_name": c.PostForm("source_database_name"),
		}
		cfgJSON, _ := json.Marshal(cfg)
		task.SourceConfig = string(cfgJSON)
	}

	// Target
	targetMode := c.PostForm("target_mode")
	if targetMode == "existing" {
		id, _ := strconv.ParseUint(c.PostForm("target_ds_id"), 10, 64)
		uid := uint(id)
		task.TargetDSID = &uid
	} else {
		cfg := map[string]interface{}{
			"db_type":       c.PostForm("target_db_type"),
			"host":          c.PostForm("target_host"),
			"port":          c.PostForm("target_port"),
			"username":      c.PostForm("target_username"),
			"password":      c.PostForm("target_password"),
			"database_name": c.PostForm("target_database_name"),
		}
		cfgJSON, _ := json.Marshal(cfg)
		task.TargetConfig = string(cfgJSON)
	}

	if err := h.TaskService.Create(task); err != nil {
		dsList, _ := h.DataSourceService.List()
		username, _ := c.Get("username")
		c.HTML(http.StatusOK, "task_form", gin.H{
			"error":       err.Error(),
			"task":        task,
			"datasources": dsList,
			"username":    username,
		})
		return
	}

	// Auto-map fields if both source and target are existing datasources
	if task.SourceDSID != nil && task.TargetDSID != nil && task.SourceTable != "" && task.TargetTable != "" {
		sourceDS, err1 := h.DataSourceService.GetByID(*task.SourceDSID)
		targetDS, err2 := h.DataSourceService.GetByID(*task.TargetDSID)
		if err1 == nil && err2 == nil {
			_ = h.TaskService.AutoMapFields(task.ID, *sourceDS, *targetDS, task.SourceTable, task.TargetTable)
		}
	}

	// Schedule cron task if applicable
	if task.SyncMode == "cron" && h.Scheduler != nil {
		_ = h.Scheduler.AddTask(*task)
	}

	c.Redirect(http.StatusFound, "/tasks")
}

func (h *TaskHandler) EditForm(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	task, err := h.TaskService.GetByID(uint(id))
	if err != nil {
		c.Redirect(http.StatusFound, "/tasks")
		return
	}
	dsList, _ := h.DataSourceService.List()
	username, _ := c.Get("username")
	c.HTML(http.StatusOK, "task_form", gin.H{
		"task":        task,
		"datasources": dsList,
		"username":    username,
	})
}

func (h *TaskHandler) Update(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	task, err := h.TaskService.GetByID(uint(id))
	if err != nil {
		c.Redirect(http.StatusFound, "/tasks")
		return
	}

	task.Name = c.PostForm("name")
	task.SourceTable = c.PostForm("source_table")
	task.TargetTable = c.PostForm("target_table")
	task.SyncType = c.PostForm("sync_type")
	task.SyncMode = c.PostForm("sync_mode")
	task.CronExpr = c.PostForm("cron_expr")
	task.FilterCondition = c.PostForm("filter_condition")
	task.WatermarkColumn = c.PostForm("watermark_column")
	task.WatermarkType = c.PostForm("watermark_type")
	if n, err := strconv.Atoi(c.PostForm("concurrency")); err == nil {
		if n < 1 {
			n = 1
		}
		if n > 8 {
			n = 8
		}
		task.Concurrency = n
	}
	task.EnableQualityCheck = c.PostForm("enable_quality_check") == "true"
	if c.PostForm("reset_watermark") == "1" {
		task.LastWatermarkValue = ""
	}

	// Source
	sourceMode := c.PostForm("source_mode")
	if sourceMode == "existing" {
		sid, _ := strconv.ParseUint(c.PostForm("source_ds_id"), 10, 64)
		uid := uint(sid)
		task.SourceDSID = &uid
		task.SourceConfig = ""
	} else {
		task.SourceDSID = nil
		cfg := map[string]interface{}{
			"db_type":       c.PostForm("source_db_type"),
			"host":          c.PostForm("source_host"),
			"port":          c.PostForm("source_port"),
			"username":      c.PostForm("source_username"),
			"password":      c.PostForm("source_password"),
			"database_name": c.PostForm("source_database_name"),
		}
		cfgJSON, _ := json.Marshal(cfg)
		task.SourceConfig = string(cfgJSON)
	}

	// Target
	targetMode := c.PostForm("target_mode")
	if targetMode == "existing" {
		tid, _ := strconv.ParseUint(c.PostForm("target_ds_id"), 10, 64)
		uid := uint(tid)
		task.TargetDSID = &uid
		task.TargetConfig = ""
	} else {
		task.TargetDSID = nil
		cfg := map[string]interface{}{
			"db_type":       c.PostForm("target_db_type"),
			"host":          c.PostForm("target_host"),
			"port":          c.PostForm("target_port"),
			"username":      c.PostForm("target_username"),
			"password":      c.PostForm("target_password"),
			"database_name": c.PostForm("target_database_name"),
		}
		cfgJSON, _ := json.Marshal(cfg)
		task.TargetConfig = string(cfgJSON)
	}

	if err := h.TaskService.Update(task); err != nil {
		dsList, _ := h.DataSourceService.List()
		username, _ := c.Get("username")
		c.HTML(http.StatusOK, "task_form", gin.H{
			"error":       err.Error(),
			"task":        task,
			"datasources": dsList,
			"username":    username,
		})
		return
	}

	// Update scheduler
	if h.Scheduler != nil {
		if task.SyncMode == "cron" {
			_ = h.Scheduler.AddTask(*task)
		} else {
			h.Scheduler.RemoveTask(task.ID)
		}
	}

	c.Redirect(http.StatusFound, "/tasks")
}

func (h *TaskHandler) Delete(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	if h.Scheduler != nil {
		h.Scheduler.RemoveTask(uint(id))
	}
	h.TaskService.Delete(uint(id))
	c.Redirect(http.StatusFound, "/tasks")
}

func (h *TaskHandler) Detail(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	task, err := h.TaskService.GetByID(uint(id))
	if err != nil {
		c.Redirect(http.StatusFound, "/tasks")
		return
	}

	mappings, _ := h.TaskService.GetMappings(task.ID)
	logs, _ := h.TaskService.GetLogs(task.ID, 10)

	var latestLog *model.SyncLog
	if len(logs) > 0 {
		latestLog = &logs[0]
	}

	// Resolve datasource names
	var sourceName, targetName string
	if task.SourceDSID != nil {
		if ds, err := h.DataSourceService.GetByID(*task.SourceDSID); err == nil {
			sourceName = ds.Name
		}
	} else {
		sourceName = "临时连接"
	}
	if task.TargetDSID != nil {
		if ds, err := h.DataSourceService.GetByID(*task.TargetDSID); err == nil {
			targetName = ds.Name
		}
	} else {
		targetName = "临时连接"
	}

	username, _ := c.Get("username")
	c.HTML(http.StatusOK, "task_detail", gin.H{
		"task":       task,
		"mappings":   mappings,
		"logs":       logs,
		"latestLog":  latestLog,
		"sourceName": sourceName,
		"targetName": targetName,
		"username":   username,
		"error":      c.Query("error"),
	})
}

func (h *TaskHandler) Mappings(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	mappings, err := h.TaskService.GetMappings(uint(id))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"mappings": mappings})
}

func (h *TaskHandler) SaveMappings(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	taskID := uint(id)

	// Auto-map mode
	if c.Query("auto") == "1" {
		task, err := h.TaskService.GetByID(taskID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
			return
		}
		if task.SourceDSID != nil && task.TargetDSID != nil {
			sourceDS, err1 := h.DataSourceService.GetByID(*task.SourceDSID)
			targetDS, err2 := h.DataSourceService.GetByID(*task.TargetDSID)
			if err1 != nil || err2 != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load datasources"})
				return
			}
			if err := h.TaskService.AutoMapFields(taskID, *sourceDS, *targetDS, task.SourceTable, task.TargetTable); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"success": true, "message": "自动映射完成"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "auto-map requires existing datasources on both sides"})
		return
	}

	var body struct {
		Mappings []model.FieldMapping `json:"mappings"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.TaskService.SaveMappings(taskID, body.Mappings); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "message": "映射已保存"})
}

func (h *TaskHandler) Run(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	isJSON := c.GetHeader("Accept") == "application/json" || c.Query("format") == "json"
	if err := h.Executor.Run(uint(id)); err != nil {
		if isJSON {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.Redirect(http.StatusFound, "/tasks/"+c.Param("id")+"?error="+url.QueryEscape(err.Error()))
		return
	}
	if isJSON {
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "任务已启动"})
		return
	}
	c.Redirect(http.StatusFound, "/tasks/"+c.Param("id"))
}

func (h *TaskHandler) Stop(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	isJSON := c.GetHeader("Accept") == "application/json" || c.Query("format") == "json"
	if err := h.Executor.Stop(uint(id)); err != nil {
		if isJSON {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.Redirect(http.StatusFound, "/tasks/"+c.Param("id")+"?error="+url.QueryEscape(err.Error()))
		return
	}
	if isJSON {
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "任务已停止"})
		return
	}
	c.Redirect(http.StatusFound, "/tasks/"+c.Param("id"))
}

// ProgressSnapshot returns the latest progress event for a task as JSON (for polling).
func (h *TaskHandler) ProgressSnapshot(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	ev, ok := h.Executor.GetProgress(uint(id))
	if !ok {
		c.JSON(http.StatusOK, gin.H{"running": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{"running": true, "progress": ev})
}

func (h *TaskHandler) ProgressStream(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	taskID := uint(id)

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		// SSE requires a flushable writer; Gin's default writer implements it
		fmt.Fprintf(c.Writer, "data: {\"phase\":\"failed\",\"message\":\"不支持流式响应\"}\n\n")
		return
	}

	ch, ok := h.Executor.Subscribe(taskID)
	if !ok {
		// Task not running — send done immediately
		ev := service.ProgressEvent{Phase: "done", Message: "任务未运行"}
		data, _ := json.Marshal(ev)
		fmt.Fprintf(c.Writer, "data: %s\n\n", data)
		c.Writer.Flush()
		return
	}

	ctx := c.Request.Context()

	for {
		select {
		case ev, open := <-ch:
			if !open {
				return
			}
			data, err := json.Marshal(ev)
			if err != nil {
				fmt.Fprintf(c.Writer, "data: {\"phase\":\"failed\",\"message\":\"序列化错误\"}\n\n")
				if flusher != nil {
					flusher.Flush()
				}
				return
			}
			fmt.Fprintf(c.Writer, "data: %s\n\n", data)
			if flusher != nil {
				flusher.Flush()
			}
			if ev.Phase == "done" || ev.Phase == "failed" {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// Verify runs a data quality check on the task's tables and updates the latest SyncLog.
func (h *TaskHandler) Verify(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	taskID := uint(id)

	task, err := h.TaskService.GetByID(taskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
		return
	}

	sourceDS, err := resolveDS(h, task.SourceDSID, task.SourceConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "解析源数据源失败: " + err.Error()})
		return
	}
	targetDS, err := resolveDS(h, task.TargetDSID, task.TargetConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "解析目标数据源失败: " + err.Error()})
		return
	}

	sourceDB, err := h.Executor.Pool.Get(sourceDS)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "连接源库失败: " + err.Error()})
		return
	}
	targetDB, err := h.Executor.Pool.Get(targetDS)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "连接目标库失败: " + err.Error()})
		return
	}

	mappings, _ := h.TaskService.GetMappings(taskID)
	qOpts := engine.QualityCheckOptions{
		SourceDB:     sourceDB,
		TargetDB:     targetDB,
		SourceDBType: sourceDS.DBType,
		TargetDBType: targetDS.DBType,
		SourceTable:  task.SourceTable,
		TargetTable:  task.TargetTable,
		Mappings:     mappings,
		SampleSize:   50,
	}

	qRes, err := engine.CheckQuality(c.Request.Context(), qOpts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Update the latest SyncLog for this task
	logs, _ := h.TaskService.GetLogs(taskID, 1)
	if len(logs) > 0 {
		h.TaskService.DB.Model(&logs[0]).Updates(map[string]interface{}{
			"source_rows":    qRes.SourceRows,
			"target_rows":    qRes.TargetRows,
			"sample_total":   qRes.SampleTotal,
			"sample_matched": qRes.SampleMatched,
			"quality_status": qRes.Status,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"source_rows":    qRes.SourceRows,
		"target_rows":    qRes.TargetRows,
		"sample_total":   qRes.SampleTotal,
		"sample_matched": qRes.SampleMatched,
		"status":         qRes.Status,
	})
}

// resolveDS resolves a DataSource from a saved ID or inline JSON config.
func resolveDS(h *TaskHandler, dsID *uint, configJSON string) (model.DataSource, error) {
	if dsID != nil {
		ds, err := h.DataSourceService.GetByID(*dsID)
		if err != nil {
			return model.DataSource{}, err
		}
		return *ds, nil
	}
	if configJSON != "" {
		var ds model.DataSource
		if err := json.Unmarshal([]byte(configJSON), &ds); err != nil {
			return model.DataSource{}, fmt.Errorf("解析数据源配置失败: %w", err)
		}
		return ds, nil
	}
	return model.DataSource{}, fmt.Errorf("未指定数据源")
}
