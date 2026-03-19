package handler

import (
	"datasync/internal/model"
	"datasync/internal/service"
	"encoding/json"
	"net/http"
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
		CronExpr:    c.PostForm("cron_expr"),
		Status:      "idle",
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
		"sourceName": sourceName,
		"targetName": targetName,
		"username":   username,
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
	if err := h.Executor.Run(uint(id)); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// Check if request expects JSON
	if c.GetHeader("Accept") == "application/json" || c.Query("format") == "json" {
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "任务已启动"})
		return
	}
	c.Redirect(http.StatusFound, "/tasks/"+c.Param("id"))
}

func (h *TaskHandler) Stop(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	if err := h.Executor.Stop(uint(id)); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if c.GetHeader("Accept") == "application/json" || c.Query("format") == "json" {
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "任务已停止"})
		return
	}
	c.Redirect(http.StatusFound, "/tasks/"+c.Param("id"))
}
