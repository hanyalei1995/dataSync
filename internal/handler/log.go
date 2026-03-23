package handler

import (
	"datasync/internal/model"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type LogHandler struct {
	DB *gorm.DB
}

type logEntry struct {
	model.SyncLog
	TaskName string
}

func (h *LogHandler) List(c *gin.Context) {
	status := c.Query("status")

	var logs []logEntry
	query := h.DB.Table("sync_logs").
		Select("sync_logs.*, sync_tasks.name as task_name").
		Joins("LEFT JOIN sync_tasks ON sync_tasks.id = sync_logs.task_id").
		Order("sync_logs.id DESC")

	if status != "" {
		query = query.Where("sync_logs.status = ?", status)
	}

	query.Find(&logs)

	username, _ := c.Get("username")
	c.HTML(http.StatusOK, "log_list", gin.H{
		"logs":          logs,
		"currentStatus": status,
		"username":      username,
	})
}

func (h *LogHandler) Download(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	var log model.SyncLog
	if err := h.DB.First(&log, id).Error; err != nil {
		c.String(http.StatusNotFound, "日志不存在")
		return
	}
	if log.FilePath == "" {
		c.String(http.StatusNotFound, "该日志没有关联的文件")
		return
	}
	if _, err := os.Stat(log.FilePath); err != nil {
		c.String(http.StatusNotFound, "文件不存在: "+log.FilePath)
		return
	}
	c.FileAttachment(log.FilePath, filepath.Base(log.FilePath))
}

func (h *LogHandler) TaskLogs(c *gin.Context) {
	taskID := c.Param("id")

	var logs []model.SyncLog
	h.DB.Where("task_id = ?", taskID).Order("id DESC").Find(&logs)

	c.JSON(http.StatusOK, gin.H{"logs": logs})
}
