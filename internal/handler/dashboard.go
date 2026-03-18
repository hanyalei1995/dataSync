package handler

import (
	"datasync/internal/model"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type DashboardHandler struct {
	DB *gorm.DB
}

func (h *DashboardHandler) Index(c *gin.Context) {
	username, _ := c.Get("username")

	// Count datasources
	var dsCount int64
	h.DB.Model(&model.DataSource{}).Count(&dsCount)

	// Count tasks
	var taskCount int64
	h.DB.Model(&model.SyncTask{}).Count(&taskCount)

	// Count running tasks
	var runningCount int64
	h.DB.Model(&model.SyncTask{}).Where("status = ?", "running").Count(&runningCount)

	// Count today's executions
	var todayCount int64
	h.DB.Model(&model.SyncLog{}).Where("DATE(start_time) = DATE('now')").Count(&todayCount)

	// Recent 10 logs with task names
	type LogWithTask struct {
		model.SyncLog
		TaskName string
	}
	var recentLogs []LogWithTask
	h.DB.Table("sync_logs").
		Select("sync_logs.*, sync_tasks.name as task_name").
		Joins("LEFT JOIN sync_tasks ON sync_logs.task_id = sync_tasks.id").
		Order("sync_logs.id DESC").
		Limit(10).
		Find(&recentLogs)

	c.HTML(http.StatusOK, "dashboard", gin.H{
		"username":     username,
		"dsCount":      dsCount,
		"taskCount":    taskCount,
		"runningCount": runningCount,
		"todayCount":   todayCount,
		"recentLogs":   recentLogs,
	})
}
