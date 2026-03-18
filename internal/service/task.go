package service

import (
	"datasync/internal/model"
	"encoding/json"
	"strings"

	"gorm.io/gorm"
)

type TaskService struct {
	DB *gorm.DB
}

func (s *TaskService) List() ([]model.SyncTask, error) {
	var list []model.SyncTask
	err := s.DB.Order("id desc").Find(&list).Error
	return list, err
}

func (s *TaskService) GetByID(id uint) (*model.SyncTask, error) {
	var task model.SyncTask
	err := s.DB.First(&task, id).Error
	return &task, err
}

func (s *TaskService) Create(task *model.SyncTask) error {
	return s.DB.Create(task).Error
}

func (s *TaskService) Update(task *model.SyncTask) error {
	return s.DB.Save(task).Error
}

func (s *TaskService) Delete(id uint) error {
	return s.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("task_id = ?", id).Delete(&model.FieldMapping{}).Error; err != nil {
			return err
		}
		return tx.Delete(&model.SyncTask{}, id).Error
	})
}

func (s *TaskService) GetMappings(taskID uint) ([]model.FieldMapping, error) {
	var mappings []model.FieldMapping
	err := s.DB.Where("task_id = ?", taskID).Find(&mappings).Error
	return mappings, err
}

func (s *TaskService) SaveMappings(taskID uint, mappings []model.FieldMapping) error {
	return s.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("task_id = ?", taskID).Delete(&model.FieldMapping{}).Error; err != nil {
			return err
		}
		for i := range mappings {
			mappings[i].ID = 0
			mappings[i].TaskID = taskID
		}
		if len(mappings) > 0 {
			return tx.Create(&mappings).Error
		}
		return nil
	})
}

func (s *TaskService) GetLogs(taskID uint, limit int) ([]model.SyncLog, error) {
	var logs []model.SyncLog
	err := s.DB.Where("task_id = ?", taskID).Order("id desc").Limit(limit).Find(&logs).Error
	return logs, err
}

func (s *TaskService) AutoMapFields(taskID uint, sourceDS model.DataSource, targetDS model.DataSource, sourceTable, targetTable string) error {
	dsSvc := &DataSourceService{DB: s.DB}

	sourceCols, err := dsSvc.GetColumns(sourceDS, sourceTable)
	if err != nil {
		return err
	}
	targetCols, err := dsSvc.GetColumns(targetDS, targetTable)
	if err != nil {
		return err
	}

	targetMap := make(map[string]bool)
	for _, col := range targetCols {
		targetMap[strings.ToLower(col.Name)] = true
	}

	var mappings []model.FieldMapping
	for _, col := range sourceCols {
		if targetMap[strings.ToLower(col.Name)] {
			mappings = append(mappings, model.FieldMapping{
				TaskID:      taskID,
				SourceField: col.Name,
				TargetField: col.Name,
				Enabled:     true,
			})
		}
	}

	return s.SaveMappings(taskID, mappings)
}

// ParseDSConfig parses a JSON config string into a DataSource for temporary connections.
func ParseDSConfig(configStr string) (model.DataSource, error) {
	var ds model.DataSource
	err := json.Unmarshal([]byte(configStr), &ds)
	return ds, err
}
