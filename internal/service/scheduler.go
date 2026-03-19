package service

import (
	"datasync/internal/model"
	"log"
	"sync"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

type Scheduler struct {
	cron     *cron.Cron
	executor *Executor
	db       *gorm.DB
	entries  map[uint]cron.EntryID
	mu       sync.Mutex
}

func NewScheduler(db *gorm.DB, executor *Executor) *Scheduler {
	return &Scheduler{
		cron:     cron.New(),
		executor: executor,
		db:       db,
		entries:  make(map[uint]cron.EntryID),
	}
}

func (s *Scheduler) Start() error {
	var tasks []model.SyncTask
	s.db.Where("sync_mode = ?", "cron").Find(&tasks)
	for _, t := range tasks {
		if err := s.AddTask(t); err != nil {
			log.Printf("failed to schedule task %d: %v", t.ID, err)
		}
	}
	s.cron.Start()
	return nil
}

func (s *Scheduler) AddTask(task model.SyncTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Remove existing entry if any
	if entryID, ok := s.entries[task.ID]; ok {
		s.cron.Remove(entryID)
		delete(s.entries, task.ID)
	}
	if task.CronExpr == "" {
		return nil
	}
	taskID := task.ID
	entryID, err := s.cron.AddFunc(task.CronExpr, func() {
		if err := s.executor.Run(taskID); err != nil {
			log.Printf("scheduled task %d failed: %v", taskID, err)
		}
	})
	if err != nil {
		return err
	}
	s.entries[task.ID] = entryID
	return nil
}

func (s *Scheduler) RemoveTask(taskID uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entryID, ok := s.entries[taskID]; ok {
		s.cron.Remove(entryID)
		delete(s.entries, taskID)
	}
}

func (s *Scheduler) Stop() {
	s.cron.Stop()
}
