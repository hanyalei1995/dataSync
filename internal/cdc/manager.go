package cdc

import (
	"context"
	"sync"
)

// CDCListener defines the interface for a CDC (Change Data Capture) listener.
type CDCListener interface {
	Start(ctx context.Context) error
	Stop() error
}

// Manager manages multiple CDC listeners, one per task.
type Manager struct {
	listeners map[uint]CDCListener
	cancels   map[uint]context.CancelFunc
	mu        sync.Mutex
}

// NewManager creates a new CDC Manager.
func NewManager() *Manager {
	return &Manager{
		listeners: make(map[uint]CDCListener),
		cancels:   make(map[uint]context.CancelFunc),
	}
}

// StartListener starts a CDC listener for the given task. If a listener is
// already running for this task, it is stopped first.
func (m *Manager) StartListener(taskID uint, listener CDCListener) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop existing listener if any
	if cancel, ok := m.cancels[taskID]; ok {
		cancel()
		if old, exists := m.listeners[taskID]; exists {
			_ = old.Stop()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.listeners[taskID] = listener
	m.cancels[taskID] = cancel

	go func() {
		if err := listener.Start(ctx); err != nil {
			// Log error — listener exited
		}
	}()

	return nil
}

// StopListener stops the CDC listener for the given task.
func (m *Manager) StopListener(taskID uint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cancel, ok := m.cancels[taskID]; ok {
		cancel()
		if listener, exists := m.listeners[taskID]; exists {
			_ = listener.Stop()
		}
		delete(m.cancels, taskID)
		delete(m.listeners, taskID)
	}
	return nil
}

// IsRunning returns true if a CDC listener is active for the given task.
func (m *Manager) IsRunning(taskID uint) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.listeners[taskID]
	return ok
}

// StopAll stops all running CDC listeners.
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, cancel := range m.cancels {
		cancel()
		if listener, exists := m.listeners[id]; exists {
			_ = listener.Stop()
		}
		delete(m.cancels, id)
		delete(m.listeners, id)
	}
}
