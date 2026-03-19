package service

import (
	"crypto/md5"
	"database/sql"
	"datasync/internal/engine"
	"datasync/internal/model"
	"fmt"
	"sync"
	"time"
)

// ConnPool caches *sql.DB instances to avoid reconnecting on every task run.
type ConnPool struct {
	mu    sync.Mutex
	conns map[string]*sql.DB
}

func NewConnPool() *ConnPool {
	return &ConnPool{conns: make(map[string]*sql.DB)}
}

// Get returns a cached connection for the datasource, creating one if needed.
// The caller must NOT close the returned *sql.DB.
func (p *ConnPool) Get(ds model.DataSource) (*sql.DB, error) {
	key := dsKey(ds)
	p.mu.Lock()
	defer p.mu.Unlock()

	if db, ok := p.conns[key]; ok {
		if err := db.Ping(); err == nil {
			return db, nil
		}
		// Stale connection — close and reconnect
		db.Close()
		delete(p.conns, key)
	}

	db, err := engine.Connect(ds)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(3)
	db.SetConnMaxLifetime(30 * time.Minute)
	p.conns[key] = db
	return db, nil
}

// CloseAll closes all cached connections. Call on app shutdown.
func (p *ConnPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, db := range p.conns {
		db.Close()
		delete(p.conns, k)
	}
}

func dsKey(ds model.DataSource) string {
	if ds.ID != 0 {
		return fmt.Sprintf("id:%d", ds.ID)
	}
	h := md5.Sum([]byte(fmt.Sprintf("%s:%s:%d:%s:%s:%s",
		ds.DBType, ds.Host, ds.Port, ds.Username, ds.Password, ds.DatabaseName)))
	return fmt.Sprintf("hash:%x", h)
}
