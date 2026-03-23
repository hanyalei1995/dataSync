package service

import (
	"context"
	"crypto/md5"
	"datasync/internal/connector"
	"datasync/internal/model"
	"fmt"
	"sync"
)

// ConnPool 缓存 Connector 实例，避免每次任务重新建连。
type ConnPool struct {
	mu    sync.Mutex
	conns map[string]connector.Connector
}

func NewConnPool() *ConnPool {
	return &ConnPool{conns: make(map[string]connector.Connector)}
}

// Put stores a connector for the given datasource key. Mainly used when a
// caller already has an open connector they want the pool to reuse.
func (p *ConnPool) Put(ds model.DataSource, conn connector.Connector) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns[dsKey(ds)] = conn
}

// Get 返回缓存的 Connector，不存在或 Ping 失败则重新创建。
func (p *ConnPool) Get(ds model.DataSource) (connector.Connector, error) {
	key := dsKey(ds)
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[key]; ok {
		if err := conn.Ping(context.Background()); err == nil {
			return conn, nil
		}
		conn.Close()
		delete(p.conns, key)
	}

	conn, err := connector.FromDataSource(ds)
	if err != nil {
		return nil, err
	}
	p.conns[key] = conn
	return conn, nil
}

// CloseAll 关闭所有缓存连接，应用关闭时调用。
func (p *ConnPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, conn := range p.conns {
		conn.Close()
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
