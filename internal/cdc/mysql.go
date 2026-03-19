package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

// MySQLListener listens to MySQL binlog events for a specific table and
// replicates changes to a target database.
type MySQLListener struct {
	SourceHost     string
	SourcePort     uint16
	SourceUser     string
	SourcePassword string
	SourceDB       string
	SourceTable    string
	TargetDB       *sql.DB
	TargetDBType   string
	TargetTable    string
	ColumnMappings map[string]string // source_col -> target_col
	canal          *canal.Canal
}

// Start begins listening for binlog events. It blocks until ctx is cancelled.
func (l *MySQLListener) Start(ctx context.Context) error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", l.SourceHost, l.SourcePort)
	cfg.User = l.SourceUser
	cfg.Password = l.SourcePassword
	cfg.Dump.ExecutionPath = "" // disable mysqldump
	cfg.IncludeTableRegex = []string{fmt.Sprintf("%s\\.%s", l.SourceDB, l.SourceTable)}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("创建canal失败: %w", err)
	}
	l.canal = c

	handler := &mysqlEventHandler{
		listener: l,
	}
	c.SetEventHandler(handler)

	// Stop canal when context is cancelled
	go func() {
		<-ctx.Done()
		c.Close()
	}()

	// Start from the current position
	pos, err := c.GetMasterPos()
	if err != nil {
		return fmt.Errorf("获取binlog位置失败: %w", err)
	}

	return c.RunFrom(pos)
}

// Stop closes the canal connection.
func (l *MySQLListener) Stop() error {
	if l.canal != nil {
		l.canal.Close()
	}
	return nil
}

// mapColumn returns the target column name for a given source column.
func (l *MySQLListener) mapColumn(sourceCol string) string {
	if len(l.ColumnMappings) > 0 {
		if mapped, ok := l.ColumnMappings[sourceCol]; ok {
			return mapped
		}
	}
	return sourceCol
}

// mysqlEventHandler implements canal.EventHandler.
type mysqlEventHandler struct {
	canal.DummyEventHandler
	listener *MySQLListener
}

func (h *mysqlEventHandler) OnRow(e *canal.RowsEvent) error {
	table := e.Table
	columns := table.Columns

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			if err := h.handleInsert(columns, row); err != nil {
				log.Printf("CDC INSERT error: %v", err)
			}
		}
	case canal.UpdateAction:
		// Update events come in pairs: [before, after, before, after, ...]
		for i := 0; i+1 < len(e.Rows); i += 2 {
			after := e.Rows[i+1]
			if err := h.handleUpdate(table, columns, after); err != nil {
				log.Printf("CDC UPDATE error: %v", err)
			}
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			if err := h.handleDelete(table, columns, row); err != nil {
				log.Printf("CDC DELETE error: %v", err)
			}
		}
	}
	return nil
}

func (h *mysqlEventHandler) handleInsert(columns []schema.TableColumn, row []interface{}) error {
	l := h.listener
	targetCols := make([]string, 0, len(columns))
	placeholders := make([]string, 0, len(columns))
	values := make([]interface{}, 0, len(columns))

	for i, col := range columns {
		targetCol := l.mapColumn(col.Name)
		targetCols = append(targetCols, targetCol)
		placeholders = append(placeholders, "?")
		values = append(values, row[i])
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		l.TargetTable,
		strings.Join(targetCols, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := l.TargetDB.Exec(query, values...)
	return err
}

func (h *mysqlEventHandler) handleUpdate(table *schema.Table, columns []schema.TableColumn, row []interface{}) error {
	l := h.listener

	// Find primary key column(s)
	pkIndex := table.PKColumns
	if len(pkIndex) == 0 {
		return fmt.Errorf("表 %s 没有主键，无法处理UPDATE", l.SourceTable)
	}

	setClauses := make([]string, 0, len(columns))
	setValues := make([]interface{}, 0, len(columns))

	for i, col := range columns {
		targetCol := l.mapColumn(col.Name)
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", targetCol))
		setValues = append(setValues, row[i])
	}

	whereClauses := make([]string, 0, len(pkIndex))
	whereValues := make([]interface{}, 0, len(pkIndex))
	for _, idx := range pkIndex {
		targetCol := l.mapColumn(columns[idx].Name)
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", targetCol))
		whereValues = append(whereValues, row[idx])
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		l.TargetTable,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	args := append(setValues, whereValues...)
	_, err := l.TargetDB.Exec(query, args...)
	return err
}

func (h *mysqlEventHandler) handleDelete(table *schema.Table, columns []schema.TableColumn, row []interface{}) error {
	l := h.listener

	pkIndex := table.PKColumns
	if len(pkIndex) == 0 {
		return fmt.Errorf("表 %s 没有主键，无法处理DELETE", l.SourceTable)
	}

	whereClauses := make([]string, 0, len(pkIndex))
	whereValues := make([]interface{}, 0, len(pkIndex))
	for _, idx := range pkIndex {
		targetCol := l.mapColumn(columns[idx].Name)
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", targetCol))
		whereValues = append(whereValues, row[idx])
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		l.TargetTable,
		strings.Join(whereClauses, " AND "),
	)

	_, err := l.TargetDB.Exec(query, whereValues...)
	return err
}

func (h *mysqlEventHandler) String() string {
	return "mysqlEventHandler"
}
