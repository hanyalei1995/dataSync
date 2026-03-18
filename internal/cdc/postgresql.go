package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// PGListener listens to PostgreSQL WAL (logical replication) events for a
// specific table and replicates changes to a target database.
type PGListener struct {
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
	SlotName       string            // replication slot name, e.g. "datasync_task_123"
	stopped        chan struct{}
	cancel         context.CancelFunc
}

// relationInfo caches column metadata from Relation messages.
type relationInfo struct {
	RelationID uint32
	Columns    []relationColumn
}

type relationColumn struct {
	Name     string
	DataType uint32
	IsKey    bool
}

// Start begins listening for WAL events via logical replication. It blocks
// until ctx is cancelled or an unrecoverable error occurs.
func (l *PGListener) Start(ctx context.Context) error {
	l.stopped = make(chan struct{})
	defer close(l.stopped)

	ctx, l.cancel = context.WithCancel(ctx)

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database",
		l.SourceUser, l.SourcePassword, l.SourceHost, l.SourcePort, l.SourceDB)

	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("连接PostgreSQL复制失败: %w", err)
	}
	defer conn.Close(context.Background())

	pubName := fmt.Sprintf("datasync_pub_%s", l.SlotName)

	// Create publication if it doesn't exist
	createPubSQL := fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s",
		pgIdentifier(pubName), pgIdentifier(l.SourceTable),
	)
	result := conn.Exec(ctx, createPubSQL)
	_, err = result.ReadAll()
	if err != nil {
		// Publication may already exist; check if it's a "duplicate" error
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("创建publication失败: %w", err)
		}
	}

	// Create replication slot if it doesn't exist
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, l.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("创建复制槽失败: %w", err)
		}
	}

	// Identify system to get start LSN
	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("IdentifySystem失败: %w", err)
	}
	startLSN := sysident.XLogPos

	// Start replication
	err = pglogrepl.StartReplication(ctx, conn, l.SlotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", pubName),
			},
		})
	if err != nil {
		return fmt.Errorf("启动复制流失败: %w", err)
	}

	log.Printf("PGListener: 开始监听 %s (slot=%s, LSN=%s)", l.SourceTable, l.SlotName, startLSN)

	// Cache relation info keyed by relation ID
	relations := make(map[uint32]*relationInfo)
	clientXLogPos := startLSN
	standbyTicker := time.NewTicker(10 * time.Second)
	defer standbyTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			l.cleanup(conn, pubName)
			return nil
		case <-standbyTicker.C:
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Printf("PGListener: 发送standby状态失败: %v", err)
			}
		default:
		}

		// Use a short deadline so we can check ctx.Done() periodically
		receiveCtx, receiveCancel := context.WithDeadline(ctx, time.Now().Add(1*time.Second))
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		receiveCancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				l.cleanup(conn, pubName)
				return nil
			}
			return fmt.Errorf("接收WAL消息失败: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("收到PostgreSQL错误: %s", errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Printf("PGListener: 解析XLogData失败: %v", err)
				continue
			}
			l.handleWALData(xld.WALData, relations)
			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Printf("PGListener: 解析Keepalive失败: %v", err)
				continue
			}
			if pkm.ReplyRequested {
				_ = pglogrepl.SendStandbyStatusUpdate(ctx, conn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			}
		}
	}
}

// Stop signals the listener to stop and waits for cleanup.
func (l *PGListener) Stop() error {
	if l.cancel != nil {
		l.cancel()
	}
	if l.stopped != nil {
		<-l.stopped
	}
	return nil
}

// cleanup drops the replication slot and publication.
func (l *PGListener) cleanup(conn *pgconn.PgConn, pubName string) {
	cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanCancel()

	err := pglogrepl.DropReplicationSlot(cleanCtx, conn, l.SlotName,
		pglogrepl.DropReplicationSlotOptions{Wait: false})
	if err != nil {
		log.Printf("PGListener: 删除复制槽 %s 失败: %v", l.SlotName, err)
	}

	dropPubSQL := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgIdentifier(pubName))
	result := conn.Exec(cleanCtx, dropPubSQL)
	_, err = result.ReadAll()
	if err != nil {
		log.Printf("PGListener: 删除publication %s 失败: %v", pubName, err)
	}
}

// handleWALData parses pgoutput protocol messages and applies changes to the
// target database.
func (l *PGListener) handleWALData(walData []byte, relations map[uint32]*relationInfo) {
	if len(walData) == 0 {
		return
	}

	msgType := walData[0]
	switch msgType {
	case 'R': // Relation
		l.handleRelation(walData, relations)
	case 'I': // Insert
		l.handleInsert(walData, relations)
	case 'U': // Update
		l.handleUpdate(walData, relations)
	case 'D': // Delete
		l.handleDelete(walData, relations)
	case 'B', 'C': // Begin, Commit — no action needed
	}
}

// handleRelation parses a Relation message and caches column metadata.
func (l *PGListener) handleRelation(data []byte, relations map[uint32]*relationInfo) {
	// pgoutput Relation message format (after msg type byte):
	// RelationID (uint32) | Namespace (string\0) | RelationName (string\0) |
	// ReplicaIdentity (byte) | NumColumns (uint16) | columns...
	if len(data) < 6 {
		return
	}

	pos := 1 // skip msg type
	relID := uint32(data[pos])<<24 | uint32(data[pos+1])<<16 | uint32(data[pos+2])<<8 | uint32(data[pos+3])
	pos += 4

	// Skip namespace (null-terminated string)
	for pos < len(data) && data[pos] != 0 {
		pos++
	}
	pos++ // skip null terminator

	// Skip relation name (null-terminated string)
	for pos < len(data) && data[pos] != 0 {
		pos++
	}
	pos++ // skip null terminator

	// Skip replica identity (1 byte)
	pos++

	if pos+2 > len(data) {
		return
	}
	numCols := int(uint16(data[pos])<<8 | uint16(data[pos+1]))
	pos += 2

	cols := make([]relationColumn, 0, numCols)
	for i := 0; i < numCols && pos < len(data); i++ {
		// Flags (1 byte): 1 = part of key
		flags := data[pos]
		pos++

		// Column name (null-terminated)
		nameStart := pos
		for pos < len(data) && data[pos] != 0 {
			pos++
		}
		colName := string(data[nameStart:pos])
		pos++ // skip null

		// Data type OID (uint32)
		if pos+4 > len(data) {
			break
		}
		dataType := uint32(data[pos])<<24 | uint32(data[pos+1])<<16 | uint32(data[pos+2])<<8 | uint32(data[pos+3])
		pos += 4

		// Type modifier (int32)
		pos += 4

		cols = append(cols, relationColumn{
			Name:     colName,
			DataType: dataType,
			IsKey:    flags == 1,
		})
	}

	relations[relID] = &relationInfo{
		RelationID: relID,
		Columns:    cols,
	}
}

// parseTupleData parses a TupleData section from pgoutput messages.
// Returns column values as strings (or nil for NULL).
func parseTupleData(data []byte, pos int) ([]interface{}, int) {
	if pos+2 > len(data) {
		return nil, pos
	}
	numCols := int(uint16(data[pos])<<8 | uint16(data[pos+1]))
	pos += 2

	values := make([]interface{}, numCols)
	for i := 0; i < numCols && pos < len(data); i++ {
		colType := data[pos]
		pos++
		switch colType {
		case 'n': // NULL
			values[i] = nil
		case 'u': // Unchanged TOASTed value
			values[i] = nil
		case 't': // Text value
			if pos+4 > len(data) {
				return values, pos
			}
			valLen := int(uint32(data[pos])<<24 | uint32(data[pos+1])<<16 | uint32(data[pos+2])<<8 | uint32(data[pos+3]))
			pos += 4
			if pos+valLen > len(data) {
				return values, pos
			}
			values[i] = string(data[pos : pos+valLen])
			pos += valLen
		case 'b': // Binary value
			if pos+4 > len(data) {
				return values, pos
			}
			valLen := int(uint32(data[pos])<<24 | uint32(data[pos+1])<<16 | uint32(data[pos+2])<<8 | uint32(data[pos+3]))
			pos += 4
			if pos+valLen > len(data) {
				return values, pos
			}
			values[i] = data[pos : pos+valLen]
			pos += valLen
		}
	}
	return values, pos
}

// handleInsert processes an Insert pgoutput message.
func (l *PGListener) handleInsert(data []byte, relations map[uint32]*relationInfo) {
	// Insert: 'I' | RelationID (uint32) | 'N' | TupleData
	if len(data) < 7 {
		return
	}
	pos := 1
	relID := uint32(data[pos])<<24 | uint32(data[pos+1])<<16 | uint32(data[pos+2])<<8 | uint32(data[pos+3])
	pos += 4

	rel, ok := relations[relID]
	if !ok {
		log.Printf("PGListener: INSERT收到未知relation %d", relID)
		return
	}

	// Skip 'N' marker for new tuple
	pos++

	values, _ := parseTupleData(data, pos)
	if len(values) == 0 {
		return
	}

	targetCols := make([]string, 0, len(rel.Columns))
	placeholders := make([]string, 0, len(rel.Columns))
	args := make([]interface{}, 0, len(rel.Columns))

	for i, col := range rel.Columns {
		if i >= len(values) {
			break
		}
		targetCol := l.mapColumn(col.Name)
		targetCols = append(targetCols, targetCol)
		placeholders = append(placeholders, l.placeholder(len(placeholders)+1))
		args = append(args, convertValue(values[i], col.DataType))
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		l.TargetTable,
		strings.Join(targetCols, ", "),
		strings.Join(placeholders, ", "),
	)

	if _, err := l.TargetDB.Exec(query, args...); err != nil {
		log.Printf("PGListener: INSERT目标失败: %v", err)
	}
}

// handleUpdate processes an Update pgoutput message.
func (l *PGListener) handleUpdate(data []byte, relations map[uint32]*relationInfo) {
	// Update: 'U' | RelationID (uint32) | ['K' TupleData | 'O' TupleData] | 'N' TupleData
	if len(data) < 7 {
		return
	}
	pos := 1
	relID := uint32(data[pos])<<24 | uint32(data[pos+1])<<16 | uint32(data[pos+2])<<8 | uint32(data[pos+3])
	pos += 4

	rel, ok := relations[relID]
	if !ok {
		log.Printf("PGListener: UPDATE收到未知relation %d", relID)
		return
	}

	// There may be a 'K' (key) or 'O' (old) tuple before 'N' (new)
	var oldValues []interface{}
	if pos < len(data) && (data[pos] == 'K' || data[pos] == 'O') {
		pos++ // skip marker
		oldValues, pos = parseTupleData(data, pos)
	}

	if pos >= len(data) || data[pos] != 'N' {
		return
	}
	pos++ // skip 'N'
	newValues, _ := parseTupleData(data, pos)
	if len(newValues) == 0 {
		return
	}

	// Build SET clause
	setClauses := make([]string, 0, len(rel.Columns))
	setArgs := make([]interface{}, 0, len(rel.Columns))
	paramIdx := 1

	for i, col := range rel.Columns {
		if i >= len(newValues) {
			break
		}
		targetCol := l.mapColumn(col.Name)
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", targetCol, l.placeholder(paramIdx)))
		setArgs = append(setArgs, convertValue(newValues[i], col.DataType))
		paramIdx++
	}

	// Build WHERE clause using key columns
	whereClauses := make([]string, 0)
	whereArgs := make([]interface{}, 0)

	for i, col := range rel.Columns {
		if !col.IsKey {
			continue
		}
		targetCol := l.mapColumn(col.Name)
		// Use old values if available, otherwise use new values for key
		var keyVal interface{}
		if oldValues != nil && i < len(oldValues) {
			keyVal = convertValue(oldValues[i], col.DataType)
		} else if i < len(newValues) {
			keyVal = convertValue(newValues[i], col.DataType)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", targetCol, l.placeholder(paramIdx)))
		whereArgs = append(whereArgs, keyVal)
		paramIdx++
	}

	if len(whereClauses) == 0 {
		log.Printf("PGListener: UPDATE跳过: 表 %s 没有主键列", l.SourceTable)
		return
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		l.TargetTable,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	args := append(setArgs, whereArgs...)
	if _, err := l.TargetDB.Exec(query, args...); err != nil {
		log.Printf("PGListener: UPDATE目标失败: %v", err)
	}
}

// handleDelete processes a Delete pgoutput message.
func (l *PGListener) handleDelete(data []byte, relations map[uint32]*relationInfo) {
	// Delete: 'D' | RelationID (uint32) | 'K' TupleData | or 'O' TupleData
	if len(data) < 7 {
		return
	}
	pos := 1
	relID := uint32(data[pos])<<24 | uint32(data[pos+1])<<16 | uint32(data[pos+2])<<8 | uint32(data[pos+3])
	pos += 4

	rel, ok := relations[relID]
	if !ok {
		log.Printf("PGListener: DELETE收到未知relation %d", relID)
		return
	}

	// Skip 'K' or 'O' marker
	if pos < len(data) {
		pos++
	}

	values, _ := parseTupleData(data, pos)
	if len(values) == 0 {
		return
	}

	// Build WHERE clause using key columns
	whereClauses := make([]string, 0)
	whereArgs := make([]interface{}, 0)
	paramIdx := 1

	for i, col := range rel.Columns {
		if !col.IsKey {
			continue
		}
		if i >= len(values) {
			break
		}
		targetCol := l.mapColumn(col.Name)
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", targetCol, l.placeholder(paramIdx)))
		whereArgs = append(whereArgs, convertValue(values[i], col.DataType))
		paramIdx++
	}

	if len(whereClauses) == 0 {
		// Fall back to using all provided columns for the WHERE clause
		for i, col := range rel.Columns {
			if i >= len(values) || values[i] == nil {
				continue
			}
			targetCol := l.mapColumn(col.Name)
			whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", targetCol, l.placeholder(paramIdx)))
			whereArgs = append(whereArgs, convertValue(values[i], col.DataType))
			paramIdx++
		}
	}

	if len(whereClauses) == 0 {
		log.Printf("PGListener: DELETE跳过: 无法构建WHERE条件")
		return
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		l.TargetTable,
		strings.Join(whereClauses, " AND "),
	)

	if _, err := l.TargetDB.Exec(query, whereArgs...); err != nil {
		log.Printf("PGListener: DELETE目标失败: %v", err)
	}
}

// mapColumn returns the target column name for a given source column.
func (l *PGListener) mapColumn(sourceCol string) string {
	if len(l.ColumnMappings) > 0 {
		if mapped, ok := l.ColumnMappings[sourceCol]; ok {
			return mapped
		}
	}
	return sourceCol
}

// placeholder returns a placeholder string appropriate for the target DB type.
func (l *PGListener) placeholder(index int) string {
	switch l.TargetDBType {
	case "postgresql":
		return fmt.Sprintf("$%d", index)
	default:
		return "?"
	}
}

// convertValue converts a pgoutput text value to a Go value suitable for
// database/sql, using the column's OID to guide the conversion.
func convertValue(val interface{}, oid uint32) interface{} {
	if val == nil {
		return nil
	}

	s, ok := val.(string)
	if !ok {
		return val
	}

	// Common PostgreSQL type OIDs
	switch oid {
	case pgtype.BoolOID:
		return s == "t" || s == "true"
	default:
		// For most types, the text representation works fine with database/sql
		return s
	}
}

// pgIdentifier is a minimal helper to quote an identifier for safe use in SQL.
// For production use, a more robust quoting mechanism should be used.
func pgIdentifier(name string) string {
	// Simple safety: replace any double quotes in name
	safe := strings.ReplaceAll(name, "\"", "\"\"")
	return fmt.Sprintf("\"%s\"", safe)
}
