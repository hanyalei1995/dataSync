package engine

import (
	"context"
	"database/sql"
	"datasync/internal/model"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DataSyncOptions configures a full table data sync operation.
type DataSyncOptions struct {
	SourceDB      *sql.DB
	TargetDB      *sql.DB
	SourceDBType  string
	TargetDBType  string
	SourceTable   string
	TargetTable   string
	Mappings      []model.FieldMapping
	BatchSize     int    // default 1000
	WriteStrategy string // "insert" or "upsert"
	OnProgress    func(synced, total int64)
	WhereClause   string
	Concurrency   int // 0 or 1 = serial (default); >1 = parallel shards
}

// SyncResult holds the outcome of a data sync operation.
type SyncResult struct {
	RowsSynced        int64
	Duration          time.Duration
	Error             error
	MaxWatermarkValue string
}

// SyncData performs a full table data sync from source to target with batch processing.
func SyncData(ctx context.Context, opts DataSyncOptions) (*SyncResult, error) {
	start := time.Now()

	// Apply defaults
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	if opts.WriteStrategy == "" {
		opts.WriteStrategy = "insert"
	}

	// Use parallel sharding if Concurrency > 1
	if opts.Concurrency > 1 {
		return syncDataParallel(ctx, opts)
	}

	// Count source rows
	var totalRows int64
	countSQL := buildCountSQL(opts.SourceDBType, opts.SourceTable, opts.WhereClause)
	if err := opts.SourceDB.QueryRowContext(ctx, countSQL).Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("count source rows: %w", err)
	}

	// Determine column lists from mappings (enabled only)
	sourceCols, targetCols := columnsFromMappings(opts.Mappings)
	if len(sourceCols) == 0 {
		// No mappings provided — query columns from source DB
		cols, err := getSourceColumns(opts.SourceDB, opts.SourceDBType, opts.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("get source columns: %w", err)
		}
		sourceCols = cols
		targetCols = cols
	}

	// For upsert strategy, we need primary key columns of the target table
	var pkColumns []string
	if opts.WriteStrategy == "upsert" {
		var err error
		pkColumns, err = getPrimaryKeyColumns(opts.TargetDB, opts.TargetDBType, opts.TargetTable)
		if err != nil {
			return nil, fmt.Errorf("get primary key columns: %w", err)
		}
		if len(pkColumns) == 0 {
			return nil, fmt.Errorf("upsert requires primary key columns but none found on target table %s", opts.TargetTable)
		}
	}

	// Batch loop
	var rowsSynced int64
	for offset := int64(0); offset < totalRows; offset += int64(opts.BatchSize) {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return &SyncResult{
				RowsSynced: rowsSynced,
				Duration:   time.Since(start),
				Error:      ctx.Err(),
			}, ctx.Err()
		default:
		}

		// Read batch from source
		selectSQL := buildBatchSelectSQL(opts.SourceDBType, opts.SourceTable, sourceCols, offset, int64(opts.BatchSize), opts.WhereClause)
		rows, err := opts.SourceDB.QueryContext(ctx, selectSQL)
		if err != nil {
			return nil, fmt.Errorf("query source batch at offset %d: %w", offset, err)
		}

		// Collect batch rows
		batch, err := scanRows(rows, len(sourceCols))
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("scan source batch at offset %d: %w", offset, err)
		}

		if len(batch) == 0 {
			break
		}

		// Write batch to target
		if err := writeBatch(ctx, opts, targetCols, pkColumns, batch); err != nil {
			return nil, fmt.Errorf("write batch at offset %d: %w", offset, err)
		}

		rowsSynced += int64(len(batch))

		// Report progress
		if opts.OnProgress != nil {
			opts.OnProgress(rowsSynced, totalRows)
		}
	}

	return &SyncResult{
		RowsSynced: rowsSynced,
		Duration:   time.Since(start),
	}, nil
}

// columnsFromMappings extracts enabled source and target column names from field mappings.
func columnsFromMappings(mappings []model.FieldMapping) (sourceCols, targetCols []string) {
	for _, m := range mappings {
		if !m.Enabled {
			continue
		}
		sourceCols = append(sourceCols, m.SourceField)
		target := m.TargetField
		if target == "" {
			target = m.SourceField
		}
		targetCols = append(targetCols, target)
	}
	return
}

// getSourceColumns queries column names from the source database when no mappings are provided.
func getSourceColumns(db *sql.DB, dbType, table string) ([]string, error) {
	schema, err := ReadTableSchema(db, dbType, table)
	if err != nil {
		return nil, err
	}
	cols := make([]string, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		cols = append(cols, c.Name)
	}
	return cols, nil
}

// buildCountSQL builds a COUNT query with an optional WHERE clause.
func buildCountSQL(dbType, table, whereClause string) string {
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(dbType, table))
	if whereClause != "" {
		q += " WHERE " + whereClause
	}
	return q
}

// buildBatchSelectSQL builds a paginated SELECT statement for the given database type.
func buildBatchSelectSQL(dbType, table string, columns []string, offset, limit int64, whereClause string) string {
	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = quoteIdentifier(dbType, c)
	}
	colList := strings.Join(quotedCols, ", ")
	tbl := quoteIdentifier(dbType, table)

	whereFragment := ""
	if whereClause != "" {
		whereFragment = " WHERE " + whereClause
	}

	switch strings.ToLower(dbType) {
	case "oracle":
		// Oracle ROWNUM-based pagination
		return fmt.Sprintf(
			"SELECT %s FROM (SELECT a.*, ROWNUM rn FROM (SELECT %s FROM %s%s) a WHERE ROWNUM <= %d) WHERE rn > %d",
			colList, colList, tbl, whereFragment, offset+limit, offset,
		)
	default:
		// MySQL and PostgreSQL both support LIMIT/OFFSET
		return fmt.Sprintf("SELECT %s FROM %s%s LIMIT %d OFFSET %d", colList, tbl, whereFragment, limit, offset)
	}
}

// buildInsertSQL builds a batch INSERT statement for the target database.
func buildInsertSQL(dbType, table string, columns []string, rowCount int) string {
	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = quoteIdentifier(dbType, c)
	}
	colList := strings.Join(quotedCols, ", ")
	tbl := quoteIdentifier(dbType, table)

	placeholder := buildPlaceholderRow(dbType, len(columns), 0)
	var valueRows []string
	for i := 0; i < rowCount; i++ {
		valueRows = append(valueRows, "("+buildPlaceholderRow(dbType, len(columns), i*len(columns))+")")
	}
	_ = placeholder
	values := strings.Join(valueRows, ", ")

	switch strings.ToLower(dbType) {
	case "mysql":
		return fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s", tbl, colList, values)
	case "postgresql":
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING", tbl, colList, values)
	default:
		// Oracle basic INSERT
		if rowCount == 1 {
			return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tbl, colList, values)
		}
		// Oracle INSERT ALL for multiple rows
		var sb strings.Builder
		sb.WriteString("INSERT ALL")
		idx := 0
		for i := 0; i < rowCount; i++ {
			sb.WriteString(fmt.Sprintf(" INTO %s (%s) VALUES (", tbl, colList))
			for j := 0; j < len(columns); j++ {
				if j > 0 {
					sb.WriteString(", ")
				}
				idx++
				sb.WriteString(fmt.Sprintf(":%d", idx))
			}
			sb.WriteString(")")
		}
		sb.WriteString(" SELECT 1 FROM DUAL")
		return sb.String()
	}
}

// buildUpsertSQL builds a batch UPSERT statement for the target database.
func buildUpsertSQL(dbType, table string, columns, pkColumns []string, rowCount int) string {
	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = quoteIdentifier(dbType, c)
	}
	colList := strings.Join(quotedCols, ", ")
	tbl := quoteIdentifier(dbType, table)

	// Build non-PK columns for the UPDATE SET clause
	pkSet := make(map[string]bool)
	for _, pk := range pkColumns {
		pkSet[strings.ToLower(pk)] = true
	}

	switch strings.ToLower(dbType) {
	case "mysql":
		var valueRows []string
		for i := 0; i < rowCount; i++ {
			valueRows = append(valueRows, "("+buildPlaceholderRow(dbType, len(columns), i*len(columns))+")")
		}
		values := strings.Join(valueRows, ", ")

		var updateParts []string
		for _, c := range columns {
			if pkSet[strings.ToLower(c)] {
				continue
			}
			qc := quoteIdentifier(dbType, c)
			updateParts = append(updateParts, fmt.Sprintf("%s = VALUES(%s)", qc, qc))
		}
		updateClause := strings.Join(updateParts, ", ")
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s", tbl, colList, values, updateClause)

	case "postgresql":
		var valueRows []string
		for i := 0; i < rowCount; i++ {
			valueRows = append(valueRows, "("+buildPlaceholderRow(dbType, len(columns), i*len(columns))+")")
		}
		values := strings.Join(valueRows, ", ")

		quotedPKs := make([]string, len(pkColumns))
		for i, pk := range pkColumns {
			quotedPKs[i] = quoteIdentifier(dbType, pk)
		}
		conflictCols := strings.Join(quotedPKs, ", ")

		var updateParts []string
		for _, c := range columns {
			if pkSet[strings.ToLower(c)] {
				continue
			}
			qc := quoteIdentifier(dbType, c)
			updateParts = append(updateParts, fmt.Sprintf("%s = EXCLUDED.%s", qc, qc))
		}
		updateClause := strings.Join(updateParts, ", ")
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s", tbl, colList, values, conflictCols, updateClause)

	default:
		// Oracle MERGE INTO (single row at a time for simplicity)
		// For batch, we generate a MERGE for each row
		// Returning single-row MERGE template; caller handles iteration
		srcCols := make([]string, len(columns))
		for i := range columns {
			srcCols[i] = fmt.Sprintf(":%d AS %s", i+1, quoteIdentifier(dbType, columns[i]))
		}
		srcSelect := strings.Join(srcCols, ", ")

		quotedPKs := make([]string, len(pkColumns))
		for i, pk := range pkColumns {
			quotedPKs[i] = fmt.Sprintf("t.%s = s.%s", quoteIdentifier(dbType, pk), quoteIdentifier(dbType, pk))
		}
		onClause := strings.Join(quotedPKs, " AND ")

		var updateParts []string
		for _, c := range columns {
			if pkSet[strings.ToLower(c)] {
				continue
			}
			qc := quoteIdentifier(dbType, c)
			updateParts = append(updateParts, fmt.Sprintf("t.%s = s.%s", qc, qc))
		}
		updateClause := strings.Join(updateParts, ", ")

		insertCols := make([]string, len(columns))
		insertVals := make([]string, len(columns))
		for i, c := range columns {
			insertCols[i] = fmt.Sprintf("t.%s", quoteIdentifier(dbType, c))
			insertVals[i] = fmt.Sprintf("s.%s", quoteIdentifier(dbType, c))
		}

		return fmt.Sprintf(
			"MERGE INTO %s t USING (SELECT %s FROM DUAL) s ON (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
			tbl, srcSelect, onClause, updateClause,
			strings.Join(insertCols, ", "), strings.Join(insertVals, ", "),
		)
	}
}

// getPrimaryKeyColumns returns the primary key column names for a table.
func getPrimaryKeyColumns(db *sql.DB, dbType, table string) ([]string, error) {
	schema, err := ReadTableSchema(db, dbType, table)
	if err != nil {
		return nil, err
	}
	var pks []string
	for _, col := range schema.Columns {
		if col.IsPrimary {
			pks = append(pks, col.Name)
		}
	}
	return pks, nil
}

// buildPlaceholderRow builds a comma-separated list of placeholders for one row.
// startIdx is the 0-based index of the first parameter in this row.
func buildPlaceholderRow(dbType string, colCount, startIdx int) string {
	parts := make([]string, colCount)
	switch strings.ToLower(dbType) {
	case "postgresql":
		for i := 0; i < colCount; i++ {
			parts[i] = fmt.Sprintf("$%d", startIdx+i+1)
		}
	case "oracle":
		for i := 0; i < colCount; i++ {
			parts[i] = fmt.Sprintf(":%d", startIdx+i+1)
		}
	default: // mysql
		for i := 0; i < colCount; i++ {
			parts[i] = "?"
		}
	}
	return strings.Join(parts, ", ")
}

// scanRows reads all rows from a result set into a slice of value slices.
func scanRows(rows *sql.Rows, colCount int) ([][]interface{}, error) {
	var batch [][]interface{}
	for rows.Next() {
		values := make([]interface{}, colCount)
		ptrs := make([]interface{}, colCount)
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		batch = append(batch, values)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return batch, nil
}

// writeBatch writes a batch of rows to the target database.
func writeBatch(ctx context.Context, opts DataSyncOptions, targetCols, pkColumns []string, batch [][]interface{}) error {
	dbType := strings.ToLower(opts.TargetDBType)

	// Oracle upsert uses MERGE which is single-row, handle separately
	if dbType == "oracle" && opts.WriteStrategy == "upsert" {
		return writeOracleUpsertBatch(ctx, opts, targetCols, pkColumns, batch)
	}

	// Oracle insert with multiple rows uses INSERT ALL, handle separately
	if dbType == "oracle" && len(batch) > 1 {
		return writeOracleInsertBatch(ctx, opts, targetCols, batch)
	}

	// Build the SQL for the full batch
	var sqlStr string
	if opts.WriteStrategy == "upsert" {
		sqlStr = buildUpsertSQL(opts.TargetDBType, opts.TargetTable, targetCols, pkColumns, len(batch))
	} else {
		sqlStr = buildInsertSQL(opts.TargetDBType, opts.TargetTable, targetCols, len(batch))
	}

	// Flatten batch values into a single args slice
	args := make([]interface{}, 0, len(batch)*len(targetCols))
	for _, row := range batch {
		args = append(args, row...)
	}

	_, err := opts.TargetDB.ExecContext(ctx, sqlStr, args...)
	return err
}

// writeOracleInsertBatch handles Oracle multi-row INSERT ALL.
func writeOracleInsertBatch(ctx context.Context, opts DataSyncOptions, targetCols []string, batch [][]interface{}) error {
	sqlStr := buildInsertSQL(opts.TargetDBType, opts.TargetTable, targetCols, len(batch))
	args := make([]interface{}, 0, len(batch)*len(targetCols))
	for _, row := range batch {
		args = append(args, row...)
	}
	_, err := opts.TargetDB.ExecContext(ctx, sqlStr, args...)
	return err
}

// writeOracleUpsertBatch handles Oracle MERGE INTO one row at a time.
func writeOracleUpsertBatch(ctx context.Context, opts DataSyncOptions, targetCols, pkColumns []string, batch [][]interface{}) error {
	mergeSQL := buildUpsertSQL(opts.TargetDBType, opts.TargetTable, targetCols, pkColumns, 1)
	stmt, err := opts.TargetDB.PrepareContext(ctx, mergeSQL)
	if err != nil {
		return fmt.Errorf("prepare oracle merge: %w", err)
	}
	defer stmt.Close()

	for _, row := range batch {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return err
		}
	}
	return nil
}

// detectIntPK returns the name of a single integer primary key column, or "".
func detectIntPK(db *sql.DB, dbType, table string) string {
	schema, err := ReadTableSchema(db, dbType, table)
	if err != nil {
		return ""
	}
	for _, col := range schema.Columns {
		if !col.IsPrimary {
			continue
		}
		t := strings.ToLower(col.Type)
		if strings.Contains(t, "int") || strings.Contains(t, "serial") || strings.Contains(t, "number") {
			return col.Name
		}
	}
	return ""
}

// syncDataParallel runs SyncData using N concurrent goroutines, each handling a shard.
// If an integer PK exists, uses range-based sharding. Otherwise falls back to serial.
func syncDataParallel(ctx context.Context, opts DataSyncOptions) (*SyncResult, error) {
	start := time.Now()
	n := opts.Concurrency

	pkCol := detectIntPK(opts.SourceDB, opts.SourceDBType, opts.SourceTable)

	if pkCol != "" {
		// Range-based sharding on integer PK
		quotedPK := quoteIdentifier(opts.SourceDBType, pkCol)

		baseWhere := ""
		if opts.WhereClause != "" {
			baseWhere = " WHERE " + opts.WhereClause
		}

		var minVal, maxVal int64
		row := opts.SourceDB.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COALESCE(MIN(%s),0), COALESCE(MAX(%s),0) FROM %s%s",
				quotedPK, quotedPK,
				quoteIdentifier(opts.SourceDBType, opts.SourceTable), baseWhere))
		if err := row.Scan(&minVal, &maxVal); err != nil || maxVal == 0 {
			// Fallback to serial
			opts.Concurrency = 1
			return SyncData(ctx, opts)
		}

		step := (maxVal - minVal + int64(n)) / int64(n)
		shards := make([]string, 0, n)
		for i := 0; i < n; i++ {
			lo := minVal + int64(i)*step
			hi := lo + step
			var clause string
			if i == n-1 {
				clause = fmt.Sprintf("%s >= %d", quotedPK, lo)
			} else {
				clause = fmt.Sprintf("%s >= %d AND %s < %d", quotedPK, lo, quotedPK, hi)
			}
			if opts.WhereClause != "" {
				clause = opts.WhereClause + " AND " + clause
			}
			shards = append(shards, clause)
		}

		return runShards(ctx, opts, shards, start)
	}

	// No integer PK — fall back to serial to avoid duplicate rows
	opts.Concurrency = 1
	return SyncData(ctx, opts)
}

// runShards executes each WHERE-clause shard concurrently and merges results.
func runShards(ctx context.Context, opts DataSyncOptions, shards []string, start time.Time) (*SyncResult, error) {
	type shardResult struct {
		rows int64
		err  error
	}
	results := make([]shardResult, len(shards))
	var wg sync.WaitGroup
	var totalSynced int64

	for i, clause := range shards {
		wg.Add(1)
		go func(idx int, whereClause string) {
			defer wg.Done()
			shardOpts := opts
			shardOpts.Concurrency = 1
			shardOpts.WhereClause = whereClause
			shardOpts.OnProgress = nil
			res, err := SyncData(ctx, shardOpts)
			if err != nil {
				results[idx] = shardResult{err: err}
				return
			}
			results[idx] = shardResult{rows: res.RowsSynced}
			current := atomic.AddInt64(&totalSynced, res.RowsSynced)
			if opts.OnProgress != nil {
				opts.OnProgress(current, 0)
			}
		}(i, clause)
	}
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return &SyncResult{RowsSynced: atomic.LoadInt64(&totalSynced), Duration: time.Since(start)}, r.err
		}
	}
	return &SyncResult{RowsSynced: atomic.LoadInt64(&totalSynced), Duration: time.Since(start)}, nil
}
