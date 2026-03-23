package engine

import (
	"context"
	"database/sql"
	"datasync/internal/connector"
	"datasync/internal/model"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DataSyncOptions configures a full table data sync operation.
type DataSyncOptions struct {
	Source        connector.Connector
	Target        connector.Connector
	SourceTable   string
	TargetTable   string
	Mappings      []model.FieldMapping
	BatchSize     int    // default 1000
	WriteStrategy string // "insert" or "upsert"
	OnProgress    func(synced, total int64)
	WhereClause   string
	Concurrency   int         // 0 or 1 = serial (default); >1 = parallel shards
	StartOffset   int64       // 断点续传起始 offset
	OnCheckpoint  func(int64) // 每批完成后回调，保存 checkpoint
	SourceSQL     string      // non-empty: run SQL import instead of table sync
	SourceSQLArgs []any
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

	if opts.SourceSQL != "" {
		return syncDataFromSQL(ctx, opts)
	}

	// Use parallel sharding if Concurrency > 1
	if opts.Concurrency > 1 {
		return syncDataParallel(ctx, opts)
	}

	// Count source rows
	totalRows, err := opts.Source.CountRows(ctx, opts.SourceTable, opts.WhereClause)
	if err != nil {
		return nil, fmt.Errorf("count source rows: %w", err)
	}

	// Determine column lists from mappings (enabled only)
	sourceCols, targetCols := columnsFromMappings(opts.Mappings)
	if len(sourceCols) == 0 {
		// No mappings provided — query columns from source via Connector
		schema, err := opts.Source.GetSchema(ctx, opts.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("get source columns: %w", err)
		}
		for _, c := range schema.Columns {
			sourceCols = append(sourceCols, c.Name)
			targetCols = append(targetCols, c.Name)
		}
	}

	// For upsert strategy, we need primary key columns of the target table
	var pkColumns []string
	if opts.WriteStrategy == "upsert" {
		schema, err := opts.Target.GetSchema(ctx, opts.TargetTable)
		if err != nil {
			return nil, fmt.Errorf("get primary key columns: %w", err)
		}
		for _, col := range schema.Columns {
			if col.IsPrimary {
				pkColumns = append(pkColumns, col.Name)
			}
		}
		if len(pkColumns) == 0 {
			return nil, fmt.Errorf("upsert requires primary key columns but none found on target table %s", opts.TargetTable)
		}
	}

	// Batch loop — start from StartOffset for resume support
	var rowsSynced int64
	for offset := opts.StartOffset; offset < totalRows; offset += int64(opts.BatchSize) {
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
		batch, err := opts.Source.ReadBatch(ctx, connector.ReadOptions{
			Table:   opts.SourceTable,
			Columns: sourceCols,
			Where:   opts.WhereClause,
			Offset:  offset,
			Limit:   int64(opts.BatchSize),
		})
		if err != nil {
			return nil, fmt.Errorf("read source batch at offset %d: %w", offset, err)
		}

		if len(batch) == 0 {
			break
		}

		// Remap column names from source to target
		rows := remapColumns(batch, sourceCols, targetCols)

		// Write batch to target
		writeStrategy := connector.StrategyInsert
		if opts.WriteStrategy == "upsert" {
			writeStrategy = connector.StrategyUpsert
		}
		if err := opts.Target.WriteBatch(ctx, connector.WriteOptions{
			Table:    opts.TargetTable,
			Columns:  targetCols,
			Rows:     rows,
			Strategy: writeStrategy,
			PKCols:   pkColumns,
		}); err != nil {
			return nil, fmt.Errorf("write batch at offset %d: %w", offset, err)
		}

		rowsSynced += int64(len(batch))

		// Report checkpoint
		if opts.OnCheckpoint != nil {
			opts.OnCheckpoint(offset + int64(len(batch)))
		}

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

// remapColumns replaces source column names with target column names in each row.
func remapColumns(rows []connector.Row, srcCols, tgtCols []string) []connector.Row {
	result := make([]connector.Row, len(rows))
	for i, row := range rows {
		newRow := make(connector.Row, len(row))
		for j, src := range srcCols {
			if j < len(tgtCols) {
				newRow[tgtCols[j]] = row[src]
			}
		}
		result[i] = newRow
	}
	return result
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

// detectIntPKFromConnector returns the name of a single integer primary key column, or "".
func detectIntPKFromConnector(ctx context.Context, src connector.Connector, table string) string {
	schema, err := src.GetSchema(ctx, table)
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
// If an integer PK exists on a SQLConnector source, uses range-based sharding. Otherwise falls back to serial.
func syncDataParallel(ctx context.Context, opts DataSyncOptions) (*SyncResult, error) {
	start := time.Now()
	n := opts.Concurrency

	pkCol := detectIntPKFromConnector(ctx, opts.Source, opts.SourceTable)

	if pkCol != "" {
		// Range-based sharding on integer PK — only possible with a SQL backend
		sqlSrc, ok := opts.Source.(*connector.SQLConnector)
		if !ok {
			// Non-SQL connector: fall back to serial
			opts.Concurrency = 1
			return SyncData(ctx, opts)
		}

		dbType := opts.Source.DBType()
		quotedPK := quoteIdentifier(dbType, pkCol)

		baseWhere := ""
		if opts.WhereClause != "" {
			baseWhere = " WHERE " + opts.WhereClause
		}

		var minVal, maxVal int64
		row := sqlSrc.RawDB().QueryRowContext(ctx,
			fmt.Sprintf("SELECT COALESCE(MIN(%s),0), COALESCE(MAX(%s),0) FROM %s%s",
				quotedPK, quotedPK,
				quoteIdentifier(dbType, opts.SourceTable), baseWhere))
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

		// Query total rows for progress reporting
		var totalRows int64
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s%s",
			quoteIdentifier(dbType, opts.SourceTable), baseWhere)
		_ = sqlSrc.RawDB().QueryRowContext(ctx, countSQL).Scan(&totalRows)

		return runShards(ctx, opts, shards, start, totalRows)
	}

	// No integer PK — fall back to serial to avoid duplicate rows
	opts.Concurrency = 1
	return SyncData(ctx, opts)
}

// syncDataFromSQL executes a user-provided SQL on the source (SQL databases only),
// paginates results via LIMIT/OFFSET subquery, and writes each batch to the target.
// Note: for deterministic results, the user SQL should include an ORDER BY clause.
func syncDataFromSQL(ctx context.Context, opts DataSyncOptions) (*SyncResult, error) {
	start := time.Now()

	type rawDBer interface {
		RawDB() *sql.DB
	}
	rdb, ok := opts.Source.(rawDBer)
	if !ok {
		return nil, fmt.Errorf("sql_import requires a SQL data source (MySQL/PostgreSQL/Oracle/ClickHouse/Doris); MongoDB and file sources are not supported")
	}
	db := rdb.RawDB()
	dbType := opts.Source.DBType()

	// Strip trailing semicolons — Oracle and others reject semicolons inside subqueries
	srcSQL := strings.TrimRight(strings.TrimSpace(opts.SourceSQL), ";")

	// Estimate total rows via subquery
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) datasync_cnt", srcSQL)
	var totalRows int64
	if err := db.QueryRowContext(ctx, countSQL, opts.SourceSQLArgs...).Scan(&totalRows); err != nil {
		return nil, fmt.Errorf("count sql rows: %w", err)
	}

	// Get target PKs for upsert
	var pkColumns []string
	if opts.WriteStrategy == "upsert" {
		schema, err := opts.Target.GetSchema(ctx, opts.TargetTable)
		if err != nil {
			return nil, fmt.Errorf("get target pk: %w", err)
		}
		for _, col := range schema.Columns {
			if col.IsPrimary {
				pkColumns = append(pkColumns, col.Name)
			}
		}
		if len(pkColumns) == 0 {
			return nil, fmt.Errorf("upsert requires primary key on target table %s", opts.TargetTable)
		}
	}

	writeStrategy := connector.StrategyInsert
	if opts.WriteStrategy == "upsert" {
		writeStrategy = connector.StrategyUpsert
	}

	var rowsSynced int64
	for offset := opts.StartOffset; offset < totalRows; offset += int64(opts.BatchSize) {
		select {
		case <-ctx.Done():
			return &SyncResult{RowsSynced: rowsSynced, Duration: time.Since(start), Error: ctx.Err()}, ctx.Err()
		default:
		}

		rows, cols, err := readSQLBatch(ctx, db, dbType, srcSQL, opts.SourceSQLArgs, opts.BatchSize, offset)
		if err != nil {
			return nil, fmt.Errorf("read batch at offset %d: %w", offset, err)
		}

		if len(rows) == 0 {
			break
		}

		if err := opts.Target.WriteBatch(ctx, connector.WriteOptions{
			Table:    opts.TargetTable,
			Columns:  cols,
			Rows:     rows,
			Strategy: writeStrategy,
			PKCols:   pkColumns,
		}); err != nil {
			return nil, fmt.Errorf("write batch at offset %d: %w", offset, err)
		}

		rowsSynced += int64(len(rows))

		if opts.OnCheckpoint != nil {
			opts.OnCheckpoint(offset + int64(len(rows)))
		}
		if opts.OnProgress != nil {
			opts.OnProgress(rowsSynced, totalRows)
		}
	}

	return &SyncResult{RowsSynced: rowsSynced, Duration: time.Since(start)}, nil
}

// readSQLBatch executes a paginated query and returns the rows and column names for one batch.
func readSQLBatch(ctx context.Context, db *sql.DB, dbType, query string, args []any, batchSize int, offset int64) ([]connector.Row, []string, error) {
	batchSQL := buildPreviewPageSQL(dbType, query, batchSize, offset)
	sqlRows, err := db.QueryContext(ctx, batchSQL, args...)
	if err != nil {
		return nil, nil, err
	}
	defer sqlRows.Close()

	allCols, err := sqlRows.Columns()
	if err != nil {
		return nil, nil, err
	}
	// Strip the internal pagination column injected for Oracle ROWNUM pagination.
	cols := make([]string, 0, len(allCols))
	rnIdx := -1
	for i, c := range allCols {
		if strings.EqualFold(c, "datasync_rn") {
			rnIdx = i
		} else {
			cols = append(cols, c)
		}
	}

	var rows []connector.Row
	for sqlRows.Next() {
		vals := make([]interface{}, len(allCols))
		ptrs := make([]interface{}, len(allCols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := sqlRows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make(connector.Row, len(cols))
		for i, col := range allCols {
			if i == rnIdx {
				continue
			}
			row[col] = vals[i]
		}
		rows = append(rows, row)
	}
	return rows, cols, sqlRows.Err()
}

// runShards executes each WHERE-clause shard concurrently and merges results.
// totalRows is the full row count used for progress reporting (0 means unknown).
func runShards(ctx context.Context, opts DataSyncOptions, shards []string, start time.Time, totalRows int64) (*SyncResult, error) {
	type shardResult struct {
		rows int64
		err  error
	}
	n := len(shards)
	results := make([]shardResult, n)

	// Per-shard in-progress row counters for real-time aggregated progress.
	shardProgress := make([]int64, n)

	// Create a cancellable child context so a failing shard cancels the rest.
	shardCtx, cancelShards := context.WithCancel(ctx)
	defer cancelShards()

	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for i, clause := range shards {
		wg.Add(1)
		go func(idx int, whereClause string) {
			defer wg.Done()
			shardOpts := opts
			shardOpts.Concurrency = 1
			shardOpts.WhereClause = whereClause
			// Each shard updates its own slot; the callback sums all slots.
			shardOpts.OnProgress = func(synced, _ int64) {
				atomic.StoreInt64(&shardProgress[idx], synced)
				if opts.OnProgress == nil {
					return
				}
				var current int64
				for j := 0; j < n; j++ {
					current += atomic.LoadInt64(&shardProgress[j])
				}
				opts.OnProgress(current, totalRows)
			}
			res, err := SyncData(shardCtx, shardOpts)
			if err != nil {
				errOnce.Do(func() {
					firstErr = err
					cancelShards()
				})
				results[idx] = shardResult{err: err}
				return
			}
			// Mark shard as fully done in the progress slot.
			atomic.StoreInt64(&shardProgress[idx], res.RowsSynced)
			results[idx] = shardResult{rows: res.RowsSynced}
			// Emit one final aggregated update for this shard.
			if opts.OnProgress != nil {
				var current int64
				for j := 0; j < n; j++ {
					current += atomic.LoadInt64(&shardProgress[j])
				}
				opts.OnProgress(current, totalRows)
			}
		}(i, clause)
	}
	wg.Wait()

	var totalSynced int64
	for _, r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		totalSynced += r.rows
	}
	if firstErr != nil {
		return &SyncResult{RowsSynced: totalSynced, Duration: time.Since(start)}, firstErr
	}
	return &SyncResult{RowsSynced: totalSynced, Duration: time.Since(start)}, nil
}
