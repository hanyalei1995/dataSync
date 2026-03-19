package engine

import (
	"context"
	"database/sql"
	"datasync/internal/model"
	"fmt"
	"math"
	"strings"
)

// QualityCheckOptions configures a data quality check.
type QualityCheckOptions struct {
	SourceDB     *sql.DB
	TargetDB     *sql.DB
	SourceDBType string
	TargetDBType string
	SourceTable  string
	TargetTable  string
	WhereClause  string // applied to source COUNT
	Mappings     []model.FieldMapping
	SampleSize   int // default 50
}

// QualityCheckResult holds the outcome of a quality check.
type QualityCheckResult struct {
	SourceRows    int64
	TargetRows    int64
	SampleTotal   int
	SampleMatched int
	Status        string // "passed" | "warning" | "failed"
}

// CheckQuality runs row-count comparison and random-sample field comparison.
func CheckQuality(ctx context.Context, opts QualityCheckOptions) (*QualityCheckResult, error) {
	if opts.SampleSize <= 0 {
		opts.SampleSize = 50
	}

	res := &QualityCheckResult{}

	// 1. Count source rows (with filter)
	countSQL := buildCountSQL(opts.SourceDBType, opts.SourceTable, opts.WhereClause)
	if err := opts.SourceDB.QueryRowContext(ctx, countSQL).Scan(&res.SourceRows); err != nil {
		return nil, fmt.Errorf("count source rows: %w", err)
	}

	// 2. Count target rows (total, no WHERE)
	tgtCountSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(opts.TargetDBType, opts.TargetTable))
	if err := opts.TargetDB.QueryRowContext(ctx, tgtCountSQL).Scan(&res.TargetRows); err != nil {
		return nil, fmt.Errorf("count target rows: %w", err)
	}

	// 3. Sample comparison (requires int PK)
	pkCol := detectIntPK(opts.SourceDB, opts.SourceDBType, opts.SourceTable)
	if pkCol != "" {
		matched, total, err := sampleCompare(ctx, opts, pkCol)
		if err == nil {
			res.SampleTotal = total
			res.SampleMatched = matched
		}
	}

	// 4. Determine overall status
	res.Status = determineQualityStatus(res)
	return res, nil
}

// sampleCompare picks random PKs from source, fetches same rows from target, compares field by field.
func sampleCompare(ctx context.Context, opts QualityCheckOptions, pkCol string) (matched, total int, err error) {
	quotedSrcPK := quoteIdentifier(opts.SourceDBType, pkCol)
	quotedTgtPK := quoteIdentifier(opts.TargetDBType, pkCol)

	// Random sample of PKs from source
	var sampleSQL string
	switch strings.ToLower(opts.SourceDBType) {
	case "postgresql":
		sampleSQL = fmt.Sprintf("SELECT %s FROM %s ORDER BY RANDOM() LIMIT %d",
			quotedSrcPK, quoteIdentifier(opts.SourceDBType, opts.SourceTable), opts.SampleSize)
	default:
		sampleSQL = fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT %d",
			quotedSrcPK, quoteIdentifier(opts.SourceDBType, opts.SourceTable), opts.SampleSize)
	}

	rows, err := opts.SourceDB.QueryContext(ctx, sampleSQL)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var pkVals []interface{}
	for rows.Next() {
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			continue
		}
		pkVals = append(pkVals, v)
	}
	if len(pkVals) == 0 {
		return 0, 0, nil
	}

	// Build column list from mappings
	sourceCols, _ := columnsFromMappings(opts.Mappings)
	if len(sourceCols) == 0 {
		cols, err := getSourceColumns(opts.SourceDB, opts.SourceDBType, opts.SourceTable)
		if err != nil {
			return 0, 0, err
		}
		sourceCols = cols
	}
	_, targetCols := columnsFromMappings(opts.Mappings)
	if len(targetCols) == 0 {
		targetCols = sourceCols
	}

	// Build IN placeholders for source DB
	srcPlaceholders := makePlaceholders(opts.SourceDBType, len(pkVals))
	srcFetchSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
		buildQualColList(opts.SourceDBType, sourceCols),
		quoteIdentifier(opts.SourceDBType, opts.SourceTable),
		quotedSrcPK,
		strings.Join(srcPlaceholders, ","))

	srcRows, err := opts.SourceDB.QueryContext(ctx, srcFetchSQL, pkVals...)
	if err != nil {
		return 0, 0, err
	}
	srcData, err := scanRows(srcRows, len(sourceCols))
	srcRows.Close()
	if err != nil {
		return 0, 0, err
	}

	// Build IN placeholders for target DB
	tgtPlaceholders := makePlaceholders(opts.TargetDBType, len(pkVals))
	tgtFetchSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
		buildQualColList(opts.TargetDBType, targetCols),
		quoteIdentifier(opts.TargetDBType, opts.TargetTable),
		quotedTgtPK,
		strings.Join(tgtPlaceholders, ","))

	tgtRows, err := opts.TargetDB.QueryContext(ctx, tgtFetchSQL, pkVals...)
	if err != nil {
		return 0, 0, err
	}
	tgtData, err := scanRows(tgtRows, len(targetCols))
	tgtRows.Close()
	if err != nil {
		return 0, 0, err
	}

	// Find PK column index in source columns
	pkIdx := 0
	for i, c := range sourceCols {
		if strings.EqualFold(c, pkCol) {
			pkIdx = i
			break
		}
	}

	// Build target lookup map keyed by PK string representation
	tgtMap := make(map[string][]interface{})
	for _, row := range tgtData {
		if len(row) > pkIdx {
			tgtMap[fmt.Sprintf("%v", row[pkIdx])] = row
		}
	}

	total = len(srcData)
	for _, srcRow := range srcData {
		if len(srcRow) <= pkIdx {
			continue
		}
		pk := fmt.Sprintf("%v", srcRow[pkIdx])
		tgtRow, ok := tgtMap[pk]
		if !ok {
			continue
		}
		if rowsMatch(srcRow, tgtRow) {
			matched++
		}
	}
	return matched, total, nil
}

func makePlaceholders(dbType string, n int) []string {
	placeholders := make([]string, n)
	for i := range placeholders {
		switch strings.ToLower(dbType) {
		case "postgresql":
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		case "oracle":
			placeholders[i] = fmt.Sprintf(":%d", i+1)
		default:
			placeholders[i] = "?"
		}
	}
	return placeholders
}

func buildQualColList(dbType string, cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quoteIdentifier(dbType, c)
	}
	return strings.Join(quoted, ", ")
}

// rowsMatch compares two rows field by field, tolerating small float differences.
func rowsMatch(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		av := fmt.Sprintf("%v", a[i])
		bv := fmt.Sprintf("%v", b[i])
		if av == bv {
			continue
		}
		// Tolerate tiny float differences
		var af, bf float64
		if _, err := fmt.Sscanf(av, "%f", &af); err == nil {
			if _, err2 := fmt.Sscanf(bv, "%f", &bf); err2 == nil {
				if math.Abs(af-bf) < 0.0001 {
					continue
				}
			}
		}
		return false
	}
	return true
}

// determineQualityStatus derives the overall status from row count and sample results.
func determineQualityStatus(r *QualityCheckResult) string {
	// Row count check
	if r.SourceRows > 0 {
		diff := float64(r.SourceRows-r.TargetRows) / float64(r.SourceRows)
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.01 { // >1% difference
			return "failed"
		}
		if diff > 0 {
			return "warning"
		}
	}
	// Sample check
	if r.SampleTotal > 0 {
		ratio := float64(r.SampleMatched) / float64(r.SampleTotal)
		if ratio < 0.98 {
			return "failed"
		}
	}
	return "passed"
}
