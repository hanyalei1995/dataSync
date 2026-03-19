package engine

import (
	"context"
	"datasync/internal/connector"
	"datasync/internal/model"
	"fmt"
	"math"
)

// QualityCheckOptions configures a data quality check.
type QualityCheckOptions struct {
	Source      connector.Connector
	Target      connector.Connector
	SourceTable string
	TargetTable string
	WhereClause string // applied to source COUNT
	Mappings    []model.FieldMapping
	SampleSize  int // default 50
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
	var err error
	res.SourceRows, err = opts.Source.CountRows(ctx, opts.SourceTable, opts.WhereClause)
	if err != nil {
		return nil, fmt.Errorf("count source rows: %w", err)
	}

	// 2. Count target rows (total, no WHERE)
	res.TargetRows, err = opts.Target.CountRows(ctx, opts.TargetTable, "")
	if err != nil {
		return nil, fmt.Errorf("count target rows: %w", err)
	}

	// 3. Sample comparison (requires int PK)
	pkCol := detectIntPKFromConnector(ctx, opts.Source, opts.SourceTable)
	if pkCol != "" {
		matched, total, sampleErr := sampleCompare(ctx, opts, pkCol)
		if sampleErr == nil {
			res.SampleTotal = total
			res.SampleMatched = matched
		}
	}

	// 4. Determine overall status
	res.Status = determineQualityStatus(res)
	return res, nil
}

// sampleCompare picks sample rows from source via ReadBatch, fetches the same rows
// from target, and compares field by field.
func sampleCompare(ctx context.Context, opts QualityCheckOptions, pkCol string) (matched, total int, err error) {
	srcDBType := opts.Source.DBType()
	tgtDBType := opts.Target.DBType()

	// Build column list from mappings
	sourceCols, targetCols := columnsFromMappings(opts.Mappings)

	// If no mappings provided, fetch column names from source schema
	if len(sourceCols) == 0 {
		s, schemaErr := opts.Source.GetSchema(ctx, opts.SourceTable)
		if schemaErr != nil {
			return 0, 0, schemaErr
		}
		for _, c := range s.Columns {
			sourceCols = append(sourceCols, c.Name)
		}
		targetCols = sourceCols
	}

	// Read a sample of rows (first SampleSize rows matching WhereClause) to get PK values.
	sampleRows, readErr := opts.Source.ReadBatch(ctx, connector.ReadOptions{
		Table:   opts.SourceTable,
		Columns: []string{pkCol},
		Where:   opts.WhereClause,
		Offset:  0,
		Limit:   int64(opts.SampleSize),
	})
	if readErr != nil {
		return 0, 0, readErr
	}
	if len(sampleRows) == 0 {
		return 0, 0, nil
	}

	quotedSrcPK := quoteIdentifier(srcDBType, pkCol)
	quotedTgtPK := quoteIdentifier(tgtDBType, pkCol)

	// Fetch source and target rows one by one using the sampled PK values.
	srcDataMap := make(map[string]connector.Row, len(sampleRows))
	for _, pkRow := range sampleRows {
		pkVal := pkRow[pkCol]
		whereExpr := fmt.Sprintf("%s = %v", quotedSrcPK, pkVal)
		rows, batchErr := opts.Source.ReadBatch(ctx, connector.ReadOptions{
			Table:   opts.SourceTable,
			Columns: sourceCols,
			Where:   whereExpr,
			Offset:  0,
			Limit:   1,
		})
		if batchErr != nil || len(rows) == 0 {
			continue
		}
		srcDataMap[fmt.Sprintf("%v", pkVal)] = rows[0]
	}

	tgtDataMap := make(map[string]connector.Row, len(sampleRows))
	for _, pkRow := range sampleRows {
		pkVal := pkRow[pkCol]
		whereExpr := fmt.Sprintf("%s = %v", quotedTgtPK, pkVal)
		rows, batchErr := opts.Target.ReadBatch(ctx, connector.ReadOptions{
			Table:   opts.TargetTable,
			Columns: targetCols,
			Where:   whereExpr,
			Offset:  0,
			Limit:   1,
		})
		if batchErr != nil || len(rows) == 0 {
			continue
		}
		tgtDataMap[fmt.Sprintf("%v", pkVal)] = rows[0]
	}

	total = len(srcDataMap)
	for pkKey, srcRow := range srcDataMap {
		tgtRow, ok := tgtDataMap[pkKey]
		if !ok {
			continue
		}
		if connRowsMatch(srcRow, tgtRow, sourceCols, targetCols) {
			matched++
		}
	}
	return matched, total, nil
}

// connRowsMatch compares two connector.Row values field-by-field using the provided column lists.
func connRowsMatch(src, tgt connector.Row, srcCols, tgtCols []string) bool {
	if len(srcCols) != len(tgtCols) {
		return false
	}
	for i, sc := range srcCols {
		tc := tgtCols[i]
		av := fmt.Sprintf("%v", src[sc])
		bv := fmt.Sprintf("%v", tgt[tc])
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
