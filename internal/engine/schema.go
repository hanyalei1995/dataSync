package engine

import (
	"context"
	"database/sql"
	"datasync/internal/connector"
	"datasync/internal/model"
	"fmt"
	"strings"
)

// ColumnSchema represents a single column in a table.
type ColumnSchema struct {
	Name       string
	Type       string // raw type from source DB
	Nullable   bool
	DefaultVal string
	IsPrimary  bool
}

// TableSchema represents the schema of a table.
type TableSchema struct {
	TableName    string
	SourceDBType string // "mysql", "postgresql", "oracle"
	Columns      []ColumnSchema
}

// SchemaDiff represents a difference between source and target schemas.
type SchemaDiff struct {
	Action string // "add", "modify", "drop"
	Column ColumnSchema
}

// connectorSchemaToTableSchema converts a *connector.Schema to the internal *TableSchema.
func connectorSchemaToTableSchema(s *connector.Schema, dbType string) *TableSchema {
	ts := &TableSchema{
		TableName:    s.TableName,
		SourceDBType: dbType,
		Columns:      make([]ColumnSchema, 0, len(s.Columns)),
	}
	for _, c := range s.Columns {
		ts.Columns = append(ts.Columns, ColumnSchema{
			Name:      c.Name,
			Type:      c.Type,
			Nullable:  c.Nullable,
			IsPrimary: c.IsPrimary,
		})
	}
	return ts
}

// rawDBFromConnector tries to extract the underlying *sql.DB from a Connector.
// Returns an error if the connector does not expose a raw DB.
func rawDBFromConnector(c connector.Connector) (*sql.DB, error) {
	type rawDBer interface {
		RawDB() *sql.DB
	}
	if r, ok := c.(rawDBer); ok {
		return r.RawDB(), nil
	}
	return nil, fmt.Errorf("connector of type %T does not support RawDB(); cannot execute DDL", c)
}

// ReadTableSchema reads the table structure from the database.
// Kept for backward-compatibility with data.go and other callers.
func ReadTableSchema(db *sql.DB, dbType, table string) (*TableSchema, error) {
	switch strings.ToLower(dbType) {
	case "mysql":
		return readMySQLSchema(db, table)
	case "postgresql":
		return readPostgreSQLSchema(db, table)
	case "oracle":
		return readOracleSchema(db, table)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

func readMySQLSchema(db *sql.DB, table string) (*TableSchema, error) {
	query := `
		SELECT
			c.COLUMN_NAME,
			c.COLUMN_TYPE,
			c.IS_NULLABLE,
			COALESCE(c.COLUMN_DEFAULT, ''),
			CASE WHEN c.COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END
		FROM information_schema.COLUMNS c
		WHERE c.TABLE_NAME = ?
		ORDER BY c.ORDINAL_POSITION`

	rows, err := db.Query(query, table)
	if err != nil {
		return nil, fmt.Errorf("query mysql schema: %w", err)
	}
	defer rows.Close()

	schema := &TableSchema{TableName: table, SourceDBType: "mysql"}
	for rows.Next() {
		var col ColumnSchema
		var nullable string
		var isPrimary int
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &col.DefaultVal, &isPrimary); err != nil {
			return nil, fmt.Errorf("scan mysql column: %w", err)
		}
		col.Nullable = (nullable == "YES")
		col.IsPrimary = (isPrimary == 1)
		schema.Columns = append(schema.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schema, nil
}

func readPostgreSQLSchema(db *sql.DB, table string) (*TableSchema, error) {
	query := `
		SELECT
			c.column_name,
			c.data_type,
			c.is_nullable,
			COALESCE(c.column_default, ''),
			CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN 1 ELSE 0 END
		FROM information_schema.columns c
		LEFT JOIN information_schema.key_column_usage kcu
			ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name
		LEFT JOIN information_schema.table_constraints tc
			ON kcu.constraint_name = tc.constraint_name
			AND tc.constraint_type = 'PRIMARY KEY'
		WHERE c.table_name = $1
		ORDER BY c.ordinal_position`

	rows, err := db.Query(query, table)
	if err != nil {
		return nil, fmt.Errorf("query postgresql schema: %w", err)
	}
	defer rows.Close()

	schema := &TableSchema{TableName: table, SourceDBType: "postgresql"}
	for rows.Next() {
		var col ColumnSchema
		var nullable string
		var isPrimary int
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &col.DefaultVal, &isPrimary); err != nil {
			return nil, fmt.Errorf("scan postgresql column: %w", err)
		}
		col.Nullable = (nullable == "YES")
		col.IsPrimary = (isPrimary == 1)
		schema.Columns = append(schema.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schema, nil
}

func readOracleSchema(db *sql.DB, table string) (*TableSchema, error) {
	query := `
		SELECT
			utc.COLUMN_NAME,
			utc.DATA_TYPE,
			utc.NULLABLE,
			COALESCE(utc.DATA_DEFAULT, ''),
			CASE WHEN uc.CONSTRAINT_TYPE = 'P' THEN 1 ELSE 0 END
		FROM USER_TAB_COLUMNS utc
		LEFT JOIN (
			SELECT ucc.TABLE_NAME, ucc.COLUMN_NAME, uc2.CONSTRAINT_TYPE
			FROM USER_CONS_COLUMNS ucc
			JOIN USER_CONSTRAINTS uc2 ON ucc.CONSTRAINT_NAME = uc2.CONSTRAINT_NAME
			WHERE uc2.CONSTRAINT_TYPE = 'P'
		) uc ON utc.TABLE_NAME = uc.TABLE_NAME AND utc.COLUMN_NAME = uc.COLUMN_NAME
		WHERE utc.TABLE_NAME = :1
		ORDER BY utc.COLUMN_ID`

	rows, err := db.Query(query, strings.ToUpper(table))
	if err != nil {
		return nil, fmt.Errorf("query oracle schema: %w", err)
	}
	defer rows.Close()

	schema := &TableSchema{TableName: table, SourceDBType: "oracle"}
	for rows.Next() {
		var col ColumnSchema
		var nullable string
		var isPrimary int
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &col.DefaultVal, &isPrimary); err != nil {
			return nil, fmt.Errorf("scan oracle column: %w", err)
		}
		col.Nullable = (nullable == "Y")
		col.IsPrimary = (isPrimary == 1)
		schema.Columns = append(schema.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schema, nil
}

// GenerateCreateSQL generates a CREATE TABLE SQL for the target database.
// It applies field mappings to rename columns and skip disabled fields,
// and converts types using MapType.
func GenerateCreateSQL(schema *TableSchema, targetDBType, targetTable string, mappings []model.FieldMapping) string {
	// Build mapping lookups
	mappingBySource := make(map[string]model.FieldMapping)
	for _, m := range mappings {
		mappingBySource[m.SourceField] = m
	}

	var colDefs []string
	var pkCols []string

	for _, col := range schema.Columns {
		targetColName := col.Name

		if m, ok := mappingBySource[col.Name]; ok {
			if !m.Enabled {
				continue
			}
			if m.TargetField != "" {
				targetColName = m.TargetField
			}
		}

		mappedType := MapType(schema.SourceDBType, targetDBType, col.Type)
		colDef := quoteIdentifier(targetDBType, targetColName) + " " + mappedType

		if !col.Nullable && !col.IsPrimary {
			colDef += " NOT NULL"
		}

		if col.DefaultVal != "" {
			colDef += " DEFAULT " + col.DefaultVal
		}

		colDefs = append(colDefs, colDef)

		if col.IsPrimary {
			pkCols = append(pkCols, quoteIdentifier(targetDBType, targetColName))
		}
	}

	if len(pkCols) > 0 {
		colDefs = append(colDefs, "PRIMARY KEY ("+strings.Join(pkCols, ", ")+")")
	}

	return fmt.Sprintf("CREATE TABLE %s (\n  %s\n)",
		quoteIdentifier(targetDBType, targetTable),
		strings.Join(colDefs, ",\n  "))
}

// CompareSchema compares source and target schemas and returns diffs.
func CompareSchema(source, target *TableSchema) []SchemaDiff {
	targetCols := make(map[string]ColumnSchema)
	for _, col := range target.Columns {
		targetCols[strings.ToLower(col.Name)] = col
	}

	sourceCols := make(map[string]bool)
	var diffs []SchemaDiff

	for _, srcCol := range source.Columns {
		sourceCols[strings.ToLower(srcCol.Name)] = true
		if tgtCol, exists := targetCols[strings.ToLower(srcCol.Name)]; exists {
			// Column exists in both — check if type differs
			if !strings.EqualFold(srcCol.Type, tgtCol.Type) {
				diffs = append(diffs, SchemaDiff{
					Action: "modify",
					Column: srcCol,
				})
			}
		} else {
			// Column in source but not in target
			diffs = append(diffs, SchemaDiff{
				Action: "add",
				Column: srcCol,
			})
		}
	}

	// Columns in target but not in source — conservative, mark as drop
	for _, tgtCol := range target.Columns {
		if !sourceCols[strings.ToLower(tgtCol.Name)] {
			diffs = append(diffs, SchemaDiff{
				Action: "drop",
				Column: tgtCol,
			})
		}
	}

	return diffs
}

// GenerateAlterSQL generates ALTER TABLE statements for each diff.
func GenerateAlterSQL(diffs []SchemaDiff, targetDBType, targetTable string) []string {
	var stmts []string
	tbl := quoteIdentifier(targetDBType, targetTable)

	for _, diff := range diffs {
		col := quoteIdentifier(targetDBType, diff.Column.Name)
		colType := diff.Column.Type

		switch diff.Action {
		case "add":
			nullClause := ""
			if !diff.Column.Nullable {
				nullClause = " NOT NULL"
			}
			switch strings.ToLower(targetDBType) {
			case "oracle":
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ADD (%s %s%s)", tbl, col, colType, nullClause))
			default:
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s%s", tbl, col, colType, nullClause))
			}

		case "modify":
			switch strings.ToLower(targetDBType) {
			case "mysql":
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", tbl, col, colType))
			case "postgresql":
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", tbl, col, colType))
			case "oracle":
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s MODIFY (%s %s)", tbl, col, colType))
			}

		case "drop":
			switch strings.ToLower(targetDBType) {
			case "oracle":
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s DROP (%s)", tbl, col))
			default:
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tbl, col))
			}
		}
	}

	return stmts
}

// SyncStructure is the main entry point for synchronizing table structure.
// It uses connector.Connector for schema introspection and falls back to the
// raw *sql.DB (via RawDB()) for DDL execution.
func SyncStructure(src, dst connector.Connector, sourceTable, targetTable string, mappings []model.FieldMapping) error {
	ctx := context.Background()
	targetType := dst.DBType()

	// 1. Read source schema via Connector.GetSchema
	connSrcSchema, err := src.GetSchema(ctx, sourceTable)
	if err != nil {
		return fmt.Errorf("read source schema: %w", err)
	}
	srcSchema := connectorSchemaToTableSchema(connSrcSchema, src.DBType())

	// 2. Obtain raw *sql.DB from target connector for DDL execution
	targetRawDB, err := rawDBFromConnector(dst)
	if err != nil {
		return err
	}

	// 3. Try to read target schema via Connector.GetSchema
	connTgtSchema, tgtErr := dst.GetSchema(ctx, targetTable)
	if tgtErr != nil || len(connTgtSchema.Columns) == 0 {
		// 4. Target doesn't exist — create it
		createSQL := GenerateCreateSQL(srcSchema, targetType, targetTable, mappings)
		if _, execErr := targetRawDB.Exec(createSQL); execErr != nil {
			return fmt.Errorf("create target table: %w", execErr)
		}
		return nil
	}

	tgtSchema := connectorSchemaToTableSchema(connTgtSchema, targetType)

	// 5. Target exists — compare and alter
	diffs := CompareSchema(srcSchema, tgtSchema)
	if len(diffs) == 0 {
		return nil
	}

	alterStmts := GenerateAlterSQL(diffs, targetType, targetTable)
	for _, stmt := range alterStmts {
		if _, execErr := targetRawDB.Exec(stmt); execErr != nil {
			return fmt.Errorf("alter target table: %w (SQL: %s)", execErr, stmt)
		}
	}

	return nil
}

// quoteIdentifier quotes an identifier based on the target DB type.
func quoteIdentifier(dbType, name string) string {
	switch strings.ToLower(dbType) {
	case "mysql":
		return "`" + name + "`"
	case "postgresql":
		return "\"" + name + "\""
	case "oracle":
		return "\"" + strings.ToUpper(name) + "\""
	default:
		return name
	}
}
