package engine

import (
	"regexp"
	"strings"
)

// typeMapping holds the cross-database type mapping tables for all 6 directions.
var typeMapping = map[string]map[string]map[string]string{
	"mysql": {
		"postgresql": {
			"INT":        "INTEGER",
			"INTEGER":    "INTEGER",
			"TINYINT":    "SMALLINT",
			"TINYINT(1)": "BOOLEAN",
			"SMALLINT":   "SMALLINT",
			"BIGINT":     "BIGINT",
			"DECIMAL":    "DECIMAL",
			"NUMERIC":    "NUMERIC",
			"FLOAT":      "REAL",
			"DOUBLE":     "DOUBLE PRECISION",
			"REAL":       "REAL",
			"VARCHAR":    "VARCHAR",
			"CHAR":       "CHAR",
			"TEXT":       "TEXT",
			"CLOB":       "TEXT",
			"BLOB":       "BYTEA",
			"BOOLEAN":    "BOOLEAN",
			"DATE":       "DATE",
			"TIME":       "TIME",
			"DATETIME":   "TIMESTAMP",
			"TIMESTAMP":  "TIMESTAMP",
			"JSON":       "JSON",
		},
		"oracle": {
			"INT":        "NUMBER(10)",
			"INTEGER":    "NUMBER(10)",
			"TINYINT":    "NUMBER(3)",
			"TINYINT(1)": "NUMBER(1)",
			"SMALLINT":   "NUMBER(5)",
			"BIGINT":     "NUMBER(19)",
			"DECIMAL":    "NUMBER",
			"NUMERIC":    "NUMBER",
			"FLOAT":      "FLOAT",
			"DOUBLE":     "FLOAT",
			"REAL":       "FLOAT",
			"VARCHAR":    "VARCHAR2",
			"CHAR":       "CHAR",
			"TEXT":       "CLOB",
			"CLOB":       "CLOB",
			"BLOB":       "BLOB",
			"BOOLEAN":    "NUMBER(1)",
			"DATE":       "DATE",
			"TIME":       "TIMESTAMP",
			"DATETIME":   "TIMESTAMP",
			"TIMESTAMP":  "TIMESTAMP",
			"JSON":       "CLOB",
		},
	},
	"postgresql": {
		"mysql": {
			"INTEGER":          "INT",
			"INT":              "INT",
			"SMALLINT":         "SMALLINT",
			"BIGINT":           "BIGINT",
			"DECIMAL":          "DECIMAL",
			"NUMERIC":          "DECIMAL",
			"REAL":             "FLOAT",
			"DOUBLE PRECISION": "DOUBLE",
			"VARCHAR":          "VARCHAR",
			"CHAR":             "CHAR",
			"TEXT":             "TEXT",
			"BYTEA":            "BLOB",
			"BOOLEAN":          "TINYINT(1)",
			"DATE":             "DATE",
			"TIME":             "TIME",
			"TIMESTAMP":        "DATETIME",
			"JSON":             "JSON",
			"SERIAL":           "INT AUTO_INCREMENT",
		},
		"oracle": {
			"INTEGER":          "NUMBER(10)",
			"INT":              "NUMBER(10)",
			"SMALLINT":         "NUMBER(5)",
			"BIGINT":           "NUMBER(19)",
			"DECIMAL":          "NUMBER",
			"NUMERIC":          "NUMBER",
			"REAL":             "FLOAT",
			"DOUBLE PRECISION": "FLOAT",
			"VARCHAR":          "VARCHAR2",
			"CHAR":             "CHAR",
			"TEXT":             "CLOB",
			"BYTEA":            "BLOB",
			"BOOLEAN":          "NUMBER(1)",
			"DATE":             "DATE",
			"TIME":             "TIMESTAMP",
			"TIMESTAMP":        "TIMESTAMP",
			"JSON":             "CLOB",
		},
	},
	"oracle": {
		"mysql": {
			"NUMBER":    "DECIMAL",
			"NUMBER(1)": "TINYINT(1)",
			"VARCHAR2":  "VARCHAR",
			"CHAR":      "CHAR",
			"CLOB":      "TEXT",
			"BLOB":      "BLOB",
			"DATE":      "DATETIME",
			"TIMESTAMP": "TIMESTAMP",
			"FLOAT":     "DOUBLE",
			"INTEGER":   "INT",
		},
		"postgresql": {
			"NUMBER":    "NUMERIC",
			"NUMBER(1)": "BOOLEAN",
			"VARCHAR2":  "VARCHAR",
			"CHAR":      "CHAR",
			"CLOB":      "TEXT",
			"BLOB":      "BYTEA",
			"DATE":      "TIMESTAMP",
			"TIMESTAMP": "TIMESTAMP",
			"FLOAT":     "DOUBLE PRECISION",
			"INTEGER":   "INTEGER",
		},
	},
}

// sizePattern matches type names with size specs like VARCHAR(100) or NUMBER(10,2).
var sizePattern = regexp.MustCompile(`^([A-Z0-9 ]+?)(\([\d,]+\))$`)

// MapType maps a column type from one database to another.
// It normalizes the input type to uppercase, tries exact match first,
// then tries matching without size info while preserving size where appropriate.
func MapType(fromDB, toDB, colType string) string {
	colType = strings.TrimSpace(colType)
	upper := strings.ToUpper(colType)

	fromMap, ok := typeMapping[strings.ToLower(fromDB)]
	if !ok {
		return upper
	}
	toMap, ok := fromMap[strings.ToLower(toDB)]
	if !ok {
		return upper
	}

	// Try exact match first (e.g. TINYINT(1), NUMBER(1))
	if mapped, ok := toMap[upper]; ok {
		return mapped
	}

	// Extract base type and size
	matches := sizePattern.FindStringSubmatch(upper)
	if matches != nil {
		baseType := strings.TrimSpace(matches[1])
		size := matches[2]

		// Try base type lookup
		if mapped, ok := toMap[baseType]; ok {
			// If the mapped type already contains size info (e.g. NUMBER(10)),
			// return it as-is without appending original size
			if strings.Contains(mapped, "(") {
				return mapped
			}
			// For types that naturally carry size (VARCHAR, CHAR, DECIMAL, NUMERIC),
			// preserve the original size
			mappedUpper := strings.ToUpper(mapped)
			if mappedUpper == "VARCHAR" || mappedUpper == "VARCHAR2" ||
				mappedUpper == "CHAR" || mappedUpper == "DECIMAL" ||
				mappedUpper == "NUMERIC" || mappedUpper == "NUMBER" {
				return mapped + size
			}
			return mapped
		}
	}

	// Try without size as a last resort
	if matches != nil {
		baseType := strings.TrimSpace(matches[1])
		if mapped, ok := toMap[baseType]; ok {
			return mapped
		}
	}

	// Unknown type, return as-is (uppercased)
	return upper
}
