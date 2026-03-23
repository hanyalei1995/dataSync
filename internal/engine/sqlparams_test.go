package engine

import (
	"datasync/internal/model"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanSQLParams_DetectsUniqueNamesInOrder(t *testing.T) {
	sql := "select * from orders where created_at >= {{start_date}} and shop_type = {{shop_type}} or backup_type = {{shop_type}}"

	got := ScanSQLParams(sql)

	assert.Equal(t, []string{"start_date", "shop_type"}, got)
}

func TestCompileSQLTemplate_UsesDriverSpecificPlaceholders(t *testing.T) {
	values := map[string]any{
		"start_date": "2026-03-01",
		"shop_type":  "Shopee",
	}
	template := "select * from orders where created_at >= {{start_date}} and shop_type = {{shop_type}} or backup_type = {{shop_type}}"

	tests := []struct {
		name     string
		dbType   string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "mysql",
			dbType:   "mysql",
			wantSQL:  "select * from orders where created_at >= ? and shop_type = ? or backup_type = ?",
			wantArgs: []any{"2026-03-01", "Shopee", "Shopee"},
		},
		{
			name:     "postgresql",
			dbType:   "postgresql",
			wantSQL:  "select * from orders where created_at >= $1 and shop_type = $2 or backup_type = $3",
			wantArgs: []any{"2026-03-01", "Shopee", "Shopee"},
		},
		{
			name:     "oracle",
			dbType:   "oracle",
			wantSQL:  "select * from orders where created_at >= :1 and shop_type = :2 or backup_type = :3",
			wantArgs: []any{"2026-03-01", "Shopee", "Shopee"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotArgs, err := CompileSQLTemplate(tt.dbType, template, values)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, gotSQL)
			assert.Equal(t, tt.wantArgs, gotArgs)
		})
	}
}

func TestCompileSQLTemplate_InlinesPlaceholdersInsideQuotedStrings(t *testing.T) {
	values := map[string]any{
		"name1":     "20260301",
		"shop_type": "Shopee",
	}
	template := "select * from orders where shop_type = {{shop_type}} and expresstime between to_date('{{name1}} 00:00:00','yyyymmdd hh24:mi:ss') and to_date('{{name1}} 23:59:59','yyyymmdd hh24:mi:ss')"

	gotSQL, gotArgs, err := CompileSQLTemplate("oracle", template, values)

	require.NoError(t, err)
	assert.Equal(t, "select * from orders where shop_type = :1 and expresstime between to_date('20260301 00:00:00','yyyymmdd hh24:mi:ss') and to_date('20260301 23:59:59','yyyymmdd hh24:mi:ss')", gotSQL)
	assert.Equal(t, []any{"Shopee"}, gotArgs)
}

func TestResolveSQLParamValues_UsesDefaultsAndValidatesTypes(t *testing.T) {
	defs := []model.SQLParamDefinition{
		{
			Name:         "start_date",
			Label:        "开始日期",
			InputType:    "date",
			Required:     true,
			DefaultValue: "2026-03-01",
		},
		{
			Name:      "limit",
			Label:     "限制条数",
			InputType: "number",
			Required:  true,
		},
		{
			Name:      "shop_type",
			Label:     "店铺类型",
			InputType: "select",
			Options: []model.SQLParamOption{
				{Label: "Shopee", Value: "Shopee"},
				{Label: "Lazada", Value: "Lazada"},
			},
		},
	}

	values, err := ResolveSQLParamValues(defs, map[string]string{
		"limit":     "50",
		"shop_type": "Shopee",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(50), values["limit"])
	assert.Equal(t, "Shopee", values["shop_type"])
	_, ok := values["start_date"].(time.Time)
	assert.True(t, ok)
}

func TestResolveSQLParamValues_RejectsInvalidSelectValue(t *testing.T) {
	defs := []model.SQLParamDefinition{
		{
			Name:      "shop_type",
			Label:     "店铺类型",
			InputType: "select",
			Required:  true,
			Options: []model.SQLParamOption{
				{Label: "Shopee", Value: "Shopee"},
			},
		},
	}

	_, err := ResolveSQLParamValues(defs, map[string]string{"shop_type": "Amazon"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "shop_type")
}

func TestNormalizeSQLParamDefinitions_AutoCreatesDefinitionsForDetectedParams(t *testing.T) {
	query := "select * from orders where shop_type = {{shop_type}} and status = {{status}}"

	result, err := NormalizeSQLParamDefinitions(query, nil)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, "shop_type", result[0].Name)
	assert.Equal(t, "text", result[0].InputType)
	assert.Equal(t, "status", result[1].Name)
	assert.True(t, result[0].Required)
}

func TestNormalizeSQLParamDefinitions_IgnoresParamsInsideLineComments(t *testing.T) {
	query := "select * from orders\n-- where shop_type = {{shop_type}}\nwhere status = {{status}}"

	result, err := NormalizeSQLParamDefinitions(query, nil)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "status", result[0].Name)
}

func TestNormalizeSQLParamDefinitions_IgnoresParamsInsideBlockComments(t *testing.T) {
	query := "select * from orders /* {{shop_type}} */ where status = {{status}}"

	result, err := NormalizeSQLParamDefinitions(query, nil)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "status", result[0].Name)
}

func TestNormalizeSQLParamDefinitions_ErrorsOnUnusedExplicitDefinition(t *testing.T) {
	query := "select * from orders where status = {{status}}"
	defs := []model.SQLParamDefinition{
		{Name: "shop_type", InputType: "text"},
	}

	_, err := NormalizeSQLParamDefinitions(query, defs)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "shop_type")
}

func TestNormalizeSQLParamDefinitions_SortsByOrderField(t *testing.T) {
	query := "select * from t where a = {{alpha}} and b = {{beta}}"
	defs := []model.SQLParamDefinition{
		{Name: "alpha", InputType: "text", Order: 2},
		{Name: "beta", InputType: "text", Order: 1},
	}

	result, err := NormalizeSQLParamDefinitions(query, defs)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, "beta", result[0].Name)
	assert.Equal(t, "alpha", result[1].Name)
}
