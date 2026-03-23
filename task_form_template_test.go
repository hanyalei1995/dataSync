package main

import (
	"os"
	"strings"
	"testing"
)

func TestTaskFormUsesTomSelectForSearchableDropdowns(t *testing.T) {
	content, err := os.ReadFile("templates/task/form.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	src := string(content)

	required := []string{
		"tom-select.bootstrap5.min.css",
		"tom-select.complete.min.js",
		"new TomSelect(",
	}
	for _, needle := range required {
		if !strings.Contains(src, needle) {
			t.Fatalf("expected task form template to include %q", needle)
		}
	}
}

func TestTaskFormPlacesSyncControlsAboveDatasourceSections(t *testing.T) {
	content, err := os.ReadFile("templates/task/form.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	src := string(content)

	nameIdx := strings.Index(src, "任务名称")
	syncTypeIdx := strings.Index(src, "同步类型")
	sourceSectionIdx := strings.Index(src, "源端配置")
	if nameIdx == -1 || syncTypeIdx == -1 || sourceSectionIdx == -1 {
		t.Fatalf("expected task form template to contain task name, sync type, and source section labels")
	}
	if !(nameIdx < syncTypeIdx && syncTypeIdx < sourceSectionIdx) {
		t.Fatalf("expected 同步类型 to appear between 任务名称 and 源端配置")
	}
}

func TestTaskFormIncludesSQLParamConfigSection(t *testing.T) {
	content, err := os.ReadFile("templates/task/form.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	src := string(content)

	required := []string{
		"SQL 参数配置",
		"id=\"sql_params_section\"",
		"id=\"sql_params_payload\"",
		"重新识别参数",
	}
	for _, needle := range required {
		if !strings.Contains(src, needle) {
			t.Fatalf("expected task form template to include %q", needle)
		}
	}
}

func TestTaskFormKeepsSQLParamEditorWideAndScrollable(t *testing.T) {
	content, err := os.ReadFile("templates/task/form.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	src := string(content)

	required := []string{
		"max-w-5xl",
		"id=\"sql_params_rows\" class=\"space-y-3\"",
		"grid gap-3 md:grid-cols-2 xl:grid-cols-12",
	}
	for _, needle := range required {
		if !strings.Contains(src, needle) {
			t.Fatalf("expected task form template to include %q", needle)
		}
	}
	forbidden := []string{
		"min-w-[1120px]",
		"<table class=\"min-w",
	}
	for _, needle := range forbidden {
		if strings.Contains(src, needle) {
			t.Fatalf("expected task form template not to include %q", needle)
		}
	}
}

func TestTaskFormLoadsDatasourceTablesForSearchableSelects(t *testing.T) {
	content, err := os.ReadFile("templates/task/form.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	src := string(content)

	required := []string{
		"function closeTomSelectDropdown(instance) {",
		"new TomSelect('#source_ds_id'",
		"new TomSelect('#target_ds_id'",
		"new TomSelect('#source_table'",
		"new TomSelect('#target_table'",
		"tsInstance.clearOptions();",
		"tsInstance.addOption({value: '', text: '请选择表'});",
	}
	for _, needle := range required {
		if !strings.Contains(src, needle) {
			t.Fatalf("expected task form template to include %q", needle)
		}
	}
}

func TestTaskFormKeepsDatasourceNativeSelectElements(t *testing.T) {
	content, err := os.ReadFile("templates/task/form.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	src := string(content)

	required := []string{
		"id=\"source_ds_id\"",
		"id=\"target_ds_id\"",
		"id=\"source_table\"",
		"id=\"target_table\"",
	}
	for _, needle := range required {
		if !strings.Contains(src, needle) {
			t.Fatalf("expected task form template to include %q", needle)
		}
	}
}

func TestTaskFormShowsOptionalHintForSQLImportTarget(t *testing.T) {
	content, err := os.ReadFile("templates/task/form.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	src := string(content)

	required := []string{
		"id=\"sql_import_target_hint\"",
		"留空则系统自动生成",
	}
	for _, needle := range required {
		if !strings.Contains(src, needle) {
			t.Fatalf("expected form to include %q", needle)
		}
	}
}
