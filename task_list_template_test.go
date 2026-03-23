package main

import (
	"os"
	"strings"
	"testing"
)

func TestTaskListShowsResultsLinkForSQLImportTasks(t *testing.T) {
	content, err := os.ReadFile("templates/task/list.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	src := string(content)

	required := []string{
		"resultsEligibleIDs",
		"/tasks/{{.ID}}/results",
		"查看结果",
	}
	for _, needle := range required {
		if !strings.Contains(src, needle) {
			t.Fatalf("expected task list template to include %q", needle)
		}
	}
}

func TestTaskListIncludesRunParamModalHooks(t *testing.T) {
	content, err := os.ReadFile("templates/task/list.html")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	src := string(content)

	required := []string{
		"id=\"run-param-modal\"",
		"data-has-params=",
		"/api/tasks/' + taskID + '/run-params",
		"function openRunParamModal",
	}
	for _, needle := range required {
		if !strings.Contains(src, needle) {
			t.Fatalf("expected task list template to include %q", needle)
		}
	}
}
