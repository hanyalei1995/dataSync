package service

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExportFilePathAt_UsesTaskNameWhenBasePathEmpty(t *testing.T) {
	ts := time.Date(2026, 3, 20, 9, 8, 7, 0, time.UTC)

	got := exportFilePathAt("", "SMT备货单数据", "excel", ts)

	assert.Equal(t, filepath.Join(os.TempDir(), "SMT备货单数据-20260320-090807.xlsx"), got)
}

func TestExportFilePathAt_EmptyBasePathUsesTempDir(t *testing.T) {
	ts := time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC)

	got := exportFilePathAt("", "daily export", "csv", ts)

	assert.Equal(t, filepath.Join(os.TempDir(), "daily-export-20260323-100000.csv"), got)
}

func TestExportFilePathAt_UsesParentDirWhenBasePathLooksLikeFile(t *testing.T) {
	ts := time.Date(2026, 3, 20, 9, 8, 7, 0, time.UTC)

	got := exportFilePathAt("/tmp/exports/old.xlsx", "daily export", "csv", ts)

	assert.Equal(t, filepath.Join("/tmp/exports", "daily-export-20260320-090807.csv"), got)
}

func TestExportFilePathAt_ChangesAcrossRuns(t *testing.T) {
	first := exportFilePathAt("", "orders", "excel", time.Date(2026, 3, 20, 9, 8, 7, 0, time.UTC))
	second := exportFilePathAt("", "orders", "excel", time.Date(2026, 3, 20, 9, 8, 8, 0, time.UTC))

	assert.NotEqual(t, first, second)
}

func TestResolveDefaultFileTarget_ReturnsCsvDataSource(t *testing.T) {
	got := resolveDefaultFileTarget()

	assert.Equal(t, "csv", got.DBType)
	assert.Equal(t, "", got.Host)
}
