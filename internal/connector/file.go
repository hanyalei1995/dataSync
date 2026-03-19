package connector

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xuri/excelize/v2"
)

// FileConnector 实现基于本地 CSV / Excel 文件的 Connector。
// 对于 CSV，table 参数忽略（文件即表）。
// 对于 Excel，table 参数为 sheet 名。
// Host 字段存储文件路径（复用 DataSource.Host）。
type FileConnector struct {
	filePath string
	fileType string // "csv" | "excel"
	// 全量缓存（文件较小，一次性载入）
	loadedTable string // 记录已缓存的 table/sheet 名，避免跨 sheet 误用旧缓存
	headers     []string
	data        [][]string
}

// NewFileConnector 按文件扩展名自动识别 CSV 或 Excel。
func NewFileConnector(filePath string) (*FileConnector, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	var fileType string
	switch ext {
	case ".csv":
		fileType = "csv"
	case ".xlsx", ".xls":
		fileType = "excel"
	default:
		return nil, fmt.Errorf("unsupported file type: %s", ext)
	}
	return &FileConnector{filePath: filePath, fileType: fileType}, nil
}

func (c *FileConnector) DBType() string { return c.fileType }

func (c *FileConnector) Ping(_ context.Context) error {
	_, err := os.Stat(c.filePath)
	return err
}

func (c *FileConnector) Close() error { return nil }

func (c *FileConnector) ListTables(_ context.Context) ([]string, error) {
	if c.fileType == "csv" {
		return []string{filepath.Base(c.filePath)}, nil
	}
	f, err := excelize.OpenFile(c.filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.GetSheetList(), nil
}

func (c *FileConnector) GetSchema(_ context.Context, table string) (*Schema, error) {
	if err := c.loadData(table); err != nil {
		return nil, err
	}
	cols := make([]ColumnInfo, len(c.headers))
	for i, h := range c.headers {
		cols[i] = ColumnInfo{Name: h, Type: "string", Nullable: true}
	}
	return &Schema{TableName: table, Columns: cols}, nil
}

func (c *FileConnector) CountRows(_ context.Context, table, _ string) (int64, error) {
	if err := c.loadData(table); err != nil {
		return 0, err
	}
	return int64(len(c.data)), nil
}

func (c *FileConnector) ReadBatch(_ context.Context, opts ReadOptions) ([]Row, error) {
	if err := c.loadData(opts.Table); err != nil {
		return nil, err
	}
	start := opts.Offset
	end := opts.Offset + opts.Limit
	if opts.Limit == 0 {
		end = int64(len(c.data))
	}
	if start >= int64(len(c.data)) {
		return nil, nil
	}
	if end > int64(len(c.data)) {
		end = int64(len(c.data))
	}
	slice := c.data[start:end]
	rows := make([]Row, len(slice))
	for i, record := range slice {
		row := make(Row, len(c.headers))
		for j, h := range c.headers {
			if j < len(record) {
				row[h] = record[j]
			}
		}
		rows[i] = row
	}
	return rows, nil
}

func (c *FileConnector) WriteBatch(_ context.Context, opts WriteOptions) error {
	cols := opts.Columns
	if len(cols) == 0 && len(opts.Rows) > 0 {
		for k := range opts.Rows[0] {
			cols = append(cols, k)
		}
		sort.Strings(cols) // map 遍历顺序不确定，排序保证列顺序稳定
	}
	if c.fileType == "csv" {
		return c.writeCSV(cols, opts.Rows)
	}
	return c.writeExcel(opts.Table, cols, opts.Rows)
}

func (c *FileConnector) writeCSV(cols []string, rows []Row) error {
	fileExists := true
	if _, err := os.Stat(c.filePath); os.IsNotExist(err) {
		fileExists = false
	}
	f, err := os.OpenFile(c.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if !fileExists {
		if err := w.Write(cols); err != nil {
			return err
		}
	}
	for _, row := range rows {
		record := make([]string, len(cols))
		for i, col := range cols {
			record[i] = fmt.Sprintf("%v", row[col])
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func (c *FileConnector) writeExcel(sheet string, cols []string, rows []Row) error {
	var f *excelize.File
	if _, err := os.Stat(c.filePath); os.IsNotExist(err) {
		f = excelize.NewFile()
		if sheet == "" {
			sheet = "Sheet1"
		} else {
			f.SetSheetName("Sheet1", sheet)
		}
	} else {
		var err error
		f, err = excelize.OpenFile(c.filePath)
		if err != nil {
			return err
		}
	}
	defer f.Close()

	existingRows, _ := f.GetRows(sheet)
	startRow := len(existingRows) + 1
	if startRow == 1 {
		for j, col := range cols {
			cell, _ := excelize.CoordinatesToCellName(j+1, 1)
			f.SetCellValue(sheet, cell, col)
		}
		startRow = 2
	}
	for i, row := range rows {
		for j, col := range cols {
			cell, _ := excelize.CoordinatesToCellName(j+1, startRow+i)
			f.SetCellValue(sheet, cell, row[col])
		}
	}
	return f.SaveAs(c.filePath)
}

func (c *FileConnector) loadData(table string) error {
	if c.headers != nil && c.loadedTable == table {
		return nil // 已缓存且 sheet 相同，直接复用
	}
	// 清空旧缓存
	c.headers = nil
	c.data = nil
	c.loadedTable = table
	if c.fileType == "csv" {
		return c.loadCSV()
	}
	return c.loadExcel(table)
}

func (c *FileConnector) loadCSV() error {
	f, err := os.Open(c.filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	c.headers = records[0]
	c.data = records[1:]
	return nil
}

func (c *FileConnector) loadExcel(sheet string) error {
	f, err := excelize.OpenFile(c.filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	if sheet == "" {
		sheets := f.GetSheetList()
		if len(sheets) == 0 {
			return fmt.Errorf("excel file has no sheets")
		}
		sheet = sheets[0]
	}
	rows, err := f.GetRows(sheet)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	c.headers = rows[0]
	c.data = rows[1:]
	return nil
}
