package service

import (
	"datasync/internal/engine"
	"datasync/internal/model"

	"gorm.io/gorm"
)

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable string `json:"nullable"`
}

type DataSourceService struct {
	DB *gorm.DB
}

func (s *DataSourceService) List() ([]model.DataSource, error) {
	var list []model.DataSource
	err := s.DB.Order("id desc").Find(&list).Error
	return list, err
}

func (s *DataSourceService) GetByID(id uint) (*model.DataSource, error) {
	var ds model.DataSource
	err := s.DB.First(&ds, id).Error
	return &ds, err
}

func (s *DataSourceService) Create(ds *model.DataSource) error {
	return s.DB.Create(ds).Error
}

func (s *DataSourceService) Update(ds *model.DataSource) error {
	return s.DB.Save(ds).Error
}

func (s *DataSourceService) Delete(id uint) error {
	return s.DB.Delete(&model.DataSource{}, id).Error
}

func (s *DataSourceService) GetTables(ds model.DataSource) ([]string, error) {
	db, err := engine.Connect(ds)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var query string
	var args []interface{}

	switch ds.DBType {
	case "mysql":
		query = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?"
		args = append(args, ds.DatabaseName)
	case "postgresql":
		query = "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
	case "oracle":
		query = "SELECT TABLE_NAME FROM USER_TABLES"
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func (s *DataSourceService) GetColumns(ds model.DataSource, table string) ([]Column, error) {
	db, err := engine.Connect(ds)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var query string
	var args []interface{}

	switch ds.DBType {
	case "mysql":
		query = "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
		args = append(args, ds.DatabaseName, table)
	case "postgresql":
		query = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ?"
		args = append(args, table)
	case "oracle":
		query = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?"
		args = append(args, table)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		if err := rows.Scan(&col.Name, &col.Type, &col.Nullable); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, rows.Err()
}
