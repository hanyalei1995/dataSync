package service

import (
	"context"
	"datasync/internal/connector"
	"datasync/internal/engine"
	"datasync/internal/model"
	"encoding/json"
	"fmt"
	"strings"
)

type SQLResultsService struct {
	TaskSvc *TaskService
	DSSvc   *DataSourceService
	Pool    *ConnPool
}

type SQLResultsTaskContext struct {
	Task       *model.SyncTask
	SourceDS   model.DataSource
	TargetDS   model.DataSource
	SourceName string
	TargetName string
	ParamDefs  []model.SQLParamDefinition
}

func (s *SQLResultsService) LoadTaskContext(taskID uint) (*SQLResultsTaskContext, error) {
	task, err := s.TaskSvc.GetByID(taskID)
	if err != nil {
		return nil, err
	}
	if task.SyncType != "sql_import" {
		return nil, fmt.Errorf("结果页仅支持目标为 CSV/Excel 的 SQL 导入任务")
	}

	sourceDS, err := s.resolveDataSource(task.SourceDSID, task.SourceConfig)
	if err != nil {
		return nil, fmt.Errorf("加载源数据源失败: %w", err)
	}
	var targetDS model.DataSource
	if (task.TargetDSID != nil && *task.TargetDSID > 0) || task.TargetConfig != "" {
		targetDS, err = s.resolveDataSource(task.TargetDSID, task.TargetConfig)
		if err != nil {
			return nil, fmt.Errorf("加载目标数据源失败: %w", err)
		}
	} else {
		targetDS = resolveDefaultFileTarget()
	}

	if !IsSQLResultsEligible(task, targetDS) {
		return nil, fmt.Errorf("结果页仅支持目标为 CSV/Excel 的 SQL 导入任务")
	}

	paramDefs, err := ParseEffectiveSQLParams(task)
	if err != nil {
		return nil, err
	}

	sourceName := sourceDS.Name
	if strings.TrimSpace(sourceName) == "" {
		sourceName = "临时连接"
	}
	targetName := targetDS.Name
	if strings.TrimSpace(targetName) == "" {
		targetName = "临时连接"
	}

	return &SQLResultsTaskContext{
		Task:       task,
		SourceDS:   sourceDS,
		TargetDS:   targetDS,
		SourceName: sourceName,
		TargetName: targetName,
		ParamDefs:  paramDefs,
	}, nil
}

func IsSQLResultsEligible(task *model.SyncTask, targetDS model.DataSource) bool {
	if task == nil || task.SyncType != "sql_import" {
		return false
	}
	dbType := strings.ToLower(targetDS.DBType)
	return dbType == "csv" || dbType == "excel" || dbType == ""
}

func (s *SQLResultsService) Preview(ctx context.Context, taskID uint, runtimeParams map[string]string, page, pageSize int) (*engine.SQLPreviewResult, error) {
	taskCtx, err := s.LoadTaskContext(taskID)
	if err != nil {
		return nil, err
	}

	sourceConn, sourceSQL, sourceArgs, err := s.prepareSourceQuery(taskCtx, runtimeParams)
	if err != nil {
		return nil, err
	}

	return engine.PreviewSQL(ctx, sourceConn, sourceSQL, sourceArgs, page, pageSize)
}

func (s *SQLResultsService) Export(ctx context.Context, taskID uint, runtimeParams map[string]string, format string) (string, error) {
	taskCtx, err := s.LoadTaskContext(taskID)
	if err != nil {
		return "", err
	}

	format = strings.ToLower(strings.TrimSpace(format))
	if format != "csv" && format != "excel" {
		return "", fmt.Errorf("不支持的导出格式: %s", format)
	}

	sourceConn, sourceSQL, sourceArgs, err := s.prepareSourceQuery(taskCtx, runtimeParams)
	if err != nil {
		return "", err
	}

	filePath := exportFilePath(taskCtx.TargetDS.Host, taskCtx.Task.Name+"-preview", format)
	targetConn, err := connector.NewFileConnectorWithType(filePath, format)
	if err != nil {
		return "", err
	}
	defer targetConn.Close()

	targetTable := taskCtx.Task.TargetTable
	if targetTable == "" && format == "excel" {
		targetTable = "Sheet1"
	}

	_, err = engine.SyncData(ctx, engine.DataSyncOptions{
		Source:        sourceConn,
		Target:        targetConn,
		SourceSQL:     sourceSQL,
		SourceSQLArgs: sourceArgs,
		TargetTable:   targetTable,
		BatchSize:     1000,
		WriteStrategy: "insert",
	})
	if err != nil {
		return "", err
	}

	return targetConn.FilePath(), nil
}

func (s *SQLResultsService) prepareSourceQuery(taskCtx *SQLResultsTaskContext, runtimeParams map[string]string) (connector.Connector, string, []any, error) {
	sourceConn, err := s.Pool.Get(taskCtx.SourceDS)
	if err != nil {
		return nil, "", nil, fmt.Errorf("连接源数据源失败: %w", err)
	}

	sourceSQL := taskCtx.Task.SourceSQL
	var sourceArgs []any
	if len(taskCtx.ParamDefs) > 0 {
		values, err := engine.ResolveSQLParamValues(taskCtx.ParamDefs, runtimeParams)
		if err != nil {
			return nil, "", nil, err
		}
		sourceSQL, sourceArgs, err = engine.CompileSQLTemplate(sourceConn.DBType(), sourceSQL, values)
		if err != nil {
			return nil, "", nil, err
		}
	}
	return sourceConn, sourceSQL, sourceArgs, nil
}

func (s *SQLResultsService) resolveDataSource(dsID *uint, configJSON string) (model.DataSource, error) {
	if dsID != nil && *dsID > 0 {
		ds, err := s.DSSvc.GetByID(*dsID)
		if err != nil {
			return model.DataSource{}, err
		}
		return *ds, nil
	}
	if configJSON != "" {
		var ds model.DataSource
		if err := json.Unmarshal([]byte(configJSON), &ds); err != nil {
			return model.DataSource{}, fmt.Errorf("解析数据源配置失败: %w", err)
		}
		return ds, nil
	}
	return model.DataSource{}, fmt.Errorf("缺少数据源配置")
}
