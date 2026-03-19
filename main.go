package main

import (
	"context"
	"datasync/internal/config"
	"datasync/internal/database"
	"datasync/internal/handler"
	"datasync/internal/middleware"
	"datasync/internal/service"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

//go:embed templates
var templatesFS embed.FS

func main() {
	cfg := config.Default()
	db, err := database.Init(cfg)
	if err != nil {
		log.Fatal("failed to init database:", err)
	}

	userSvc := &service.UserService{DB: db}
	if err := userSvc.EnsureAdmin(cfg.AdminUser, cfg.AdminPass); err != nil {
		log.Fatal("failed to ensure admin user:", err)
	}

	dsSvc := &service.DataSourceService{DB: db}
	dsHandler := &handler.DataSourceHandler{Service: dsSvc}

	taskSvc := &service.TaskService{DB: db}
	pool := service.NewConnPool()
	executor := &service.Executor{
		DB:      db,
		DSSvc:   dsSvc,
		TaskSvc: taskSvc,
		Pool:    pool,
	}
	scheduler := service.NewScheduler(db, executor)
	scheduler.Start()

	taskHandler := &handler.TaskHandler{
		TaskService:       taskSvc,
		DataSourceService: dsSvc,
		Executor:          executor,
		Scheduler:         scheduler,
	}
	logHandler := &handler.LogHandler{DB: db}
	dashboardHandler := &handler.DashboardHandler{DB: db}

	r := gin.Default()

	// Register custom template functions and load embedded templates
	funcMap := template.FuncMap{
		"deref": func(p *uint) uint {
			if p != nil {
				return *p
			}
			return 0
		},
	}
	var htmlFiles []string
	fs.WalkDir(templatesFS, "templates", func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".html") {
			htmlFiles = append(htmlFiles, path)
		}
		return nil
	})
	tmpl := template.Must(template.New("").Funcs(funcMap).ParseFS(templatesFS, htmlFiles...))
	r.SetHTMLTemplate(tmpl)

	authHandler := &handler.AuthHandler{
		UserService: userSvc,
		JWTSecret:   cfg.JWTSecret,
	}

	// Public routes
	r.GET("/login", authHandler.LoginPage)
	r.POST("/login", authHandler.Login)

	// Protected routes
	protected := r.Group("/", middleware.AuthMiddleware(cfg.JWTSecret))
	{
		protected.GET("/", dashboardHandler.Index)
		protected.GET("/logout", authHandler.Logout)

		// Datasource routes
		protected.GET("/datasources", dsHandler.List)
		protected.GET("/datasources/new", dsHandler.CreateForm)
		protected.POST("/datasources", dsHandler.Create)
		protected.GET("/datasources/:id/edit", dsHandler.EditForm)
		protected.POST("/datasources/:id", dsHandler.Update)
		protected.POST("/datasources/:id/delete", dsHandler.Delete)

		// Task routes (register /tasks/new BEFORE /tasks/:id to avoid route conflict)
		protected.GET("/tasks", taskHandler.List)
		protected.GET("/tasks/new", taskHandler.CreateForm)
		protected.POST("/tasks", taskHandler.Create)
		protected.GET("/tasks/:id", taskHandler.Detail)
		protected.GET("/tasks/:id/edit", taskHandler.EditForm)
		protected.POST("/tasks/:id", taskHandler.Update)
		protected.POST("/tasks/:id/delete", taskHandler.Delete)
		protected.POST("/tasks/:id/run", taskHandler.Run)
		protected.POST("/tasks/:id/stop", taskHandler.Stop)

		// Log routes
		protected.GET("/logs", logHandler.List)
	}

	// API routes
	api := r.Group("/api", middleware.AuthMiddleware(cfg.JWTSecret))
	{
		api.POST("/datasources/test", dsHandler.TestConn)
		api.GET("/datasources/:id/tables", dsHandler.Tables)
		api.GET("/datasources/:id/tables/:table/columns", dsHandler.Columns)

		// Task API routes
		api.GET("/tasks/:id/mappings", taskHandler.Mappings)
		api.PUT("/tasks/:id/mappings", taskHandler.SaveMappings)
		api.GET("/tasks/:id/progress", taskHandler.ProgressSnapshot)
		api.GET("/tasks/:id/progress/stream", taskHandler.ProgressStream)
		api.POST("/tasks/:id/verify", taskHandler.Verify)
		api.GET("/tasks/:id/logs", logHandler.TaskLogs)
	}

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("failed to start server:", err)
		}
	}()

	fmt.Printf("DataSync server started on :%d\n", cfg.Port)

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	scheduler.Stop()
	pool.CloseAll()
	if executor.CDCManager != nil {
		executor.CDCManager.StopAll()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	log.Println("Server exited")
}
