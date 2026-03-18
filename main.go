package main

import (
	"datasync/internal/config"
	"datasync/internal/database"
	"datasync/internal/handler"
	"datasync/internal/middleware"
	"datasync/internal/service"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

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

	r := gin.Default()
	r.LoadHTMLGlob("templates/**/*.html")

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
		protected.GET("/", func(c *gin.Context) {
			username, _ := c.Get("username")
			c.HTML(http.StatusOK, "dashboard", gin.H{
				"username": username,
			})
		})
		protected.GET("/logout", authHandler.Logout)

		// Datasource routes
		protected.GET("/datasources", dsHandler.List)
		protected.GET("/datasources/new", dsHandler.CreateForm)
		protected.POST("/datasources", dsHandler.Create)
		protected.GET("/datasources/:id/edit", dsHandler.EditForm)
		protected.POST("/datasources/:id", dsHandler.Update)
		protected.POST("/datasources/:id/delete", dsHandler.Delete)
	}

	// API routes
	api := r.Group("/api", middleware.AuthMiddleware(cfg.JWTSecret))
	{
		api.POST("/datasources/test", dsHandler.TestConn)
		api.GET("/datasources/:id/tables", dsHandler.Tables)
		api.GET("/datasources/:id/tables/:table/columns", dsHandler.Columns)
	}

	fmt.Printf("DataSync server starting on :%d\n", cfg.Port)
	if err := r.Run(fmt.Sprintf(":%d", cfg.Port)); err != nil {
		log.Fatal("failed to start server:", err)
	}
}
