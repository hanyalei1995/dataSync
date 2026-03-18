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
	}

	fmt.Printf("DataSync server starting on :%d\n", cfg.Port)
	if err := r.Run(fmt.Sprintf(":%d", cfg.Port)); err != nil {
		log.Fatal("failed to start server:", err)
	}
}
