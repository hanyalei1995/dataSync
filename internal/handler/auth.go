package handler

import (
	"datasync/internal/middleware"
	"datasync/internal/service"
	"net/http"

	"github.com/gin-gonic/gin"
)

type AuthHandler struct {
	UserService *service.UserService
	JWTSecret   string
}

func (h *AuthHandler) LoginPage(c *gin.Context) {
	c.HTML(http.StatusOK, "login.html", gin.H{
		"error": "",
	})
}

func (h *AuthHandler) Login(c *gin.Context) {
	username := c.PostForm("username")
	password := c.PostForm("password")

	user, err := h.UserService.Authenticate(username, password)
	if err != nil {
		c.HTML(http.StatusOK, "login.html", gin.H{
			"error": "Invalid username or password",
		})
		return
	}

	token, err := middleware.GenerateToken(h.JWTSecret, user.ID, user.Username)
	if err != nil {
		c.HTML(http.StatusInternalServerError, "login.html", gin.H{
			"error": "Internal server error",
		})
		return
	}

	c.SetCookie("token", token, 86400, "/", "", false, true)
	c.Redirect(http.StatusFound, "/")
}

func (h *AuthHandler) Logout(c *gin.Context) {
	c.SetCookie("token", "", -1, "/", "", false, true)
	c.Redirect(http.StatusFound, "/login")
}
