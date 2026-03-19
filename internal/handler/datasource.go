package handler

import (
	"datasync/internal/connector"
	"datasync/internal/model"
	"datasync/internal/service"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
)

type DataSourceHandler struct {
	Service *service.DataSourceService
}

func (h *DataSourceHandler) List(c *gin.Context) {
	list, err := h.Service.List()
	if err != nil {
		c.HTML(http.StatusInternalServerError, "datasource_list", gin.H{"error": err.Error()})
		return
	}
	username, _ := c.Get("username")
	c.HTML(http.StatusOK, "datasource_list", gin.H{
		"datasources": list,
		"username":    username,
	})
}

func (h *DataSourceHandler) CreateForm(c *gin.Context) {
	username, _ := c.Get("username")
	c.HTML(http.StatusOK, "datasource_form", gin.H{
		"username": username,
	})
}

func (h *DataSourceHandler) Create(c *gin.Context) {
	port, _ := strconv.Atoi(c.PostForm("port"))
	ds := &model.DataSource{
		Name:         c.PostForm("name"),
		DBType:       c.PostForm("db_type"),
		Host:         c.PostForm("host"),
		Port:         port,
		Username:     c.PostForm("username"),
		Password:     c.PostForm("password"),
		DatabaseName: c.PostForm("database_name"),
		ExtraParams:  c.PostForm("extra_params"),
	}
	if uid, exists := c.Get("userID"); exists {
		ds.CreatedBy = uid.(uint)
	}
	if err := h.Service.Create(ds); err != nil {
		username, _ := c.Get("username")
		c.HTML(http.StatusOK, "datasource_form", gin.H{
			"error":      err.Error(),
			"datasource": ds,
			"username":   username,
		})
		return
	}
	c.Redirect(http.StatusFound, "/datasources")
}

func (h *DataSourceHandler) EditForm(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	ds, err := h.Service.GetByID(uint(id))
	if err != nil {
		c.Redirect(http.StatusFound, "/datasources")
		return
	}
	username, _ := c.Get("username")
	c.HTML(http.StatusOK, "datasource_form", gin.H{
		"datasource": ds,
		"username":   username,
	})
}

func (h *DataSourceHandler) Update(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	ds, err := h.Service.GetByID(uint(id))
	if err != nil {
		c.Redirect(http.StatusFound, "/datasources")
		return
	}
	port, _ := strconv.Atoi(c.PostForm("port"))
	ds.Name = c.PostForm("name")
	ds.DBType = c.PostForm("db_type")
	ds.Host = c.PostForm("host")
	ds.Port = port
	ds.Username = c.PostForm("username")
	ds.Password = c.PostForm("password")
	ds.DatabaseName = c.PostForm("database_name")
	ds.ExtraParams = c.PostForm("extra_params")

	if err := h.Service.Update(ds); err != nil {
		username, _ := c.Get("username")
		c.HTML(http.StatusOK, "datasource_form", gin.H{
			"error":      err.Error(),
			"datasource": ds,
			"username":   username,
		})
		return
	}
	c.Redirect(http.StatusFound, "/datasources")
}

func (h *DataSourceHandler) Delete(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	h.Service.Delete(uint(id))
	c.Redirect(http.StatusFound, "/datasources")
}

func (h *DataSourceHandler) TestConn(c *gin.Context) {
	port, _ := strconv.Atoi(c.PostForm("port"))
	ds := model.DataSource{
		DBType:       c.PostForm("db_type"),
		Host:         c.PostForm("host"),
		Port:         port,
		Username:     c.PostForm("username"),
		Password:     c.PostForm("password"),
		DatabaseName: c.PostForm("database_name"),
		ExtraParams:  c.PostForm("extra_params"),
	}
	conn, err := connector.FromDataSource(ds)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "error": err.Error()})
		return
	}
	defer conn.Close()
	if err := conn.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusOK, gin.H{"success": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

// Upload saves an uploaded file to ./uploads/ and returns the file path.
func (h *DataSourceHandler) Upload(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	defer file.Close()
	if err := os.MkdirAll("./uploads", 0755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	dst := "./uploads/" + header.Filename
	out, err := os.Create(dst)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer out.Close()
	if _, err := io.Copy(out, file); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"path": dst})
}

func (h *DataSourceHandler) Tables(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	ds, err := h.Service.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "datasource not found"})
		return
	}
	tables, err := h.Service.GetTables(*ds)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tables": tables})
}

func (h *DataSourceHandler) Columns(c *gin.Context) {
	id, _ := strconv.ParseUint(c.Param("id"), 10, 64)
	ds, err := h.Service.GetByID(uint(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "datasource not found"})
		return
	}
	table := c.Param("table")
	columns, err := h.Service.GetColumns(*ds, table)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"columns": columns})
}
