package main

import (
	"datasync/internal/config"
	"datasync/internal/database"
	"fmt"
	"log"
)

func main() {
	cfg := config.Default()
	db, err := database.Init(cfg)
	if err != nil {
		log.Fatal("failed to init database:", err)
	}
	_ = db
	fmt.Printf("DataSync server starting on :%d\n", cfg.Port)
}
