package config

type Config struct {
	Port      int    `json:"port"`
	DBPath    string `json:"db_path"`
	JWTSecret string `json:"jwt_secret"`
	AdminUser string `json:"admin_user"`
	AdminPass string `json:"admin_pass"`
}

func Default() *Config {
	return &Config{
		Port:      9090,
		DBPath:    "datasync.db",
		JWTSecret: "change-me-in-production",
		AdminUser: "admin",
		AdminPass: "admin123",
	}
}
