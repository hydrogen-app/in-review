package config

import "os"

type Config struct {
	GitHubToken string
	DBPath      string
	Port        string
	RedisURL    string
}

func Load() *Config {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "data/inreview.db"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	return &Config{
		GitHubToken: os.Getenv("GITHUB_TOKEN"),
		DBPath:      dbPath,
		Port:        port,
		RedisURL:    redisURL,
	}
}
