package config

import "os"

type Config struct {
	GitHubToken string
	DatabaseURL string
	Port        string
	RedisURL    string
}

func Load() *Config {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/inreview?sslmode=disable"
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
		DatabaseURL: databaseURL,
		Port:        port,
		RedisURL:    redisURL,
	}
}
