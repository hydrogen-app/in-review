package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	GitHubToken             string
	DatabaseURL             string
	Port                    string
	RedisURL                string
	GitHubAppID             int64
	GitHubAppSlug           string
	GitHubAppPrivateKey     string
	GitHubAppWebhookSecret  string
	GitHubOAuthClientID     string
	GitHubOAuthClientSecret string
	SessionSecret           string
	BaseURL                 string
	PostHogAPIKey           string
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
	appID, _ := strconv.ParseInt(os.Getenv("GITHUB_APP_ID"), 10, 64)
	baseURL := os.Getenv("BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:" + port
	}
	return &Config{
		GitHubToken:             os.Getenv("GITHUB_TOKEN"),
		DatabaseURL:             databaseURL,
		Port:                    port,
		RedisURL:                redisURL,
		GitHubAppID:             appID,
		GitHubAppSlug:           os.Getenv("GITHUB_APP_SLUG"),
		GitHubAppPrivateKey:     os.Getenv("GITHUB_APP_PRIVATE_KEY"),
		GitHubAppWebhookSecret:  os.Getenv("GITHUB_APP_WEBHOOK_SECRET"),
		GitHubOAuthClientID:     os.Getenv("GITHUB_OAUTH_CLIENT_ID"),
		GitHubOAuthClientSecret: os.Getenv("GITHUB_OAUTH_CLIENT_SECRET"),
		SessionSecret:           os.Getenv("SESSION_SECRET"),
		BaseURL:                 baseURL,
		PostHogAPIKey:           os.Getenv("POSTHOG_API_KEY"),
	}
}

// Validate returns an error if any required configuration values are missing.
// Call this at startup to surface missing env vars immediately.
func (c *Config) Validate() error {
	var missing []string
	if c.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if c.SessionSecret == "" {
		missing = append(missing, "SESSION_SECRET")
	}
	if c.GitHubOAuthClientID == "" {
		missing = append(missing, "GITHUB_OAUTH_CLIENT_ID")
	}
	if c.GitHubOAuthClientSecret == "" {
		missing = append(missing, "GITHUB_OAUTH_CLIENT_SECRET")
	}
	if len(missing) > 0 {
		return errors.New("missing required env vars: " + strings.Join(missing, ", "))
	}
	return nil
}
