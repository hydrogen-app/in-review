package main

import (
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/joho/godotenv"

	"inreview/internal/config"
	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/handlers"
	"inreview/internal/rdb"
	"inreview/internal/worker"
)

func main() {
	_ = godotenv.Load()

	cfg := config.Load()
	if cfg.GitHubToken == "" {
		log.Println("WARNING: GITHUB_TOKEN not set — using unauthenticated API (60 req/hr limit)")
	}

	database, err := db.New(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	cache, err := rdb.New(cfg.RedisURL)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	defer cache.Close()

	ghClient := github.NewClient(cfg.GitHubToken)
	// Create the worker for queue operations (Queue/IsSyncing/QueuePosition)
	// but do NOT call Start() — sync goroutines run in the sync binary.
	q := worker.New(ghClient, database, cache, cfg.GitHubAppID, cfg.GitHubAppPrivateKey)

	h := handlers.New(database, ghClient, q, cache, cfg)
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Use(middleware.Compress(5))
	r.Use(cache.RateLimit(300, time.Minute))
	r.Use(h.SessionLoader)

	r.Get("/badge/{owner}/{name}.svg", h.Badge)

	r.Post("/api/sync/{owner}/{name}", h.TriggerSync)

	r.Get("/auth/login", h.AuthLogin)
	r.Get("/auth/github", h.AuthGitHub)
	r.Get("/auth/github/callback", h.AuthGitHubCallback)
	r.Post("/auth/logout", h.AuthLogout)
	r.Post("/api/github/webhook", h.GitHubWebhook)

	r.Post("/api/repos/add", h.RequireAuth(h.AddRepo))
	h.RegisterNextRoutes(r)

	// Rebuild materialized leaderboard tables and keep them fresh.
	// Also warms the home page Redis cache. Prevents 30+ second cold-cache hits.
	if cfg.WarmLeaderboards {
		go h.WarmLeaderboards()
	} else {
		log.Println("leaderboards: background refresh disabled (WARM_LEADERBOARDS=false)")
	}

	log.Printf("ngmi web listening on http://localhost:%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, r))
}
