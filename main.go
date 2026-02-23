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
	// Load .env if present (ignored if missing)
	_ = godotenv.Load()

	cfg := config.Load()
	if cfg.GitHubToken == "" {
		log.Println("WARNING: GITHUB_TOKEN not set — using unauthenticated API (60 req/hr limit)")
	}

	// Database
	database, err := db.New(cfg.DBPath)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	// Redis
	cache, err := rdb.New(cfg.RedisURL)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	defer cache.Close()

	// GitHub client
	ghClient := github.NewClient(cfg.GitHubToken)

	// Sync worker
	w := worker.New(ghClient, database, cache)
	w.Start()

	// HTTP router
	h := handlers.New(database, ghClient, w, cache, cfg)
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Use(middleware.Compress(5))
	r.Use(cache.RateLimit(300, time.Minute))

	r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	r.Get("/", h.Home)
	r.Get("/search", h.Search)
	r.Get("/repo/{owner}/{name}", h.Repo)
	r.Get("/user/{username}", h.User)
	r.Get("/org/{org}", h.Org)

	r.Get("/leaderboard/{category}", h.LeaderboardPage)
	r.Get("/leaderboard/{category}/rows", h.LeaderboardRows)
	r.Get("/leaderboard/{category}/search", h.LeaderboardSearch)
	r.Get("/api/leaderboard", h.LeaderboardAPI)
	r.Post("/api/sync/{owner}/{name}", h.TriggerSync)
	r.Get("/api/sync-status/{owner}/{name}", h.SyncStatus)
	r.Get("/hi-wall", h.HiWall)
	r.Get("/api/hi", h.HiGet)
	r.Post("/api/hi", h.HiPost)

	log.Printf("InReview listening on http://localhost:%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, r))
}
