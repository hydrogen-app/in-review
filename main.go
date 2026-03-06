package main

import (
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/joho/godotenv"

	"inreview/internal/analytics"
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
	database, err := db.New(cfg.DatabaseURL)
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

	// Analytics
	ph := analytics.New(cfg.PostHogAPIKey)
	defer ph.Close()

	// Sync worker
	w := worker.New(ghClient, database, cache, cfg.GitHubAppID, cfg.GitHubAppPrivateKey)
	w.Start()

	// HTTP router
	h := handlers.New(database, ghClient, w, cache, cfg, ph)
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Use(middleware.Compress(5))
	r.Use(cache.RateLimit(300, time.Minute))
	r.Use(h.SessionLoader)
	r.Use(h.TrackPageViews)

	r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	r.Get("/", h.Home)
	r.Get("/stats", h.Stats)
	r.Get("/search", h.Search)
	r.Get("/repo/{owner}/{name}", h.Repo)
	r.Get("/user/{username}", h.User)
	r.Get("/org/{org}", h.Org)

	r.Get("/badge/{owner}/{name}.svg", h.Badge)

	r.Get("/leaderboard/{category}", h.LeaderboardPage)
	r.Get("/leaderboard/{category}/rows", h.LeaderboardRows)
	r.Get("/leaderboard/{category}/search", h.LeaderboardSearch)
	r.Get("/api/leaderboard", h.LeaderboardAPI)
	r.Post("/api/sync/{owner}/{name}", h.TriggerSync)
	r.Get("/api/sync-status/{owner}/{name}", h.SyncStatus)
	r.Get("/hi-wall", h.HiWall)
	r.Get("/api/hi", h.HiGet)
	r.Post("/api/hi", h.HiPost)

	r.Get("/data", h.DataExplorer)
	r.Get("/data/repos", h.DataRepos)
	r.Get("/data/prs", h.DataPRs)
	r.Get("/data/reviews", h.DataReviews)
	r.Get("/data/users", h.DataUsers)

	r.Get("/blog", h.Blog)
	r.Get("/api/blog/stats", h.BlogLiveStats)

	// JSON API v1
	r.Get("/api/v1/me", h.MeJSON)
	r.Get("/api/v1/home", h.HomeJSON)
	r.Get("/api/v1/search", h.SearchJSON)
	r.Get("/api/v1/repo/{owner}/{name}", h.RepoJSON)
	r.Get("/api/v1/sync-status/{owner}/{name}", h.SyncStatusJSON)
	r.Get("/api/v1/user/{username}", h.UserJSON)
	r.Get("/api/v1/org/{org}", h.OrgJSON)
	r.Get("/api/v1/leaderboard/{category}", h.LeaderboardPageJSON)
	r.Get("/api/v1/leaderboard/{category}/search", h.LeaderboardSearchJSON)
	r.Get("/api/v1/stats", h.StatsJSON)
	r.Get("/api/v1/data/repos", h.DataReposJSON)
	r.Get("/api/v1/data/prs", h.DataPRsJSON)
	r.Get("/api/v1/data/reviews", h.DataReviewsJSON)
	r.Get("/api/v1/data/users", h.DataUsersJSON)
	r.Get("/api/v1/blog", h.BlogJSON)
	r.Get("/api/v1/dashboard", h.RequireAuth(h.DashboardJSON))
	r.Get("/api/v1/hi", h.HiGetJSON)
	r.Post("/api/v1/hi", h.HiPostJSON)
	r.Get("/api/v1/hi-wall", h.HiWallJSON)

	// Auth routes
	r.Get("/auth/login", h.AuthLogin)
	r.Get("/auth/github", h.AuthGitHub)
	r.Get("/auth/github/callback", h.AuthGitHubCallback)
	r.Post("/auth/logout", h.AuthLogout)
	r.Post("/api/github/webhook", h.GitHubWebhook)

	// Authenticated routes
	r.Get("/dashboard", h.RequireAuth(h.Dashboard))
	r.Post("/api/repos/add", h.RequireAuth(h.AddRepo))

	log.Printf("ngmi listening on http://localhost:%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, r))
}
