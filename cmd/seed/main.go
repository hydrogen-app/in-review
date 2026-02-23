// seed queues a list of repos for sync via the InReview worker.
// Run while the main server is running:
//
//	go run ./cmd/seed
package main

import (
	"log"
	"time"

	"github.com/joho/godotenv"

	"inreview/internal/config"
	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/rdb"
	"inreview/internal/worker"
)

var repos = []string{
	"golang/go",
	"facebook/react",
	"microsoft/vscode",
	"kubernetes/kubernetes",
	"vercel/next.js",
	"rust-lang/rust",
	"vuejs/vue",
	"angular/angular",
	"django/django",
	"rails/rails",
	"tensorflow/tensorflow",
	"pytorch/pytorch",
	"denoland/deno",
	"astro-build/astro",
	"sveltejs/svelte",
	"laravel/laravel",
	"expressjs/express",
	"fastapi/fastapi",
	"tailwindlabs/tailwindcss",
	"vitejs/vite",
}

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	database, err := db.New(cfg.DBPath)
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
	w := worker.New(ghClient, database, cache)
	w.Start()

	queued := 0
	for _, repo := range repos {
		existing, _ := database.GetRepo(repo)
		if existing != nil {
			log.Printf("skip %s (already in DB)", repo)
			continue
		}
		w.Queue(repo, false)
		log.Printf("queued %s", repo)
		queued++
		if queued > 1 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	if queued == 0 {
		log.Println("nothing to seed — all repos already in DB")
		return
	}

	log.Printf("queued %d repos, waiting for syncs to complete…", queued)
	for {
		allDone := true
		for _, repo := range repos {
			if w.IsSyncing(repo) {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		time.Sleep(5 * time.Second)
	}
	log.Println("seed complete")
}
