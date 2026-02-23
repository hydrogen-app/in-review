# InReview

Global leaderboards for GitHub PR review time. Search any public repo, user, or org.

Live at [inreview.dev](https://inreview.dev).

## Stack

- Go 1.21+
- SQLite (via `modernc.org/sqlite` — pure Go, no cgo)
- Redis (sync queue, response cache, rate limiting)
- HTMX (no JS build step)

## Deploying on Railway

### Services

1. **Go app** — this repo. Set the start command to `go run .` or build with `go build -o server . && ./server`.
2. **Redis** — add a Redis plugin from the Railway dashboard. The `REDIS_URL` env var is injected automatically.

### Volumes

SQLite writes to disk, so you need a persistent volume mounted at the `DB_PATH` directory.

In Railway: go to your app service → **Volumes** → add a volume mounted at `/data`, then set `DB_PATH=/data/inreview.db`.

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `GITHUB_TOKEN` | Recommended | GitHub personal access token. Without it you get 60 API req/hr (unauthenticated) vs 5,000/hr. |
| `REDIS_URL` | Yes | Injected automatically by Railway's Redis plugin. |
| `DB_PATH` | Yes | Set to `/data/inreview.db` (or wherever your volume is mounted). |
| `PORT` | No | Defaults to `8080`. Railway sets this automatically. |

### GitHub token

GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic) → Generate new token. No special scopes needed for public repos.

## Running locally

```bash
cp .env.example .env
# add GITHUB_TOKEN to .env
go run .
```

Open [http://localhost:8080](http://localhost:8080).

Requires a local Redis instance (`redis-server`). Defaults to `redis://localhost:6379`.

On first boot, 20 popular repos are synced in the background to seed the leaderboards.

## How it works

- **Search** a repo (`owner/repo`), user, or org — it gets queued for sync and added to the leaderboard
- **Repo pages** show avg/fastest/slowest merge time, top reviewers, and recent PRs
- **User pages** show reviewer stats (approvals, changes requested) and author stats
- **Org pages** show in-org leaderboards and all tracked repos
- Data is stored in SQLite and re-synced every 6 hours via background workers
- Redis holds the sync queue, in-progress locks, response cache (5 min TTL), and rate limiting (300 req/min per IP)
