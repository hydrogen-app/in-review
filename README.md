# InReview

Global leaderboards for GitHub PR review time. Search any public repo, user, or org.

Live at [inreview.dev](https://inreview.dev).

## Stack

- Go 1.21+
- Next.js frontend
- PostgreSQL
- Redis (sync queue, response cache, rate limiting)

## Deploying on Railway

### Services

1. **Go API app** — this repo with `Dockerfile.web`. It still serves the legacy HTML routes and now exposes `/api/next/*` JSON for the Next app.
2. **Next.js frontend** — this repo with `Dockerfile.frontend`. Set `API_BASE_URL` to the public/internal URL of the Go API service.
3. **Sync worker** — this repo with `Dockerfile.sync`. This is the only service that should run GitHub sync workers.
4. **PostgreSQL** — add a Postgres plugin from the Railway dashboard. The `DATABASE_URL` env var is injected automatically.
5. **Redis** — add a Redis plugin from the Railway dashboard. The `REDIS_URL` env var is injected automatically.

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `GITHUB_TOKEN` | Recommended | GitHub personal access token. Without it you get 60 API req/hr (unauthenticated) vs 5,000/hr. |
| `DATABASE_URL` | Yes | Injected automatically by Railway's Postgres plugin. |
| `REDIS_URL` | Yes | Injected automatically by Railway's Redis plugin. |
| `PORT` | No | Defaults to `8080`. Railway sets this automatically. |
| `API_BASE_URL` | Frontend | URL of the Go API service for Next rewrites and server data fetching. |
| `DB_MAX_OPEN_CONNS` | No | Defaults to `5`. Lower this on small Railway Postgres plans. |
| `DB_MAX_IDLE_CONNS` | No | Defaults to `2`. |
| `WARM_LEADERBOARDS` | No | Defaults to `true`. Set `false` to disable periodic materialized leaderboard refreshes from the web process. |
| `LEADERBOARD_REFRESH_INTERVAL` | No | Defaults to `1h`. Go duration format, e.g. `2h`, `30m`. |
| `WARM_STATS` | No | Defaults to `false`. Stats percentile cache warming is expensive and should stay off on memory-constrained Postgres plans. |

### GitHub token

GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic) → Generate new token. No special scopes needed for public repos.

## Running locally

```bash
cp .env.example .env
# add GITHUB_TOKEN to .env
go run ./cmd/web
```

In another shell:

```bash
cd frontend
npm install
API_BASE_URL=http://localhost:8080 npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

Requires a local Postgres instance (defaults to `postgres://postgres:postgres@localhost:5432/inreview?sslmode=disable`) and Redis (`redis://localhost:6379`). The schema is created automatically on startup.

On first boot, run the seed script to populate the leaderboards:

```bash
go run ./cmd/seed
```

## Populating data: seed and crawl

### Seed

Queues 20 popular repos for sync:

```bash
go run ./cmd/seed
# or on Railway:
railway run go run ./cmd/seed
```

### Crawl

BFS graph crawler that discovers repos organically — starting from seeds, expands to org repos, finds contributors, finds what those contributors worked on elsewhere, and repeats.

```bash
# Run against your deployed Railway instance (uses GITHUB_TOKEN from env)
railway run go run ./cmd/crawl --rate 4500 --state-file crawl.json

# Resume after stopping — picks up exactly where it left off
railway run go run ./cmd/crawl --rate 4500 --state-file crawl.json

# Custom seeds and depth
railway run go run ./cmd/crawl --seeds golang/go,facebook/react --depth 3
```

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--seeds` | 20 popular repos | Comma-separated `owner/repo` list (ignored if `--state-file` exists) |
| `--depth` | `2` | BFS hops from seeds |
| `--rate` | `4500` | GitHub API requests per hour. Leave headroom for the main server's sync workers. |
| `--state-file` | _(none)_ | JSON file to save/restore BFS state. Enables stopping and resuming across sessions. |
| `--org-limit` | `20` | Max repos fetched per org |
| `--user-limit` | `10` | Max repos fetched per user |
| `--pr-limit` | `50` | PRs scanned per repo for contributor discovery |

**Rate math at 5,000 req/hr:**

Each fully-expanded repo costs ~23 API calls (owner lookup + org repos + contributors + their owned/reviewed repos). At 4,500 req/hr that's ~195 repos expanded per hour, each discovering 20–50 new repos — roughly 4,000–10,000 repos queued per hour of runtime.

State is saved after every repo, so `Ctrl+C` loses no progress.

## How it works

- **Search** a repo (`owner/repo`), user, or org — it gets queued for sync and added to the leaderboard
- **Repo pages** show avg/fastest/slowest merge time, top reviewers, and recent PRs
- **User pages** show reviewer stats (approvals, changes requested) and author stats
- **Org pages** show in-org leaderboards and all tracked repos
- Data is stored in PostgreSQL and re-synced every 6 hours via background workers
- Redis holds the sync queue, in-progress locks, response cache (5 min TTL), and rate limiting (300 req/min per IP)
change
