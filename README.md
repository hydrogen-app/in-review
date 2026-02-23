# InReview

Global leaderboards for GitHub PR review time. Search any public repo, user, or org.

## Requirements

- Go 1.21+
- A GitHub personal access token (optional but strongly recommended)

## Setup

```bash
cp .env.example .env
```

Edit `.env` and add your GitHub token:

```
GITHUB_TOKEN=ghp_your_token_here
```

To create a token: GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic) → Generate new token. No special scopes needed for public repos.

Without a token you get 60 API requests/hour (unauthenticated). With one: 5,000/hour.

## Run

```bash
go run .
```

Open [http://localhost:8080](http://localhost:8080).

On first boot, 20 popular repos are synced in the background to seed the leaderboards. This takes a few minutes depending on your rate limit.

## How it works

- **Search** a repo (`owner/repo`), user, or org — it gets added to the global leaderboard
- **Repo pages** show avg/fastest/slowest merge time, top reviewers, and recent PRs
- **User pages** show reviewer stats (approvals, changes requested) and author stats
- **Org pages** show in-org leaderboards and all tracked repos
- Data is cached in SQLite (`data/inreview.db`) and re-synced every 6 hours

## Config

| Variable | Default | Description |
|---|---|---|
| `GITHUB_TOKEN` | — | GitHub personal access token |
| `PORT` | `8080` | HTTP server port |
| `DB_PATH` | `data/inreview.db` | SQLite database path |
