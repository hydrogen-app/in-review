package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// DB wraps the Postgres connection pool.
type DB struct {
	conn *sql.DB
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

// ── Model types ────────────────────────────────────────────────────────────────

type Repo struct {
	FullName         string
	Owner            string
	Name             string
	Description      string
	Stars            int
	Language         string
	OrgName          string
	LastSynced       *time.Time
	SyncStatus       string
	PRCount          int
	MergedPRCount    int
	AvgMergeTimeSecs int64
	MinMergeTimeSecs int64
	MaxMergeTimeSecs int64
}

type PullRequest struct {
	ID                    string
	RepoFullName          string
	Number                int
	Title                 string
	AuthorLogin           string
	Merged                bool
	OpenedAt              time.Time
	MergedAt              *time.Time
	MergeTimeSecs         *int64
	ReviewCount           int
	ChangesRequestedCount int
	Additions             int
	Deletions             int
	FirstReviewAt         *time.Time
}

type Review struct {
	ID            string
	RepoFullName  string
	PRNumber      int
	ReviewerLogin string
	State         string
	SubmittedAt   time.Time
}

type User struct {
	Login       string
	Name        string
	AvatarURL   string
	Bio         string
	PublicRepos int
	Followers   int
	Company     string
	Location    string
	IsOrg       bool
	LastFetched *time.Time
}

type LeaderboardEntry struct {
	Rank  int
	Name  string
	Value int64
	Count int
	Extra string // avatar_url or secondary metric
}

type ReviewerStats struct {
	Login            string
	AvatarURL        string
	TotalReviews     int
	Approvals        int
	ChangesRequested int
	Comments         int
	LastReviewedAt   *time.Time
}

type AuthorStats struct {
	Login               string
	TotalPRs            int
	MergedPRs           int
	AvgMergeTimeSecs    int64
	TotalLinesWritten   int64
	AvgPRSize           float64
	CleanApprovalRate   float64
	AvgChangesRequested float64
	AvgFirstReviewSecs  float64
	MedFirstReviewSecs  float64
}

// UserActivityPoint holds one month's author + reviewer activity for a user.
type UserActivityPoint struct {
	Label                string
	PRCount              int
	ReviewCount          int
	ChangesRequestedRate float64
}

// CollabEntry is a collaborator with a review count (reviewer of my PRs, or author I review).
type CollabEntry struct {
	Login     string
	AvatarURL string
	Count     int
}

// UserRepoReview is a repo the user has reviewed PRs in, with a count.
type UserRepoReview struct {
	FullName string
	Count    int
}

// UserRecordPR is a single PR representing a personal record (fastest or slowest merge).
type UserRecordPR struct {
	Number        int
	RepoFullName  string
	MergeTimeSecs int64
	Title         string
}

// ── Constructor ────────────────────────────────────────────────────────────────

func New(databaseURL string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(databaseURL), 0o755); err != nil {
		// Not a file path — that's fine for a Postgres URL.
		_ = err
	}
	conn, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}
	maxOpen := envInt("DB_MAX_OPEN_CONNS", 5)
	maxIdle := envInt("DB_MAX_IDLE_CONNS", 2)
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	conn.SetMaxOpenConns(maxOpen)
	conn.SetMaxIdleConns(maxIdle)
	conn.SetConnMaxLifetime(5 * time.Minute)
	conn.SetConnMaxIdleTime(2 * time.Minute)

	d := &DB{conn: conn}
	return d, d.migrate()
}

func (d *DB) Close() error { return d.conn.Close() }

// ── Schema ─────────────────────────────────────────────────────────────────────

const migrationAdvisoryLockID int64 = 743928475610293841

func (d *DB) migrate() error {
	ctx := context.Background()
	conn, err := d.conn.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migration connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLockID)
	}()

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS repos (
			full_name           TEXT PRIMARY KEY,
			owner               TEXT NOT NULL,
			name                TEXT NOT NULL,
			description         TEXT        DEFAULT '',
			stars               INTEGER     DEFAULT 0,
			language            TEXT        DEFAULT '',
			org_name            TEXT        DEFAULT '',
			last_synced         TIMESTAMPTZ,
			sync_status         TEXT        DEFAULT 'pending',
			pr_count            INTEGER     DEFAULT 0,
			merged_pr_count     INTEGER     DEFAULT 0,
			avg_merge_time_secs BIGINT      DEFAULT 0,
			min_merge_time_secs BIGINT      DEFAULT 0,
			max_merge_time_secs BIGINT      DEFAULT 0,
			updated_at          TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS pull_requests (
			id                      TEXT PRIMARY KEY,
			repo_full_name          TEXT        NOT NULL,
			number                  INTEGER     NOT NULL,
			title                   TEXT        DEFAULT '',
			author_login            TEXT        NOT NULL,
			merged                  BOOLEAN     DEFAULT FALSE,
			opened_at               TIMESTAMPTZ NOT NULL,
			merged_at               TIMESTAMPTZ,
			merge_time_secs         BIGINT,
			review_count            INTEGER     DEFAULT 0,
			changes_requested_count INTEGER     DEFAULT 0,
			UNIQUE(repo_full_name, number)
		)`,
		`CREATE TABLE IF NOT EXISTS reviews (
			id             TEXT PRIMARY KEY,
			repo_full_name TEXT        NOT NULL,
			pr_number      INTEGER     NOT NULL,
			reviewer_login TEXT        NOT NULL,
			state          TEXT        NOT NULL,
			submitted_at   TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS users (
			login        TEXT PRIMARY KEY,
			name         TEXT        DEFAULT '',
			avatar_url   TEXT        DEFAULT '',
			bio          TEXT        DEFAULT '',
			public_repos INTEGER     DEFAULT 0,
			followers    INTEGER     DEFAULT 0,
			company      TEXT        DEFAULT '',
			location     TEXT        DEFAULT '',
			is_org       BOOLEAN     DEFAULT FALSE,
			last_fetched TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_prs_repo     ON pull_requests(repo_full_name)`,
		`CREATE INDEX IF NOT EXISTS idx_prs_author   ON pull_requests(author_login)`,
		`CREATE INDEX IF NOT EXISTS idx_prs_merged   ON pull_requests(merged)`,
		`CREATE INDEX IF NOT EXISTS idx_rev_reviewer ON reviews(reviewer_login)`,
		`CREATE INDEX IF NOT EXISTS idx_rev_repo_pr  ON reviews(repo_full_name, pr_number)`,
		`CREATE INDEX IF NOT EXISTS idx_repos_org    ON repos(org_name)`,
		`CREATE TABLE IF NOT EXISTS page_visits (
			path         TEXT PRIMARY KEY,
			kind         TEXT NOT NULL,
			label        TEXT NOT NULL,
			count        INTEGER     DEFAULT 1,
			last_visited TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS page_hi (
			path  TEXT PRIMARY KEY,
			count INTEGER DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS page_hi_reactions (
			path     TEXT NOT NULL,
			reaction TEXT NOT NULL,
			count    INTEGER DEFAULT 0,
			PRIMARY KEY (path, reaction)
		)`,
		`CREATE TABLE IF NOT EXISTS page_hi_log (
			id       BIGSERIAL   PRIMARY KEY,
			path     TEXT        NOT NULL,
			reaction TEXT        NOT NULL DEFAULT 'wave',
			ts       TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_hi_log_path ON page_hi_log(path)`,
		`CREATE INDEX IF NOT EXISTS idx_hi_log_ts   ON page_hi_log(ts)`,
		`ALTER TABLE pull_requests ADD COLUMN IF NOT EXISTS additions INTEGER DEFAULT 0`,
		`ALTER TABLE pull_requests ADD COLUMN IF NOT EXISTS deletions INTEGER DEFAULT 0`,
		`CREATE INDEX IF NOT EXISTS idx_prs_merged_at ON pull_requests(merged_at) WHERE merged=TRUE`,
		`CREATE INDEX IF NOT EXISTS idx_prs_merged_contrib ON pull_requests(merged, repo_full_name, author_login) WHERE merged=TRUE`,
		`CREATE INDEX IF NOT EXISTS idx_rev_repo_pr_time ON reviews(repo_full_name, pr_number, submitted_at)`,
		`ALTER TABLE pull_requests ADD COLUMN IF NOT EXISTS first_review_at TIMESTAMPTZ`,
		`UPDATE pull_requests SET first_review_at = (
			SELECT MIN(submitted_at) FROM reviews
			WHERE reviews.repo_full_name = pull_requests.repo_full_name
			  AND reviews.pr_number = pull_requests.number
		) WHERE first_review_at IS NULL AND review_count > 0`,
		`CREATE TABLE IF NOT EXISTS app_installations (
			installation_id BIGINT      PRIMARY KEY,
			github_login    TEXT        NOT NULL,
			installed_at    TIMESTAMPTZ DEFAULT NOW(),
			uninstalled_at  TIMESTAMPTZ
		)`,
		`CREATE TABLE IF NOT EXISTS user_sessions (
			session_id      TEXT        PRIMARY KEY,
			github_login    TEXT        NOT NULL,
			installation_id BIGINT,
			created_at      TIMESTAMPTZ DEFAULT NOW(),
			expires_at      TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS user_tracked_repos (
			github_login   TEXT        NOT NULL,
			repo_full_name TEXT        NOT NULL,
			tracked_at     TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (github_login, repo_full_name)
		)`,
		`CREATE TABLE IF NOT EXISTS user_installations (
			github_login    TEXT   NOT NULL,
			installation_id BIGINT NOT NULL,
			PRIMARY KEY (github_login, installation_id)
		)`,
		// Indexes for leaderboard GROUP BY queries.
		`CREATE INDEX IF NOT EXISTS idx_rev_state ON reviews(state)`,
		`CREATE INDEX IF NOT EXISTS idx_prs_author_merged ON pull_requests(author_login) WHERE merged=TRUE`,
		// Retry tracking: stop re-queuing repos that have failed repeatedly.
		`ALTER TABLE repos ADD COLUMN IF NOT EXISTS error_count INT NOT NULL DEFAULT 0`,
		// Performance: composite indexes that cover the most expensive GROUP BY paths.
		// idx_rev_state_reviewer covers state-filtered reviewer aggregations (gatekeeper queries).
		`CREATE INDEX IF NOT EXISTS idx_rev_state_reviewer ON reviews(state, reviewer_login)`,
		// idx_prs_repo_merged_secs covers UpdateRepoStats and RepoSizeChartData which filter
		// repo_full_name + merged=TRUE and aggregate merge_time_secs.
		`CREATE INDEX IF NOT EXISTS idx_prs_repo_merged_secs ON pull_requests(repo_full_name, merge_time_secs) WHERE merged=TRUE`,
		// idx_prs_author_merged_at covers UserActivitySeries which groups merged PRs by month per author.
		`CREATE INDEX IF NOT EXISTS idx_prs_author_merged_at ON pull_requests(author_login, merged_at) WHERE merged=TRUE`,
		// idx_rev_repo_reviewer covers OrgReviewerLeaderboard and OrgGatekeeperLeaderboard which
		// JOIN reviews to repos by repo_full_name and GROUP BY reviewer_login. Without this index
		// both queries do a full 30M-row scan of reviews for every org page load.
		`CREATE INDEX IF NOT EXISTS idx_rev_repo_reviewer ON reviews(repo_full_name, reviewer_login, state)`,
		// idx_page_visits_count covers PopularVisits which sorts by count DESC.
		`CREATE INDEX IF NOT EXISTS idx_page_visits_count ON page_visits(count DESC)`,
		// Aggressive autovacuum for high-write tables. The worker updates repos
		// sync_status on every sync cycle, generating dead tuples faster than the
		// default 20% threshold triggers cleanup.
		`ALTER TABLE repos SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_analyze_scale_factor = 0.005)`,
		`ALTER TABLE pull_requests SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_analyze_scale_factor = 0.005)`,
		// Drop redundant indexes to reclaim ~2.6 GB of RAM.
		// idx_rev_repo_pr_time (2254 MB): covered by idx_rev_repo_pr(repo_full_name, pr_number).
		// submitted_at is never range-filtered alongside repo+pr_number.
		`DROP INDEX IF EXISTS idx_rev_repo_pr_time`,
		// idx_rev_state (205 MB): reviews(state) has 3 distinct values — too low-cardinality
		// to be useful. All state-filtered queries use idx_rev_state_reviewer(state, reviewer_login).
		`DROP INDEX IF EXISTS idx_rev_state`,
		// idx_prs_merged (158 MB): pull_requests(merged) is a boolean. Every WHERE merged=TRUE
		// query also filters by author or repo, which partial indexes cover more efficiently.
		`DROP INDEX IF EXISTS idx_prs_merged`,
		// Materialized leaderboard tables. Rebuilt by RefreshLeaderboards() on a background
		// timer so all leaderboard queries become simple indexed range scans instead of
		// full GROUP BY aggregations on 25M+ rows.
		`CREATE TABLE IF NOT EXISTS mat_leaderboard_reviewers (
			rank              INTEGER PRIMARY KEY,
			login             TEXT    NOT NULL,
			avatar_url        TEXT    NOT NULL DEFAULT '',
			total_reviews     INTEGER NOT NULL,
			approvals         INTEGER NOT NULL,
			changes_requested INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS mat_leaderboard_gatekeepers (
			rank              INTEGER PRIMARY KEY,
			login             TEXT    NOT NULL,
			avatar_url        TEXT    NOT NULL DEFAULT '',
			total             INTEGER NOT NULL,
			approvals         INTEGER NOT NULL,
			changes_requested INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS mat_leaderboard_authors (
			rank                INTEGER PRIMARY KEY,
			login               TEXT    NOT NULL,
			avatar_url          TEXT    NOT NULL DEFAULT '',
			merged_prs          INTEGER NOT NULL,
			avg_merge_time_secs BIGINT  NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS mat_leaderboard_clean (
			rank           INTEGER PRIMARY KEY,
			repo_full_name TEXT    NOT NULL,
			total_prs      INTEGER NOT NULL,
			clean_pct      INTEGER NOT NULL,
			avg_secs       BIGINT  NOT NULL DEFAULT 0
		)`,
		// pg_trgm kept for repos/users prefix search (lightweight tables).
		`CREATE EXTENSION IF NOT EXISTS pg_trgm`,
		// Timestamp indexes missing from original schema — used by time-series queries.
		`CREATE INDEX IF NOT EXISTS idx_rev_submitted_at ON reviews(submitted_at)`,
		`CREATE INDEX IF NOT EXISTS idx_prs_opened_at ON pull_requests(opened_at) WHERE merged=TRUE`,

		// Drop ALL GIN trigram indexes — they consume 3-6 GB RAM each on large tables.
		// Replaced by small B-tree text_pattern_ops indexes below.
		`DROP INDEX IF EXISTS idx_prs_author_login_trgm`,
		`DROP INDEX IF EXISTS idx_prs_repo_full_name_trgm`,
		`DROP INDEX IF EXISTS idx_rev_reviewer_login_trgm`,
		`DROP INDEX IF EXISTS idx_repos_full_name_trgm`,
		`DROP INDEX IF EXISTS idx_users_login_trgm`,
		`DROP INDEX IF EXISTS idx_users_name_trgm`,
		// mat_repo_contribs: precomputes COUNT(DISTINCT author_login) per repo.
		// Replaces the inline repo_contribs CTE in all four global stats queries,
		// eliminating a repeated GROUP BY over 30M rows on every cache miss.
		// Refreshed atomically by RefreshLeaderboards().
		`CREATE TABLE IF NOT EXISTS mat_repo_contribs (
			repo_full_name   TEXT    PRIMARY KEY,
			distinct_authors INTEGER NOT NULL DEFAULT 0
		)`,
		// mat_total_stats: precomputes the three global counts used by TotalStats().
		// COUNT(*) over 30M-row tables takes ~1s each; reading one row here is <1ms.
		`CREATE TABLE IF NOT EXISTS mat_total_stats (
			id          INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
			repos_done  INTEGER NOT NULL DEFAULT 0,
			merged_prs  INTEGER NOT NULL DEFAULT 0,
			total_reviews INTEGER NOT NULL DEFAULT 0
		)`,
		`INSERT INTO mat_total_stats (id) VALUES (1) ON CONFLICT DO NOTHING`,

		// ── BIGSERIAL primary key migration ───────────────────────────────────────
		// Replaces TEXT PKs (~1 GB each) with BIGINT PKs (~150-300 MB each).
		//
		// BEFORE FIRST DEPLOY: run the heavy steps manually against the live DB:
		//   ALTER TABLE pull_requests ADD COLUMN IF NOT EXISTS db_id BIGINT;
		//   ALTER TABLE reviews ADD COLUMN IF NOT EXISTS db_id BIGINT;
		//   CREATE SEQUENCE IF NOT EXISTS pull_requests_db_id_seq;
		//   CREATE SEQUENCE IF NOT EXISTS reviews_db_id_seq;
		//   UPDATE pull_requests SET db_id = nextval('pull_requests_db_id_seq') WHERE db_id IS NULL;
		//   UPDATE reviews SET db_id = nextval('reviews_db_id_seq') WHERE db_id IS NULL;
		//   DELETE FROM reviews r USING (
		//     SELECT db_id FROM (
		//       SELECT db_id, ROW_NUMBER() OVER (
		//         PARTITION BY repo_full_name, pr_number, reviewer_login, submitted_at
		//         ORDER BY db_id
		//       ) AS rn
		//       FROM reviews
		//     ) d WHERE rn > 1
		//   ) dup WHERE r.db_id = dup.db_id;
		//   CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_reviews_dedup
		//     ON reviews(repo_full_name, pr_number, reviewer_login, submitted_at);
		//
		// After those finish, deploy — the steps below complete in milliseconds
		// because the columns and index already exist.

		// ── pull_requests ────────────────────────────────────────────────────────
		// id is kept as a stored column; no unique index needed because UpsertPR
		// deduplicates via ON CONFLICT(repo_full_name, number).
		`ALTER TABLE pull_requests ADD COLUMN IF NOT EXISTS db_id BIGINT`,
		`CREATE SEQUENCE IF NOT EXISTS pull_requests_db_id_seq`,
		`UPDATE pull_requests SET db_id = nextval('pull_requests_db_id_seq') WHERE db_id IS NULL`,
		`DO $$ BEGIN ALTER TABLE pull_requests ALTER COLUMN db_id SET DEFAULT nextval('pull_requests_db_id_seq'); EXCEPTION WHEN OTHERS THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE pull_requests ALTER COLUMN db_id SET NOT NULL; EXCEPTION WHEN OTHERS THEN NULL; END $$`,
		`SELECT setval('pull_requests_db_id_seq', COALESCE((SELECT MAX(db_id) FROM pull_requests), 1))`,
		// Drop old TEXT PK only if it still points to id.
		`DO $$ DECLARE col text; BEGIN SELECT a.attname INTO col FROM pg_constraint c JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=c.conkey[1] WHERE c.conname='pull_requests_pkey' AND c.conrelid='pull_requests'::regclass AND c.contype='p'; IF col='id' THEN ALTER TABLE pull_requests DROP CONSTRAINT pull_requests_pkey; END IF; EXCEPTION WHEN OTHERS THEN NULL; END $$`,
		// Add BIGINT PK if none exists.
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='pull_requests_pkey' AND conrelid='pull_requests'::regclass AND contype='p') THEN ALTER TABLE pull_requests ADD PRIMARY KEY (db_id); END IF; END $$`,

		// ── reviews ──────────────────────────────────────────────────────────────
		// Deduplication moves from ON CONFLICT(id) to the composite unique index
		// idx_reviews_dedup(repo_full_name, pr_number, reviewer_login, submitted_at).
		// id is retained as a stored column for audit purposes.
		`ALTER TABLE reviews ADD COLUMN IF NOT EXISTS db_id BIGINT`,
		`CREATE SEQUENCE IF NOT EXISTS reviews_db_id_seq`,
		`UPDATE reviews SET db_id = nextval('reviews_db_id_seq') WHERE db_id IS NULL`,
		`DO $$ BEGIN ALTER TABLE reviews ALTER COLUMN db_id SET DEFAULT nextval('reviews_db_id_seq'); EXCEPTION WHEN OTHERS THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE reviews ALTER COLUMN db_id SET NOT NULL; EXCEPTION WHEN OTHERS THEN NULL; END $$`,
		`SELECT setval('reviews_db_id_seq', COALESCE((SELECT MAX(db_id) FROM reviews), 1))`,
		// Older versions used GitHub's review ID as the primary key, but retries
		// and pagination edge cases could still leave duplicate logical reviews in
		// local/dev databases. Keep the oldest row so the unique dedupe index can
		// be created idempotently.
		`DELETE FROM reviews r USING (
			SELECT db_id FROM (
				SELECT db_id,
				       ROW_NUMBER() OVER (
				           PARTITION BY repo_full_name, pr_number, reviewer_login, submitted_at
				           ORDER BY db_id
				       ) AS rn
				FROM reviews
			) d
			WHERE rn > 1
		) dup
		WHERE r.db_id = dup.db_id`,
		// NOT CONCURRENTLY — CONCURRENTLY cannot run inside a transaction and
		// sql.DB.Exec uses implicit transactions, which caused this to silently
		// fail and left UpsertReview broken (no unique constraint to match ON CONFLICT).
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_reviews_dedup ON reviews(repo_full_name, pr_number, reviewer_login, submitted_at)`,

		// Small B-tree prefix indexes for Data Explorer searches.
		// text_pattern_ops allows LIKE 'foo%' (prefix) queries without locale constraints.
		`CREATE INDEX IF NOT EXISTS idx_prs_author_prefix ON pull_requests(author_login text_pattern_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_prs_repo_prefix ON pull_requests(repo_full_name text_pattern_ops)`,
		`CREATE INDEX IF NOT EXISTS idx_rev_reviewer_prefix ON reviews(reviewer_login text_pattern_ops)`,

		// Drop old TEXT PK only if it still points to id.
		`DO $$ DECLARE col text; BEGIN SELECT a.attname INTO col FROM pg_constraint c JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=c.conkey[1] WHERE c.conname='reviews_pkey' AND c.conrelid='reviews'::regclass AND c.contype='p'; IF col='id' THEN ALTER TABLE reviews DROP CONSTRAINT reviews_pkey; END IF; EXCEPTION WHEN OTHERS THEN NULL; END $$`,
		// Add BIGINT PK if none exists.
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='reviews_pkey' AND conrelid='reviews'::regclass AND contype='p') THEN ALTER TABLE reviews ADD PRIMARY KEY (db_id); END IF; END $$`,
	}
	for _, s := range stmts {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// ── Upserts ────────────────────────────────────────────────────────────────────

func (d *DB) UpsertRepo(r Repo) error {
	_, err := d.conn.Exec(`
		INSERT INTO repos (full_name, owner, name, description, stars, language, org_name, sync_status, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
		ON CONFLICT(full_name) DO UPDATE SET
			description  = EXCLUDED.description,
			stars        = EXCLUDED.stars,
			language     = EXCLUDED.language,
			org_name     = EXCLUDED.org_name,
			updated_at   = NOW()
	`, r.FullName, r.Owner, r.Name, r.Description, r.Stars, r.Language, r.OrgName, r.SyncStatus)
	return err
}

func (d *DB) UpdateSyncStatus(fullName, status string) error {
	switch status {
	case "done":
		_, err := d.conn.Exec(
			`UPDATE repos SET sync_status=$1, last_synced=NOW(), updated_at=NOW(), error_count=0 WHERE full_name=$2`,
			status, fullName)
		return err
	case "error":
		_, err := d.conn.Exec(
			`UPDATE repos SET sync_status=$1, updated_at=NOW(), error_count=error_count+1 WHERE full_name=$2`,
			status, fullName)
		return err
	default:
		_, err := d.conn.Exec(
			`UPDATE repos SET sync_status=$1, updated_at=NOW() WHERE full_name=$2`,
			status, fullName)
		return err
	}
}

func (d *DB) UpsertPR(pr PullRequest) error {
	_, err := d.conn.Exec(`
		INSERT INTO pull_requests
			(id, repo_full_name, number, title, author_login, merged, opened_at, merged_at, merge_time_secs, review_count, changes_requested_count, additions, deletions, first_review_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT(repo_full_name, number) DO UPDATE SET
			title                   = EXCLUDED.title,
			merged                  = EXCLUDED.merged,
			merged_at               = EXCLUDED.merged_at,
			merge_time_secs         = EXCLUDED.merge_time_secs,
			review_count            = EXCLUDED.review_count,
			changes_requested_count = EXCLUDED.changes_requested_count,
			additions               = EXCLUDED.additions,
			deletions               = EXCLUDED.deletions,
			first_review_at         = EXCLUDED.first_review_at
	`, pr.ID, pr.RepoFullName, pr.Number, pr.Title, pr.AuthorLogin, pr.Merged,
		pr.OpenedAt.UTC(), pr.MergedAt, pr.MergeTimeSecs,
		pr.ReviewCount, pr.ChangesRequestedCount, pr.Additions, pr.Deletions, pr.FirstReviewAt)
	return err
}

func (d *DB) UpsertReview(r Review) error {
	_, err := d.conn.Exec(`
		INSERT INTO reviews (id, repo_full_name, pr_number, reviewer_login, state, submitted_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT(repo_full_name, pr_number, reviewer_login, submitted_at) DO NOTHING
	`, r.ID, r.RepoFullName, r.PRNumber, r.ReviewerLogin, r.State, r.SubmittedAt.UTC())
	return err
}

func (d *DB) UpsertUser(u User) error {
	_, err := d.conn.Exec(`
		INSERT INTO users (login, name, avatar_url, bio, public_repos, followers, company, location, is_org, last_fetched)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
		ON CONFLICT(login) DO UPDATE SET
			name         = EXCLUDED.name,
			avatar_url   = EXCLUDED.avatar_url,
			bio          = EXCLUDED.bio,
			public_repos = EXCLUDED.public_repos,
			followers    = EXCLUDED.followers,
			company      = EXCLUDED.company,
			location     = EXCLUDED.location,
			is_org       = EXCLUDED.is_org,
			last_fetched = NOW()
	`, u.Login, u.Name, u.AvatarURL, u.Bio, u.PublicRepos, u.Followers,
		u.Company, u.Location, u.IsOrg)
	return err
}

// ── Stats recalculation ────────────────────────────────────────────────────────

func (d *DB) UpdateRepoStats(fullName string) error {
	_, err := d.conn.Exec(`
		WITH stats AS (
			SELECT
				COUNT(*)                            AS cnt,
				COALESCE(AVG(merge_time_secs)::BIGINT, 0) AS avg_secs,
				COALESCE(MIN(merge_time_secs), 0)   AS min_secs,
				COALESCE(MAX(merge_time_secs), 0)   AS max_secs
			FROM pull_requests
			WHERE repo_full_name=$1 AND merged=TRUE
		)
		UPDATE repos SET
			pr_count            = stats.cnt,
			merged_pr_count     = stats.cnt,
			avg_merge_time_secs = stats.avg_secs,
			min_merge_time_secs = stats.min_secs,
			max_merge_time_secs = stats.max_secs,
			updated_at          = NOW()
		FROM stats
		WHERE full_name=$1
	`, fullName)
	return err
}

// ── Getters ────────────────────────────────────────────────────────────────────

func (d *DB) GetRepo(fullName string) (*Repo, error) {
	r := &Repo{}
	var lastSynced sql.NullTime
	err := d.conn.QueryRow(`
		SELECT full_name, owner, name, description, stars, language, org_name,
		       last_synced, sync_status, pr_count, merged_pr_count,
		       avg_merge_time_secs, min_merge_time_secs, max_merge_time_secs
		FROM repos WHERE full_name=$1
	`, fullName).Scan(
		&r.FullName, &r.Owner, &r.Name, &r.Description, &r.Stars, &r.Language, &r.OrgName,
		&lastSynced, &r.SyncStatus, &r.PRCount, &r.MergedPRCount,
		&r.AvgMergeTimeSecs, &r.MinMergeTimeSecs, &r.MaxMergeTimeSecs,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if lastSynced.Valid {
		t := lastSynced.Time
		r.LastSynced = &t
	}
	return r, nil
}

func (d *DB) GetUser(login string) (*User, error) {
	u := &User{}
	var lastFetched sql.NullTime
	err := d.conn.QueryRow(`
		SELECT login, name, avatar_url, bio, public_repos, followers, company, location, is_org, last_fetched
		FROM users WHERE login=$1
	`, login).Scan(
		&u.Login, &u.Name, &u.AvatarURL, &u.Bio, &u.PublicRepos, &u.Followers,
		&u.Company, &u.Location, &u.IsOrg, &lastFetched,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if lastFetched.Valid {
		t := lastFetched.Time
		u.LastFetched = &t
	}
	return u, nil
}

// ── Materialized leaderboard refresh ───────────────────────────────────────────

// RefreshLeaderboards rebuilds the four mat_leaderboard_* tables in a single
// transaction. Readers see either the old complete dataset or the new one —
// never an empty intermediate state. Call this from a background goroutine;
// it runs the same heavy GROUP BY queries that previously ran on every request.
func (d *DB) RefreshLeaderboards() error {
	ctx := context.Background()
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("RefreshLeaderboards begin: %w", err)
	}
	defer tx.Rollback()

	steps := []string{
		// ── Total stats (global counts) ────────────────────────────────────────
		`UPDATE mat_total_stats SET
			repos_done    = (SELECT COUNT(*) FROM repos WHERE sync_status='done'),
			merged_prs    = (SELECT COUNT(*) FROM pull_requests WHERE merged=TRUE),
			total_reviews = (SELECT COUNT(*) FROM reviews)
		 WHERE id = 1`,

		// ── Repo contribs (distinct PR authors per repo) ────────────────────────
		// TRUNCATE avoids dead tuples; safe because the stats page is Redis-cached
		// so the brief empty window during this refresh is never visible to users.
		`TRUNCATE mat_repo_contribs`,
		`INSERT INTO mat_repo_contribs (repo_full_name, distinct_authors)
		 SELECT repo_full_name, COUNT(DISTINCT author_login)
		 FROM pull_requests WHERE merged=TRUE
		 GROUP BY repo_full_name`,

		// ── Reviewers ──────────────────────────────────────────────────────────
		`DELETE FROM mat_leaderboard_reviewers`,
		`INSERT INTO mat_leaderboard_reviewers
			  (rank, login, avatar_url, total_reviews, approvals, changes_requested)
		 WITH top AS (
		 	SELECT r.reviewer_login,
		 	       MAX(COALESCE(u.avatar_url,''))                                  AS avatar_url,
		 	       COUNT(*)                                                        AS total_reviews,
		 	       SUM(CASE WHEN r.state='APPROVED'           THEN 1 ELSE 0 END)  AS approvals,
		 	       SUM(CASE WHEN r.state='CHANGES_REQUESTED'  THEN 1 ELSE 0 END)  AS changes_requested
		 	FROM reviews r
		 	LEFT JOIN users u ON u.login = r.reviewer_login
		 	GROUP BY r.reviewer_login
		 	ORDER BY total_reviews DESC
		 	LIMIT 10000
		 )
		 SELECT ROW_NUMBER() OVER (ORDER BY total_reviews DESC),
		        reviewer_login, avatar_url, total_reviews, approvals, changes_requested
		 FROM top`,

		// ── Gatekeepers ────────────────────────────────────────────────────────
		`DELETE FROM mat_leaderboard_gatekeepers`,
		`INSERT INTO mat_leaderboard_gatekeepers
			  (rank, login, avatar_url, total, approvals, changes_requested)
		 WITH top AS (
		 	SELECT r.reviewer_login,
		 	       MAX(COALESCE(u.avatar_url,''))                                  AS avatar_url,
		 	       COUNT(*)                                                        AS total,
		 	       SUM(CASE WHEN r.state='APPROVED'           THEN 1 ELSE 0 END)  AS approvals,
		 	       SUM(CASE WHEN r.state='CHANGES_REQUESTED'  THEN 1 ELSE 0 END)  AS changes_requested
		 	FROM reviews r
		 	LEFT JOIN users u ON u.login = r.reviewer_login
		 	WHERE r.state = 'CHANGES_REQUESTED'
		 	GROUP BY r.reviewer_login
		 	ORDER BY total DESC
		 	LIMIT 10000
		 )
		 SELECT ROW_NUMBER() OVER (ORDER BY total DESC),
		        reviewer_login, avatar_url, total, approvals, changes_requested
		 FROM top`,

		// ── Authors ────────────────────────────────────────────────────────────
		`DELETE FROM mat_leaderboard_authors`,
		`INSERT INTO mat_leaderboard_authors
			  (rank, login, avatar_url, merged_prs, avg_merge_time_secs)
		 WITH top AS (
		 	SELECT p.author_login,
		 	       MAX(COALESCE(u.avatar_url,''))         AS avatar_url,
		 	       COUNT(*)                               AS merged_prs,
		 	       COALESCE(AVG(p.merge_time_secs),0)::BIGINT AS avg_merge_time_secs
		 	FROM pull_requests p
		 	LEFT JOIN users u ON u.login = p.author_login
		 	WHERE p.merged = TRUE
		 	GROUP BY p.author_login
		 	ORDER BY merged_prs DESC
		 	LIMIT 10000
		 )
		 SELECT ROW_NUMBER() OVER (ORDER BY merged_prs DESC),
		        author_login, avatar_url, merged_prs, avg_merge_time_secs
		 FROM top`,

		// ── Clean approvals ────────────────────────────────────────────────────
		`DELETE FROM mat_leaderboard_clean`,
		`INSERT INTO mat_leaderboard_clean
			  (rank, repo_full_name, total_prs, clean_pct, avg_secs)
		 WITH top AS (
		 	SELECT repo_full_name,
		 	       COUNT(*)                                                                                 AS total_prs,
		 	       CAST(ROUND(100.0 * SUM(CASE WHEN changes_requested_count=0 AND review_count>0 THEN 1 ELSE 0 END) / COUNT(*)) AS INTEGER) AS clean_pct,
		 	       COALESCE(AVG(merge_time_secs),0)::BIGINT                                                AS avg_secs
		 	FROM pull_requests
		 	WHERE merged=TRUE AND review_count>0
		 	GROUP BY repo_full_name
		 	HAVING COUNT(*) >= 5
		 	ORDER BY clean_pct DESC
		 	LIMIT 10000
		 )
		 SELECT ROW_NUMBER() OVER (ORDER BY clean_pct DESC),
		        repo_full_name, total_prs, clean_pct, avg_secs
		 FROM top`,
	}

	for _, s := range steps {
		if _, err := tx.Exec(s); err != nil {
			return fmt.Errorf("RefreshLeaderboards: %w", err)
		}
	}
	return tx.Commit()
}

// ── Leaderboards ───────────────────────────────────────────────────────────────

func (d *DB) LeaderboardReposBySpeed(order string, limit int) ([]LeaderboardEntry, error) {
	q := fmt.Sprintf(`
		SELECT full_name, avg_merge_time_secs, merged_pr_count
		FROM repos
		WHERE merged_pr_count >= 3 AND avg_merge_time_secs > 0
		ORDER BY avg_merge_time_secs %s
		LIMIT $1`, order)
	return d.queryEntries(q, limit)
}

func (d *DB) LeaderboardReviewers(limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT login, total_reviews, avatar_url
		FROM mat_leaderboard_reviewers
		ORDER BY rank
		LIMIT $1`, limit)
}

func (d *DB) LeaderboardGatekeepers(limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT login, total, avatar_url
		FROM mat_leaderboard_gatekeepers
		ORDER BY rank
		LIMIT $1`, limit)
}

func (d *DB) LeaderboardAuthors(limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT login, merged_prs, avatar_url
		FROM mat_leaderboard_authors
		ORDER BY rank
		LIMIT $1`, limit)
}

func (d *DB) LeaderboardCleanApprovals(limit int) ([]LeaderboardEntry, error) {
	rows, err := d.conn.Query(`
		SELECT rank, repo_full_name, clean_pct
		FROM mat_leaderboard_clean
		ORDER BY rank
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []LeaderboardEntry
	for rows.Next() {
		var e LeaderboardEntry
		var cleanPct int
		if err := rows.Scan(&e.Rank, &e.Name, &cleanPct); err != nil {
			continue
		}
		e.Value = int64(cleanPct)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func (d *DB) queryEntries(query string, limit int) ([]LeaderboardEntry, error) {
	rows, err := d.conn.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("queryEntries: %w", err)
	}
	defer rows.Close()
	return scanLeaderboardEntries(rows)
}

func scanLeaderboardEntries(rows *sql.Rows) ([]LeaderboardEntry, error) {
	var entries []LeaderboardEntry
	rank := 1
	for rows.Next() {
		var e LeaderboardEntry
		var val int64
		if err := rows.Scan(&e.Name, &val, &e.Extra); err != nil {
			log.Printf("db: scanLeaderboardEntries scan error (rank %d): %v", rank, err)
			continue
		}
		e.Value = val
		e.Count = int(val)
		e.Rank = rank
		entries = append(entries, e)
		rank++
	}
	return entries, rows.Err()
}

// ── Repo detail queries ────────────────────────────────────────────────────────

func (d *DB) RepoTopReviewers(fullName string, limit int) ([]ReviewerStats, error) {
	rows, err := d.conn.Query(`
		SELECT r.reviewer_login,
		       MAX(COALESCE(u.avatar_url,'')),
		       COUNT(*) as total,
		       SUM(CASE WHEN r.state='APPROVED'           THEN 1 ELSE 0 END),
		       SUM(CASE WHEN r.state='CHANGES_REQUESTED'  THEN 1 ELSE 0 END),
		       SUM(CASE WHEN r.state='COMMENTED'          THEN 1 ELSE 0 END)
		FROM reviews r
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE r.repo_full_name=$1
		GROUP BY r.reviewer_login
		ORDER BY total DESC
		LIMIT $2
	`, fullName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stats []ReviewerStats
	for rows.Next() {
		var s ReviewerStats
		if err := rows.Scan(&s.Login, &s.AvatarURL, &s.TotalReviews, &s.Approvals, &s.ChangesRequested, &s.Comments); err != nil {
			continue
		}
		stats = append(stats, s)
	}
	return stats, rows.Err()
}

func (d *DB) RecentMergedPRs(fullName string, limit int) ([]PullRequest, error) {
	rows, err := d.conn.Query(`
		SELECT id, repo_full_name, number, title, author_login, merged,
		       opened_at, merged_at, merge_time_secs, review_count, changes_requested_count,
		       additions, deletions
		FROM pull_requests
		WHERE repo_full_name=$1 AND merged=TRUE
		ORDER BY merged_at DESC
		LIMIT $2
	`, fullName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPRs(rows)
}

// ── User queries ───────────────────────────────────────────────────────────────

func (d *DB) UserReviewerStats(login string) (*ReviewerStats, error) {
	s := &ReviewerStats{Login: login}
	err := d.conn.QueryRow(`
		SELECT MAX(COALESCE(u.avatar_url,'')),
		       COUNT(*),
		       SUM(CASE WHEN r.state='APPROVED'          THEN 1 ELSE 0 END),
		       SUM(CASE WHEN r.state='CHANGES_REQUESTED' THEN 1 ELSE 0 END),
		       SUM(CASE WHEN r.state='COMMENTED'         THEN 1 ELSE 0 END),
		       MAX(r.submitted_at)
		FROM reviews r
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE r.reviewer_login=$1
		GROUP BY r.reviewer_login
	`, login).Scan(&s.AvatarURL, &s.TotalReviews, &s.Approvals, &s.ChangesRequested, &s.Comments, &s.LastReviewedAt)
	if err == sql.ErrNoRows {
		return s, nil
	}
	return s, err
}

func (d *DB) UserAuthorStats(login string) (*AuthorStats, error) {
	s := &AuthorStats{Login: login}
	err := d.conn.QueryRow(`
		SELECT
			COUNT(*),
			SUM(CASE WHEN merged=TRUE THEN 1 ELSE 0 END),
			COALESCE(AVG(CASE WHEN merged=TRUE THEN merge_time_secs END)::BIGINT, 0),
			COALESCE(SUM(CASE WHEN merged=TRUE THEN additions + deletions ELSE 0 END)::BIGINT, 0),
			COALESCE(AVG(CASE WHEN merged=TRUE THEN (additions + deletions) END)::FLOAT, 0),
			COALESCE(
				100.0 * SUM(CASE WHEN merged=TRUE AND changes_requested_count=0 AND review_count>0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(SUM(CASE WHEN merged=TRUE AND review_count>0 THEN 1 ELSE 0 END), 0),
				0
			),
			COALESCE(AVG(CASE WHEN merged=TRUE THEN changes_requested_count END)::FLOAT, 0),
			COALESCE(AVG(CASE WHEN merged=TRUE AND first_review_at IS NOT NULL
				THEN GREATEST(0, EXTRACT(EPOCH FROM first_review_at) - EXTRACT(EPOCH FROM opened_at))
				END)::FLOAT, 0),
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY
				CASE WHEN merged=TRUE AND first_review_at IS NOT NULL
				THEN GREATEST(0, EXTRACT(EPOCH FROM first_review_at) - EXTRACT(EPOCH FROM opened_at))
				END), 0)
		FROM pull_requests
		WHERE author_login=$1
	`, login).Scan(
		&s.TotalPRs, &s.MergedPRs, &s.AvgMergeTimeSecs,
		&s.TotalLinesWritten, &s.AvgPRSize,
		&s.CleanApprovalRate, &s.AvgChangesRequested,
		&s.AvgFirstReviewSecs, &s.MedFirstReviewSecs,
	)
	if err == sql.ErrNoRows {
		return s, nil
	}
	return s, err
}

func (d *DB) UserReviewerRank(login string) (int, error) {
	return d.rankQuery(`SELECT rank FROM mat_leaderboard_reviewers WHERE login=$1`, login)
}

func (d *DB) UserGatekeeperRank(login string) (int, error) {
	return d.rankQuery(`SELECT rank FROM mat_leaderboard_gatekeepers WHERE login=$1`, login)
}

func (d *DB) RepoSpeedRank(fullName string) (int, error) {
	return d.rankQuery(`
		SELECT rank FROM (
			SELECT full_name, ROW_NUMBER() OVER (ORDER BY avg_merge_time_secs ASC) as rank
			FROM repos WHERE merged_pr_count>=3 AND avg_merge_time_secs>0
		) sub WHERE full_name=$1
	`, fullName)
}

func (d *DB) RepoGraveyardRank(fullName string) (int, error) {
	return d.rankQuery(`
		SELECT rank FROM (
			SELECT full_name, ROW_NUMBER() OVER (ORDER BY avg_merge_time_secs DESC) as rank
			FROM repos WHERE merged_pr_count>=3 AND avg_merge_time_secs>0
		) sub WHERE full_name=$1
	`, fullName)
}

func (d *DB) UserAuthorRank(login string) (int, error) {
	return d.rankQuery(`SELECT rank FROM mat_leaderboard_authors WHERE login=$1`, login)
}

func (d *DB) rankQuery(q, arg string) (int, error) {
	var rank int
	err := d.conn.QueryRow(q, arg).Scan(&rank)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return rank, err
}

// ── Org queries ────────────────────────────────────────────────────────────────

func (d *DB) OrgRepos(orgName string) ([]Repo, error) {
	rows, err := d.conn.Query(`
		SELECT full_name, owner, name, description, stars, language, org_name,
		       last_synced, sync_status, pr_count, merged_pr_count,
		       avg_merge_time_secs, min_merge_time_secs, max_merge_time_secs
		FROM repos WHERE org_name=$1
		ORDER BY merged_pr_count DESC
		LIMIT 200
	`, orgName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRepos(rows)
}

func (d *DB) OrgReviewerLeaderboard(orgName string, limit int) ([]LeaderboardEntry, error) {
	rows, err := d.conn.Query(`
		SELECT r.reviewer_login, COUNT(*) as cnt, MAX(COALESCE(u.avatar_url,''))
		FROM reviews r
		JOIN repos repo ON repo.full_name=r.repo_full_name
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE repo.org_name=$1
		GROUP BY r.reviewer_login
		ORDER BY cnt DESC
		LIMIT $2`, orgName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLeaderboardEntries(rows)
}

func (d *DB) OrgGatekeeperLeaderboard(orgName string, limit int) ([]LeaderboardEntry, error) {
	rows, err := d.conn.Query(`
		SELECT r.reviewer_login, COUNT(*) as cnt, MAX(COALESCE(u.avatar_url,''))
		FROM reviews r
		JOIN repos repo ON repo.full_name=r.repo_full_name
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE repo.org_name=$1 AND r.state='CHANGES_REQUESTED'
		GROUP BY r.reviewer_login
		ORDER BY cnt DESC
		LIMIT $2`, orgName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLeaderboardEntries(rows)
}

// ── Global stats ───────────────────────────────────────────────────────────────

func (d *DB) TotalStats() (repos, prs, reviews int) {
	d.conn.QueryRow(`
		SELECT repos_done, merged_prs, total_reviews FROM mat_total_stats WHERE id=1
	`).Scan(&repos, &prs, &reviews)
	return
}

// ── Full leaderboard rows (for dedicated leaderboard pages) ───────────────────

type RepoLeaderboardRow struct {
	Rank     int
	FullName string
	AvgSecs  int64
	MinSecs  int64
	MaxSecs  int64
	PRCount  int
}

type UserLeaderboardRow struct {
	Rank             int
	Login            string
	AvatarURL        string
	Total            int
	Approvals        int
	ChangesRequested int
	MergedPRs        int
	AvgMergeTimeSecs int64
}

type CleanLeaderboardRow struct {
	Rank     int
	FullName string
	CleanPct int
	Total    int
	AvgSecs  int64
}

func (d *DB) FullLeaderboardRepoSpeed(order string, limit, offset int) ([]RepoLeaderboardRow, error) {
	q := fmt.Sprintf(`
		SELECT full_name,
		       avg_merge_time_secs,
		       min_merge_time_secs,
		       max_merge_time_secs,
		       merged_pr_count
		FROM repos
		WHERE merged_pr_count >= 3 AND avg_merge_time_secs > 0
		ORDER BY avg_merge_time_secs %s
		LIMIT $1 OFFSET $2`, order)
	rows, err := d.conn.Query(q, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("FullLeaderboardRepoSpeed: %w", err)
	}
	defer rows.Close()
	var out []RepoLeaderboardRow
	rank := offset + 1
	for rows.Next() {
		var r RepoLeaderboardRow
		if err := rows.Scan(&r.FullName, &r.AvgSecs, &r.MinSecs, &r.MaxSecs, &r.PRCount); err != nil {
			log.Printf("db: FullLeaderboardRepoSpeed scan error: %v", err)
			continue
		}
		r.Rank = rank
		out = append(out, r)
		rank++
	}
	return out, rows.Err()
}

func (d *DB) FullLeaderboardReviewers(limit, offset int) ([]UserLeaderboardRow, error) {
	rows, err := d.conn.Query(`
		SELECT rank, login, avatar_url, total_reviews, approvals, changes_requested
		FROM mat_leaderboard_reviewers
		ORDER BY rank
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMatUserRows(rows)
}

func (d *DB) FullLeaderboardGatekeepers(limit, offset int) ([]UserLeaderboardRow, error) {
	rows, err := d.conn.Query(`
		SELECT rank, login, avatar_url, total, approvals, changes_requested
		FROM mat_leaderboard_gatekeepers
		ORDER BY rank
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMatUserRows(rows)
}

func (d *DB) FullLeaderboardAuthors(limit, offset int) ([]UserLeaderboardRow, error) {
	rows, err := d.conn.Query(`
		SELECT rank, login, avatar_url, merged_prs, avg_merge_time_secs
		FROM mat_leaderboard_authors
		ORDER BY rank
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("FullLeaderboardAuthors: %w", err)
	}
	defer rows.Close()
	var out []UserLeaderboardRow
	rank := offset + 1
	for rows.Next() {
		var r UserLeaderboardRow
		if err := rows.Scan(&r.Rank, &r.Login, &r.AvatarURL, &r.MergedPRs, &r.AvgMergeTimeSecs); err != nil {
			log.Printf("db: FullLeaderboardAuthors scan error: %v", err)
			continue
		}
		r.Total = r.MergedPRs
		_ = rank
		out = append(out, r)
		rank++
	}
	return out, rows.Err()
}

func (d *DB) FullLeaderboardCleanApprovals(limit, offset int) ([]CleanLeaderboardRow, error) {
	rows, err := d.conn.Query(`
		SELECT rank, repo_full_name, total_prs, clean_pct, avg_secs
		FROM mat_leaderboard_clean
		ORDER BY rank
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CleanLeaderboardRow
	for rows.Next() {
		var r CleanLeaderboardRow
		if err := rows.Scan(&r.Rank, &r.FullName, &r.Total, &r.CleanPct, &r.AvgSecs); err != nil {
			log.Printf("db: FullLeaderboardCleanApprovals scan error: %v", err)
			continue
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// scanMatUserRows scans rows from mat_leaderboard_reviewers / mat_leaderboard_gatekeepers
// which have columns: rank, login, avatar_url, total, approvals, changes_requested.
func scanMatUserRows(rows *sql.Rows) ([]UserLeaderboardRow, error) {
	var out []UserLeaderboardRow
	for rows.Next() {
		var r UserLeaderboardRow
		if err := rows.Scan(&r.Rank, &r.Login, &r.AvatarURL, &r.Total, &r.Approvals, &r.ChangesRequested); err != nil {
			log.Printf("db: scanMatUserRows scan error: %v", err)
			continue
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanUserRows(rows *sql.Rows, startRank int) ([]UserLeaderboardRow, error) {
	var out []UserLeaderboardRow
	rank := startRank
	for rows.Next() {
		var r UserLeaderboardRow
		if err := rows.Scan(&r.Login, &r.AvatarURL, &r.Total, &r.Approvals, &r.ChangesRequested); err != nil {
			log.Printf("db: scanUserRows scan error: %v", err)
			continue
		}
		r.Rank = rank
		out = append(out, r)
		rank++
	}
	return out, rows.Err()
}

// ── Search ─────────────────────────────────────────────────────────────────────

func (d *DB) UserContributedRepos(login string, limit int) ([]Repo, error) {
	rows, err := d.conn.Query(`
		SELECT r.full_name, r.owner, r.name, r.description, r.stars, r.language, r.org_name,
		       r.last_synced, r.sync_status, r.pr_count, r.merged_pr_count,
		       r.avg_merge_time_secs, r.min_merge_time_secs, r.max_merge_time_secs
		FROM repos r
		WHERE r.full_name IN (
			SELECT DISTINCT repo_full_name FROM pull_requests WHERE author_login=$1
			UNION
			SELECT DISTINCT repo_full_name FROM reviews WHERE reviewer_login=$2
		)
		AND r.merged_pr_count > 0
		ORDER BY r.merged_pr_count DESC
		LIMIT $3
	`, login, login, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRepos(rows)
}

// UserRecordPRs returns the fastest and slowest merged PRs for a user.
func (d *DB) UserRecordPRs(login string) (fastest, slowest *UserRecordPR, err error) {
	q := `
		SELECT number, repo_full_name, merge_time_secs, title
		FROM pull_requests
		WHERE author_login=$1 AND merged=TRUE AND merge_time_secs > 0
		ORDER BY merge_time_secs %s
		LIMIT 1
	`
	var f, s UserRecordPR
	if err2 := d.conn.QueryRow(fmt.Sprintf(q, "ASC"), login).Scan(&f.Number, &f.RepoFullName, &f.MergeTimeSecs, &f.Title); err2 == nil {
		fastest = &f
	}
	if err2 := d.conn.QueryRow(fmt.Sprintf(q, "DESC"), login).Scan(&s.Number, &s.RepoFullName, &s.MergeTimeSecs, &s.Title); err2 == nil {
		slowest = &s
	}
	return fastest, slowest, nil
}

// UserActivitySeries returns monthly counts of PRs authored and reviews given.
func (d *DB) UserActivitySeries(login string) ([]UserActivityPoint, error) {
	rows, err := d.conn.Query(`
		WITH pr_months AS (
			SELECT DATE_TRUNC('month', merged_at) AS month,
			       TO_CHAR(DATE_TRUNC('month', merged_at), 'Mon YYYY') AS label,
			       COUNT(*) AS pr_count,
			       COALESCE(
			           100.0 * SUM(CASE WHEN changes_requested_count > 0 THEN 1 ELSE 0 END)::FLOAT /
			           NULLIF(COUNT(*), 0), 0
			       ) AS cr_rate
			FROM pull_requests
			WHERE author_login=$1 AND merged=TRUE AND merged_at IS NOT NULL
			GROUP BY DATE_TRUNC('month', merged_at)
		),
		review_months AS (
			SELECT DATE_TRUNC('month', submitted_at) AS month,
			       COUNT(*) AS review_count
			FROM reviews
			WHERE reviewer_login=$1 AND submitted_at IS NOT NULL
			GROUP BY DATE_TRUNC('month', submitted_at)
		)
		SELECT
			COALESCE(p.label, TO_CHAR(r.month, 'Mon YYYY')) AS label,
			COALESCE(p.pr_count, 0)     AS pr_count,
			COALESCE(r.review_count, 0) AS review_count,
			COALESCE(p.cr_rate, 0)      AS cr_rate
		FROM pr_months p
		FULL OUTER JOIN review_months r ON p.month = r.month
		ORDER BY COALESCE(p.month, r.month)
	`, login)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UserActivityPoint
	for rows.Next() {
		var p UserActivityPoint
		if err := rows.Scan(&p.Label, &p.PRCount, &p.ReviewCount, &p.ChangesRequestedRate); err != nil {
			continue
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// UserTopCollaborators returns the top reviewers of a user's PRs and the top
// authors whose PRs the user reviews most, each limited to `limit` entries.
func (d *DB) UserTopCollaborators(login string, limit int) (reviewersOfMe, authorsIReview []CollabEntry, err error) {
	reviewQ := `
		SELECT rv.reviewer_login, COALESCE(u.avatar_url,''), COUNT(*) AS cnt
		FROM reviews rv
		JOIN pull_requests pr
		  ON rv.repo_full_name = pr.repo_full_name AND rv.pr_number = pr.number
		LEFT JOIN users u ON u.login = rv.reviewer_login
		WHERE pr.author_login = $1 AND rv.reviewer_login != $1
		GROUP BY rv.reviewer_login, u.avatar_url
		ORDER BY cnt DESC
		LIMIT $2
	`
	rows, err := d.conn.Query(reviewQ, login, limit)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var e CollabEntry
			if err2 := rows.Scan(&e.Login, &e.AvatarURL, &e.Count); err2 == nil {
				reviewersOfMe = append(reviewersOfMe, e)
			}
		}
	}

	authorQ := `
		SELECT pr.author_login, COALESCE(u.avatar_url,''), COUNT(*) AS cnt
		FROM reviews rv
		JOIN pull_requests pr
		  ON rv.repo_full_name = pr.repo_full_name AND rv.pr_number = pr.number
		LEFT JOIN users u ON u.login = pr.author_login
		WHERE rv.reviewer_login = $1 AND pr.author_login != $1
		GROUP BY pr.author_login, u.avatar_url
		ORDER BY cnt DESC
		LIMIT $2
	`
	rows2, err2 := d.conn.Query(authorQ, login, limit)
	if err2 == nil {
		defer rows2.Close()
		for rows2.Next() {
			var e CollabEntry
			if err3 := rows2.Scan(&e.Login, &e.AvatarURL, &e.Count); err3 == nil {
				authorsIReview = append(authorsIReview, e)
			}
		}
	}
	return reviewersOfMe, authorsIReview, nil
}

// UserTopReviewedRepos returns the repos a user has reviewed the most PRs in.
func (d *DB) UserTopReviewedRepos(login string, limit int) ([]UserRepoReview, error) {
	rows, err := d.conn.Query(`
		SELECT repo_full_name, COUNT(*) AS cnt
		FROM reviews
		WHERE reviewer_login=$1
		GROUP BY repo_full_name
		ORDER BY cnt DESC
		LIMIT $2
	`, login, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UserRepoReview
	for rows.Next() {
		var r UserRepoReview
		if err := rows.Scan(&r.FullName, &r.Count); err != nil {
			continue
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UserPRSizeDist returns the distribution of a user's merged PRs across size buckets.
func (d *DB) UserPRSizeDist(login string) ([]PRSizeBucket, error) {
	rows, err := d.conn.Query(`
		SELECT
			CASE
				WHEN additions+deletions <=   50 THEN '≤50'
				WHEN additions+deletions <=  200 THEN '51–200'
				WHEN additions+deletions <=  500 THEN '201–500'
				WHEN additions+deletions <= 1000 THEN '501–1k'
				WHEN additions+deletions <= 5000 THEN '1k–5k'
				ELSE '5k+'
			END AS bucket,
			COUNT(*) AS pr_count
		FROM pull_requests
		WHERE author_login=$1 AND merged=TRUE AND (additions+deletions) > 0
		GROUP BY bucket
		ORDER BY MIN(additions+deletions)
	`, login)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PRSizeBucket
	for rows.Next() {
		var b PRSizeBucket
		if err := rows.Scan(&b.Label, &b.PRCount); err != nil {
			continue
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (d *DB) SearchRepos(query string, limit int) ([]Repo, error) {
	rows, err := d.conn.Query(`
		SELECT full_name, owner, name, description, stars, language, org_name,
		       last_synced, sync_status, pr_count, merged_pr_count,
		       avg_merge_time_secs, min_merge_time_secs, max_merge_time_secs
		FROM repos
		WHERE full_name ILIKE $1 OR name ILIKE $2
		ORDER BY stars DESC
		LIMIT $3
	`, "%"+query+"%", "%"+query+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRepos(rows)
}

// ── Scanners ───────────────────────────────────────────────────────────────────

func scanRepos(rows *sql.Rows) ([]Repo, error) {
	var repos []Repo
	for rows.Next() {
		var r Repo
		var lastSynced sql.NullTime
		if err := rows.Scan(
			&r.FullName, &r.Owner, &r.Name, &r.Description, &r.Stars, &r.Language, &r.OrgName,
			&lastSynced, &r.SyncStatus, &r.PRCount, &r.MergedPRCount,
			&r.AvgMergeTimeSecs, &r.MinMergeTimeSecs, &r.MaxMergeTimeSecs,
		); err != nil {
			log.Printf("db: scanRepos scan error: %v", err)
			continue
		}
		if lastSynced.Valid {
			t := lastSynced.Time
			r.LastSynced = &t
		}
		repos = append(repos, r)
	}
	return repos, rows.Err()
}

func scanPRs(rows *sql.Rows) ([]PullRequest, error) {
	var prs []PullRequest
	for rows.Next() {
		var pr PullRequest
		var mergedAt sql.NullTime
		var mts sql.NullInt64
		if err := rows.Scan(
			&pr.ID, &pr.RepoFullName, &pr.Number, &pr.Title, &pr.AuthorLogin, &pr.Merged,
			&pr.OpenedAt, &mergedAt, &mts, &pr.ReviewCount, &pr.ChangesRequestedCount,
			&pr.Additions, &pr.Deletions,
		); err != nil {
			continue
		}
		if mergedAt.Valid {
			t := mergedAt.Time
			pr.MergedAt = &t
		}
		if mts.Valid {
			pr.MergeTimeSecs = &mts.Int64
		}
		prs = append(prs, pr)
	}
	return prs, rows.Err()
}

// ── PR size chart data ─────────────────────────────────────────────────────────

// PRSizeBucket holds aggregated stats for one size range of pull requests.
type PRSizeBucket struct {
	Label        string
	PRCount      int
	AvgSecs      float64
	ApprovalRate float64
}

// RepoSizeChartData returns PR count, avg review time, and approval rate
// grouped into five size buckets. cutoffPct (0.0–1.0) trims high-outlier PRs
// by merge time; pass 1.0 for no trimming.
func (d *DB) RepoSizeChartData(fullName string, cutoffPct float64) ([]PRSizeBucket, error) {
	rows, err := d.conn.Query(`
		WITH cutoff AS (
			SELECT COALESCE(
				percentile_cont($2) WITHIN GROUP (ORDER BY merge_time_secs::FLOAT),
				9999999999.0
			) AS p
			FROM pull_requests WHERE repo_full_name=$1 AND merged=TRUE AND merge_time_secs > 0
		)
		SELECT
			CASE
				WHEN (additions + deletions) <= 50   THEN 1
				WHEN (additions + deletions) <= 200  THEN 2
				WHEN (additions + deletions) <= 500  THEN 3
				WHEN (additions + deletions) <= 1000 THEN 4
				WHEN (additions + deletions) <= 5000 THEN 5
				ELSE 6
			END AS bucket,
			COUNT(*) AS pr_count,
			COALESCE(AVG(merge_time_secs)::FLOAT, 0) AS avg_secs,
			COALESCE(
				100.0 * SUM(CASE WHEN changes_requested_count=0 AND review_count>0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(SUM(CASE WHEN review_count>0 THEN 1 ELSE 0 END), 0),
				0
			) AS approval_rate
		FROM pull_requests, cutoff
		WHERE repo_full_name=$1 AND merged=TRUE AND (additions + deletions) > 0
		  AND (merge_time_secs IS NULL OR merge_time_secs::FLOAT <= p)
		GROUP BY bucket
		ORDER BY bucket
	`, fullName, cutoffPct)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	labels := []string{"≤50", "51–200", "201–500", "501–1k", "1k–5k", "5k+"}
	var out []PRSizeBucket
	for rows.Next() {
		var bucketNum int
		var b PRSizeBucket
		if err := rows.Scan(&bucketNum, &b.PRCount, &b.AvgSecs, &b.ApprovalRate); err != nil {
			continue
		}
		if bucketNum >= 1 && bucketNum <= 6 {
			b.Label = labels[bucketNum-1]
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ── Global stats (cross-repo aggregations) ────────────────────────────────────

// GlobalSizeBucket holds aggregated stats for one size range across all repos.
type GlobalSizeBucket struct {
	Label                string
	PRCount              int
	AvgSecs              float64
	MedianSecs           float64
	ApprovalRate         float64
	ChangesRequestedRate float64
	AvgChangesRequested  float64
}

// GlobalOverallStats holds high-level aggregate stats across all tracked repos.
type GlobalOverallStats struct {
	TotalPRs   int
	TotalRepos int
	AvgSecs    int64
	MedianSecs int64
}

// GlobalSizeChartData returns per-bucket metrics across all repos.
// cutoffPct trims high-outlier PRs by merge time; pass 1.0 for no trimming.
// minStars / minContribs filter by repo stars and distinct PR-author count; pass 0 to skip.
func (d *DB) GlobalSizeChartData(cutoffPct float64, minStars, minContribs int) ([]GlobalSizeBucket, error) {
	rows, err := d.conn.Query(`
		WITH cutoff AS (
			SELECT COALESCE(
				percentile_cont($1) WITHIN GROUP (ORDER BY merge_time_secs::FLOAT),
				9999999999.0
			) AS p
			FROM pull_requests WHERE merged=TRUE AND merge_time_secs > 0
		)
		SELECT
			CASE
				WHEN (pr.additions + pr.deletions) <= 50   THEN 1
				WHEN (pr.additions + pr.deletions) <= 200  THEN 2
				WHEN (pr.additions + pr.deletions) <= 500  THEN 3
				WHEN (pr.additions + pr.deletions) <= 1000 THEN 4
				WHEN (pr.additions + pr.deletions) <= 5000 THEN 5
				ELSE 6
			END AS bucket,
			COUNT(*) AS pr_count,
			COALESCE(AVG(pr.merge_time_secs)::FLOAT, 0) AS avg_secs,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY pr.merge_time_secs::FLOAT), 0) AS median_secs,
			COALESCE(
				100.0 * SUM(CASE WHEN pr.changes_requested_count=0 AND pr.review_count>0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(SUM(CASE WHEN pr.review_count>0 THEN 1 ELSE 0 END), 0),
				0
			) AS approval_rate,
			COALESCE(
				100.0 * SUM(CASE WHEN pr.changes_requested_count > 0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(COUNT(*), 0),
				0
			) AS changes_requested_rate,
			COALESCE(AVG(pr.changes_requested_count)::FLOAT, 0) AS avg_changes_requested
		FROM pull_requests pr
		JOIN repos r ON r.full_name = pr.repo_full_name
		LEFT JOIN mat_repo_contribs mrc ON mrc.repo_full_name = pr.repo_full_name
		CROSS JOIN cutoff
		WHERE pr.merged=TRUE AND (pr.additions + pr.deletions) > 0
		  AND (pr.merge_time_secs IS NULL OR pr.merge_time_secs::FLOAT <= p)
		  AND ($2 <= 0 OR r.stars >= $2)
		  AND ($3 <= 0 OR COALESCE(mrc.distinct_authors, 0) >= $3)
		GROUP BY bucket
		ORDER BY bucket
	`, cutoffPct, minStars, minContribs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	labels := []string{"≤50", "51–200", "201–500", "501–1k", "1k–5k", "5k+"}
	var out []GlobalSizeBucket
	for rows.Next() {
		var bucketNum int
		var b GlobalSizeBucket
		if err := rows.Scan(&bucketNum, &b.PRCount, &b.AvgSecs, &b.MedianSecs,
			&b.ApprovalRate, &b.ChangesRequestedRate, &b.AvgChangesRequested); err != nil {
			continue
		}
		if bucketNum >= 1 && bucketNum <= 6 {
			b.Label = labels[bucketNum-1]
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// GlobalOverallStats returns aggregate review-time stats across all tracked repos.
// minStars and minContribs filter to repos with at least that many stars / distinct PR authors;
// pass 0 for either to apply no filter.
func (d *DB) GlobalOverallStats(minStars, minContribs int) (GlobalOverallStats, error) {
	var s GlobalOverallStats
	var avgF, medianF float64
	err := d.conn.QueryRow(`
		SELECT
			COUNT(*) AS total_prs,
			COUNT(DISTINCT pr.repo_full_name) AS total_repos,
			COALESCE(AVG(pr.merge_time_secs)::FLOAT, 0) AS avg_secs,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY pr.merge_time_secs::FLOAT), 0) AS median_secs
		FROM pull_requests pr
		JOIN repos r ON r.full_name = pr.repo_full_name
		LEFT JOIN mat_repo_contribs mrc ON mrc.repo_full_name = pr.repo_full_name
		WHERE pr.merged=TRUE AND pr.merge_time_secs > 0
		  AND ($1 <= 0 OR r.stars >= $1)
		  AND ($2 <= 0 OR COALESCE(mrc.distinct_authors, 0) >= $2)
	`, minStars, minContribs).Scan(&s.TotalPRs, &s.TotalRepos, &avgF, &medianF)
	s.AvgSecs = int64(avgF)
	s.MedianSecs = int64(medianF)
	return s, err
}

// TimeSeriesPoint holds one month's aggregated PR metrics.
type TimeSeriesPoint struct {
	Label                string
	PRCount              int
	AvgSize              float64
	MedianSize           float64
	AvgSecs              float64
	MedianSecs           float64
	ChangesRequestedRate float64
	AvgFirstReviewSecs   float64
	MedFirstReviewSecs   float64
	UnreviewedRate       float64
	LinesPerContrib      float64
}

// GlobalTimeSeriesData returns monthly aggregated PR metrics across all repos.
// cutoffPct trims high-outlier PRs by merge time; pass 1.0 for no trimming.
// minStars / minContribs filter by repo stars and distinct PR-author count; pass 0 to skip.
func (d *DB) GlobalTimeSeriesData(cutoffPct float64, minStars, minContribs int) ([]TimeSeriesPoint, error) {
	rows, err := d.conn.Query(`
		WITH cutoff AS (
			SELECT COALESCE(
				percentile_cont($1) WITHIN GROUP (ORDER BY merge_time_secs::FLOAT),
				9999999999.0
			) AS cutoff_val
			FROM pull_requests WHERE merged=TRUE AND merge_time_secs > 0
		)
		SELECT
			TO_CHAR(DATE_TRUNC('month', pr.merged_at), 'Mon YYYY') AS label,
			COUNT(*) AS pr_count,
			COALESCE(AVG(pr.additions + pr.deletions)::FLOAT, 0) AS avg_size,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY (pr.additions + pr.deletions)::FLOAT), 0) AS median_size,
			COALESCE(AVG(pr.merge_time_secs)::FLOAT, 0) AS avg_secs,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY pr.merge_time_secs::FLOAT), 0) AS median_secs,
			COALESCE(
				100.0 * SUM(CASE WHEN pr.changes_requested_count > 0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(COUNT(*), 0),
				0
			) AS changes_requested_rate,
			COALESCE(AVG(CASE WHEN pr.first_review_at IS NOT NULL
				THEN GREATEST(0, EXTRACT(EPOCH FROM pr.first_review_at) - EXTRACT(EPOCH FROM pr.opened_at))
				ELSE NULL END), 0) AS avg_first_review_secs,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY
				CASE WHEN pr.first_review_at IS NOT NULL
				THEN GREATEST(0, EXTRACT(EPOCH FROM pr.first_review_at) - EXTRACT(EPOCH FROM pr.opened_at))
				ELSE NULL END), 0) AS median_first_review_secs,
			COALESCE(
				100.0 * SUM(CASE WHEN pr.review_count = 0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(COUNT(*), 0),
				0
			) AS unreviewed_rate,
		COALESCE(
			SUM(pr.additions + pr.deletions)::FLOAT / NULLIF(COUNT(DISTINCT pr.author_login), 0),
			0
		) AS lines_per_contrib
		FROM pull_requests pr
		CROSS JOIN cutoff
		JOIN repos r ON r.full_name = pr.repo_full_name
		LEFT JOIN mat_repo_contribs mrc ON mrc.repo_full_name = pr.repo_full_name
		WHERE pr.merged=TRUE AND pr.merged_at IS NOT NULL AND (pr.additions + pr.deletions) > 0
		  AND (pr.merge_time_secs IS NULL OR pr.merge_time_secs::FLOAT <= cutoff_val)
		  AND ($2 <= 0 OR r.stars >= $2)
		  AND ($3 <= 0 OR COALESCE(mrc.distinct_authors, 0) >= $3)
		GROUP BY DATE_TRUNC('month', pr.merged_at)
		ORDER BY DATE_TRUNC('month', pr.merged_at)
	`, cutoffPct, minStars, minContribs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TimeSeriesPoint
	for rows.Next() {
		var p TimeSeriesPoint
		if err := rows.Scan(&p.Label, &p.PRCount, &p.AvgSize, &p.MedianSize,
			&p.AvgSecs, &p.MedianSecs, &p.ChangesRequestedRate,
			&p.AvgFirstReviewSecs, &p.MedFirstReviewSecs, &p.UnreviewedRate,
			&p.LinesPerContrib); err != nil {
			continue
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// RepoTimeSeriesData returns monthly aggregated PR metrics for a single repo.
// cutoffPct trims high-outlier PRs by merge time; pass 1.0 for no trimming.
func (d *DB) RepoTimeSeriesData(fullName string, cutoffPct float64) ([]TimeSeriesPoint, error) {
	rows, err := d.conn.Query(`
		WITH cutoff AS (
			SELECT COALESCE(
				percentile_cont($2) WITHIN GROUP (ORDER BY merge_time_secs::FLOAT),
				9999999999.0
			) AS cutoff_val
			FROM pull_requests WHERE repo_full_name=$1 AND merged=TRUE AND merge_time_secs > 0
		)
		SELECT
			TO_CHAR(DATE_TRUNC('month', pr.merged_at), 'Mon YYYY') AS label,
			COUNT(*) AS pr_count,
			COALESCE(AVG(pr.additions + pr.deletions)::FLOAT, 0) AS avg_size,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY (pr.additions + pr.deletions)::FLOAT), 0) AS median_size,
			COALESCE(AVG(pr.merge_time_secs)::FLOAT, 0) AS avg_secs,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY pr.merge_time_secs::FLOAT), 0) AS median_secs,
			COALESCE(
				100.0 * SUM(CASE WHEN pr.changes_requested_count > 0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(COUNT(*), 0),
				0
			) AS changes_requested_rate,
			COALESCE(AVG(CASE WHEN pr.first_review_at IS NOT NULL
				THEN GREATEST(0, EXTRACT(EPOCH FROM pr.first_review_at) - EXTRACT(EPOCH FROM pr.opened_at))
				ELSE NULL END), 0) AS avg_first_review_secs,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY
				CASE WHEN pr.first_review_at IS NOT NULL
				THEN GREATEST(0, EXTRACT(EPOCH FROM pr.first_review_at) - EXTRACT(EPOCH FROM pr.opened_at))
				ELSE NULL END), 0) AS median_first_review_secs,
			COALESCE(
				100.0 * SUM(CASE WHEN pr.review_count = 0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(COUNT(*), 0),
				0
			) AS unreviewed_rate,
		COALESCE(
			SUM(pr.additions + pr.deletions)::FLOAT / NULLIF(COUNT(DISTINCT pr.author_login), 0),
			0
		) AS lines_per_contrib
		FROM pull_requests pr
		CROSS JOIN cutoff
		WHERE pr.repo_full_name=$1 AND pr.merged=TRUE AND pr.merged_at IS NOT NULL AND (pr.additions + pr.deletions) > 0
		  AND (pr.merge_time_secs IS NULL OR pr.merge_time_secs::FLOAT <= cutoff_val)
		GROUP BY DATE_TRUNC('month', pr.merged_at)
		ORDER BY DATE_TRUNC('month', pr.merged_at)
	`, fullName, cutoffPct)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TimeSeriesPoint
	for rows.Next() {
		var p TimeSeriesPoint
		if err := rows.Scan(&p.Label, &p.PRCount, &p.AvgSize, &p.MedianSize,
			&p.AvgSecs, &p.MedianSecs, &p.ChangesRequestedRate,
			&p.AvgFirstReviewSecs, &p.MedFirstReviewSecs, &p.UnreviewedRate,
			&p.LinesPerContrib); err != nil {
			continue
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// GlobalOpenedSeriesData returns monthly PR counts grouped by the month PRs were opened.
// minStars / minContribs filter by repo stars and distinct PR-author count; pass 0 to skip.
func (d *DB) GlobalOpenedSeriesData(minStars, minContribs int) ([]TimeSeriesPoint, error) {
	rows, err := d.conn.Query(`
		SELECT
			TO_CHAR(DATE_TRUNC('month', pr.opened_at), 'Mon YYYY') AS label,
			COUNT(*) AS pr_count
		FROM pull_requests pr
		JOIN repos r ON r.full_name = pr.repo_full_name
		LEFT JOIN mat_repo_contribs mrc ON mrc.repo_full_name = pr.repo_full_name
		WHERE pr.merged=TRUE AND pr.opened_at IS NOT NULL
		  AND ($1 <= 0 OR r.stars >= $1)
		  AND ($2 <= 0 OR COALESCE(mrc.distinct_authors, 0) >= $2)
		GROUP BY DATE_TRUNC('month', pr.opened_at)
		ORDER BY DATE_TRUNC('month', pr.opened_at)
	`, minStars, minContribs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TimeSeriesPoint
	for rows.Next() {
		var p TimeSeriesPoint
		if err := rows.Scan(&p.Label, &p.PRCount); err != nil {
			continue
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// OrgTimeSeriesData returns monthly aggregated PR metrics across all repos
// belonging to an org. cutoffPct trims high-outlier PRs; pass 1.0 for no trimming.
func (d *DB) OrgTimeSeriesData(orgName string, cutoffPct float64) ([]TimeSeriesPoint, error) {
	rows, err := d.conn.Query(`
		WITH org_repos AS (
			SELECT full_name FROM repos WHERE owner=$1 OR org_name=$1
		),
		cutoff AS (
			SELECT COALESCE(
				percentile_cont($2) WITHIN GROUP (ORDER BY merge_time_secs::FLOAT),
				9999999999.0
			) AS cutoff_val
			FROM pull_requests
			WHERE repo_full_name IN (SELECT full_name FROM org_repos)
			  AND merged=TRUE AND merge_time_secs > 0
		)
		SELECT
			TO_CHAR(DATE_TRUNC('month', pr.merged_at), 'Mon YYYY') AS label,
			COUNT(*) AS pr_count,
			COALESCE(AVG(pr.additions + pr.deletions)::FLOAT, 0) AS avg_size,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY (pr.additions + pr.deletions)::FLOAT), 0) AS median_size,
			COALESCE(AVG(pr.merge_time_secs)::FLOAT, 0) AS avg_secs,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY pr.merge_time_secs::FLOAT), 0) AS median_secs,
			COALESCE(
				100.0 * SUM(CASE WHEN pr.changes_requested_count > 0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(COUNT(*), 0),
				0
			) AS changes_requested_rate,
			COALESCE(AVG(CASE WHEN pr.first_review_at IS NOT NULL
				THEN GREATEST(0, EXTRACT(EPOCH FROM pr.first_review_at) - EXTRACT(EPOCH FROM pr.opened_at))
				ELSE NULL END), 0) AS avg_first_review_secs,
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY
				CASE WHEN pr.first_review_at IS NOT NULL
				THEN GREATEST(0, EXTRACT(EPOCH FROM pr.first_review_at) - EXTRACT(EPOCH FROM pr.opened_at))
				ELSE NULL END), 0) AS median_first_review_secs,
			COALESCE(
				100.0 * SUM(CASE WHEN pr.review_count = 0 THEN 1 ELSE 0 END)::FLOAT /
				NULLIF(COUNT(*), 0),
				0
			) AS unreviewed_rate,
		COALESCE(
			SUM(pr.additions + pr.deletions)::FLOAT / NULLIF(COUNT(DISTINCT pr.author_login), 0),
			0
		) AS lines_per_contrib
		FROM pull_requests pr
		CROSS JOIN cutoff
		WHERE pr.repo_full_name IN (SELECT full_name FROM org_repos)
		  AND pr.merged=TRUE AND pr.merged_at IS NOT NULL AND (pr.additions + pr.deletions) > 0
		  AND (pr.merge_time_secs IS NULL OR pr.merge_time_secs::FLOAT <= cutoff_val)
		GROUP BY DATE_TRUNC('month', pr.merged_at)
		ORDER BY DATE_TRUNC('month', pr.merged_at)
	`, orgName, cutoffPct)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TimeSeriesPoint
	for rows.Next() {
		var p TimeSeriesPoint
		if err := rows.Scan(&p.Label, &p.PRCount, &p.AvgSize, &p.MedianSize,
			&p.AvgSecs, &p.MedianSecs, &p.ChangesRequestedRate,
			&p.AvgFirstReviewSecs, &p.MedFirstReviewSecs, &p.UnreviewedRate,
			&p.LinesPerContrib); err != nil {
			continue
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ── Page visits (for "Try:" pills) ────────────────────────────────────────────

type PageVisit struct {
	Path  string
	Kind  string
	Label string
	Count int
}

func (d *DB) RecordVisit(path, kind, label string) {
	d.conn.Exec(`
		INSERT INTO page_visits (path, kind, label, count, last_visited)
		VALUES ($1,$2,$3,1,NOW())
		ON CONFLICT(path) DO UPDATE SET
			count        = page_visits.count + 1,
			last_visited = NOW()
	`, path, kind, label)
}

func (d *DB) PopularVisits(limit int) ([]PageVisit, error) {
	rows, err := d.conn.Query(`
		SELECT path, kind, label, count FROM page_visits
		ORDER BY count DESC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanVisits(rows)
}

func (d *DB) RecentVisits(limit int, exclude []string) ([]PageVisit, error) {
	if len(exclude) == 0 {
		rows, err := d.conn.Query(`
			SELECT path, kind, label, count FROM page_visits
			ORDER BY last_visited DESC LIMIT $1
		`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanVisits(rows)
	}
	// Build NOT IN clause with numbered placeholders.
	placeholders := "$1"
	args := []interface{}{exclude[0]}
	for i, p := range exclude[1:] {
		placeholders += fmt.Sprintf(",$%d", i+2)
		args = append(args, p)
	}
	args = append(args, limit)
	rows, err := d.conn.Query(`
		SELECT path, kind, label, count FROM page_visits
		WHERE path NOT IN (`+placeholders+`)
		ORDER BY last_visited DESC LIMIT $`+fmt.Sprintf("%d", len(args)),
		args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanVisits(rows)
}

func scanVisits(rows *sql.Rows) ([]PageVisit, error) {
	var visits []PageVisit
	for rows.Next() {
		var v PageVisit
		if err := rows.Scan(&v.Path, &v.Kind, &v.Label, &v.Count); err != nil {
			continue
		}
		visits = append(visits, v)
	}
	return visits, rows.Err()
}

// ── Hi wall ───────────────────────────────────────────────────────────────────

// HiWallPage is one entry on the hi wall.
type HiWallPage struct {
	Path       string
	Label      string
	Kind       string
	TotalCount int
	TodayCount int
}

// HiGetAll returns the total hi count, per-reaction breakdown, and today's count for a path.
func (d *DB) HiGetAll(path string) (total int, reactions map[string]int, todayCount int) {
	reactions = make(map[string]int)
	rows, err := d.conn.Query(`SELECT reaction, count FROM page_hi_reactions WHERE path=$1`, path)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var r string
			var c int
			if rows.Scan(&r, &c) == nil {
				reactions[r] = c
				total += c
			}
		}
	}
	d.conn.QueryRow(`
		SELECT COUNT(*) FROM page_hi_log
		WHERE path=$1 AND ts > NOW() - INTERVAL '1 day'
	`, path).Scan(&todayCount)
	return
}

// HiIncrementReaction records a reaction and returns the updated totals.
func (d *DB) HiIncrementReaction(path, reaction string) (total int, reactions map[string]int, todayCount int) {
	d.conn.Exec(`
		INSERT INTO page_hi_reactions (path, reaction, count) VALUES ($1,$2,1)
		ON CONFLICT(path, reaction) DO UPDATE SET count = page_hi_reactions.count + 1
	`, path, reaction)
	d.conn.Exec(`INSERT INTO page_hi_log (path, reaction) VALUES ($1,$2)`, path, reaction)
	return d.HiGetAll(path)
}

// HiTopWallPages returns the most-hi'd pages.
func (d *DB) HiTopWallPages(limit int) ([]HiWallPage, error) {
	rows, err := d.conn.Query(`
		SELECT
			r.path,
			COALESCE(v.label, r.path) AS label,
			COALESCE(v.kind, '')      AS kind,
			SUM(r.count)              AS total,
			(SELECT COUNT(*) FROM page_hi_log l
			 WHERE l.path=r.path AND l.ts > NOW() - INTERVAL '1 day') AS today
		FROM page_hi_reactions r
		LEFT JOIN page_visits v ON v.path = r.path
		GROUP BY r.path, v.label, v.kind
		ORDER BY total DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pages []HiWallPage
	for rows.Next() {
		var p HiWallPage
		if err := rows.Scan(&p.Path, &p.Label, &p.Kind, &p.TotalCount, &p.TodayCount); err != nil {
			continue
		}
		pages = append(pages, p)
	}
	return pages, rows.Err()
}

// ── Hi-wall users ─────────────────────────────────────────────────────────────

// RandomTrackedUsers returns users with avatars in random order.
func (d *DB) RandomTrackedUsers(limit int) ([]User, error) {
	rows, err := d.conn.Query(`
		SELECT login, name, avatar_url FROM users TABLESAMPLE BERNOULLI(20)
		WHERE avatar_url != '' AND NOT is_org
		ORDER BY RANDOM() LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.Login, &u.Name, &u.AvatarURL); err != nil {
			continue
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// UserPeerReviewers returns users who reviewed PRs in repos where login authored PRs.
func (d *DB) UserPeerReviewers(login string, limit int) ([]User, error) {
	// Cap candidate pool to 500 before random sort to avoid sorting 30M review rows.
	// peer_repos limits to the user's 20 most recent repos to bound the IN subquery.
	rows, err := d.conn.Query(`
		WITH peer_repos AS (
			SELECT DISTINCT repo_full_name FROM pull_requests
			WHERE author_login = $1 LIMIT 20
		),
		candidates AS (
			SELECT DISTINCT r.reviewer_login
			FROM reviews r
			JOIN peer_repos pr ON pr.repo_full_name = r.repo_full_name
			WHERE r.reviewer_login != $2
			LIMIT 500
		)
		SELECT u.login, COALESCE(u.name,''), COALESCE(u.avatar_url,'')
		FROM users u
		JOIN candidates c ON c.reviewer_login = u.login
		WHERE u.avatar_url != ''
		ORDER BY RANDOM() LIMIT $3
	`, login, login, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.Login, &u.Name, &u.AvatarURL); err != nil {
			continue
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// ── Auth: sessions & installations ────────────────────────────────────────────

// SessionInfo holds data about a validated user session.
type SessionInfo struct {
	Login          string
	InstallationID *int64
	ExpiresAt      time.Time
}

func (d *DB) CreateSession(sessionID, login string, installationID *int64, expiresAt time.Time) error {
	_, err := d.conn.Exec(`
		INSERT INTO user_sessions (session_id, github_login, installation_id, expires_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT(session_id) DO UPDATE SET
			github_login    = EXCLUDED.github_login,
			installation_id = EXCLUDED.installation_id,
			expires_at      = EXCLUDED.expires_at
	`, sessionID, login, installationID, expiresAt)
	return err
}

func (d *DB) GetSession(sessionID string) (*SessionInfo, error) {
	s := &SessionInfo{}
	var instID sql.NullInt64
	err := d.conn.QueryRow(`
		SELECT github_login, installation_id, expires_at
		FROM user_sessions
		WHERE session_id=$1 AND expires_at > NOW()
	`, sessionID).Scan(&s.Login, &instID, &s.ExpiresAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if instID.Valid {
		s.InstallationID = &instID.Int64
	}
	return s, nil
}

func (d *DB) DeleteSession(sessionID string) error {
	_, err := d.conn.Exec(`DELETE FROM user_sessions WHERE session_id=$1`, sessionID)
	return err
}

func (d *DB) UpsertInstallation(installationID int64, login string) error {
	_, err := d.conn.Exec(`
		INSERT INTO app_installations (installation_id, github_login)
		VALUES ($1, $2)
		ON CONFLICT(installation_id) DO UPDATE SET
			github_login   = EXCLUDED.github_login,
			uninstalled_at = NULL
	`, installationID, login)
	return err
}

func (d *DB) DeactivateInstallation(installationID int64) error {
	_, err := d.conn.Exec(`
		UPDATE app_installations SET uninstalled_at=NOW()
		WHERE installation_id=$1
	`, installationID)
	return err
}

// GetInstallationByLogin returns the active installation ID for a GitHub login.
// Checks both app_installations (direct installs) and user_installations (org installs
// where the user was the sender but the account is an org).
func (d *DB) GetInstallationByLogin(login string) (*int64, error) {
	var id int64
	err := d.conn.QueryRow(`
		SELECT installation_id FROM app_installations
		WHERE github_login=$1 AND uninstalled_at IS NULL
		ORDER BY installed_at DESC LIMIT 1
	`, login).Scan(&id)
	if err == nil {
		return &id, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}
	// Fall back to user_installations (org installs linked to this user).
	err = d.conn.QueryRow(`
		SELECT installation_id FROM user_installations
		WHERE github_login=$1
		LIMIT 1
	`, login).Scan(&id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// UpsertInstallationForUser links an installation to a user in user_installations.
// Used when a user installs the app on an org — we link the org's installation
// to the installing user so their dashboard can discover it.
func (d *DB) UpsertInstallationForUser(installationID int64, login string) error {
	_, err := d.conn.Exec(`
		INSERT INTO user_installations (github_login, installation_id)
		VALUES ($1, $2)
		ON CONFLICT (github_login, installation_id) DO NOTHING
	`, login, installationID)
	return err
}

// LinkSessionInstallation updates the installation_id on an existing session.
func (d *DB) LinkSessionInstallation(sessionID string, installationID int64) error {
	_, err := d.conn.Exec(`
		UPDATE user_sessions SET installation_id=$1 WHERE session_id=$2
	`, installationID, sessionID)
	return err
}

// TrackRepoForUser records that a user has explicitly chosen to track a repo,
// regardless of whether they own it (supports org repos).
func (d *DB) TrackRepoForUser(login, repoFullName string) error {
	_, err := d.conn.Exec(`
		INSERT INTO user_tracked_repos (github_login, repo_full_name)
		VALUES ($1, $2)
		ON CONFLICT (github_login, repo_full_name) DO NOTHING
	`, login, repoFullName)
	return err
}

// UserOwnedTrackedRepos returns repos the user has tracked — either repos they
// own/belong to by org, or repos they explicitly added via TrackRepoForUser.
func (d *DB) UserOwnedTrackedRepos(login string) ([]Repo, error) {
	rows, err := d.conn.Query(`
		SELECT full_name, owner, name, description, stars, language, org_name,
		       last_synced, sync_status, pr_count, merged_pr_count,
		       avg_merge_time_secs, min_merge_time_secs, max_merge_time_secs
		FROM repos
		WHERE (owner = $1 OR org_name = $1)
		   OR full_name IN (
		       SELECT repo_full_name FROM user_tracked_repos WHERE github_login = $1
		   )
		ORDER BY merged_pr_count DESC
		LIMIT 50
	`, login)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRepos(rows)
}

// UnsyncedRepos returns up to limit repo full_names that need syncing:
// - sync_status = 'pending' (never attempted), OR
// - sync_status = 'error'   AND last updated more than errorCooldown ago (retry after backoff).
// Ordered oldest-first so the backlog drains in FIFO order.
func (d *DB) UnsyncedRepos(limit int, errorCooldown time.Duration) ([]string, error) {
	rows, err := d.conn.Query(`
		SELECT full_name FROM repos
		WHERE sync_status = 'pending'
		   OR (sync_status = 'error' AND error_count < 5 AND updated_at < NOW() - $2::interval)
		ORDER BY updated_at ASC
		LIMIT $1
	`, limit, fmt.Sprintf("%d seconds", int(errorCooldown.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			continue
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ── Data Explorer queries ───────────────────────────────────────────────────────

// ListReposFiltered returns a page of repos and total count matching filters.
// sortBy: "stars", "speed", "slow", or "" (default: most PRs).
func (d *DB) ListReposFiltered(limit, offset int, sortBy, search, status string) ([]Repo, int, error) {
	col, dir := "merged_pr_count", "DESC"
	switch sortBy {
	case "stars":
		col, dir = "stars", "DESC"
	case "speed":
		col, dir = "avg_merge_time_secs", "ASC"
	case "slow":
		col, dir = "avg_merge_time_secs", "DESC"
	}
	q := fmt.Sprintf(`
		SELECT full_name, owner, name, description, stars, language, org_name,
		       last_synced, sync_status, pr_count, merged_pr_count,
		       avg_merge_time_secs, min_merge_time_secs, max_merge_time_secs,
		       COUNT(*) OVER() AS total
		FROM repos
		WHERE ($1 = '' OR full_name ILIKE '%%' || $1 || '%%')
		  AND ($2 = '' OR sync_status = $2)
		ORDER BY %s %s
		LIMIT $3 OFFSET $4
	`, col, dir)
	rows, err := d.conn.Query(q, search, status, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var repos []Repo
	var total int
	for rows.Next() {
		var r Repo
		var lastSynced sql.NullTime
		if err := rows.Scan(
			&r.FullName, &r.Owner, &r.Name, &r.Description, &r.Stars, &r.Language, &r.OrgName,
			&lastSynced, &r.SyncStatus, &r.PRCount, &r.MergedPRCount,
			&r.AvgMergeTimeSecs, &r.MinMergeTimeSecs, &r.MaxMergeTimeSecs,
			&total,
		); err != nil {
			log.Printf("db: ListReposFiltered scan: %v", err)
			continue
		}
		if lastSynced.Valid {
			t := lastSynced.Time
			r.LastSynced = &t
		}
		repos = append(repos, r)
	}
	return repos, total, rows.Err()
}

// ListPRsFiltered returns a page of merged PRs and total count matching filters.
// sortBy: "speed", "slow", "size", or "" (default: most recent).
func (d *DB) ListPRsFiltered(limit, offset int, repo, author, sortBy string) ([]PullRequest, int, error) {
	col, dir := "merged_at", "DESC"
	switch sortBy {
	case "speed":
		col, dir = "merge_time_secs", "ASC"
	case "slow":
		col, dir = "merge_time_secs", "DESC"
	case "size":
		col, dir = "(additions + deletions)", "DESC"
	}
	q := fmt.Sprintf(`
		SELECT id, repo_full_name, number, title, author_login, merged,
		       opened_at, merged_at, merge_time_secs, review_count, changes_requested_count,
		       additions, deletions,
		       COUNT(*) OVER() AS total
		FROM pull_requests
		WHERE merged = TRUE
		  AND ($1 = '' OR repo_full_name LIKE $1 || '%%')
		  AND ($2 = '' OR author_login LIKE $2 || '%%')
		ORDER BY %s %s NULLS LAST
		LIMIT $3 OFFSET $4
	`, col, dir)
	rows, err := d.conn.Query(q, repo, author, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var prs []PullRequest
	var total int
	for rows.Next() {
		var pr PullRequest
		var mergedAt sql.NullTime
		var mts sql.NullInt64
		if err := rows.Scan(
			&pr.ID, &pr.RepoFullName, &pr.Number, &pr.Title, &pr.AuthorLogin, &pr.Merged,
			&pr.OpenedAt, &mergedAt, &mts, &pr.ReviewCount, &pr.ChangesRequestedCount,
			&pr.Additions, &pr.Deletions,
			&total,
		); err != nil {
			log.Printf("db: ListPRsFiltered scan: %v", err)
			continue
		}
		if mergedAt.Valid {
			t := mergedAt.Time
			pr.MergedAt = &t
		}
		if mts.Valid {
			pr.MergeTimeSecs = &mts.Int64
		}
		prs = append(prs, pr)
	}
	return prs, total, rows.Err()
}

// ListReviewsFiltered returns a page of reviews and total count matching filters.
func (d *DB) ListReviewsFiltered(limit, offset int, reviewer, state string) ([]Review, int, error) {
	rows, err := d.conn.Query(`
		SELECT id, repo_full_name, pr_number, reviewer_login, state, submitted_at,
		       COUNT(*) OVER() AS total
		FROM reviews
		WHERE ($1 = '' OR reviewer_login LIKE $1 || '%')
		  AND ($2 = '' OR state = $2)
		ORDER BY submitted_at DESC
		LIMIT $3 OFFSET $4
	`, reviewer, state, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var revs []Review
	var total int
	for rows.Next() {
		var rev Review
		if err := rows.Scan(
			&rev.ID, &rev.RepoFullName, &rev.PRNumber, &rev.ReviewerLogin, &rev.State, &rev.SubmittedAt,
			&total,
		); err != nil {
			log.Printf("db: ListReviewsFiltered scan: %v", err)
			continue
		}
		revs = append(revs, rev)
	}
	return revs, total, rows.Err()
}

// ListUsersFiltered returns a page of users and total count matching a search term on login or name.
func (d *DB) ListUsersFiltered(limit, offset int, search string) ([]User, int, error) {
	rows, err := d.conn.Query(`
		SELECT login, name, avatar_url, bio, public_repos, followers,
		       company, location, is_org, last_fetched,
		       COUNT(*) OVER() AS total
		FROM users
		WHERE ($1 = '' OR login ILIKE '%' || $1 || '%' OR name ILIKE '%' || $1 || '%')
		ORDER BY followers DESC
		LIMIT $2 OFFSET $3
	`, search, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var users []User
	var total int
	for rows.Next() {
		var u User
		var lastFetched sql.NullTime
		if err := rows.Scan(
			&u.Login, &u.Name, &u.AvatarURL, &u.Bio, &u.PublicRepos, &u.Followers,
			&u.Company, &u.Location, &u.IsOrg, &lastFetched,
			&total,
		); err != nil {
			log.Printf("db: ListUsersFiltered scan: %v", err)
			continue
		}
		if lastFetched.Valid {
			t := lastFetched.Time
			u.LastFetched = &t
		}
		users = append(users, u)
	}
	return users, total, rows.Err()
}
