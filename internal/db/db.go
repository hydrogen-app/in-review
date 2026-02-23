package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// DB wraps the Postgres connection pool.
type DB struct {
	conn *sql.DB
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
}

type AuthorStats struct {
	Login            string
	TotalPRs         int
	MergedPRs        int
	AvgMergeTimeSecs int64
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
	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(5 * time.Minute)

	d := &DB{conn: conn}
	return d, d.migrate()
}

func (d *DB) Close() error { return d.conn.Close() }

// ── Schema ─────────────────────────────────────────────────────────────────────

func (d *DB) migrate() error {
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
	}
	for _, s := range stmts {
		if _, err := d.conn.Exec(s); err != nil {
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
	if status == "done" {
		_, err := d.conn.Exec(
			`UPDATE repos SET sync_status=$1, last_synced=NOW(), updated_at=NOW() WHERE full_name=$2`,
			status, fullName)
		return err
	}
	_, err := d.conn.Exec(
		`UPDATE repos SET sync_status=$1, updated_at=NOW() WHERE full_name=$2`,
		status, fullName)
	return err
}

func (d *DB) UpsertPR(pr PullRequest) error {
	_, err := d.conn.Exec(`
		INSERT INTO pull_requests
			(id, repo_full_name, number, title, author_login, merged, opened_at, merged_at, merge_time_secs, review_count, changes_requested_count)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT(repo_full_name, number) DO UPDATE SET
			title                   = EXCLUDED.title,
			merged                  = EXCLUDED.merged,
			merged_at               = EXCLUDED.merged_at,
			merge_time_secs         = EXCLUDED.merge_time_secs,
			review_count            = EXCLUDED.review_count,
			changes_requested_count = EXCLUDED.changes_requested_count
	`, pr.ID, pr.RepoFullName, pr.Number, pr.Title, pr.AuthorLogin, pr.Merged,
		pr.OpenedAt.UTC(), pr.MergedAt, pr.MergeTimeSecs,
		pr.ReviewCount, pr.ChangesRequestedCount)
	return err
}

func (d *DB) UpsertReview(r Review) error {
	_, err := d.conn.Exec(`
		INSERT INTO reviews (id, repo_full_name, pr_number, reviewer_login, state, submitted_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT(id) DO NOTHING
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
		UPDATE repos SET
			pr_count            = (SELECT COUNT(*)   FROM pull_requests WHERE repo_full_name=$1 AND merged=TRUE),
			merged_pr_count     = (SELECT COUNT(*)   FROM pull_requests WHERE repo_full_name=$2 AND merged=TRUE),
			avg_merge_time_secs = COALESCE((SELECT AVG(merge_time_secs) FROM pull_requests WHERE repo_full_name=$3 AND merged=TRUE AND merge_time_secs IS NOT NULL)::BIGINT, 0),
			min_merge_time_secs = COALESCE((SELECT MIN(merge_time_secs) FROM pull_requests WHERE repo_full_name=$4 AND merged=TRUE AND merge_time_secs IS NOT NULL), 0),
			max_merge_time_secs = COALESCE((SELECT MAX(merge_time_secs) FROM pull_requests WHERE repo_full_name=$5 AND merged=TRUE AND merge_time_secs IS NOT NULL), 0),
			updated_at          = NOW()
		WHERE full_name=$6
	`, fullName, fullName, fullName, fullName, fullName, fullName)
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
		SELECT r.reviewer_login, COUNT(*) as cnt, COALESCE(u.avatar_url,'')
		FROM reviews r
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE r.state IN ('APPROVED','CHANGES_REQUESTED','COMMENTED')
		GROUP BY r.reviewer_login
		ORDER BY cnt DESC
		LIMIT $1`, limit)
}

func (d *DB) LeaderboardGatekeepers(limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT r.reviewer_login, COUNT(*) as cnt, COALESCE(u.avatar_url,'')
		FROM reviews r
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE r.state='CHANGES_REQUESTED'
		GROUP BY r.reviewer_login
		ORDER BY cnt DESC
		LIMIT $1`, limit)
}

func (d *DB) LeaderboardAuthors(limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT p.author_login, COUNT(*) as cnt, COALESCE(u.avatar_url,'')
		FROM pull_requests p
		LEFT JOIN users u ON u.login=p.author_login
		WHERE p.merged=TRUE
		GROUP BY p.author_login
		ORDER BY cnt DESC
		LIMIT $1`, limit)
}

func (d *DB) LeaderboardCleanApprovals(limit int) ([]LeaderboardEntry, error) {
	rows, err := d.conn.Query(`
		SELECT repo_full_name,
		       COUNT(*) as total,
		       CAST(ROUND(100.0 * SUM(CASE WHEN changes_requested_count=0 AND review_count>0 THEN 1 ELSE 0 END) / COUNT(*)) AS INTEGER) as clean_pct
		FROM pull_requests
		WHERE merged=TRUE AND review_count>0
		GROUP BY repo_full_name
		HAVING COUNT(*) >= 5
		ORDER BY clean_pct DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []LeaderboardEntry
	rank := 1
	for rows.Next() {
		var e LeaderboardEntry
		var cleanPct int
		if err := rows.Scan(&e.Name, &e.Count, &cleanPct); err != nil {
			continue
		}
		e.Rank = rank
		e.Value = int64(cleanPct)
		entries = append(entries, e)
		rank++
	}
	return entries, rows.Err()
}

func (d *DB) queryEntries(query string, limit int) ([]LeaderboardEntry, error) {
	rows, err := d.conn.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("queryEntries: %w", err)
	}
	defer rows.Close()
	var entries []LeaderboardEntry
	rank := 1
	for rows.Next() {
		var e LeaderboardEntry
		var val int64
		if err := rows.Scan(&e.Name, &val, &e.Extra); err != nil {
			log.Printf("db: queryEntries scan error (rank %d): %v", rank, err)
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
		       COALESCE(u.avatar_url,''),
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
		       opened_at, merged_at, merge_time_secs, review_count, changes_requested_count
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
		SELECT COALESCE(u.avatar_url,''),
		       COUNT(*),
		       SUM(CASE WHEN r.state='APPROVED'          THEN 1 ELSE 0 END),
		       SUM(CASE WHEN r.state='CHANGES_REQUESTED' THEN 1 ELSE 0 END),
		       SUM(CASE WHEN r.state='COMMENTED'         THEN 1 ELSE 0 END)
		FROM reviews r
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE r.reviewer_login=$1
		GROUP BY r.reviewer_login
	`, login).Scan(&s.AvatarURL, &s.TotalReviews, &s.Approvals, &s.ChangesRequested, &s.Comments)
	if err == sql.ErrNoRows {
		return s, nil
	}
	return s, err
}

func (d *DB) UserAuthorStats(login string) (*AuthorStats, error) {
	s := &AuthorStats{Login: login}
	err := d.conn.QueryRow(`
		SELECT COUNT(*),
		       SUM(CASE WHEN merged=TRUE THEN 1 ELSE 0 END),
		       COALESCE(AVG(CASE WHEN merged=TRUE THEN merge_time_secs END), 0)
		FROM pull_requests
		WHERE author_login=$1
	`, login).Scan(&s.TotalPRs, &s.MergedPRs, &s.AvgMergeTimeSecs)
	if err == sql.ErrNoRows {
		return s, nil
	}
	return s, err
}

func (d *DB) UserReviewerRank(login string) (int, error) {
	return d.rankQuery(`
		SELECT rank FROM (
			SELECT reviewer_login, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
			FROM reviews GROUP BY reviewer_login
		) sub WHERE reviewer_login=$1
	`, login)
}

func (d *DB) UserGatekeeperRank(login string) (int, error) {
	return d.rankQuery(`
		SELECT rank FROM (
			SELECT reviewer_login, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
			FROM reviews WHERE state='CHANGES_REQUESTED' GROUP BY reviewer_login
		) sub WHERE reviewer_login=$1
	`, login)
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
	return d.rankQuery(`
		SELECT rank FROM (
			SELECT author_login, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
			FROM pull_requests WHERE merged=TRUE GROUP BY author_login
		) sub WHERE author_login=$1
	`, login)
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
	`, orgName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRepos(rows)
}

func (d *DB) OrgReviewerLeaderboard(orgName string, limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT r.reviewer_login, COUNT(*) as cnt, COALESCE(u.avatar_url,'')
		FROM reviews r
		JOIN repos repo ON repo.full_name=r.repo_full_name
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE repo.org_name=$1
		GROUP BY r.reviewer_login
		ORDER BY cnt DESC
		LIMIT $2`, limit)
}

func (d *DB) OrgGatekeeperLeaderboard(orgName string, limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT r.reviewer_login, COUNT(*) as cnt, COALESCE(u.avatar_url,'')
		FROM reviews r
		JOIN repos repo ON repo.full_name=r.repo_full_name
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE repo.org_name=$1 AND r.state='CHANGES_REQUESTED'
		GROUP BY r.reviewer_login
		ORDER BY cnt DESC
		LIMIT $2`, limit)
}

// ── Global stats ───────────────────────────────────────────────────────────────

func (d *DB) TotalStats() (repos, prs, reviews int) {
	d.conn.QueryRow(`SELECT COUNT(*) FROM repos WHERE sync_status='done'`).Scan(&repos)
	d.conn.QueryRow(`SELECT COUNT(*) FROM pull_requests WHERE merged=TRUE`).Scan(&prs)
	d.conn.QueryRow(`SELECT COUNT(*) FROM reviews`).Scan(&reviews)
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
		SELECT r.reviewer_login,
		       COALESCE(u.avatar_url, ''),
		       COUNT(*) as total,
		       SUM(CASE WHEN r.state='APPROVED'          THEN 1 ELSE 0 END),
		       SUM(CASE WHEN r.state='CHANGES_REQUESTED' THEN 1 ELSE 0 END)
		FROM reviews r
		LEFT JOIN users u ON u.login = r.reviewer_login
		GROUP BY r.reviewer_login
		ORDER BY total DESC
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanUserRows(rows, offset+1)
}

func (d *DB) FullLeaderboardGatekeepers(limit, offset int) ([]UserLeaderboardRow, error) {
	rows, err := d.conn.Query(`
		SELECT r.reviewer_login,
		       COALESCE(u.avatar_url, ''),
		       COUNT(*) as total,
		       SUM(CASE WHEN r.state='APPROVED'          THEN 1 ELSE 0 END),
		       SUM(CASE WHEN r.state='CHANGES_REQUESTED' THEN 1 ELSE 0 END)
		FROM reviews r
		LEFT JOIN users u ON u.login = r.reviewer_login
		WHERE r.state = 'CHANGES_REQUESTED'
		GROUP BY r.reviewer_login
		ORDER BY total DESC
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanUserRows(rows, offset+1)
}

func (d *DB) FullLeaderboardAuthors(limit, offset int) ([]UserLeaderboardRow, error) {
	rows, err := d.conn.Query(`
		SELECT p.author_login,
		       COALESCE(u.avatar_url, ''),
		       COUNT(*) as merged,
		       0 as approvals,
		       0 as changes,
		       COUNT(*) as merged_prs,
		       COALESCE(AVG(p.merge_time_secs), 0)::BIGINT as avg_secs
		FROM pull_requests p
		LEFT JOIN users u ON u.login = p.author_login
		WHERE p.merged = TRUE
		GROUP BY p.author_login
		ORDER BY merged DESC
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("FullLeaderboardAuthors: %w", err)
	}
	defer rows.Close()
	var out []UserLeaderboardRow
	rank := offset + 1
	for rows.Next() {
		var r UserLeaderboardRow
		if err := rows.Scan(&r.Login, &r.AvatarURL, &r.Total, &r.Approvals, &r.ChangesRequested, &r.MergedPRs, &r.AvgMergeTimeSecs); err != nil {
			log.Printf("db: FullLeaderboardAuthors scan error: %v", err)
			continue
		}
		r.Rank = rank
		out = append(out, r)
		rank++
	}
	return out, rows.Err()
}

func (d *DB) FullLeaderboardCleanApprovals(limit, offset int) ([]CleanLeaderboardRow, error) {
	rows, err := d.conn.Query(`
		SELECT repo_full_name,
		       COUNT(*) as total,
		       CAST(ROUND(100.0 * SUM(CASE WHEN changes_requested_count=0 AND review_count>0 THEN 1 ELSE 0 END) / COUNT(*)) AS INTEGER) as clean_pct,
		       COALESCE(AVG(merge_time_secs), 0) as avg_secs
		FROM pull_requests
		WHERE merged=TRUE AND review_count>0
		GROUP BY repo_full_name
		HAVING COUNT(*) >= 5
		ORDER BY clean_pct DESC
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CleanLeaderboardRow
	rank := offset + 1
	for rows.Next() {
		var r CleanLeaderboardRow
		var avgSecs float64
		if err := rows.Scan(&r.FullName, &r.Total, &r.CleanPct, &avgSecs); err != nil {
			log.Printf("db: FullLeaderboardCleanApprovals scan error: %v", err)
			continue
		}
		r.AvgSecs = int64(avgSecs)
		r.Rank = rank
		out = append(out, r)
		rank++
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
		SELECT login, name, avatar_url FROM users
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
	rows, err := d.conn.Query(`
		SELECT DISTINCT u.login, COALESCE(u.name,''), COALESCE(u.avatar_url,'')
		FROM users u
		JOIN reviews r ON r.reviewer_login = u.login
		WHERE r.repo_full_name IN (
			SELECT DISTINCT repo_full_name FROM pull_requests WHERE author_login = $1
		)
		AND r.reviewer_login != $2
		AND u.avatar_url != ''
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
