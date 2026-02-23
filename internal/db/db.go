package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// DB wraps the SQLite connection.
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

func New(dbPath string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("creating db dir: %w", err)
	}
	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}
	conn.SetMaxOpenConns(1) // SQLite is single-writer
	conn.Exec("PRAGMA journal_mode=WAL")
	conn.Exec("PRAGMA synchronous=NORMAL")
	conn.Exec("PRAGMA foreign_keys=ON")

	d := &DB{conn: conn}
	return d, d.migrate()
}

func (d *DB) Close() error { return d.conn.Close() }

// ── Schema ─────────────────────────────────────────────────────────────────────

func (d *DB) migrate() error {
	_, err := d.conn.Exec(`
		CREATE TABLE IF NOT EXISTS repos (
			full_name          TEXT PRIMARY KEY,
			owner              TEXT NOT NULL,
			name               TEXT NOT NULL,
			description        TEXT    DEFAULT '',
			stars              INTEGER DEFAULT 0,
			language           TEXT    DEFAULT '',
			org_name           TEXT    DEFAULT '',
			last_synced        DATETIME,
			sync_status        TEXT    DEFAULT 'pending',
			pr_count           INTEGER DEFAULT 0,
			merged_pr_count    INTEGER DEFAULT 0,
			avg_merge_time_secs INTEGER DEFAULT 0,
			min_merge_time_secs INTEGER DEFAULT 0,
			max_merge_time_secs INTEGER DEFAULT 0,
			updated_at         DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS pull_requests (
			id                      TEXT PRIMARY KEY,
			repo_full_name          TEXT    NOT NULL,
			number                  INTEGER NOT NULL,
			title                   TEXT    DEFAULT '',
			author_login            TEXT    NOT NULL,
			merged                  INTEGER DEFAULT 0,
			opened_at               DATETIME NOT NULL,
			merged_at               DATETIME,
			merge_time_secs         INTEGER,
			review_count            INTEGER DEFAULT 0,
			changes_requested_count INTEGER DEFAULT 0,
			UNIQUE(repo_full_name, number)
		);

		CREATE TABLE IF NOT EXISTS reviews (
			id             TEXT PRIMARY KEY,
			repo_full_name TEXT NOT NULL,
			pr_number      INTEGER NOT NULL,
			reviewer_login TEXT NOT NULL,
			state          TEXT NOT NULL,
			submitted_at   DATETIME NOT NULL
		);

		CREATE TABLE IF NOT EXISTS users (
			login        TEXT PRIMARY KEY,
			name         TEXT    DEFAULT '',
			avatar_url   TEXT    DEFAULT '',
			bio          TEXT    DEFAULT '',
			public_repos INTEGER DEFAULT 0,
			followers    INTEGER DEFAULT 0,
			company      TEXT    DEFAULT '',
			location     TEXT    DEFAULT '',
			is_org       INTEGER DEFAULT 0,
			last_fetched DATETIME
		);

		CREATE INDEX IF NOT EXISTS idx_prs_repo     ON pull_requests(repo_full_name);
		CREATE INDEX IF NOT EXISTS idx_prs_author   ON pull_requests(author_login);
		CREATE INDEX IF NOT EXISTS idx_prs_merged   ON pull_requests(merged);
		CREATE INDEX IF NOT EXISTS idx_rev_reviewer ON reviews(reviewer_login);
		CREATE INDEX IF NOT EXISTS idx_rev_repo_pr  ON reviews(repo_full_name, pr_number);
		CREATE INDEX IF NOT EXISTS idx_repos_org    ON repos(org_name);
	`)
	return err
}

// ── Upserts ────────────────────────────────────────────────────────────────────

func (d *DB) UpsertRepo(r Repo) error {
	_, err := d.conn.Exec(`
		INSERT INTO repos (full_name, owner, name, description, stars, language, org_name, sync_status, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(full_name) DO UPDATE SET
			description  = excluded.description,
			stars        = excluded.stars,
			language     = excluded.language,
			org_name     = excluded.org_name,
			updated_at   = CURRENT_TIMESTAMP
	`, r.FullName, r.Owner, r.Name, r.Description, r.Stars, r.Language, r.OrgName, r.SyncStatus)
	return err
}

func (d *DB) UpdateSyncStatus(fullName, status string) error {
	if status == "done" {
		_, err := d.conn.Exec(
			`UPDATE repos SET sync_status=?, last_synced=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE full_name=?`,
			status, fullName)
		return err
	}
	_, err := d.conn.Exec(
		`UPDATE repos SET sync_status=?, updated_at=CURRENT_TIMESTAMP WHERE full_name=?`,
		status, fullName)
	return err
}

func (d *DB) UpsertPR(pr PullRequest) error {
	var mergedAt, mergeTimeSecs interface{}
	if pr.MergedAt != nil {
		mergedAt = pr.MergedAt.UTC().Format(time.RFC3339)
	}
	if pr.MergeTimeSecs != nil {
		mergeTimeSecs = *pr.MergeTimeSecs
	}
	_, err := d.conn.Exec(`
		INSERT INTO pull_requests
			(id, repo_full_name, number, title, author_login, merged, opened_at, merged_at, merge_time_secs, review_count, changes_requested_count)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(repo_full_name, number) DO UPDATE SET
			title                   = excluded.title,
			merged                  = excluded.merged,
			merged_at               = excluded.merged_at,
			merge_time_secs         = excluded.merge_time_secs,
			review_count            = excluded.review_count,
			changes_requested_count = excluded.changes_requested_count
	`, pr.ID, pr.RepoFullName, pr.Number, pr.Title, pr.AuthorLogin, pr.Merged,
		pr.OpenedAt.UTC().Format(time.RFC3339), mergedAt, mergeTimeSecs,
		pr.ReviewCount, pr.ChangesRequestedCount)
	return err
}

func (d *DB) UpsertReview(r Review) error {
	_, err := d.conn.Exec(`
		INSERT OR IGNORE INTO reviews (id, repo_full_name, pr_number, reviewer_login, state, submitted_at)
		VALUES (?,?,?,?,?,?)
	`, r.ID, r.RepoFullName, r.PRNumber, r.ReviewerLogin, r.State,
		r.SubmittedAt.UTC().Format(time.RFC3339))
	return err
}

func (d *DB) UpsertUser(u User) error {
	_, err := d.conn.Exec(`
		INSERT INTO users (login, name, avatar_url, bio, public_repos, followers, company, location, is_org, last_fetched)
		VALUES (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
		ON CONFLICT(login) DO UPDATE SET
			name         = excluded.name,
			avatar_url   = excluded.avatar_url,
			bio          = excluded.bio,
			public_repos = excluded.public_repos,
			followers    = excluded.followers,
			company      = excluded.company,
			location     = excluded.location,
			is_org       = excluded.is_org,
			last_fetched = CURRENT_TIMESTAMP
	`, u.Login, u.Name, u.AvatarURL, u.Bio, u.PublicRepos, u.Followers,
		u.Company, u.Location, u.IsOrg)
	return err
}

// ── Stats recalculation ────────────────────────────────────────────────────────

func (d *DB) UpdateRepoStats(fullName string) error {
	_, err := d.conn.Exec(`
		UPDATE repos SET
			pr_count            = (SELECT COUNT(*)   FROM pull_requests WHERE repo_full_name=? AND merged=1),
			merged_pr_count     = (SELECT COUNT(*)   FROM pull_requests WHERE repo_full_name=? AND merged=1),
			avg_merge_time_secs = COALESCE((SELECT AVG(merge_time_secs) FROM pull_requests WHERE repo_full_name=? AND merged=1 AND merge_time_secs IS NOT NULL), 0),
			min_merge_time_secs = COALESCE((SELECT MIN(merge_time_secs) FROM pull_requests WHERE repo_full_name=? AND merged=1 AND merge_time_secs IS NOT NULL), 0),
			max_merge_time_secs = COALESCE((SELECT MAX(merge_time_secs) FROM pull_requests WHERE repo_full_name=? AND merged=1 AND merge_time_secs IS NOT NULL), 0),
			updated_at          = CURRENT_TIMESTAMP
		WHERE full_name=?
	`, fullName, fullName, fullName, fullName, fullName, fullName)
	return err
}

// ── Getters ────────────────────────────────────────────────────────────────────

func (d *DB) GetRepo(fullName string) (*Repo, error) {
	r := &Repo{}
	var lastSynced sql.NullString
	err := d.conn.QueryRow(`
		SELECT full_name, owner, name, description, stars, language, org_name,
		       last_synced, sync_status, pr_count, merged_pr_count,
		       avg_merge_time_secs, min_merge_time_secs, max_merge_time_secs
		FROM repos WHERE full_name=?
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
		t, _ := time.Parse("2006-01-02 15:04:05", lastSynced.String)
		r.LastSynced = &t
	}
	return r, nil
}

func (d *DB) GetUser(login string) (*User, error) {
	u := &User{}
	var lastFetched sql.NullString
	err := d.conn.QueryRow(`
		SELECT login, name, avatar_url, bio, public_repos, followers, company, location, is_org, last_fetched
		FROM users WHERE login=?
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
		t, _ := time.Parse("2006-01-02 15:04:05", lastFetched.String)
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
		LIMIT ?`, order)
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
		LIMIT ?`, limit)
}

func (d *DB) LeaderboardGatekeepers(limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT r.reviewer_login, COUNT(*) as cnt, COALESCE(u.avatar_url,'')
		FROM reviews r
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE r.state='CHANGES_REQUESTED'
		GROUP BY r.reviewer_login
		ORDER BY cnt DESC
		LIMIT ?`, limit)
}

func (d *DB) LeaderboardAuthors(limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT p.author_login, COUNT(*) as cnt, COALESCE(u.avatar_url,'')
		FROM pull_requests p
		LEFT JOIN users u ON u.login=p.author_login
		WHERE p.merged=1
		GROUP BY p.author_login
		ORDER BY cnt DESC
		LIMIT ?`, limit)
}

func (d *DB) LeaderboardCleanApprovals(limit int) ([]LeaderboardEntry, error) {
	rows, err := d.conn.Query(`
		SELECT repo_full_name,
		       COUNT(*) as total,
		       CAST(ROUND(100.0 * SUM(CASE WHEN changes_requested_count=0 AND review_count>0 THEN 1 ELSE 0 END) / COUNT(*)) AS INTEGER) as clean_pct
		FROM pull_requests
		WHERE merged=1 AND review_count>0
		GROUP BY repo_full_name
		HAVING total >= 5
		ORDER BY clean_pct DESC
		LIMIT ?`, limit)
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
		var rawVal interface{}
		if err := rows.Scan(&e.Name, &rawVal, &e.Extra); err != nil {
			log.Printf("db: queryEntries scan error (rank %d): %v", rank, err)
			continue
		}
		switch v := rawVal.(type) {
		case int64:
			e.Value = v
			e.Count = int(v)
		case float64:
			e.Value = int64(v)
			e.Count = int(v)
		}
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
		WHERE r.repo_full_name=?
		GROUP BY r.reviewer_login
		ORDER BY total DESC
		LIMIT ?
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
		WHERE repo_full_name=? AND merged=1
		ORDER BY merged_at DESC
		LIMIT ?
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
		WHERE r.reviewer_login=?
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
		       SUM(CASE WHEN merged=1 THEN 1 ELSE 0 END),
		       COALESCE(AVG(CASE WHEN merged=1 THEN merge_time_secs END), 0)
		FROM pull_requests
		WHERE author_login=?
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
		) WHERE reviewer_login=?
	`, login)
}

func (d *DB) UserGatekeeperRank(login string) (int, error) {
	return d.rankQuery(`
		SELECT rank FROM (
			SELECT reviewer_login, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
			FROM reviews WHERE state='CHANGES_REQUESTED' GROUP BY reviewer_login
		) WHERE reviewer_login=?
	`, login)
}

func (d *DB) RepoSpeedRank(fullName string) (int, error) {
	return d.rankQuery(`
		SELECT rank FROM (
			SELECT full_name, ROW_NUMBER() OVER (ORDER BY avg_merge_time_secs ASC) as rank
			FROM repos WHERE merged_pr_count>=3 AND avg_merge_time_secs>0
		) WHERE full_name=?
	`, fullName)
}

func (d *DB) RepoGraveyardRank(fullName string) (int, error) {
	return d.rankQuery(`
		SELECT rank FROM (
			SELECT full_name, ROW_NUMBER() OVER (ORDER BY avg_merge_time_secs DESC) as rank
			FROM repos WHERE merged_pr_count>=3 AND avg_merge_time_secs>0
		) WHERE full_name=?
	`, fullName)
}

func (d *DB) UserAuthorRank(login string) (int, error) {
	return d.rankQuery(`
		SELECT rank FROM (
			SELECT author_login, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
			FROM pull_requests WHERE merged=1 GROUP BY author_login
		) WHERE author_login=?
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
		FROM repos WHERE org_name=?
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
		WHERE repo.org_name=?
		GROUP BY r.reviewer_login
		ORDER BY cnt DESC
		LIMIT ?`, limit)
}

func (d *DB) OrgGatekeeperLeaderboard(orgName string, limit int) ([]LeaderboardEntry, error) {
	return d.queryEntries(`
		SELECT r.reviewer_login, COUNT(*) as cnt, COALESCE(u.avatar_url,'')
		FROM reviews r
		JOIN repos repo ON repo.full_name=r.repo_full_name
		LEFT JOIN users u ON u.login=r.reviewer_login
		WHERE repo.org_name=? AND r.state='CHANGES_REQUESTED'
		GROUP BY r.reviewer_login
		ORDER BY cnt DESC
		LIMIT ?`, limit)
}

// ── Global stats ───────────────────────────────────────────────────────────────

func (d *DB) TotalStats() (repos, prs, reviews int) {
	d.conn.QueryRow(`SELECT COUNT(*) FROM repos WHERE sync_status='done'`).Scan(&repos)
	d.conn.QueryRow(`SELECT COUNT(*) FROM pull_requests WHERE merged=1`).Scan(&prs)
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
	// Cast all time columns to avoid float64/int64 ambiguity from stored AVG() values.
	q := fmt.Sprintf(`
		SELECT full_name,
		       CAST(avg_merge_time_secs AS INTEGER),
		       CAST(min_merge_time_secs AS INTEGER),
		       CAST(max_merge_time_secs AS INTEGER),
		       merged_pr_count
		FROM repos
		WHERE merged_pr_count >= 3 AND avg_merge_time_secs > 0
		ORDER BY avg_merge_time_secs %s
		LIMIT ? OFFSET ?`, order)
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
		LIMIT ? OFFSET ?`, limit, offset)
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
		LIMIT ? OFFSET ?`, limit, offset)
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
		       CAST(COALESCE(AVG(p.merge_time_secs), 0) AS INTEGER) as avg_secs
		FROM pull_requests p
		LEFT JOIN users u ON u.login = p.author_login
		WHERE p.merged = 1
		GROUP BY p.author_login
		ORDER BY merged DESC
		LIMIT ? OFFSET ?`, limit, offset)
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
		WHERE merged=1 AND review_count>0
		GROUP BY repo_full_name
		HAVING total >= 5
		ORDER BY clean_pct DESC
		LIMIT ? OFFSET ?`, limit, offset)
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

func (d *DB) SearchRepos(query string, limit int) ([]Repo, error) {
	rows, err := d.conn.Query(`
		SELECT full_name, owner, name, description, stars, language, org_name,
		       last_synced, sync_status, pr_count, merged_pr_count,
		       avg_merge_time_secs, min_merge_time_secs, max_merge_time_secs
		FROM repos
		WHERE full_name LIKE ? OR name LIKE ?
		ORDER BY stars DESC
		LIMIT ?
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
		var lastSynced sql.NullString
		if err := rows.Scan(
			&r.FullName, &r.Owner, &r.Name, &r.Description, &r.Stars, &r.Language, &r.OrgName,
			&lastSynced, &r.SyncStatus, &r.PRCount, &r.MergedPRCount,
			&r.AvgMergeTimeSecs, &r.MinMergeTimeSecs, &r.MaxMergeTimeSecs,
		); err != nil {
			log.Printf("db: scanRepos scan error: %v", err)
			continue
		}
		if lastSynced.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", lastSynced.String)
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
		var mergedAt sql.NullString
		var mts sql.NullInt64
		if err := rows.Scan(
			&pr.ID, &pr.RepoFullName, &pr.Number, &pr.Title, &pr.AuthorLogin, &pr.Merged,
			&pr.OpenedAt, &mergedAt, &mts, &pr.ReviewCount, &pr.ChangesRequestedCount,
		); err != nil {
			continue
		}
		if mergedAt.Valid {
			t, _ := time.Parse(time.RFC3339, mergedAt.String)
			pr.MergedAt = &t
		}
		if mts.Valid {
			pr.MergeTimeSecs = &mts.Int64
		}
		prs = append(prs, pr)
	}
	return prs, rows.Err()
}
