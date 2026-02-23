package worker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/rdb"
)

const (
	maxPRsPerRepo = 500
	syncCooldown  = 6 * time.Hour
	workerCount   = 3
	popTimeout    = 30 * time.Second
)

// Worker manages background GitHub sync jobs.
type Worker struct {
	gh  *github.Client
	db  *db.DB
	rdb *rdb.Client
}

func New(gh *github.Client, db *db.DB, rdb *rdb.Client) *Worker {
	return &Worker{gh: gh, db: db, rdb: rdb}
}

// Start launches background sync goroutines.
func (w *Worker) Start() {
	for i := 0; i < workerCount; i++ {
		go w.run()
	}
}

func (w *Worker) run() {
	ctx := context.Background()
	for {
		fullName, ok := w.rdb.QPop(ctx, popTimeout)
		if !ok {
			continue
		}
		w.syncRepo(fullName)
	}
}

// Queue schedules a repo sync unless one is already in-flight or recently completed.
// Set force=true to bypass the cooldown check.
func (w *Worker) Queue(fullName string, force bool) {
	ctx := context.Background()
	if w.rdb.QIsInProgress(ctx, fullName) {
		return
	}
	if !force {
		repo, _ := w.db.GetRepo(fullName)
		if repo != nil && repo.LastSynced != nil &&
			time.Since(*repo.LastSynced) < syncCooldown &&
			repo.SyncStatus == "done" {
			return
		}
	}
	// Reserve the slot before pushing so concurrent Queue calls are idempotent.
	w.rdb.QMarkInProgress(ctx, fullName)
	if err := w.rdb.QPush(ctx, fullName); err != nil {
		log.Printf("[queue] failed to push %s: %v", fullName, err)
		w.rdb.QMarkDone(ctx, fullName)
	}
}

// IsSyncing returns true if the repo is currently queued or being synced.
func (w *Worker) IsSyncing(fullName string) bool {
	return w.rdb.QIsInProgress(context.Background(), fullName)
}

// syncRepo performs the full GitHub data pull for one repository.
func (w *Worker) syncRepo(fullName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	defer w.rdb.QMarkDone(ctx, fullName)

	parts := strings.SplitN(fullName, "/", 2)
	if len(parts) != 2 {
		return
	}
	owner, name := parts[0], parts[1]
	log.Printf("[sync] starting %s", fullName)
	w.db.UpdateSyncStatus(fullName, "syncing")

	// Single GraphQL call fetches repo metadata, owner, and all merged PRs with reviews.
	result, err := w.gh.SyncRepo(ctx, owner, name, maxPRsPerRepo)
	if err != nil {
		log.Printf("[sync] error fetching %s: %v", fullName, err)
		w.db.UpdateSyncStatus(fullName, "error")
		return
	}

	// ── Owner / org metadata ──────────────────────────────────────────────────
	isOrg := result.Owner.Type == "Organization"
	orgName := ""
	if isOrg {
		orgName = owner
	}
	w.db.UpsertUser(db.User{
		Login:       result.Owner.Login,
		Name:        result.Owner.Name,
		AvatarURL:   result.Owner.AvatarURL,
		Bio:         result.Owner.Bio,
		PublicRepos: result.Owner.PublicRepos,
		Followers:   result.Owner.Followers,
		Company:     result.Owner.Company,
		Location:    result.Owner.Location,
		IsOrg:       isOrg,
	})

	// ── Repo metadata ─────────────────────────────────────────────────────────
	w.db.UpsertRepo(db.Repo{
		FullName:    fullName,
		Owner:       owner,
		Name:        name,
		Description: result.Repo.Description,
		Stars:       result.Repo.Stars,
		Language:    result.Repo.Language,
		OrgName:     orgName,
		SyncStatus:  "syncing",
	})

	log.Printf("[sync] %s: %d merged PRs", fullName, len(result.PRs))

	// ── Pull requests + reviews ───────────────────────────────────────────────
	for _, ghPR := range result.PRs {
		w.db.UpsertUser(db.User{Login: ghPR.Author})

		var mts *int64
		if ghPR.MergedAt != nil {
			secs := int64(ghPR.MergedAt.Sub(ghPR.CreatedAt).Seconds())
			mts = &secs
		}

		pr := db.PullRequest{
			ID:            fmt.Sprintf("%s#%d", fullName, ghPR.Number),
			RepoFullName:  fullName,
			Number:        ghPR.Number,
			Title:         ghPR.Title,
			AuthorLogin:   ghPR.Author,
			Merged:        ghPR.MergedAt != nil,
			OpenedAt:      ghPR.CreatedAt,
			MergedAt:      ghPR.MergedAt,
			MergeTimeSecs: mts,
			ReviewCount:   len(ghPR.Reviews),
		}

		for _, rev := range ghPR.Reviews {
			if rev.State == "CHANGES_REQUESTED" {
				pr.ChangesRequestedCount++
			}
			w.db.UpsertUser(db.User{
				Login:     rev.User.Login,
				AvatarURL: rev.User.AvatarURL,
			})
			w.db.UpsertReview(db.Review{
				ID:            fmt.Sprintf("%d", rev.ID),
				RepoFullName:  fullName,
				PRNumber:      ghPR.Number,
				ReviewerLogin: rev.User.Login,
				State:         rev.State,
				SubmittedAt:   rev.SubmittedAt,
			})
		}

		w.db.UpsertPR(pr)
	}

	w.db.UpdateRepoStats(fullName)
	w.db.UpdateSyncStatus(fullName, "done")
	log.Printf("[sync] done %s", fullName)
}
