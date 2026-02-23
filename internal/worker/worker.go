package worker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"inreview/internal/db"
	"inreview/internal/github"
)

const (
	maxPRsPerRepo  = 500
	syncCooldown   = 6 * time.Hour
	prFetchDelay   = 80 * time.Millisecond // stay under rate limits
	workerCount    = 3
	queueSize      = 200
)

// Worker manages background GitHub sync jobs.
type Worker struct {
	gh         *github.Client
	db         *db.DB
	inProgress sync.Map
	queue      chan string
}

func New(gh *github.Client, db *db.DB) *Worker {
	return &Worker{
		gh:    gh,
		db:    db,
		queue: make(chan string, queueSize),
	}
}

// Start launches background sync goroutines.
func (w *Worker) Start() {
	for i := 0; i < workerCount; i++ {
		go w.run()
	}
}

func (w *Worker) run() {
	for fullName := range w.queue {
		w.syncRepo(fullName)
	}
}

// Queue schedules a repo sync unless one is already in-flight or recently completed.
// Set force=true to bypass the cooldown check.
func (w *Worker) Queue(fullName string, force bool) {
	if _, loaded := w.inProgress.LoadOrStore(fullName, true); loaded {
		return
	}
	if !force {
		repo, _ := w.db.GetRepo(fullName)
		if repo != nil && repo.LastSynced != nil &&
			time.Since(*repo.LastSynced) < syncCooldown &&
			repo.SyncStatus == "done" {
			w.inProgress.Delete(fullName)
			return
		}
	}
	select {
	case w.queue <- fullName:
	default:
		// Queue full; drop this job silently
		w.inProgress.Delete(fullName)
	}
}

// IsSyncing returns true if the repo is currently being synced.
func (w *Worker) IsSyncing(fullName string) bool {
	_, ok := w.inProgress.Load(fullName)
	return ok
}

// syncRepo performs the full GitHub data pull for one repository.
func (w *Worker) syncRepo(fullName string) {
	defer w.inProgress.Delete(fullName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	parts := strings.SplitN(fullName, "/", 2)
	if len(parts) != 2 {
		return
	}
	owner, name := parts[0], parts[1]
	log.Printf("[sync] starting %s", fullName)
	w.db.UpdateSyncStatus(fullName, "syncing")

	// ── Repo metadata ─────────────────────────────────────────────────────────
	ghRepo, err := w.gh.GetRepo(ctx, owner, name)
	if err != nil {
		log.Printf("[sync] error fetching repo %s: %v", fullName, err)
		w.db.UpdateSyncStatus(fullName, "error")
		return
	}

	// ── Owner / org metadata ──────────────────────────────────────────────────
	orgName := ""
	ghUser, err := w.gh.GetUser(ctx, owner)
	if err == nil {
		isOrg := ghUser.Type == "Organization"
		if isOrg {
			orgName = owner
		}
		w.db.UpsertUser(db.User{
			Login:       ghUser.Login,
			Name:        ghUser.Name,
			AvatarURL:   ghUser.AvatarURL,
			Bio:         ghUser.Bio,
			PublicRepos: ghUser.PublicRepos,
			Followers:   ghUser.Followers,
			Company:     ghUser.Company,
			Location:    ghUser.Location,
			IsOrg:       isOrg,
		})
	}

	w.db.UpsertRepo(db.Repo{
		FullName:    fullName,
		Owner:       owner,
		Name:        name,
		Description: ghRepo.Description,
		Stars:       ghRepo.Stars,
		Language:    ghRepo.Language,
		OrgName:     orgName,
		SyncStatus:  "syncing",
	})

	// ── Pull requests ─────────────────────────────────────────────────────────
	prs, err := w.gh.GetMergedPRs(ctx, owner, name, maxPRsPerRepo)
	if err != nil {
		log.Printf("[sync] error fetching PRs for %s: %v", fullName, err)
		w.db.UpdateSyncStatus(fullName, "error")
		return
	}
	log.Printf("[sync] %s: %d merged PRs", fullName, len(prs))

	for _, ghPR := range prs {
		// Cache PR author
		w.db.UpsertUser(db.User{Login: ghPR.User.Login})

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
			AuthorLogin:   ghPR.User.Login,
			Merged:        ghPR.MergedAt != nil,
			OpenedAt:      ghPR.CreatedAt,
			MergedAt:      ghPR.MergedAt,
			MergeTimeSecs: mts,
		}

		// ── Reviews ───────────────────────────────────────────────────────────
		reviews, err := w.gh.GetPRReviews(ctx, owner, name, ghPR.Number)
		if err == nil {
			pr.ReviewCount = len(reviews)
			for _, rev := range reviews {
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
		}

		w.db.UpsertPR(pr)
		time.Sleep(prFetchDelay)
	}

	w.db.UpdateRepoStats(fullName)
	w.db.UpdateSyncStatus(fullName, "done")
	log.Printf("[sync] done %s", fullName)
}
