package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"inreview/internal/db"
)

type RepoData struct {
	Repo         *db.Repo
	TopReviewers []db.ReviewerStats
	RecentPRs    []db.PullRequest
	SpeedRank    int
	IsSyncing    bool
	OwnerUser    *db.User
}

func (h *Handler) Repo(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name

	// Ensure repo is in DB
	repo, _ := h.db.GetRepo(fullName)
	if repo == nil {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		ghRepo, err := h.gh.GetRepo(ctx, owner, name)
		if err != nil {
			h.renderError(w, http.StatusNotFound, "Repo Not Found",
				"Could not find "+fullName+" on GitHub. Check the spelling and try again.")
			return
		}
		h.db.UpsertRepo(db.Repo{
			FullName:    fullName,
			Owner:       owner,
			Name:        ghRepo.Name,
			Description: ghRepo.Description,
			Stars:       ghRepo.Stars,
			Language:    ghRepo.Language,
			SyncStatus:  "pending",
		})
		repo, _ = h.db.GetRepo(fullName)
	}

	// Queue sync if needed
	h.worker.Queue(fullName, false)

	data := RepoData{
		Repo:      repo,
		IsSyncing: h.worker.IsSyncing(fullName),
	}

	data.TopReviewers, _ = h.db.RepoTopReviewers(fullName, 10)
	data.RecentPRs, _ = h.db.RecentMergedPRs(fullName, 20)
	data.SpeedRank, _ = h.db.RepoSpeedRank(fullName)
	data.OwnerUser, _ = h.db.GetUser(owner)

	h.render(w, "repo", data)
}

// TriggerSync forces a fresh sync for a repo.
func (h *Handler) TriggerSync(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name
	h.worker.Queue(fullName, true)
	w.Header().Set("HX-Refresh", "true")
	w.WriteHeader(http.StatusNoContent)
}

// SyncStatus returns a small HTML snippet with current sync state (polled by HTMX).
func (h *Handler) SyncStatus(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name

	isSyncing := h.worker.IsSyncing(fullName)
	repo, _ := h.db.GetRepo(fullName)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if isSyncing {
		w.Write([]byte(`<span class="sync-badge syncing" hx-get="/api/sync-status/` + owner + `/` + name + `" hx-trigger="every 2s" hx-swap="outerHTML">⟳ Syncing…</span>`))
		return
	}
	if repo != nil && repo.LastSynced != nil {
		w.Write([]byte(`<span class="sync-badge done">✓ Synced ` + timeAgo(repo.LastSynced) + `</span>`))
		return
	}
	w.Write([]byte(`<span class="sync-badge pending">⏳ Pending</span>`))
}
