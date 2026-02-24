package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"
	"inreview/internal/db"
)

type RepoData struct {
	Repo          *db.Repo
	TopReviewers  []db.ReviewerStats
	RecentPRs     []db.PullRequest
	SpeedRank     int
	IsSyncing     bool
	OwnerUser     *db.User
	SizeChartJSON template.JS
	OGTitle       string
	OGDesc        string
	OGUrl         string
	ShareURL      string
}

// sizeChartPayload is marshaled to JSON and embedded directly in the repo page.
type sizeChartPayload struct {
	Labels       []string  `json:"labels"`
	PRCounts     []int     `json:"prCounts"`
	AvgHours     []float64 `json:"avgHours"`
	ApprovalRate []float64 `json:"approvalRate"`
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
		// UpsertRepo may lose a race with the worker inserting the same repo;
		// ignore the error and re-read regardless.
		_ = h.db.UpsertRepo(db.Repo{
			FullName:    fullName,
			Owner:       owner,
			Name:        ghRepo.Name,
			Description: ghRepo.Description,
			Stars:       ghRepo.Stars,
			Language:    ghRepo.Language,
			SyncStatus:  "pending",
		})
		repo, _ = h.db.GetRepo(fullName)
		if repo == nil {
			// Build a minimal in-memory repo so the page still renders.
			repo = &db.Repo{
				FullName:    fullName,
				Owner:       owner,
				Name:        ghRepo.Name,
				Description: ghRepo.Description,
				Stars:       ghRepo.Stars,
				Language:    ghRepo.Language,
				SyncStatus:  "pending",
			}
		}
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

	if buckets, err := h.db.RepoSizeChartData(fullName); err == nil && len(buckets) > 0 {
		payload := sizeChartPayload{}
		for _, b := range buckets {
			payload.Labels = append(payload.Labels, b.Label)
			payload.PRCounts = append(payload.PRCounts, b.PRCount)
			payload.AvgHours = append(payload.AvgHours, roundTo1(b.AvgSecs/3600))
			payload.ApprovalRate = append(payload.ApprovalRate, roundTo1(b.ApprovalRate))
		}
		if raw, err := json.Marshal(payload); err == nil {
			data.SizeChartJSON = template.JS(raw)
		}
	}

	// ── OG / share metadata ───────────────────────────────────────────────────
	data.OGTitle = fullName + " — ngmi"
	data.OGUrl = "https://ngmi.review/repo/" + fullName
	ogDesc := fullName
	if repo.AvgMergeTimeSecs > 0 {
		ogDesc += " merges PRs in " + formatDuration(repo.AvgMergeTimeSecs) + " on average"
	}
	if data.SpeedRank > 0 {
		ogDesc += fmt.Sprintf(" (#%d globally)", data.SpeedRank)
	}
	ogDesc += ". Track your repo at ngmi.review."
	data.OGDesc = ogDesc

	shareText := fullName
	if repo.AvgMergeTimeSecs > 0 {
		shareText += " merges PRs in " + formatDuration(repo.AvgMergeTimeSecs)
	}
	if data.SpeedRank > 0 {
		shareText += fmt.Sprintf(", #%d globally", data.SpeedRank)
	}
	shareText += ". If you aren't reviewing, you're ngmi."
	data.ShareURL = "https://twitter.com/intent/tweet?text=" + url.QueryEscape(shareText) +
		"&url=" + url.QueryEscape(data.OGUrl)

	h.db.RecordVisit("/repo/"+fullName, "repo", fullName)
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

	qpos := h.worker.QueuePosition(fullName)
	repo, _ := h.db.GetRepo(fullName)

	poll := `hx-get="/api/sync-status/` + owner + `/` + name + `" hx-trigger="every 2s" hx-swap="outerHTML"`
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	switch {
	case qpos > 0:
		// Waiting in queue — show position
		w.Write([]byte(`<span class="sync-badge syncing" ` + poll + `>⟳ Queue position #` + fmt.Sprintf("%d", qpos) + `</span>`))
	case qpos == 0:
		// Popped from queue, worker is actively fetching
		w.Write([]byte(`<span class="sync-badge syncing" ` + poll + `>⟳ Syncing…</span>`))
	case repo != nil && repo.LastSynced != nil:
		w.Write([]byte(`<span class="sync-badge done">✓ Synced ` + timeAgo(repo.LastSynced) + `</span>`))
	default:
		w.Write([]byte(`<span class="sync-badge pending">⏳ Pending</span>`))
	}
}
