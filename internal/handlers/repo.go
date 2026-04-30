package handlers

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"inreview/internal/db"
)

type RepoData struct {
	BaseData
	Repo         *db.Repo
	TopReviewers []db.ReviewerStats
	RecentPRs    []db.PullRequest
	SpeedRank    int
	IsSyncing    bool
	OwnerUser    *db.User
	Trim         int
	OGTitle      string
	OGDesc       string
	OGUrl        string
	ShareURL     string
}

const repoPageCacheTTL = 5 * time.Minute

// repoPageCache holds the non-chart DB-query results for a repo page.
type repoPageCache struct {
	TopReviewers []db.ReviewerStats `json:"topReviewers"`
	RecentPRs    []db.PullRequest   `json:"recentPRs"`
	SpeedRank    int                `json:"speedRank"`
}

// RepoChartsData is passed to the repo_charts partial.
type RepoChartsData struct {
	Owner         string
	Name          string
	Trim          int
	SizeChartJSON string
	TimeChartJSON string
}

const repoChartsCacheTTL = 5 * time.Minute

// repoChartsCache holds the chart query results for the lazy-loaded charts partial.
type repoChartsCache struct {
	SizeChartJSON string `json:"sizeChartJSON"`
	TimeChartJSON string `json:"timeChartJSON"`
}

// sizeChartPayload is marshaled to JSON and embedded directly in the repo page.
type sizeChartPayload struct {
	Labels       []string  `json:"labels"`
	PRCounts     []int     `json:"prCounts"`
	AvgHours     []float64 `json:"avgHours"`
	ApprovalRate []float64 `json:"approvalRate"`
}

// TriggerSync forces a fresh sync for a repo.
func (h *Handler) TriggerSync(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name
	h.worker.Queue(fullName, true)
	w.WriteHeader(http.StatusNoContent)
}
