package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"inreview/internal/db"
)

// BlogData is passed to the blog page template and the blog_stats partial.
type BlogData struct {
	BaseData
	LiveStats    db.GlobalOverallStats
	TopReviewers []db.LeaderboardEntry
	TopSpeed     []db.LeaderboardEntry
	TotalRepos   int
	TotalPRs     int
	TotalReviews int
	OGTitle      string
	OGDesc       string
	OGUrl        string
}

// cachedDefaultOverallStats avoids running the expensive global median query
// from the blog/live-stats path. The /stats page owns that cache; if it has not
// been populated yet, the live widget still shows cheap precomputed counts.
func (h *Handler) cachedDefaultOverallStats(ctx context.Context) db.GlobalOverallStats {
	if h.cache != nil {
		if raw, ok := h.cache.Get(ctx, "stats:v4:0:0:0"); ok {
			var stats StatsData
			if json.Unmarshal(raw, &stats) == nil {
				return stats.Overall
			}
		}
	}
	totalRepos, totalPRs, _ := h.db.TotalStats()
	return db.GlobalOverallStats{TotalPRs: totalPRs, TotalRepos: totalRepos}
}

func (h *Handler) Blog(w http.ResponseWriter, r *http.Request) {
	overall := h.cachedDefaultOverallStats(r.Context())
	topReviewers, _ := h.db.LeaderboardReviewers(5)
	topSpeed, _ := h.db.LeaderboardReposBySpeed("ASC", 5)
	totalRepos, totalPRs, totalReviews := h.db.TotalStats()

	data := BlogData{
		BaseData:     h.baseData(r),
		LiveStats:    overall,
		TopReviewers: topReviewers,
		TopSpeed:     topSpeed,
		TotalRepos:   totalRepos,
		TotalPRs:     totalPRs,
		TotalReviews: totalReviews,
		OGTitle:      "PR Review Time: What the Data Says — ngmi",
		OGDesc:       "Analysis of PR review patterns across thousands of GitHub repositories.",
		OGUrl:        "https://ngmi.review/blog",
	}
	h.render(w, "blog", data)
}

// BlogLiveStats serves the live stats partial for HTMX polling.
func (h *Handler) BlogLiveStats(w http.ResponseWriter, r *http.Request) {
	overall := h.cachedDefaultOverallStats(r.Context())
	topReviewers, _ := h.db.LeaderboardReviewers(5)
	topSpeed, _ := h.db.LeaderboardReposBySpeed("ASC", 5)
	totalRepos, totalPRs, totalReviews := h.db.TotalStats()

	data := BlogData{
		LiveStats:    overall,
		TopReviewers: topReviewers,
		TopSpeed:     topSpeed,
		TotalRepos:   totalRepos,
		TotalPRs:     totalPRs,
		TotalReviews: totalReviews,
	}
	h.renderPartial(w, "blog_stats", data)
}
