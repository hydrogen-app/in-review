package handlers

import (
	"context"
	"encoding/json"

	"inreview/internal/db"
)

// BlogData is returned by the blog JSON endpoint.
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
