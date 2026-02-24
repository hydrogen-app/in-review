package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"inreview/internal/db"
)

type HomeData struct {
	BaseData
	TotalRepos      int
	TotalPRs        int
	TotalReviews    int
	SpeedDemons     []db.LeaderboardEntry
	PRGraveyard     []db.LeaderboardEntry
	ReviewChamps    []db.LeaderboardEntry
	Gatekeepers     []db.LeaderboardEntry
	MergeMasters    []db.LeaderboardEntry
	OneShot         []db.LeaderboardEntry
	PopularVisits   []db.PageVisit
	RecentVisits    []db.PageVisit
	OGTitle         string
	OGDesc          string
	OGUrl           string
}

// homeLBCache holds the six mini-leaderboard slices rendered on the home page.
type homeLBCache struct {
	SpeedDemons []db.LeaderboardEntry
	PRGraveyard []db.LeaderboardEntry
	ReviewChamps []db.LeaderboardEntry
	Gatekeepers  []db.LeaderboardEntry
	MergeMasters []db.LeaderboardEntry
	OneShot      []db.LeaderboardEntry
}

const homeLBCacheKey = "home:lb"
const homeLBCacheTTL = 3 * time.Minute

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	data := HomeData{}
	data.TotalRepos, data.TotalPRs, data.TotalReviews = h.db.TotalStats()

	ctx := context.Background()
	var lb homeLBCache
	if raw, ok := h.cache.Get(ctx, homeLBCacheKey); ok {
		_ = json.Unmarshal(raw, &lb)
	} else {
		var err error
		lb.SpeedDemons, _ = h.db.LeaderboardReposBySpeed("ASC", 5)
		lb.PRGraveyard, _ = h.db.LeaderboardReposBySpeed("DESC", 5)
		lb.ReviewChamps, err = h.db.LeaderboardReviewers(5)
		if err != nil {
			log.Printf("home: LeaderboardReviewers error: %v", err)
		}
		lb.Gatekeepers, err = h.db.LeaderboardGatekeepers(5)
		if err != nil {
			log.Printf("home: LeaderboardGatekeepers error: %v", err)
		}
		lb.MergeMasters, err = h.db.LeaderboardAuthors(5)
		if err != nil {
			log.Printf("home: LeaderboardAuthors error: %v", err)
		}
		lb.OneShot, _ = h.db.LeaderboardCleanApprovals(5)
		if raw, err := json.Marshal(lb); err == nil {
			h.cache.Set(ctx, homeLBCacheKey, raw, homeLBCacheTTL)
		}
	}
	data.SpeedDemons = lb.SpeedDemons
	data.PRGraveyard = lb.PRGraveyard
	data.ReviewChamps = lb.ReviewChamps
	data.Gatekeepers = lb.Gatekeepers
	data.MergeMasters = lb.MergeMasters
	data.OneShot = lb.OneShot


	data.OGDesc = fmt.Sprintf("%d PRs analyzed across %d repos. Global leaderboards for GitHub PR review time. If you aren't reviewing, you're ngmi.", data.TotalPRs, data.TotalRepos)

	data.PopularVisits, _ = h.db.PopularVisits(3)
	if len(data.PopularVisits) > 0 {
		exclude := make([]string, len(data.PopularVisits))
		for i, v := range data.PopularVisits {
			exclude[i] = v.Path
		}
		data.RecentVisits, _ = h.db.RecentVisits(5, exclude)
	} else {
		data.RecentVisits, _ = h.db.RecentVisits(5, nil)
	}

	data.BaseData = h.baseData(r)
	h.render(w, "home", data)
}

// LeaderboardAPI returns a leaderboard partial for HTMX category updates.
func (h *Handler) LeaderboardAPI(w http.ResponseWriter, r *http.Request) {
	category := r.URL.Query().Get("cat")

	type LeaderboardData struct {
		Category string
		Entries  []db.LeaderboardEntry
	}

	data := LeaderboardData{Category: category}

	switch category {
	case "speed":
		data.Entries, _ = h.db.LeaderboardReposBySpeed("ASC", 10)
	case "graveyard":
		data.Entries, _ = h.db.LeaderboardReposBySpeed("DESC", 10)
	case "reviewers":
		data.Entries, _ = h.db.LeaderboardReviewers(10)
	case "gatekeepers":
		data.Entries, _ = h.db.LeaderboardGatekeepers(10)
	case "authors":
		data.Entries, _ = h.db.LeaderboardAuthors(10)
	case "oneshot":
		data.Entries, _ = h.db.LeaderboardCleanApprovals(10)
	}

	h.renderPartial(w, "leaderboard", data)
}
