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

// homeCache holds all data rendered on the home page.
type homeCache struct {
	TotalRepos   int
	TotalPRs     int
	TotalReviews int
	SpeedDemons  []db.LeaderboardEntry
	PRGraveyard  []db.LeaderboardEntry
	ReviewChamps []db.LeaderboardEntry
	Gatekeepers  []db.LeaderboardEntry
	MergeMasters []db.LeaderboardEntry
	OneShot      []db.LeaderboardEntry
	PopularVisits []db.PageVisit
	RecentVisits  []db.PageVisit
}

const homeCacheKey = "home:v2"
const homeCacheTTL = 3 * time.Minute

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var hc homeCache
	if raw, ok := h.cache.Get(ctx, homeCacheKey); ok {
		_ = json.Unmarshal(raw, &hc)
	} else {
		// Run all queries in parallel.
		type lbResult struct {
			entries []db.LeaderboardEntry
			err     error
		}
		type statsResult struct{ repos, prs, reviews int }
		type visitsResult struct{ popular, recent []db.PageVisit }

		statsCh    := make(chan statsResult, 1)
		speedCh    := make(chan lbResult, 1)
		graveCh    := make(chan lbResult, 1)
		champsCh   := make(chan lbResult, 1)
		gatesCh    := make(chan lbResult, 1)
		mastersCh  := make(chan lbResult, 1)
		oneShotCh  := make(chan lbResult, 1)
		visitsCh   := make(chan visitsResult, 1)

		go func() {
			r, p, rv := h.db.TotalStats()
			statsCh <- statsResult{r, p, rv}
		}()
		go func() { v, e := h.db.LeaderboardReposBySpeed("ASC", 5); speedCh <- lbResult{v, e} }()
		go func() { v, e := h.db.LeaderboardReposBySpeed("DESC", 5); graveCh <- lbResult{v, e} }()
		go func() { v, e := h.db.LeaderboardReviewers(5); champsCh <- lbResult{v, e} }()
		go func() { v, e := h.db.LeaderboardGatekeepers(5); gatesCh <- lbResult{v, e} }()
		go func() { v, e := h.db.LeaderboardAuthors(5); mastersCh <- lbResult{v, e} }()
		go func() { v, e := h.db.LeaderboardCleanApprovals(5); oneShotCh <- lbResult{v, e} }()
		go func() {
			popular, _ := h.db.PopularVisits(3)
			var exclude []string
			for _, v := range popular {
				exclude = append(exclude, v.Path)
			}
			recent, _ := h.db.RecentVisits(5, exclude)
			visitsCh <- visitsResult{popular, recent}
		}()

		stats   := <-statsCh
		speed   := <-speedCh
		grave   := <-graveCh
		champs  := <-champsCh
		gates   := <-gatesCh
		masters := <-mastersCh
		oneshot := <-oneShotCh
		visits  := <-visitsCh

		if champs.err != nil {
			log.Printf("home: LeaderboardReviewers error: %v", champs.err)
		}
		if gates.err != nil {
			log.Printf("home: LeaderboardGatekeepers error: %v", gates.err)
		}
		if masters.err != nil {
			log.Printf("home: LeaderboardAuthors error: %v", masters.err)
		}

		hc = homeCache{
			TotalRepos:    stats.repos,
			TotalPRs:      stats.prs,
			TotalReviews:  stats.reviews,
			SpeedDemons:   speed.entries,
			PRGraveyard:   grave.entries,
			ReviewChamps:  champs.entries,
			Gatekeepers:   gates.entries,
			MergeMasters:  masters.entries,
			OneShot:       oneshot.entries,
			PopularVisits: visits.popular,
			RecentVisits:  visits.recent,
		}
		if raw, err := json.Marshal(hc); err == nil {
			h.cache.Set(ctx, homeCacheKey, raw, homeCacheTTL)
		}
	}

	data := HomeData{
		TotalRepos:    hc.TotalRepos,
		TotalPRs:      hc.TotalPRs,
		TotalReviews:  hc.TotalReviews,
		SpeedDemons:   hc.SpeedDemons,
		PRGraveyard:   hc.PRGraveyard,
		ReviewChamps:  hc.ReviewChamps,
		Gatekeepers:   hc.Gatekeepers,
		MergeMasters:  hc.MergeMasters,
		OneShot:       hc.OneShot,
		PopularVisits: hc.PopularVisits,
		RecentVisits:  hc.RecentVisits,
	}
	data.OGDesc = fmt.Sprintf("%d PRs analyzed across %d repos. Global leaderboards for GitHub PR review time. If you aren't reviewing, you're ngmi.", data.TotalPRs, data.TotalRepos)
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
