package handlers

import (
	"fmt"
	"log"
	"net/http"

	"inreview/internal/db"
)

type HomeData struct {
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

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	data := HomeData{}
	data.TotalRepos, data.TotalPRs, data.TotalReviews = h.db.TotalStats()

	data.SpeedDemons, _ = h.db.LeaderboardReposBySpeed("ASC", 5)
	data.PRGraveyard, _ = h.db.LeaderboardReposBySpeed("DESC", 5)
	var err error
	data.ReviewChamps, err = h.db.LeaderboardReviewers(5)
	if err != nil {
		log.Printf("home: LeaderboardReviewers error: %v", err)
	}
	data.Gatekeepers, err = h.db.LeaderboardGatekeepers(5)
	if err != nil {
		log.Printf("home: LeaderboardGatekeepers error: %v", err)
	}
	data.MergeMasters, err = h.db.LeaderboardAuthors(5)
	if err != nil {
		log.Printf("home: LeaderboardAuthors error: %v", err)
	}
	data.OneShot, _ = h.db.LeaderboardCleanApprovals(5)

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
