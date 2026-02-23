package handlers

import (
	"net/http"

	"inreview/internal/db"
)

type HomeData struct {
	TotalRepos   int
	TotalPRs     int
	TotalReviews int
	SpeedDemons  []db.LeaderboardEntry
	PRGraveyard  []db.LeaderboardEntry
	ReviewChamps []db.LeaderboardEntry
	Gatekeepers  []db.LeaderboardEntry
	MergeMasters []db.LeaderboardEntry
	OneShot      []db.LeaderboardEntry
}

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	data := HomeData{}
	data.TotalRepos, data.TotalPRs, data.TotalReviews = h.db.TotalStats()

	data.SpeedDemons, _ = h.db.LeaderboardReposBySpeed("ASC", 5)
	data.PRGraveyard, _ = h.db.LeaderboardReposBySpeed("DESC", 5)
	data.ReviewChamps, _ = h.db.LeaderboardReviewers(5)
	data.Gatekeepers, _ = h.db.LeaderboardGatekeepers(5)
	data.MergeMasters, _ = h.db.LeaderboardAuthors(5)
	data.OneShot, _ = h.db.LeaderboardCleanApprovals(5)

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
