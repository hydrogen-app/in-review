package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"inreview/internal/db"
)

type OrgData struct {
	Org              *db.User
	Repos            []db.Repo
	ReviewerBoard    []db.LeaderboardEntry
	GatekeeperBoard  []db.LeaderboardEntry
	TotalMergedPRs   int
	TotalReviews     int
	IsSyncing        bool
}

func (h *Handler) Org(w http.ResponseWriter, r *http.Request) {
	orgName := chi.URLParam(r, "org")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	ghUser, err := h.gh.GetUser(ctx, orgName)
	if err != nil {
		h.renderError(w, http.StatusNotFound, "Org Not Found",
			"Could not find the organization "+orgName+" on GitHub. Check the spelling and try again.")
		return
	}

	// If it's actually a user, redirect
	if ghUser.Type != "Organization" {
		http.Redirect(w, r, "/user/"+orgName, http.StatusFound)
		return
	}

	h.db.UpsertUser(db.User{
		Login:       ghUser.Login,
		Name:        ghUser.Name,
		AvatarURL:   ghUser.AvatarURL,
		Bio:         ghUser.Bio,
		PublicRepos: ghUser.PublicRepos,
		Followers:   ghUser.Followers,
		IsOrg:       true,
	})

	// Queue top org repos for sync
	go func() {
		repos, err := h.gh.GetOrgRepos(context.Background(), orgName, 20)
		if err != nil {
			return
		}
		for _, repo := range repos {
			h.db.UpsertRepo(db.Repo{
				FullName:    repo.FullName,
				Owner:       repo.Owner.Login,
				Name:        repo.Name,
				Description: repo.Description,
				Stars:       repo.Stars,
				Language:    repo.Language,
				OrgName:     orgName,
				SyncStatus:  "pending",
			})
			h.worker.Queue(repo.FullName, false)
		}
	}()

	org, _ := h.db.GetUser(orgName)
	if org == nil {
		org = &db.User{
			Login:     ghUser.Login,
			Name:      ghUser.Name,
			AvatarURL: ghUser.AvatarURL,
			IsOrg:     true,
		}
	}

	repos, _ := h.db.OrgRepos(orgName)

	// Compute aggregate stats and check if any repo is still syncing.
	var totalPRs int
	isSyncing := false
	for _, rp := range repos {
		totalPRs += rp.MergedPRCount
		if rp.SyncStatus == "syncing" || h.worker.IsSyncing(rp.FullName) {
			isSyncing = true
		}
	}

	data := OrgData{
		Org:            org,
		Repos:          repos,
		TotalMergedPRs: totalPRs,
		IsSyncing:      isSyncing,
	}
	data.ReviewerBoard, _ = h.db.OrgReviewerLeaderboard(orgName, 10)
	data.GatekeeperBoard, _ = h.db.OrgGatekeeperLeaderboard(orgName, 10)

	h.render(w, "org", data)
}
