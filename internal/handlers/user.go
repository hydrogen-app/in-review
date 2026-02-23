package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"inreview/internal/db"
)

type UserData struct {
	User           *db.User
	ReviewerStats  *db.ReviewerStats
	AuthorStats    *db.AuthorStats
	ReviewerRank   int
	GatekeeperRank int
	PopularRepos   []db.Repo
	IsOrg          bool
}

func (h *Handler) User(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")

	// Fetch from GitHub to get latest info
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	ghUser, err := h.gh.GetUser(ctx, username)
	if err != nil {
		h.renderError(w, http.StatusNotFound, "User Not Found",
			"Could not find @"+username+" on GitHub. Check the spelling and try again.")
		return
	}

	isOrg := ghUser.Type == "Organization"

	// If it's an org, redirect to the org page
	if isOrg {
		http.Redirect(w, r, "/org/"+username, http.StatusFound)
		return
	}

	// Cache user
	h.db.UpsertUser(db.User{
		Login:       ghUser.Login,
		Name:        ghUser.Name,
		AvatarURL:   ghUser.AvatarURL,
		Bio:         ghUser.Bio,
		PublicRepos: ghUser.PublicRepos,
		Followers:   ghUser.Followers,
		Company:     ghUser.Company,
		Location:    ghUser.Location,
		IsOrg:       false,
	})

	user, _ := h.db.GetUser(username)
	if user == nil {
		user = &db.User{
			Login:     ghUser.Login,
			Name:      ghUser.Name,
			AvatarURL: ghUser.AvatarURL,
		}
	}

	// Queue top repos for sync
	go func() {
		repos, err := h.gh.GetUserRepos(context.Background(), username, 10)
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
				SyncStatus:  "pending",
			})
			h.worker.Queue(repo.FullName, false)
		}
	}()

	data := UserData{
		User:  user,
		IsOrg: false,
	}
	data.ReviewerStats, _ = h.db.UserReviewerStats(username)
	data.AuthorStats, _ = h.db.UserAuthorStats(username)
	data.ReviewerRank, _ = h.db.UserReviewerRank(username)
	data.GatekeeperRank, _ = h.db.UserGatekeeperRank(username)
	data.PopularRepos, _ = h.db.SearchRepos(username, 5)

	h.render(w, "user", data)
}
