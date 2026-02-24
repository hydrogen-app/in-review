package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"
	"inreview/internal/db"
)

type UserData struct {
	BaseData
	User             *db.User
	ReviewerStats    *db.ReviewerStats
	AuthorStats      *db.AuthorStats
	ReviewerRank     int
	GatekeeperRank   int
	AuthorRank       int
	ContributedRepos []db.Repo
	IsOrg            bool
	IsNGMI           bool
	OGTitle          string
	OGDesc           string
	OGUrl            string
	ShareURL         string
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

	// Queue top owned repos + repos where they've reviewed PRs for sync.
	go func() {
		bg := context.Background()
		if repos, err := h.gh.GetUserRepos(bg, username, 10); err == nil {
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
		}
		if reviewedRepos, err := h.gh.GetReviewedRepos(bg, username, 100); err == nil {
			for _, fullName := range reviewedRepos {
				h.worker.Queue(fullName, false)
			}
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
	data.AuthorRank, _ = h.db.UserAuthorRank(username)
	data.ContributedRepos, _ = h.db.UserContributedRepos(username, 10)
	data.IsNGMI = data.ReviewerStats == nil || data.ReviewerStats.TotalReviews < 10

	// ── OG / share metadata ───────────────────────────────────────────────────
	data.OGUrl = "https://ngmi.review/user/" + username
	displayName := username
	if user.Name != "" {
		displayName = user.Name
	}
	data.OGTitle = "@" + username + " — ngmi"
	if data.ReviewerStats != nil && data.ReviewerStats.TotalReviews > 0 {
		approvalPct := (data.ReviewerStats.Approvals * 100) / data.ReviewerStats.TotalReviews
		ogDesc := fmt.Sprintf("%s: %d reviews, %d%% approval rate", displayName, data.ReviewerStats.TotalReviews, approvalPct)
		if data.ReviewerRank > 0 {
			ogDesc += fmt.Sprintf(" (#%d globally)", data.ReviewerRank)
		}
		ogDesc += ". If you aren't reviewing, you're ngmi."
		data.OGDesc = ogDesc

		var shareText string
		if data.ReviewerRank > 0 {
			shareText = fmt.Sprintf("#%d code reviewer globally — %d reviews, %d%% clean approvals. If you aren't reviewing, you're ngmi.", data.ReviewerRank, data.ReviewerStats.TotalReviews, approvalPct)
		} else {
			shareText = fmt.Sprintf("%d code reviews, %d%% approval rate. If you aren't reviewing, you're ngmi.", data.ReviewerStats.TotalReviews, approvalPct)
		}
		data.ShareURL = "https://twitter.com/intent/tweet?text=" + url.QueryEscape(shareText) +
			"&url=" + url.QueryEscape(data.OGUrl)
	} else {
		data.OGDesc = "@" + username + " has no reviews on record. ngmi."
	}

	data.BaseData = h.baseData(r)
	h.db.RecordVisit("/user/"+username, "user", username)
	h.render(w, "user", data)
}
