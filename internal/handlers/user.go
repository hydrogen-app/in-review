package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"
	"inreview/internal/db"
)

type userActivityPayload struct {
	Labels      []string  `json:"labels"`
	PRCounts    []int     `json:"prCounts"`
	ReviewCounts []int    `json:"reviewCounts"`
	CRRate      []float64 `json:"crRate"`
}

type userSizePayload struct {
	Labels   []string `json:"labels"`
	PRCounts []int    `json:"prCounts"`
}

type UserData struct {
	BaseData
	User             *db.User
	ReviewerStats    *db.ReviewerStats
	AuthorStats      *db.AuthorStats
	ReviewerRank     int
	GatekeeperRank   int
	AuthorRank       int
	ContributedRepos []db.Repo
	FastestPR        *db.UserRecordPR
	SlowestPR        *db.UserRecordPR
	ActivityJSON     template.JS
	SizeBucketJSON   template.JS
	ReviewedRepos    []db.UserRepoReview
	ReviewersOfMe    []db.CollabEntry
	AuthorsIReview   []db.CollabEntry
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
		// If we have the user cached in the DB, render from that instead of erroring.
		if cached, dbErr := h.db.GetUser(username); dbErr == nil && cached != nil {
			// Serve cached page — fall through with ghUser = nil
		} else {
			h.renderGHError(w, r, err, "User Not Found",
				"Could not find @"+username+" on GitHub. Check the spelling and try again.")
			return
		}
	}

	// Determine if org and redirect early (only when we have fresh GitHub data).
	if ghUser != nil && ghUser.Type == "Organization" {
		http.Redirect(w, r, "/org/"+username, http.StatusFound)
		return
	}

	// Cache user if we got fresh data.
	if ghUser != nil {
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
	}

	user, _ := h.db.GetUser(username)
	if user == nil && ghUser != nil {
		user = &db.User{
			Login:     ghUser.Login,
			Name:      ghUser.Name,
			AvatarURL: ghUser.AvatarURL,
		}
	}
	if user == nil {
		h.renderGHError(w, r, err, "User Not Found",
			"Could not find @"+username+" on GitHub. Check the spelling and try again.")
		return
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

	type rrRes struct {
		v   *db.ReviewerStats
		err error
	}
	type arRes struct {
		v   *db.AuthorStats
		err error
	}
	type recRes struct {
		fastest, slowest *db.UserRecordPR
	}
	type actRes struct {
		v   []db.UserActivityPoint
		err error
	}
	type sizeRes struct {
		v   []db.PRSizeBucket
		err error
	}
	type collabRes struct {
		reviewersOfMe  []db.CollabEntry
		authorsIReview []db.CollabEntry
	}
	type reviewedReposRes struct {
		v   []db.UserRepoReview
		err error
	}

	rrCh   := make(chan rrRes, 1)
	arCh   := make(chan arRes, 1)
	rrankCh := make(chan int, 1)
	gkCh   := make(chan int, 1)
	auCh   := make(chan int, 1)
	contCh := make(chan []db.Repo, 1)
	recCh  := make(chan recRes, 1)
	actCh  := make(chan actRes, 1)
	sizeCh := make(chan sizeRes, 1)
	colCh  := make(chan collabRes, 1)
	rvRepCh := make(chan reviewedReposRes, 1)

	go func() { v, err := h.db.UserReviewerStats(username); rrCh <- rrRes{v, err} }()
	go func() { v, err := h.db.UserAuthorStats(username); arCh <- arRes{v, err} }()
	go func() { v, _ := h.db.UserReviewerRank(username); rrankCh <- v }()
	go func() { v, _ := h.db.UserGatekeeperRank(username); gkCh <- v }()
	go func() { v, _ := h.db.UserAuthorRank(username); auCh <- v }()
	go func() { v, _ := h.db.UserContributedRepos(username, 10); contCh <- v }()
	go func() {
		f, s, _ := h.db.UserRecordPRs(username)
		recCh <- recRes{f, s}
	}()
	go func() { v, err := h.db.UserActivitySeries(username); actCh <- actRes{v, err} }()
	go func() { v, err := h.db.UserPRSizeDist(username); sizeCh <- sizeRes{v, err} }()
	go func() {
		rm, ai, _ := h.db.UserTopCollaborators(username, 5)
		colCh <- collabRes{rm, ai}
	}()
	go func() { v, err := h.db.UserTopReviewedRepos(username, 8); rvRepCh <- reviewedReposRes{v, err} }()

	rrResult   := <-rrCh
	arResult   := <-arCh
	rec        := <-recCh
	act        := <-actCh
	sizeResult := <-sizeCh
	col        := <-colCh
	rvRep      := <-rvRepCh

	data := UserData{
		User:           user,
		IsOrg:          false,
		ReviewerStats:  rrResult.v,
		AuthorStats:    arResult.v,
		ReviewerRank:   <-rrankCh,
		GatekeeperRank: <-gkCh,
		AuthorRank:     <-auCh,
		ContributedRepos: <-contCh,
		FastestPR:      rec.fastest,
		SlowestPR:      rec.slowest,
		ReviewersOfMe:  col.reviewersOfMe,
		AuthorsIReview: col.authorsIReview,
		ReviewedRepos:  rvRep.v,
	}
	data.IsNGMI = data.ReviewerStats == nil || data.ReviewerStats.TotalReviews < 10

	if len(act.v) > 0 {
		ap := userActivityPayload{}
		for _, p := range act.v {
			ap.Labels = append(ap.Labels, p.Label)
			ap.PRCounts = append(ap.PRCounts, p.PRCount)
			ap.ReviewCounts = append(ap.ReviewCounts, p.ReviewCount)
			ap.CRRate = append(ap.CRRate, roundTo1(p.ChangesRequestedRate))
		}
		if raw, err := json.Marshal(ap); err == nil {
			data.ActivityJSON = template.JS(raw)
		}
	}

	if len(sizeResult.v) > 0 {
		sp := userSizePayload{}
		for _, b := range sizeResult.v {
			sp.Labels = append(sp.Labels, b.Label)
			sp.PRCounts = append(sp.PRCounts, b.PRCount)
		}
		if raw, err := json.Marshal(sp); err == nil {
			data.SizeBucketJSON = template.JS(raw)
		}
	}

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
