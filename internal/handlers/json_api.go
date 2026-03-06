package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/rdb"
)

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// setCachePublic sets Cache-Control headers for public, non-personalised responses.
func setCachePublic(w http.ResponseWriter, maxAge, swrAge time.Duration) {
	w.Header().Set("Cache-Control", fmt.Sprintf("public, s-maxage=%d, stale-while-revalidate=%d",
		int(maxAge.Seconds()), int(swrAge.Seconds())))
}

// ── /api/v1/me ─────────────────────────────────────────────────────────────────

func (h *Handler) MeJSON(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"login": currentUser(r)})
}

// ── /api/v1/home ───────────────────────────────────────────────────────────────

func (h *Handler) HomeJSON(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var hc homeCache
	if raw, ok := h.cache.Get(ctx, homeCacheKey); ok {
		_ = json.Unmarshal(raw, &hc)
	}
	// Nil slices become empty arrays in JSON.
	if hc.SpeedDemons == nil { hc.SpeedDemons = []db.LeaderboardEntry{} }
	if hc.PRGraveyard == nil { hc.PRGraveyard = []db.LeaderboardEntry{} }
	if hc.ReviewChamps == nil { hc.ReviewChamps = []db.LeaderboardEntry{} }
	if hc.Gatekeepers == nil { hc.Gatekeepers = []db.LeaderboardEntry{} }
	if hc.MergeMasters == nil { hc.MergeMasters = []db.LeaderboardEntry{} }
	if hc.OneShot == nil { hc.OneShot = []db.LeaderboardEntry{} }
	if hc.PopularVisits == nil { hc.PopularVisits = []db.PageVisit{} }
	if hc.RecentVisits == nil { hc.RecentVisits = []db.PageVisit{} }

	setCachePublic(w, 30*time.Second, 120*time.Second)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"TotalRepos":    hc.TotalRepos,
		"TotalPRs":      hc.TotalPRs,
		"TotalReviews":  hc.TotalReviews,
		"SpeedDemons":   hc.SpeedDemons,
		"PRGraveyard":   hc.PRGraveyard,
		"ReviewChamps":  hc.ReviewChamps,
		"Gatekeepers":   hc.Gatekeepers,
		"MergeMasters":  hc.MergeMasters,
		"OneShot":       hc.OneShot,
		"PopularVisits": hc.PopularVisits,
		"RecentVisits":  hc.RecentVisits,
	})
}

// ── /api/v1/search ─────────────────────────────────────────────────────────────

func (h *Handler) SearchJSON(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{"Query": "", "Results": []SearchResult{}})
		return
	}

	data := SearchData{Query: query}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	if strings.Contains(query, "/") {
		parts := strings.SplitN(query, "/", 2)
		owner, name := parts[0], parts[1]
		fullName := owner + "/" + name
		existing, _ := h.db.GetRepo(fullName)
		if existing == nil {
			if ghRepo, err := h.gh.GetRepo(ctx, owner, name); err == nil {
				h.db.UpsertRepo(db.Repo{
					FullName: fullName, Owner: owner, Name: ghRepo.Name,
					Description: ghRepo.Description, Stars: ghRepo.Stars,
					Language: ghRepo.Language, SyncStatus: "pending",
				})
				existing, _ = h.db.GetRepo(fullName)
			}
		}
		if existing != nil {
			h.worker.Queue(fullName, false)
			rank, _ := h.db.RepoSpeedRank(fullName)
			data.Results = append(data.Results, SearchResult{
				Type: "repo", Name: existing.Name, FullName: existing.FullName,
				Description: existing.Description, Stars: existing.Stars,
				Language: existing.Language, MergedPRs: existing.MergedPRCount,
				AvgMergeTime: existing.AvgMergeTimeSecs, SpeedRank: rank,
				IsCached: existing.SyncStatus == "done",
			})
		}
		if data.Results == nil {
			data.Results = []SearchResult{}
		}
		writeJSON(w, http.StatusOK, data)
		return
	}

	repoCh := make(chan []SearchResult, 1)
	userCh := make(chan []SearchResult, 1)

	go func() {
		ghRepos, err := h.gh.SearchRepos(ctx, query, 5)
		if err != nil {
			repoCh <- nil
			return
		}
		var results []SearchResult
		for _, r := range ghRepos {
			cached, _ := h.db.GetRepo(r.FullName)
			res := SearchResult{
				Type: "repo", Name: r.Name, FullName: r.FullName,
				Description: r.Description, Stars: r.Stars, Language: r.Language,
			}
			if cached != nil {
				res.MergedPRs = cached.MergedPRCount
				res.AvgMergeTime = cached.AvgMergeTimeSecs
				res.IsCached = cached.SyncStatus == "done"
				res.SpeedRank, _ = h.db.RepoSpeedRank(r.FullName)
			}
			results = append(results, res)
		}
		repoCh <- results
	}()

	go func() {
		ghUsers, err := h.gh.SearchUsers(ctx, query, 5)
		if err != nil {
			userCh <- nil
			return
		}
		var results []SearchResult
		for _, u := range ghUsers {
			kind := "user"
			if u.Type == "Organization" {
				kind = "org"
			}
			results = append(results, SearchResult{
				Type: kind, Name: u.Name, FullName: u.Login,
				Description: u.Bio, AvatarURL: u.AvatarURL,
			})
		}
		userCh <- results
	}()

	data.Results = append(data.Results, (<-userCh)...)
	data.Results = append(data.Results, (<-repoCh)...)
	if data.Results == nil {
		data.Results = []SearchResult{}
	}
	writeJSON(w, http.StatusOK, data)
}

// ── /api/v1/repo/{owner}/{name} ────────────────────────────────────────────────

func (h *Handler) RepoJSON(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name
	trim, cutoffPct := parseTrim(r)

	repo, _ := h.db.GetRepo(fullName)
	if repo == nil {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		ghRepo, err := h.gh.GetRepo(ctx, owner, name)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "repo not found"})
			return
		}
		_ = h.db.UpsertRepo(db.Repo{
			FullName: fullName, Owner: owner, Name: ghRepo.Name,
			Description: ghRepo.Description, Stars: ghRepo.Stars,
			Language: ghRepo.Language, SyncStatus: "pending",
		})
		repo, _ = h.db.GetRepo(fullName)
		if repo == nil {
			repo = &db.Repo{
				FullName: fullName, Owner: owner, Name: ghRepo.Name,
				Description: ghRepo.Description, Stars: ghRepo.Stars,
				Language: ghRepo.Language, SyncStatus: "pending",
			}
		}
	}

	h.worker.Queue(fullName, false)
	h.db.RecordVisit("/repo/"+fullName, "repo", fullName)

	// Run all remaining queries concurrently
	type reviewersResult struct{ v []db.ReviewerStats }
	type prsResult struct{ v []db.PullRequest }
	type rankResult struct{ v int }
	type sizeResult struct{ v []db.PRSizeBucket }
	type timeResult struct{ v []db.TimeSeriesPoint }

	reviewersCh := make(chan reviewersResult, 1)
	prsCh := make(chan prsResult, 1)
	rankCh := make(chan rankResult, 1)
	sizeCh := make(chan sizeResult, 1)
	timeCh := make(chan timeResult, 1)

	go func() { v, _ := h.db.RepoTopReviewers(fullName, 10); reviewersCh <- reviewersResult{v} }()
	go func() { v, _ := h.db.RecentMergedPRs(fullName, 20); prsCh <- prsResult{v} }()
	go func() { v, _ := h.db.RepoSpeedRank(fullName); rankCh <- rankResult{v} }()
	go func() { v, _ := h.db.RepoSizeChartData(fullName, cutoffPct); sizeCh <- sizeResult{v} }()
	go func() { v, _ := h.db.RepoTimeSeriesData(fullName, cutoffPct); timeCh <- timeResult{v} }()

	topReviewers := (<-reviewersCh).v
	recentPRs := (<-prsCh).v
	speedRank := (<-rankCh).v
	sizeBuckets := (<-sizeCh).v
	timePoints := (<-timeCh).v

	var sizeChart *sizeChartPayload
	if len(sizeBuckets) > 0 {
		p := &sizeChartPayload{}
		for _, b := range sizeBuckets {
			p.Labels = append(p.Labels, b.Label)
			p.PRCounts = append(p.PRCounts, b.PRCount)
			p.AvgHours = append(p.AvgHours, roundTo1(b.AvgSecs/3600))
			p.ApprovalRate = append(p.ApprovalRate, roundTo1(b.ApprovalRate))
		}
		sizeChart = p
	}

	var timeChart *timeChartPayload
	if len(timePoints) > 0 {
		tp := &timeChartPayload{}
		for _, p := range timePoints {
			tp.Labels = append(tp.Labels, p.Label)
			tp.PRCounts = append(tp.PRCounts, p.PRCount)
			tp.AvgSize = append(tp.AvgSize, roundTo1(p.AvgSize))
			tp.MedianSize = append(tp.MedianSize, roundTo1(p.MedianSize))
			tp.AvgHours = append(tp.AvgHours, roundTo1(p.AvgSecs/3600))
			tp.MedianHours = append(tp.MedianHours, roundTo1(p.MedianSecs/3600))
			tp.ChangesRequestedRate = append(tp.ChangesRequestedRate, roundTo1(p.ChangesRequestedRate))
			tp.AvgFirstReviewHours = append(tp.AvgFirstReviewHours, roundTo1(p.AvgFirstReviewSecs/3600))
			tp.MedFirstReviewHours = append(tp.MedFirstReviewHours, roundTo1(p.MedFirstReviewSecs/3600))
			tp.UnreviewedMergeRate = append(tp.UnreviewedMergeRate, roundTo1(p.UnreviewedRate))
			tp.LinesPerContrib = append(tp.LinesPerContrib, roundTo1(p.LinesPerContrib))
		}
		timeChart = tp
	}

	shareText := fullName
	if repo.AvgMergeTimeSecs > 0 {
		shareText += " merges PRs in " + formatDuration(repo.AvgMergeTimeSecs)
	}
	if speedRank > 0 {
		shareText += fmt.Sprintf(", #%d globally", speedRank)
	}
	shareText += ". If you aren't reviewing, you're ngmi."
	shareURL := "https://twitter.com/intent/tweet?text=" + url.QueryEscape(shareText) +
		"&url=" + url.QueryEscape("https://ngmi.review/repo/"+fullName)

	if topReviewers == nil {
		topReviewers = []db.ReviewerStats{}
	}
	if recentPRs == nil {
		recentPRs = []db.PullRequest{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"Repo":         repo,
		"TopReviewers": topReviewers,
		"RecentPRs":    recentPRs,
		"SpeedRank":    speedRank,
		"IsSyncing":    h.worker.IsSyncing(fullName),
		"Trim":         trim,
		"ShareURL":     shareURL,
		"SizeChart":    sizeChart,
		"TimeChart":    timeChart,
	})
}

// ── /api/v1/sync-status/{owner}/{name} ────────────────────────────────────────

func (h *Handler) SyncStatusJSON(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name

	qpos := h.worker.QueuePosition(fullName)
	repo, _ := h.db.GetRepo(fullName)

	status := "pending"
	queuePos := 0
	timeAgoStr := ""

	switch {
	case qpos > 0:
		status = "queued"
		queuePos = qpos
	case h.worker.IsSyncing(fullName):
		status = "syncing"
	case repo != nil && repo.LastSynced != nil:
		status = "done"
		timeAgoStr = timeAgo(repo.LastSynced)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":   status,
		"queuePos": queuePos,
		"timeAgo":  timeAgoStr,
	})
}

// ── /api/v1/user/{username} ────────────────────────────────────────────────────

func (h *Handler) UserJSON(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")

	// Use cached DB record if it's fresh (< 1h), avoiding a GitHub API call on every page view
	const userCacheTTL = time.Hour
	cached, _ := h.db.GetUser(username)
	fresh := cached != nil && cached.LastFetched != nil && time.Since(*cached.LastFetched) < userCacheTTL

	var ghUser *github.GHUser
	if !fresh {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		var err error
		ghUser, err = h.gh.GetUser(ctx, username)
		if err != nil && cached == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "user not found"})
			return
		}
	}

	if ghUser != nil && ghUser.Type == "Organization" {
		writeJSON(w, http.StatusOK, map[string]string{"redirect": "/org/" + username})
		return
	}

	if ghUser != nil {
		h.db.UpsertUser(db.User{
			Login: ghUser.Login, Name: ghUser.Name, AvatarURL: ghUser.AvatarURL,
			Bio: ghUser.Bio, PublicRepos: ghUser.PublicRepos, Followers: ghUser.Followers,
			Company: ghUser.Company, Location: ghUser.Location, IsOrg: false,
		})
	}

	user, _ := h.db.GetUser(username)
	if user == nil && ghUser != nil {
		user = &db.User{Login: ghUser.Login, Name: ghUser.Name, AvatarURL: ghUser.AvatarURL}
	}
	if user == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "user not found"})
		return
	}

	go func() {
		bg := context.Background()
		if repos, err := h.gh.GetUserRepos(bg, username, 10); err == nil {
			for _, repo := range repos {
				h.db.UpsertRepo(db.Repo{
					FullName: repo.FullName, Owner: repo.Owner.Login, Name: repo.Name,
					Description: repo.Description, Stars: repo.Stars,
					Language: repo.Language, SyncStatus: "pending",
				})
				h.worker.Queue(repo.FullName, false)
			}
		}
		if reviewedRepos, err := h.gh.GetReviewedRepos(bg, username, 100); err == nil {
			for _, fn := range reviewedRepos {
				h.worker.Queue(fn, false)
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
	type recRes struct{ fastest, slowest *db.UserRecordPR }
	type actRes struct{ v []db.UserActivityPoint }
	type sizeRes struct{ v []db.PRSizeBucket }
	type collabRes struct{ reviewersOfMe, authorsIReview []db.CollabEntry }
	type rvRepRes struct{ v []db.UserRepoReview }

	rrCh := make(chan rrRes, 1)
	arCh := make(chan arRes, 1)
	rrankCh := make(chan int, 1)
	gkCh := make(chan int, 1)
	auCh := make(chan int, 1)
	contCh := make(chan []db.Repo, 1)
	recCh := make(chan recRes, 1)
	actCh := make(chan actRes, 1)
	sizeCh := make(chan sizeRes, 1)
	colCh := make(chan collabRes, 1)
	rvRepCh := make(chan rvRepRes, 1)

	go func() { v, err := h.db.UserReviewerStats(username); rrCh <- rrRes{v, err} }()
	go func() { v, err := h.db.UserAuthorStats(username); arCh <- arRes{v, err} }()
	go func() { v, _ := h.db.UserReviewerRank(username); rrankCh <- v }()
	go func() { v, _ := h.db.UserGatekeeperRank(username); gkCh <- v }()
	go func() { v, _ := h.db.UserAuthorRank(username); auCh <- v }()
	go func() { v, _ := h.db.UserContributedRepos(username, 10); contCh <- v }()
	go func() { f, s, _ := h.db.UserRecordPRs(username); recCh <- recRes{f, s} }()
	go func() { v, _ := h.db.UserActivitySeries(username); actCh <- actRes{v} }()
	go func() { v, _ := h.db.UserPRSizeDist(username); sizeCh <- sizeRes{v} }()
	go func() {
		rm, ai, _ := h.db.UserTopCollaborators(username, 5)
		colCh <- collabRes{rm, ai}
	}()
	go func() { v, _ := h.db.UserTopReviewedRepos(username, 8); rvRepCh <- rvRepRes{v} }()

	rrResult := <-rrCh
	arResult := <-arCh
	rec := <-recCh
	act := <-actCh
	sizeResult := <-sizeCh
	col := <-colCh
	rvRep := <-rvRepCh
	reviewerRank := <-rrankCh
	gatekeeperRank := <-gkCh
	authorRank := <-auCh
	contributedRepos := <-contCh

	isNGMI := rrResult.v == nil || rrResult.v.TotalReviews < 10

	var activityChart *userActivityPayload
	if len(act.v) > 0 {
		ap := &userActivityPayload{}
		for _, p := range act.v {
			ap.Labels = append(ap.Labels, p.Label)
			ap.PRCounts = append(ap.PRCounts, p.PRCount)
			ap.ReviewCounts = append(ap.ReviewCounts, p.ReviewCount)
			ap.CRRate = append(ap.CRRate, roundTo1(p.ChangesRequestedRate))
		}
		activityChart = ap
	}

	var sizeBucketChart *userSizePayload
	if len(sizeResult.v) > 0 {
		sp := &userSizePayload{}
		for _, b := range sizeResult.v {
			sp.Labels = append(sp.Labels, b.Label)
			sp.PRCounts = append(sp.PRCounts, b.PRCount)
		}
		sizeBucketChart = sp
	}

	var shareURL string
	if rrResult.v != nil && rrResult.v.TotalReviews > 0 {
		approvalPct := (rrResult.v.Approvals * 100) / rrResult.v.TotalReviews
		var shareText string
		if reviewerRank > 0 {
			shareText = fmt.Sprintf("#%d code reviewer globally — %d reviews, %d%% clean approvals. If you aren't reviewing, you're ngmi.", reviewerRank, rrResult.v.TotalReviews, approvalPct)
		} else {
			shareText = fmt.Sprintf("%d code reviews, %d%% approval rate. If you aren't reviewing, you're ngmi.", rrResult.v.TotalReviews, approvalPct)
		}
		ogURL := "https://ngmi.review/user/" + username
		shareURL = "https://twitter.com/intent/tweet?text=" + url.QueryEscape(shareText) +
			"&url=" + url.QueryEscape(ogURL)
	}

	if contributedRepos == nil {
		contributedRepos = []db.Repo{}
	}
	if col.reviewersOfMe == nil {
		col.reviewersOfMe = []db.CollabEntry{}
	}
	if col.authorsIReview == nil {
		col.authorsIReview = []db.CollabEntry{}
	}
	if rvRep.v == nil {
		rvRep.v = []db.UserRepoReview{}
	}

	h.db.RecordVisit("/user/"+username, "user", username)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"User":             user,
		"ReviewerStats":    rrResult.v,
		"AuthorStats":      arResult.v,
		"ReviewerRank":     reviewerRank,
		"GatekeeperRank":   gatekeeperRank,
		"AuthorRank":       authorRank,
		"ContributedRepos": contributedRepos,
		"FastestPR":        rec.fastest,
		"SlowestPR":        rec.slowest,
		"ReviewedRepos":    rvRep.v,
		"ReviewersOfMe":    col.reviewersOfMe,
		"AuthorsIReview":   col.authorsIReview,
		"IsOrg":            false,
		"IsNGMI":           isNGMI,
		"ShareURL":         shareURL,
		"ActivityChart":    activityChart,
		"SizeBucketChart":  sizeBucketChart,
	})
}

// ── /api/v1/org/{org} ──────────────────────────────────────────────────────────

func (h *Handler) OrgJSON(w http.ResponseWriter, r *http.Request) {
	orgName := chi.URLParam(r, "org")
	trim, cutoffPct := parseTrim(r)

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	ghUser, err := h.gh.GetUser(ctx, orgName)
	if err != nil {
		if cached, dbErr := h.db.GetUser(orgName); dbErr == nil && cached != nil && cached.IsOrg {
			// fall through
		} else {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "org not found"})
			return
		}
	}

	if ghUser != nil && ghUser.Type != "Organization" {
		writeJSON(w, http.StatusOK, map[string]string{"redirect": "/user/" + orgName})
		return
	}

	if ghUser != nil {
		h.db.UpsertUser(db.User{
			Login: ghUser.Login, Name: ghUser.Name, AvatarURL: ghUser.AvatarURL,
			Bio: ghUser.Bio, PublicRepos: ghUser.PublicRepos, Followers: ghUser.Followers,
			IsOrg: true,
		})
	}

	go func() {
		repos, err := h.gh.GetOrgRepos(context.Background(), orgName, 20)
		if err != nil {
			return
		}
		for _, repo := range repos {
			h.db.UpsertRepo(db.Repo{
				FullName: repo.FullName, Owner: repo.Owner.Login, Name: repo.Name,
				Description: repo.Description, Stars: repo.Stars,
				Language: repo.Language, OrgName: orgName, SyncStatus: "pending",
			})
			h.worker.Queue(repo.FullName, false)
		}
	}()

	org, _ := h.db.GetUser(orgName)
	if org == nil && ghUser != nil {
		org = &db.User{Login: ghUser.Login, Name: ghUser.Name, AvatarURL: ghUser.AvatarURL, IsOrg: true}
	}
	if org == nil {
		org = &db.User{Login: orgName, IsOrg: true}
	}

	repos, _ := h.db.OrgRepos(orgName)
	var totalPRs int
	isSyncing := false
	for _, rp := range repos {
		totalPRs += rp.MergedPRCount
		if rp.SyncStatus == "syncing" || h.worker.IsSyncing(rp.FullName) {
			isSyncing = true
		}
	}

	reviewerBoard, _ := h.db.OrgReviewerLeaderboard(orgName, 10)
	gatekeeperBoard, _ := h.db.OrgGatekeeperLeaderboard(orgName, 10)

	var timeChart *timeChartPayload
	if points, err := h.db.OrgTimeSeriesData(orgName, cutoffPct); err == nil && len(points) > 0 {
		tp := &timeChartPayload{}
		for _, p := range points {
			tp.Labels = append(tp.Labels, p.Label)
			tp.PRCounts = append(tp.PRCounts, p.PRCount)
			tp.AvgSize = append(tp.AvgSize, roundTo1(p.AvgSize))
			tp.MedianSize = append(tp.MedianSize, roundTo1(p.MedianSize))
			tp.AvgHours = append(tp.AvgHours, roundTo1(p.AvgSecs/3600))
			tp.MedianHours = append(tp.MedianHours, roundTo1(p.MedianSecs/3600))
			tp.ChangesRequestedRate = append(tp.ChangesRequestedRate, roundTo1(p.ChangesRequestedRate))
			tp.AvgFirstReviewHours = append(tp.AvgFirstReviewHours, roundTo1(p.AvgFirstReviewSecs/3600))
			tp.MedFirstReviewHours = append(tp.MedFirstReviewHours, roundTo1(p.MedFirstReviewSecs/3600))
			tp.UnreviewedMergeRate = append(tp.UnreviewedMergeRate, roundTo1(p.UnreviewedRate))
			tp.LinesPerContrib = append(tp.LinesPerContrib, roundTo1(p.LinesPerContrib))
		}
		timeChart = tp
	}

	if repos == nil {
		repos = []db.Repo{}
	}
	if reviewerBoard == nil {
		reviewerBoard = []db.LeaderboardEntry{}
	}
	if gatekeeperBoard == nil {
		gatekeeperBoard = []db.LeaderboardEntry{}
	}

	h.db.RecordVisit("/org/"+orgName, "org", orgName)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"Org":             org,
		"Repos":           repos,
		"ReviewerBoard":   reviewerBoard,
		"GatekeeperBoard": gatekeeperBoard,
		"TotalMergedPRs":  totalPRs,
		"IsSyncing":       isSyncing,
		"Trim":            trim,
		"TimeChart":       timeChart,
	})
}

// ── /api/v1/leaderboard/{category} ────────────────────────────────────────────

func (h *Handler) LeaderboardPageJSON(w http.ResponseWriter, r *http.Request) {
	category := chi.URLParam(r, "category")
	meta, ok := leaderboardMeta[category]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "invalid category"})
		return
	}

	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}

	data := LeaderboardPageData{Category: category, Title: meta[0], Description: meta[1]}
	h.populateLeaderboardData(&data, category, offset)

	if data.RepoRows == nil {
		data.RepoRows = []db.RepoLeaderboardRow{}
	}
	if data.UserRows == nil {
		data.UserRows = []db.UserLeaderboardRow{}
	}
	if data.CleanRows == nil {
		data.CleanRows = []db.CleanLeaderboardRow{}
	}

	setCachePublic(w, 60*time.Second, 300*time.Second)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"Category":    data.Category,
		"Title":       data.Title,
		"Description": data.Description,
		"RepoRows":    data.RepoRows,
		"UserRows":    data.UserRows,
		"CleanRows":   data.CleanRows,
		"HasMore":     data.HasMore,
		"NextOffset":  data.NextOffset,
	})
}

func (h *Handler) LeaderboardSearchJSON(w http.ResponseWriter, r *http.Request) {
	category := chi.URLParam(r, "category")
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	result := LeaderboardSearchData{Category: category, Query: q}
	if q == "" {
		result.Empty = true
		writeJSON(w, http.StatusOK, result)
		return
	}
	isUserCategory := category == "reviewers" || category == "gatekeepers" || category == "authors"
	if isUserCategory {
		login := strings.TrimPrefix(q, "@")
		h.leaderboardUserSearch(&result, category, login)
	} else {
		h.leaderboardRepoSearch(&result, category, q)
	}
	writeJSON(w, http.StatusOK, result)
}

// ── /api/v1/stats ──────────────────────────────────────────────────────────────

func (h *Handler) StatsJSON(w http.ResponseWriter, r *http.Request) {
	trim, cutoffPct := parseTrim(r)
	minStars, _ := strconv.Atoi(r.URL.Query().Get("min_stars"))
	if minStars < 0 {
		minStars = 0
	}
	minContribs, _ := strconv.Atoi(r.URL.Query().Get("min_contribs"))
	if minContribs < 0 {
		minContribs = 0
	}

	cacheKey := fmt.Sprintf("stats:api:v1:%d:%d:%d", trim, minStars, minContribs)
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), cacheKey); ok {
			setCachePublic(w, 120*time.Second, 600*time.Second)
			w.Header().Set("Content-Type", "application/json")
			w.Write(raw)
			return
		}
	}

	type overallRes struct {
		v   db.GlobalOverallStats
		err error
	}
	type bucketsRes struct{ v []db.GlobalSizeBucket }
	type pointsRes struct{ v []db.TimeSeriesPoint }
	type openedRes struct{ v []db.TimeSeriesPoint }

	overallCh := make(chan overallRes, 1)
	bucketsCh := make(chan bucketsRes, 1)
	pointsCh := make(chan pointsRes, 1)
	openedCh := make(chan openedRes, 1)

	go func() {
		v, err := h.db.GlobalOverallStats(minStars, minContribs)
		overallCh <- overallRes{v, err}
	}()
	go func() { v, _ := h.db.GlobalSizeChartData(cutoffPct, minStars, minContribs); bucketsCh <- bucketsRes{v} }()
	go func() { v, _ := h.db.GlobalTimeSeriesData(cutoffPct, minStars, minContribs); pointsCh <- pointsRes{v} }()
	go func() { v, _ := h.db.GlobalOpenedSeriesData(minStars, minContribs); openedCh <- openedRes{v} }()

	overall := <-overallCh
	buckets := <-bucketsCh
	points := <-pointsCh
	opened := <-openedCh

	var sizeChart *statsChartPayload
	if len(buckets.v) > 0 {
		p := &statsChartPayload{}
		for _, b := range buckets.v {
			p.Labels = append(p.Labels, b.Label)
			p.PRCounts = append(p.PRCounts, b.PRCount)
			p.AvgHours = append(p.AvgHours, roundTo1(b.AvgSecs/3600))
			p.MedianHours = append(p.MedianHours, roundTo1(b.MedianSecs/3600))
			p.ApprovalRate = append(p.ApprovalRate, roundTo1(b.ApprovalRate))
			p.ChangesRequestedRate = append(p.ChangesRequestedRate, roundTo1(b.ChangesRequestedRate))
			p.AvgChangesRequested = append(p.AvgChangesRequested, roundTo1(b.AvgChangesRequested))
		}
		sizeChart = p
	}

	var timeChart *timeChartPayload
	if len(points.v) > 0 {
		tp := &timeChartPayload{}
		for _, p := range points.v {
			tp.Labels = append(tp.Labels, p.Label)
			tp.PRCounts = append(tp.PRCounts, p.PRCount)
			tp.AvgSize = append(tp.AvgSize, roundTo1(p.AvgSize))
			tp.MedianSize = append(tp.MedianSize, roundTo1(p.MedianSize))
			tp.AvgHours = append(tp.AvgHours, roundTo1(p.AvgSecs/3600))
			tp.MedianHours = append(tp.MedianHours, roundTo1(p.MedianSecs/3600))
			tp.ChangesRequestedRate = append(tp.ChangesRequestedRate, roundTo1(p.ChangesRequestedRate))
			tp.AvgFirstReviewHours = append(tp.AvgFirstReviewHours, roundTo1(p.AvgFirstReviewSecs/3600))
			tp.MedFirstReviewHours = append(tp.MedFirstReviewHours, roundTo1(p.MedFirstReviewSecs/3600))
			tp.UnreviewedMergeRate = append(tp.UnreviewedMergeRate, roundTo1(p.UnreviewedRate))
			tp.LinesPerContrib = append(tp.LinesPerContrib, roundTo1(p.LinesPerContrib))
		}
		openedMap := make(map[string]int)
		for _, p := range opened.v {
			openedMap[p.Label] = p.PRCount
		}
		for i, label := range tp.Labels {
			oc := openedMap[label]
			tp.OpenedCounts = append(tp.OpenedCounts, oc)
			rate := 0.0
			if oc > 0 {
				rate = roundTo1(float64(tp.PRCounts[i]) / float64(oc) * 100)
			}
			tp.MergeVsOpenRate = append(tp.MergeVsOpenRate, rate)
		}
		timeChart = tp
	}

	resp := map[string]interface{}{
		"Overall":     overall.v,
		"Trim":        trim,
		"MinStars":    minStars,
		"MinContribs": minContribs,
		"SizeChart":   sizeChart,
		"TimeChart":   timeChart,
	}

	if h.cache != nil {
		if raw, err := json.Marshal(resp); err == nil {
			h.cache.Set(r.Context(), cacheKey, raw, rdb.CacheTTL)
		}
	}

	setCachePublic(w, 120*time.Second, 600*time.Second)
	writeJSON(w, http.StatusOK, resp)
}

// ── /api/v1/data/* ─────────────────────────────────────────────────────────────

func (h *Handler) DataReposJSON(w http.ResponseWriter, r *http.Request) {
	page, offset, search, sortBy, status, _, _, _, _ := parseDataQuery(r)
	repos, total, _ := h.db.ListReposFiltered(dataLimit, offset, sortBy, search, status)
	if repos == nil {
		repos = []db.Repo{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"Repos":    repos,
		"Total":    total,
		"Page":     page,
		"HasPrev":  page > 0,
		"HasNext":  offset+dataLimit < total,
		"PrevPage": page - 1,
		"NextPage": page + 1,
	})
}

func (h *Handler) DataPRsJSON(w http.ResponseWriter, r *http.Request) {
	page, offset, _, sortBy, _, author, _, _, repo := parseDataQuery(r)
	prs, total, _ := h.db.ListPRsFiltered(dataLimit, offset, repo, author, sortBy)
	if prs == nil {
		prs = []db.PullRequest{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"PRs":      prs,
		"Total":    total,
		"Page":     page,
		"HasPrev":  page > 0,
		"HasNext":  offset+dataLimit < total,
		"PrevPage": page - 1,
		"NextPage": page + 1,
	})
}

func (h *Handler) DataReviewsJSON(w http.ResponseWriter, r *http.Request) {
	page, offset, _, _, _, _, reviewer, state, _ := parseDataQuery(r)
	reviews, total, _ := h.db.ListReviewsFiltered(dataLimit, offset, reviewer, state)
	if reviews == nil {
		reviews = []db.Review{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"Reviews":  reviews,
		"Total":    total,
		"Page":     page,
		"HasPrev":  page > 0,
		"HasNext":  offset+dataLimit < total,
		"PrevPage": page - 1,
		"NextPage": page + 1,
	})
}

func (h *Handler) DataUsersJSON(w http.ResponseWriter, r *http.Request) {
	page, offset, search, _, _, _, _, _, _ := parseDataQuery(r)
	users, total, _ := h.db.ListUsersFiltered(dataLimit, offset, search)
	if users == nil {
		users = []db.User{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"Users":    users,
		"Total":    total,
		"Page":     page,
		"HasPrev":  page > 0,
		"HasNext":  offset+dataLimit < total,
		"PrevPage": page - 1,
		"NextPage": page + 1,
	})
}

// ── /api/v1/blog ───────────────────────────────────────────────────────────────

func (h *Handler) BlogJSON(w http.ResponseWriter, r *http.Request) {
	overall, _ := h.db.GlobalOverallStats(0, 0)
	topReviewers, _ := h.db.LeaderboardReviewers(5)
	topSpeed, _ := h.db.LeaderboardReposBySpeed("ASC", 5)
	totalRepos, totalPRs, totalReviews := h.db.TotalStats()
	if topReviewers == nil {
		topReviewers = []db.LeaderboardEntry{}
	}
	if topSpeed == nil {
		topSpeed = []db.LeaderboardEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"LiveStats":    overall,
		"TopReviewers": topReviewers,
		"TopSpeed":     topSpeed,
		"TotalRepos":   totalRepos,
		"TotalPRs":     totalPRs,
		"TotalReviews": totalReviews,
	})
}

// ── /api/v1/dashboard ─────────────────────────────────────────────────────────

func (h *Handler) DashboardJSON(w http.ResponseWriter, r *http.Request) {
	login := currentUser(r)
	if login == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	instID := installationID(r)
	avatarURL := ""
	if u, err := h.db.GetUser(login); err == nil && u != nil {
		avatarURL = u.AvatarURL
	}

	if instID == 0 {
		if id, err := h.db.GetInstallationByLogin(login); err == nil && id != nil {
			instID = *id
		}
	}

	var trackedRepos []db.Repo
	hasInstall := instID != 0
	availableRepos := []string{}

	if hasInstall {
		if repos, err := h.db.UserOwnedTrackedRepos(login); err == nil {
			trackedRepos = repos
		}
	}
	if trackedRepos == nil {
		trackedRepos = []db.Repo{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"Login":          login,
		"AvatarURL":      avatarURL,
		"TrackedRepos":   trackedRepos,
		"AvailableRepos": availableRepos,
		"HasInstall":     hasInstall,
		"InstallURL":     fmt.Sprintf("https://github.com/apps/%s/installations/new", h.cfg.GitHubAppSlug),
	})
}

// ── /api/v1/hi ─────────────────────────────────────────────────────────────────

func (h *Handler) HiGetJSON(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" || len(path) > 200 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid path"})
		return
	}
	total, reactions, todayCount := h.db.HiGetAll(path)
	didHi, myReaction := alreadySaidHi(r, path)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total":      total,
		"reactions":  reactions,
		"todayCount": todayCount,
		"didHi":      didHi,
		"myReaction": myReaction,
	})
}

func (h *Handler) HiPostJSON(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path     string `json:"path"`
		Reaction string `json:"reaction"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
		return
	}
	path := body.Path
	reaction := body.Reaction
	if path == "" || len(path) > 200 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid path"})
		return
	}
	valid := false
	for _, rx := range hiReactions {
		if rx.Key == reaction {
			valid = true
			break
		}
	}
	if !valid {
		reaction = "wave"
	}
	didHi, myReaction := alreadySaidHi(r, path)
	var total int
	var reactions map[string]int
	var todayCount int
	if !didHi {
		total, reactions, todayCount = h.db.HiIncrementReaction(path, reaction)
		myReaction = reaction
		markSaidHi(w, r, path, reaction)
	} else {
		total, reactions, todayCount = h.db.HiGetAll(path)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total":      total,
		"reactions":  reactions,
		"todayCount": todayCount,
		"didHi":      true,
		"myReaction": myReaction,
	})
}

func (h *Handler) HiWallJSON(w http.ResponseWriter, r *http.Request) {
	pages, err := h.db.HiTopWallPages(50)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"pages": pages})
}
