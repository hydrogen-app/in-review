package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"inreview/internal/auth"
	"inreview/internal/db"
	"inreview/internal/github"
)

// RegisterNextRoutes exposes JSON endpoints consumed by the Next.js frontend.
func (h *Handler) RegisterNextRoutes(r chi.Router) {
	r.Get("/api/next/session", h.NextSession)
	r.Get("/api/next/home", h.NextHome)
	r.Get("/api/next/search", h.NextSearch)
	r.Get("/api/next/stats", h.NextStats)
	r.Get("/api/next/repo/{owner}/{name}", h.NextRepo)
	r.Get("/api/next/repo/{owner}/{name}/charts", h.NextRepoCharts)
	r.Get("/api/next/user/{username}", h.NextUser)
	r.Get("/api/next/user/{username}/charts", h.NextUserCharts)
	r.Get("/api/next/org/{org}", h.NextOrg)
	r.Get("/api/next/graph/repo/{owner}/{name}", h.NextRepoGraph)
	r.Get("/api/next/graph/user/{username}", h.NextUserGraph)
	r.Get("/api/next/graph/org/{org}", h.NextOrgGraph)
	r.Get("/api/next/leaderboard/{category}", h.NextLeaderboard)
	r.Get("/api/next/leaderboard/{category}/search", h.NextLeaderboardSearch)
	r.Get("/api/next/data/{tab}", h.NextData)
	r.Get("/api/next/blog", h.NextBlog)
	r.Get("/api/next/dashboard", h.NextDashboard)
	r.Get("/api/next/hi", h.NextHiGet)
	r.Post("/api/next/hi", h.NextHiPost)
	r.Get("/api/next/hi-wall", h.NextHiWall)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, title, message string) {
	writeJSON(w, status, map[string]interface{}{
		"error":   title,
		"message": message,
		"status":  status,
	})
}

func writeGHJSONError(w http.ResponseWriter, err error, notFoundTitle, notFoundMsg string) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, github.ErrRateLimited):
		writeJSONError(w, http.StatusTooManyRequests, "GitHub Rate Limit Reached",
			"The GitHub API hourly rate limit has been hit. Cached data is still available on pages already synced.")
	case errors.Is(err, github.ErrNotFound):
		writeJSONError(w, http.StatusNotFound, notFoundTitle, notFoundMsg)
	default:
		writeJSONError(w, http.StatusBadGateway, "GitHub Unavailable",
			"Couldn't reach GitHub right now. Try again in a moment.")
	}
	return true
}

func graphLimit(r *http.Request) int {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		return 360
	}
	if limit < 80 {
		return 80
	}
	if limit > 1200 {
		return 1200
	}
	return limit
}

func (h *Handler) NextSession(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.baseData(r))
}

func (h *Handler) NextHome(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var lb homeLBCache
	if raw, ok := h.cache.Get(ctx, homeLBCacheKey); ok {
		_ = json.Unmarshal(raw, &lb)
	} else {
		lb, _ = h.buildHomeCache(ctx)
	}

	data := HomeData{
		BaseData:      h.baseData(r),
		TotalRepos:    lb.TotalRepos,
		TotalPRs:      lb.TotalPRs,
		TotalReviews:  lb.TotalReviews,
		SpeedDemons:   lb.SpeedDemons,
		PRGraveyard:   lb.PRGraveyard,
		ReviewChamps:  lb.ReviewChamps,
		Gatekeepers:   lb.Gatekeepers,
		MergeMasters:  lb.MergeMasters,
		OneShot:       lb.OneShot,
		PopularVisits: lb.PopularVisits,
		RecentVisits:  lb.RecentVisits,
	}
	data.OGDesc = fmt.Sprintf("%d PRs analyzed across %d repos. Global leaderboards for GitHub PR review time. If you aren't reviewing, you're ngmi.", data.TotalPRs, data.TotalRepos)
	writeJSON(w, http.StatusOK, data)
}

func (h *Handler) NextSearch(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, http.StatusOK, SearchData{})
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
				_ = h.db.UpsertRepo(db.Repo{
					FullName:    fullName,
					Owner:       owner,
					Name:        ghRepo.Name,
					Description: ghRepo.Description,
					Stars:       ghRepo.Stars,
					Language:    ghRepo.Language,
					SyncStatus:  "pending",
				})
				existing, _ = h.db.GetRepo(fullName)
			}
		}
		if existing != nil {
			h.worker.Queue(fullName, false)
			rank, _ := h.db.RepoSpeedRank(fullName)
			data.Results = append(data.Results, SearchResult{
				Type:         "repo",
				Name:         existing.Name,
				FullName:     existing.FullName,
				Description:  existing.Description,
				Stars:        existing.Stars,
				Language:     existing.Language,
				MergedPRs:    existing.MergedPRCount,
				AvgMergeTime: existing.AvgMergeTimeSecs,
				SpeedRank:    rank,
				IsCached:     existing.SyncStatus == "done",
			})
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
				Type:        "repo",
				Name:        r.Name,
				FullName:    r.FullName,
				Description: r.Description,
				Stars:       r.Stars,
				Language:    r.Language,
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
				Type:        kind,
				Name:        u.Name,
				FullName:    u.Login,
				Description: u.Bio,
				AvatarURL:   u.AvatarURL,
			})
		}
		userCh <- results
	}()

	data.Results = append(data.Results, <-userCh...)
	data.Results = append(data.Results, <-repoCh...)
	writeJSON(w, http.StatusOK, data)
}

func (h *Handler) NextStats(w http.ResponseWriter, r *http.Request) {
	trim, _ := parseTrim(r)
	minStars, _ := strconv.Atoi(r.URL.Query().Get("min_stars"))
	if minStars < 0 {
		minStars = 0
	}
	minContribs, _ := strconv.Atoi(r.URL.Query().Get("min_contribs"))
	if minContribs < 0 {
		minContribs = 0
	}

	cacheKey := fmt.Sprintf("stats:v4:%d:%d:%d", trim, minStars, minContribs)
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), cacheKey); ok {
			var data StatsData
			if json.Unmarshal(raw, &data) == nil {
				data.BaseData = h.baseData(r)
				writeJSON(w, http.StatusOK, data)
				return
			}
		}
	}

	data := h.buildStatsCache(r.Context(), trim, minStars, minContribs)
	data.BaseData = h.baseData(r)
	writeJSON(w, http.StatusOK, data)
}

func (h *Handler) NextRepo(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name
	trim, _ := parseTrim(r)

	repo, _ := h.db.GetRepo(fullName)
	if repo == nil {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		ghRepo, err := h.gh.GetRepo(ctx, owner, name)
		if writeGHJSONError(w, err, "Repo Not Found",
			"Could not find "+fullName+" on GitHub. Check the spelling and try again.") {
			return
		}
		_ = h.db.UpsertRepo(db.Repo{
			FullName:    fullName,
			Owner:       owner,
			Name:        ghRepo.Name,
			Description: ghRepo.Description,
			Stars:       ghRepo.Stars,
			Language:    ghRepo.Language,
			SyncStatus:  "pending",
		})
		repo, _ = h.db.GetRepo(fullName)
		if repo == nil {
			repo = &db.Repo{
				FullName:    fullName,
				Owner:       owner,
				Name:        ghRepo.Name,
				Description: ghRepo.Description,
				Stars:       ghRepo.Stars,
				Language:    ghRepo.Language,
				SyncStatus:  "pending",
			}
		}
	}

	h.worker.Queue(fullName, false)

	data := RepoData{
		BaseData:  h.baseData(r),
		Repo:      repo,
		IsSyncing: h.worker.IsSyncing(fullName),
		Trim:      trim,
	}
	data.OwnerUser, _ = h.db.GetUser(owner)

	var rpc repoPageCache
	repoCacheKey := fmt.Sprintf("repo:v1:%s", fullName)
	repoCacheHit := false
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), repoCacheKey); ok {
			if json.Unmarshal(raw, &rpc) == nil {
				repoCacheHit = true
			}
		}
	}
	if !repoCacheHit {
		type reviewersRes struct{ v []db.ReviewerStats }
		type recentRes struct{ v []db.PullRequest }
		type rankRes struct{ v int }

		rvCh := make(chan reviewersRes, 1)
		rcCh := make(chan recentRes, 1)
		rkCh := make(chan rankRes, 1)

		go func() { v, _ := h.db.RepoTopReviewers(fullName, 10); rvCh <- reviewersRes{v} }()
		go func() { v, _ := h.db.RecentMergedPRs(fullName, 20); rcCh <- recentRes{v} }()
		go func() { v, _ := h.db.RepoSpeedRank(fullName); rkCh <- rankRes{v} }()

		rpc = repoPageCache{
			TopReviewers: (<-rvCh).v,
			RecentPRs:    (<-rcCh).v,
			SpeedRank:    (<-rkCh).v,
		}
		if h.cache != nil {
			if raw, err := json.Marshal(rpc); err == nil {
				h.cache.Set(r.Context(), repoCacheKey, raw, repoPageCacheTTL)
			}
		}
	}

	data.TopReviewers = rpc.TopReviewers
	data.RecentPRs = rpc.RecentPRs
	data.SpeedRank = rpc.SpeedRank
	data.OGTitle = fullName + " — ngmi"
	data.OGUrl = "https://ngmi.review/repo/" + fullName
	ogDesc := fullName
	if repo.AvgMergeTimeSecs > 0 {
		ogDesc += " merges PRs in " + formatDuration(repo.AvgMergeTimeSecs) + " on average"
	}
	if data.SpeedRank > 0 {
		ogDesc += fmt.Sprintf(" (#%d globally)", data.SpeedRank)
	}
	ogDesc += ". Track your repo at ngmi.review."
	data.OGDesc = ogDesc

	shareText := fullName
	if repo.AvgMergeTimeSecs > 0 {
		shareText += " merges PRs in " + formatDuration(repo.AvgMergeTimeSecs)
	}
	if data.SpeedRank > 0 {
		shareText += fmt.Sprintf(", #%d globally", data.SpeedRank)
	}
	shareText += ". If you aren't reviewing, you're ngmi."
	data.ShareURL = "https://twitter.com/intent/tweet?text=" + url.QueryEscape(shareText) +
		"&url=" + url.QueryEscape(data.OGUrl)

	h.db.RecordVisit("/repo/"+fullName, "repo", fullName)
	writeJSON(w, http.StatusOK, data)
}

func (h *Handler) NextRepoCharts(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	fullName := owner + "/" + name
	trim, cutoffPct := parseTrim(r)

	cacheKey := fmt.Sprintf("repo:charts:v1:%s:%d", fullName, trim)
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), cacheKey); ok {
			var cc repoChartsCache
			if json.Unmarshal(raw, &cc) == nil {
				writeJSON(w, http.StatusOK, cc)
				return
			}
		}
	}

	type bucketsRes struct{ v []db.PRSizeBucket }
	type pointsRes struct{ v []db.TimeSeriesPoint }
	buCh := make(chan bucketsRes, 1)
	ptCh := make(chan pointsRes, 1)
	go func() { v, _ := h.db.RepoSizeChartData(fullName, cutoffPct); buCh <- bucketsRes{v} }()
	go func() { v, _ := h.db.RepoTimeSeriesData(fullName, cutoffPct); ptCh <- pointsRes{v} }()

	buckets := (<-buCh).v
	points := (<-ptCh).v
	cd := RepoChartsData{Owner: owner, Name: name, Trim: trim}

	if len(buckets) > 0 {
		payload := sizeChartPayload{}
		for _, b := range buckets {
			payload.Labels = append(payload.Labels, b.Label)
			payload.PRCounts = append(payload.PRCounts, b.PRCount)
			payload.AvgHours = append(payload.AvgHours, roundTo1(b.AvgSecs/3600))
			payload.ApprovalRate = append(payload.ApprovalRate, roundTo1(b.ApprovalRate))
		}
		if raw, err := json.Marshal(payload); err == nil {
			cd.SizeChartJSON = string(raw)
		}
	}

	if len(points) > 0 {
		tp := timeChartPayload{}
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
		if raw, err := json.Marshal(tp); err == nil {
			cd.TimeChartJSON = string(raw)
		}
	}

	cc := repoChartsCache{
		SizeChartJSON: cd.SizeChartJSON,
		TimeChartJSON: cd.TimeChartJSON,
	}
	if h.cache != nil {
		if raw, err := json.Marshal(cc); err == nil {
			h.cache.Set(r.Context(), cacheKey, raw, repoChartsCacheTTL)
		}
	}
	writeJSON(w, http.StatusOK, cc)
}

func (h *Handler) NextUser(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	ghUser, err := h.gh.GetUser(ctx, username)
	if err != nil {
		if cached, dbErr := h.db.GetUser(username); dbErr == nil && cached != nil {
			// Serve cached data.
		} else {
			writeGHJSONError(w, err, "User Not Found",
				"Could not find @"+username+" on GitHub. Check the spelling and try again.")
			return
		}
	}

	if ghUser != nil && ghUser.Type == "Organization" {
		writeJSON(w, http.StatusOK, map[string]string{"redirect": "/org/" + username})
		return
	}

	if ghUser != nil {
		_ = h.db.UpsertUser(db.User{
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
		user = &db.User{Login: ghUser.Login, Name: ghUser.Name, AvatarURL: ghUser.AvatarURL}
	}
	if user == nil {
		writeGHJSONError(w, err, "User Not Found",
			"Could not find @"+username+" on GitHub. Check the spelling and try again.")
		return
	}

	go func() {
		bg, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if repos, err := h.gh.GetUserRepos(bg, username, 10); err == nil {
			for _, repo := range repos {
				_ = h.db.UpsertRepo(db.Repo{
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

	var ranks userRankCache
	rankCacheKey := fmt.Sprintf("user:rank:v1:%s", username)
	rankCacheHit := false
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), rankCacheKey); ok {
			if json.Unmarshal(raw, &ranks) == nil {
				rankCacheHit = true
			}
		}
	}
	if !rankCacheHit && h.cache != nil {
		cache := h.cache
		go func() {
			bg := context.Background()
			var rc userRankCache
			type r1 struct{ v int }
			c1, c2, c3 := make(chan r1, 1), make(chan r1, 1), make(chan r1, 1)
			go func() { v, _ := h.db.UserReviewerRank(username); c1 <- r1{v} }()
			go func() { v, _ := h.db.UserGatekeeperRank(username); c2 <- r1{v} }()
			go func() { v, _ := h.db.UserAuthorRank(username); c3 <- r1{v} }()
			rc.ReviewerRank = (<-c1).v
			rc.GatekeeperRank = (<-c2).v
			rc.AuthorRank = (<-c3).v
			if raw, err := json.Marshal(rc); err == nil {
				cache.Set(bg, rankCacheKey, raw, userRankCacheTTL)
			}
		}()
	}

	var pc userPageCache
	cacheKey := fmt.Sprintf("user:v1:%s", username)
	cacheHit := false
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), cacheKey); ok {
			if json.Unmarshal(raw, &pc) == nil {
				cacheHit = true
			}
		}
	}
	if !cacheHit {
		type rrRes struct {
			v   *db.ReviewerStats
			err error
		}
		type arRes struct {
			v   *db.AuthorStats
			err error
		}
		type recRes struct{ fastest, slowest *db.UserRecordPR }
		type collabRes struct {
			reviewersOfMe  []db.CollabEntry
			authorsIReview []db.CollabEntry
		}
		type reviewedReposRes struct {
			v   []db.UserRepoReview
			err error
		}

		rrCh := make(chan rrRes, 1)
		arCh := make(chan arRes, 1)
		contCh := make(chan []db.Repo, 1)
		recCh := make(chan recRes, 1)
		colCh := make(chan collabRes, 1)
		rvRepCh := make(chan reviewedReposRes, 1)

		go func() { v, err := h.db.UserReviewerStats(username); rrCh <- rrRes{v, err} }()
		go func() { v, err := h.db.UserAuthorStats(username); arCh <- arRes{v, err} }()
		go func() { v, _ := h.db.UserContributedRepos(username, 10); contCh <- v }()
		go func() { f, s, _ := h.db.UserRecordPRs(username); recCh <- recRes{f, s} }()
		go func() { rm, ai, _ := h.db.UserTopCollaborators(username, 5); colCh <- collabRes{rm, ai} }()
		go func() { v, err := h.db.UserTopReviewedRepos(username, 8); rvRepCh <- reviewedReposRes{v, err} }()

		rrResult := <-rrCh
		arResult := <-arCh
		rec := <-recCh
		col := <-colCh
		rvRep := <-rvRepCh

		pc = userPageCache{
			ReviewerStats:  rrResult.v,
			AuthorStats:    arResult.v,
			ContribRepos:   <-contCh,
			FastestPR:      rec.fastest,
			SlowestPR:      rec.slowest,
			ReviewedRepos:  rvRep.v,
			ReviewersOfMe:  col.reviewersOfMe,
			AuthorsIReview: col.authorsIReview,
		}

		if h.cache != nil {
			if raw, err := json.Marshal(pc); err == nil {
				h.cache.Set(r.Context(), cacheKey, raw, userPageCacheTTL)
			}
		}
	}

	data := UserData{
		BaseData:         h.baseData(r),
		User:             user,
		IsOrg:            false,
		ReviewerStats:    pc.ReviewerStats,
		AuthorStats:      pc.AuthorStats,
		ReviewerRank:     ranks.ReviewerRank,
		GatekeeperRank:   ranks.GatekeeperRank,
		AuthorRank:       ranks.AuthorRank,
		ContributedRepos: pc.ContribRepos,
		FastestPR:        pc.FastestPR,
		SlowestPR:        pc.SlowestPR,
		ReviewersOfMe:    pc.ReviewersOfMe,
		AuthorsIReview:   pc.AuthorsIReview,
		ReviewedRepos:    pc.ReviewedRepos,
	}
	data.IsNGMI = data.ReviewerStats == nil || data.ReviewerStats.TotalReviews < 10

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

	h.db.RecordVisit("/user/"+username, "user", username)
	writeJSON(w, http.StatusOK, data)
}

func (h *Handler) NextUserCharts(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	cacheKey := fmt.Sprintf("user:charts:v1:%s", username)
	if h.cache != nil {
		if raw, ok := h.cache.Get(r.Context(), cacheKey); ok {
			var cc userChartsCache
			if json.Unmarshal(raw, &cc) == nil {
				writeJSON(w, http.StatusOK, cc)
				return
			}
		}
	}

	type actRes struct{ v []db.UserActivityPoint }
	type sizeRes struct{ v []db.PRSizeBucket }
	actCh := make(chan actRes, 1)
	sizeCh := make(chan sizeRes, 1)
	go func() { v, _ := h.db.UserActivitySeries(username); actCh <- actRes{v} }()
	go func() { v, _ := h.db.UserPRSizeDist(username); sizeCh <- sizeRes{v} }()

	activity := (<-actCh).v
	sizeDist := (<-sizeCh).v
	cd := UserChartsData{}

	if len(activity) > 0 {
		ap := userActivityPayload{}
		for _, p := range activity {
			ap.Labels = append(ap.Labels, p.Label)
			ap.PRCounts = append(ap.PRCounts, p.PRCount)
			ap.ReviewCounts = append(ap.ReviewCounts, p.ReviewCount)
			ap.CRRate = append(ap.CRRate, roundTo1(p.ChangesRequestedRate))
		}
		if raw, err := json.Marshal(ap); err == nil {
			cd.ActivityJSON = string(raw)
		}
	}
	if len(sizeDist) > 0 {
		sp := userSizePayload{}
		for _, b := range sizeDist {
			sp.Labels = append(sp.Labels, b.Label)
			sp.PRCounts = append(sp.PRCounts, b.PRCount)
		}
		if raw, err := json.Marshal(sp); err == nil {
			cd.SizeBucketJSON = string(raw)
		}
	}

	cc := userChartsCache{
		ActivityJSON:   cd.ActivityJSON,
		SizeBucketJSON: cd.SizeBucketJSON,
	}
	if h.cache != nil {
		if raw, err := json.Marshal(cc); err == nil {
			h.cache.Set(r.Context(), cacheKey, raw, userChartsCacheTTL)
		}
	}
	writeJSON(w, http.StatusOK, cc)
}

func (h *Handler) NextOrg(w http.ResponseWriter, r *http.Request) {
	orgName := chi.URLParam(r, "org")
	trim, cutoffPct := parseTrim(r)

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	ghUser, err := h.gh.GetUser(ctx, orgName)
	if err != nil {
		if cached, dbErr := h.db.GetUser(orgName); dbErr == nil && cached != nil && cached.IsOrg {
			// Serve cached data.
		} else {
			writeGHJSONError(w, err, "Org Not Found",
				"Could not find the organization "+orgName+" on GitHub. Check the spelling and try again.")
			return
		}
	}
	if ghUser != nil && ghUser.Type != "Organization" {
		writeJSON(w, http.StatusOK, map[string]string{"redirect": "/user/" + orgName})
		return
	}
	if ghUser != nil {
		_ = h.db.UpsertUser(db.User{
			Login:       ghUser.Login,
			Name:        ghUser.Name,
			AvatarURL:   ghUser.AvatarURL,
			Bio:         ghUser.Bio,
			PublicRepos: ghUser.PublicRepos,
			Followers:   ghUser.Followers,
			IsOrg:       true,
		})
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		repos, err := h.gh.GetOrgRepos(ctx, orgName, 20)
		if err != nil {
			return
		}
		for _, repo := range repos {
			_ = h.db.UpsertRepo(db.Repo{
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
	if org == nil && ghUser != nil {
		org = &db.User{Login: ghUser.Login, Name: ghUser.Name, AvatarURL: ghUser.AvatarURL, IsOrg: true}
	}
	if org == nil {
		org = &db.User{Login: orgName, IsOrg: true}
	}

	repos, _ := h.db.OrgRepos(orgName)
	totalPRs := 0
	isSyncing := false
	for _, rp := range repos {
		totalPRs += rp.MergedPRCount
		if rp.SyncStatus == "syncing" || h.worker.IsSyncing(rp.FullName) {
			isSyncing = true
		}
	}

	data := OrgData{
		BaseData:       h.baseData(r),
		Org:            org,
		Repos:          repos,
		TotalMergedPRs: totalPRs,
		IsSyncing:      isSyncing,
		Trim:           trim,
	}
	data.ReviewerBoard, _ = h.db.OrgReviewerLeaderboard(orgName, 10)
	data.GatekeeperBoard, _ = h.db.OrgGatekeeperLeaderboard(orgName, 10)

	if points, err := h.db.OrgTimeSeriesData(orgName, cutoffPct); err == nil && len(points) > 0 {
		tp := timeChartPayload{}
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
		if raw, err := json.Marshal(tp); err == nil {
			data.TimeChartJSON = string(raw)
		}
	}

	data.OGTitle = orgName + " — ngmi"
	data.OGUrl = "https://ngmi.review/org/" + orgName
	h.db.RecordVisit("/org/"+orgName, "org", orgName)
	writeJSON(w, http.StatusOK, data)
}

func (h *Handler) NextRepoGraph(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	name := chi.URLParam(r, "name")
	graph, err := h.db.RepoRelationGraph(owner+"/"+name, graphLimit(r))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSONError(w, http.StatusNotFound, "Repo Not Found", "No graph data is available for "+owner+"/"+name+".")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "Graph Unavailable", "Could not build the repository graph.")
		return
	}
	writeJSON(w, http.StatusOK, graph)
}

func (h *Handler) NextUserGraph(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	graph, err := h.db.UserRelationGraph(username, graphLimit(r))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSONError(w, http.StatusNotFound, "User Not Found", "No graph data is available for "+username+".")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "Graph Unavailable", "Could not build the user graph.")
		return
	}
	writeJSON(w, http.StatusOK, graph)
}

func (h *Handler) NextOrgGraph(w http.ResponseWriter, r *http.Request) {
	orgName := chi.URLParam(r, "org")
	graph, err := h.db.OrgRelationGraph(orgName, graphLimit(r))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSONError(w, http.StatusNotFound, "Org Not Found", "No graph data is available for "+orgName+".")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "Graph Unavailable", "Could not build the organization graph.")
		return
	}
	writeJSON(w, http.StatusOK, graph)
}

func (h *Handler) NextLeaderboard(w http.ResponseWriter, r *http.Request) {
	category := chi.URLParam(r, "category")
	meta, ok := leaderboardMeta[category]
	if !ok {
		writeJSONError(w, http.StatusNotFound, "Leaderboard Not Found", "\""+category+"\" is not a valid leaderboard category.")
		return
	}

	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}
	data := LeaderboardPageData{
		BaseData:    h.baseData(r),
		Category:    category,
		Title:       meta[0],
		Description: meta[1],
		OGTitle:     meta[0] + " — ngmi",
		OGDesc:      meta[1] + ". Global PR review leaderboards at ngmi.review.",
		OGUrl:       "https://ngmi.review/leaderboard/" + category,
	}
	h.populateLeaderboardData(&data, category, offset)
	writeJSON(w, http.StatusOK, data)
}

func (h *Handler) NextLeaderboardSearch(w http.ResponseWriter, r *http.Request) {
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

func (h *Handler) NextData(w http.ResponseWriter, r *http.Request) {
	tab := chi.URLParam(r, "tab")
	page, offset, limit, search, sortBy, status, author, reviewer, state, repo := parseDataQuery(r)

	data := DataExplorerData{
		BaseData:   h.baseData(r),
		ActiveTab:  tab,
		Page:       page,
		Offset:     offset,
		Limit:      limit,
		Search:     search,
		SortBy:     sortBy,
		Status:     status,
		Author:     author,
		Reviewer:   reviewer,
		State:      state,
		RepoFilter: repo,
		OGTitle:    "Data Explorer — ngmi",
		OGDesc:     "Browse all tracked repos, pull requests, reviews, and users.",
		OGUrl:      "https://ngmi.review/data",
	}

	switch tab {
	case "repos", "":
		if status == "all" {
			status = ""
			data.Status = "all"
		} else if status == "" {
			status = "done"
			data.Status = status
		}
		repos, total, _ := h.db.ListReposFiltered(limit, offset, sortBy, search, status)
		data.ActiveTab = "repos"
		data.Repos = repos
		data.ReposTotal = total
		extra := ""
		if search != "" {
			extra += "&search=" + url.QueryEscape(search)
		}
		if sortBy != "" {
			extra += "&sort=" + url.QueryEscape(sortBy)
		}
		if data.Status != "" {
			extra += "&status=" + url.QueryEscape(data.Status)
		}
		if limit != dataDefaultLimit {
			extra += "&limit=" + url.QueryEscape(strconv.Itoa(limit))
		}
		setPagination(&data, "/data/repos", total, page, offset, limit, extra)
	case "prs":
		prs, total, _ := h.db.ListPRsFiltered(limit, offset, repo, author, sortBy)
		data.PRs = prs
		data.PRsTotal = total
		extra := ""
		if author != "" {
			extra += "&author=" + url.QueryEscape(author)
		}
		if sortBy != "" {
			extra += "&sort=" + url.QueryEscape(sortBy)
		}
		if repo != "" {
			extra += "&repo=" + url.QueryEscape(repo)
		}
		if limit != dataDefaultLimit {
			extra += "&limit=" + url.QueryEscape(strconv.Itoa(limit))
		}
		setPagination(&data, "/data/prs", total, page, offset, limit, extra)
	case "reviews":
		reviews, total, _ := h.db.ListReviewsFiltered(limit, offset, reviewer, state, repo)
		data.Reviews = reviews
		data.ReviewsTotal = total
		data.TotalIsApprox = total > offset+len(reviews)
		extra := ""
		if reviewer != "" {
			extra += "&reviewer=" + url.QueryEscape(reviewer)
		}
		if state != "" {
			extra += "&state=" + url.QueryEscape(state)
		}
		if repo != "" {
			extra += "&repo=" + url.QueryEscape(repo)
		}
		if limit != dataDefaultLimit {
			extra += "&limit=" + url.QueryEscape(strconv.Itoa(limit))
		}
		setPagination(&data, "/data/reviews", total, page, offset, limit, extra)
	case "users":
		users, total, _ := h.db.ListUsersFiltered(limit, offset, search)
		data.Users = users
		data.UsersTotal = total
		extra := ""
		if search != "" {
			extra += "&search=" + url.QueryEscape(search)
		}
		if limit != dataDefaultLimit {
			extra += "&limit=" + url.QueryEscape(strconv.Itoa(limit))
		}
		setPagination(&data, "/data/users", total, page, offset, limit, extra)
	default:
		writeJSONError(w, http.StatusNotFound, "Data Tab Not Found", "\""+tab+"\" is not a valid data tab.")
		return
	}

	writeJSON(w, http.StatusOK, data)
}

func (h *Handler) NextBlog(w http.ResponseWriter, r *http.Request) {
	overall := h.cachedDefaultOverallStats(r.Context())
	topReviewers, _ := h.db.LeaderboardReviewers(5)
	topSpeed, _ := h.db.LeaderboardReposBySpeed("ASC", 5)
	totalRepos, totalPRs, totalReviews := h.db.TotalStats()

	data := BlogData{
		BaseData:     h.baseData(r),
		LiveStats:    overall,
		TopReviewers: topReviewers,
		TopSpeed:     topSpeed,
		TotalRepos:   totalRepos,
		TotalPRs:     totalPRs,
		TotalReviews: totalReviews,
		OGTitle:      "PR Review Time: What the Data Says — ngmi",
		OGDesc:       "Analysis of PR review patterns across thousands of GitHub repositories.",
		OGUrl:        "https://ngmi.review/blog",
	}
	writeJSON(w, http.StatusOK, data)
}

func (h *Handler) NextDashboard(w http.ResponseWriter, r *http.Request) {
	if currentUser(r) == "" {
		writeJSONError(w, http.StatusUnauthorized, "Login Required", "Sign in with GitHub to view your dashboard.")
		return
	}
	login := currentUser(r)
	instID := installationID(r)

	data := DashboardData{
		BaseData:   h.baseData(r),
		Login:      login,
		InstallURL: fmt.Sprintf("https://github.com/apps/%s/installations/new", h.cfg.GitHubAppSlug),
	}
	if u, err := h.db.GetUser(login); err == nil && u != nil {
		data.AvatarURL = u.AvatarURL
	}
	if instID == 0 {
		if id, err := h.db.GetInstallationByLogin(login); err == nil && id != nil {
			instID = *id
		}
	}
	if instID != 0 {
		data.HasInstall = true
		if repos, err := h.db.UserOwnedTrackedRepos(login); err == nil {
			data.TrackedRepos = repos
		}
		if h.cfg.GitHubAppID != 0 && h.cfg.GitHubAppPrivateKey != "" {
			if key, err := auth.ParsePrivateKey(h.cfg.GitHubAppPrivateKey); err == nil {
				if appJWT, err := auth.GenerateAppJWT(h.cfg.GitHubAppID, key); err == nil {
					if token, _, err := auth.GetInstallationToken(appJWT, fmt.Sprintf("%d", instID)); err == nil {
						if repoNames, err := auth.ListInstallationRepos(token); err == nil {
							tracked := make(map[string]bool)
							for _, rp := range data.TrackedRepos {
								tracked[rp.FullName] = true
							}
							for _, name := range repoNames {
								if !tracked[name] {
									data.AvailableRepos = append(data.AvailableRepos, name)
								}
							}
						}
					}
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, data)
}

type nextHiReaction struct {
	Key   string `json:"key"`
	Emoji string `json:"emoji"`
}

type nextHiData struct {
	Reactions      []nextHiReaction `json:"reactions"`
	ReactionCounts map[string]int   `json:"reactionCounts"`
	Total          int              `json:"total"`
	TodayCount     int              `json:"todayCount"`
	DidHi          bool             `json:"didHi"`
	MyReaction     string           `json:"myReaction"`
}

func nextHiPayload(total int, reactions map[string]int, todayCount int, didHi bool, myReaction string) nextHiData {
	payload := nextHiData{
		ReactionCounts: reactions,
		Total:          total,
		TodayCount:     todayCount,
		DidHi:          didHi,
		MyReaction:     myReaction,
	}
	for _, rx := range hiReactions {
		payload.Reactions = append(payload.Reactions, nextHiReaction{Key: rx.Key, Emoji: rx.Emoji})
	}
	return payload
}

func (h *Handler) NextHiGet(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" || len(path) > 200 {
		writeJSONError(w, http.StatusBadRequest, "Invalid Path", "A page path is required.")
		return
	}
	total, reactions, todayCount := h.db.HiGetAll(path)
	didHi, myReaction := alreadySaidHi(r, path)
	writeJSON(w, http.StatusOK, nextHiPayload(total, reactions, todayCount, didHi, myReaction))
}

func (h *Handler) NextHiPost(w http.ResponseWriter, r *http.Request) {
	path := r.FormValue("path")
	reaction := r.FormValue("reaction")
	if path == "" || len(path) > 200 {
		writeJSONError(w, http.StatusBadRequest, "Invalid Path", "A page path is required.")
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
	writeJSON(w, http.StatusOK, nextHiPayload(total, reactions, todayCount, true, myReaction))
}

func (h *Handler) NextHiWall(w http.ResponseWriter, r *http.Request) {
	pages, err := h.db.HiTopWallPages(50)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "Error", "Could not load hi wall.")
		return
	}
	writeJSON(w, http.StatusOK, HiWallData{BaseData: h.baseData(r), Pages: pages})
}
