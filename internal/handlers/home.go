package handlers

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"inreview/internal/db"
)

type HomeData struct {
	BaseData
	TotalRepos    int
	TotalPRs      int
	TotalReviews  int
	SpeedDemons   []db.LeaderboardEntry
	PRGraveyard   []db.LeaderboardEntry
	ReviewChamps  []db.LeaderboardEntry
	Gatekeepers   []db.LeaderboardEntry
	MergeMasters  []db.LeaderboardEntry
	OneShot       []db.LeaderboardEntry
	PopularVisits []db.PageVisit
	RecentVisits  []db.PageVisit
	OGTitle       string
	OGDesc        string
	OGUrl         string
}

// homeLBCache holds all data needed to render the home page, cached together
// to eliminate all DB queries on cache hit.
type homeLBCache struct {
	SpeedDemons   []db.LeaderboardEntry
	PRGraveyard   []db.LeaderboardEntry
	ReviewChamps  []db.LeaderboardEntry
	Gatekeepers   []db.LeaderboardEntry
	MergeMasters  []db.LeaderboardEntry
	OneShot       []db.LeaderboardEntry
	TotalRepos    int
	TotalPRs      int
	TotalReviews  int
	PopularVisits []db.PageVisit
	RecentVisits  []db.PageVisit
}

const homeLBCacheKey = "home:lb"
const homeLBCacheTTL = 15 * time.Minute

// homeSF deduplicates concurrent cache-miss rebuilds so only one set of
// leaderboard queries runs even when many requests arrive simultaneously.
var homeSF singleflight.Group

// buildHomeCache runs all leaderboard queries and stores the result in Redis.
func (h *Handler) buildHomeCache(ctx context.Context) (homeLBCache, error) {
	v, err, _ := homeSF.Do(homeLBCacheKey, func() (interface{}, error) {
		var lb homeLBCache
		var wg sync.WaitGroup
		wg.Add(8)
		go func() { defer wg.Done(); lb.SpeedDemons, _ = h.db.LeaderboardReposBySpeed("ASC", 5) }()
		go func() { defer wg.Done(); lb.PRGraveyard, _ = h.db.LeaderboardReposBySpeed("DESC", 5) }()
		go func() { defer wg.Done(); lb.ReviewChamps, _ = h.db.LeaderboardReviewers(5) }()
		go func() { defer wg.Done(); lb.Gatekeepers, _ = h.db.LeaderboardGatekeepers(5) }()
		go func() { defer wg.Done(); lb.MergeMasters, _ = h.db.LeaderboardAuthors(5) }()
		go func() { defer wg.Done(); lb.OneShot, _ = h.db.LeaderboardCleanApprovals(5) }()
		go func() { defer wg.Done(); lb.TotalRepos, lb.TotalPRs, lb.TotalReviews = h.db.TotalStats() }()
		go func() {
			defer wg.Done()
			pop, _ := h.db.PopularVisits(3)
			lb.PopularVisits = pop
			if len(pop) > 0 {
				exclude := make([]string, len(pop))
				for i, v := range pop {
					exclude[i] = v.Path
				}
				lb.RecentVisits, _ = h.db.RecentVisits(5, exclude)
			} else {
				lb.RecentVisits, _ = h.db.RecentVisits(5, nil)
			}
		}()
		wg.Wait()

		if raw, err := json.Marshal(lb); err == nil {
			h.cache.Set(ctx, homeLBCacheKey, raw, homeLBCacheTTL)
		}
		return lb, nil
	})
	if err != nil {
		return homeLBCache{}, err
	}
	return v.(homeLBCache), nil
}

// WarmLeaderboards rebuilds the materialized leaderboard tables then keeps
// them fresh on a timer. The home page Redis cache is also refreshed afterwards
// so it immediately reflects the new data. Call once at startup in a goroutine.
func (h *Handler) WarmLeaderboards() {
	ctx := context.Background()

	rebuild := func() {
		log.Printf("leaderboards: refreshing materialized tables…")
		if err := h.db.RefreshLeaderboards(); err != nil {
			log.Printf("leaderboards: refresh error: %v", err)
			return
		}
		log.Printf("leaderboards: materialized tables ready, warming caches…")
		// Invalidate the home Redis cache so the next rebuild reads fresh mat data.
		h.cache.Del(ctx, homeLBCacheKey)
		if _, err := h.buildHomeCache(ctx); err != nil {
			log.Printf("leaderboards: home cache warm error: %v", err)
		} else {
			log.Printf("leaderboards: home cache ready")
		}
		if h.cfg.WarmStats {
			// The stats queries use percentile calculations over the full PR table.
			// Keep this opt-in on managed Postgres plans; cold /stats requests can
			// populate the cache without making every deploy and refresh expensive.
			h.cache.Del(ctx, "stats:v4:0:0:0")
			h.buildStatsCache(ctx, 0, 0, 0)
			log.Printf("leaderboards: stats cache ready")
		}
	}

	rebuild()

	if h.cfg.LeaderboardRefreshEvery <= 0 {
		return
	}

	ticker := time.NewTicker(h.cfg.LeaderboardRefreshEvery)
	defer ticker.Stop()
	for range ticker.C {
		rebuild()
	}
}

// WarmHomeCache pre-builds the home leaderboard cache and then refreshes it
// on a timer so it is never cold during normal operation. Call once at startup
// in a goroutine. Note: if WarmLeaderboards is also running, it handles the
// home cache too — only one of these needs to be started.
func (h *Handler) WarmHomeCache() {
	ctx := context.Background()
	log.Printf("home: warming leaderboard cache…")
	if _, err := h.buildHomeCache(ctx); err != nil {
		log.Printf("home: warm error: %v", err)
	} else {
		log.Printf("home: leaderboard cache ready")
	}

	// Refresh slightly before TTL expires so users never hit a cold cache.
	ticker := time.NewTicker(homeLBCacheTTL - 2*time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		log.Printf("home: refreshing leaderboard cache…")
		if _, err := h.buildHomeCache(ctx); err != nil {
			log.Printf("home: refresh error: %v", err)
		}
	}
}
