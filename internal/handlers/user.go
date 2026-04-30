package handlers

import (
	"time"

	"inreview/internal/db"
)

const userPageCacheTTL = 5 * time.Minute

// userPageCache holds the fast DB-query results for a user page.
// Rank data is excluded — it's slow and lives in its own cache (userRankCache).
type userPageCache struct {
	ReviewerStats  *db.ReviewerStats   `json:"reviewerStats"`
	AuthorStats    *db.AuthorStats     `json:"authorStats"`
	ContribRepos   []db.Repo           `json:"contribRepos"`
	FastestPR      *db.UserRecordPR    `json:"fastestPR"`
	SlowestPR      *db.UserRecordPR    `json:"slowestPR"`
	ReviewedRepos  []db.UserRepoReview `json:"reviewedRepos"`
	ReviewersOfMe  []db.CollabEntry    `json:"reviewersOfMe"`
	AuthorsIReview []db.CollabEntry    `json:"authorsIReview"`
}

// userRankCache holds the pre-computed global rank numbers for a user.
// These are computed asynchronously and cached for 30 minutes because the
// underlying ROW_NUMBER() GROUP BY queries scan 25M+ rows.
const userRankCacheTTL = 30 * time.Minute

type userRankCache struct {
	ReviewerRank   int `json:"reviewerRank"`
	GatekeeperRank int `json:"gatekeeperRank"`
	AuthorRank     int `json:"authorRank"`
}

// UserChartsData is passed to the user_charts partial.
type UserChartsData struct {
	ActivityJSON   string
	SizeBucketJSON string
}

const userChartsCacheTTL = 5 * time.Minute

type userChartsCache struct {
	ActivityJSON   string `json:"activityJSON"`
	SizeBucketJSON string `json:"sizeBucketJSON"`
}

type userActivityPayload struct {
	Labels       []string  `json:"labels"`
	PRCounts     []int     `json:"prCounts"`
	ReviewCounts []int     `json:"reviewCounts"`
	CRRate       []float64 `json:"crRate"`
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
