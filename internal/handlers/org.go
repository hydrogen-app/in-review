package handlers

import (
	"inreview/internal/db"
)

type OrgData struct {
	BaseData
	Org             *db.User
	Repos           []db.Repo
	ReviewerBoard   []db.LeaderboardEntry
	GatekeeperBoard []db.LeaderboardEntry
	TotalMergedPRs  int
	TotalReviews    int
	IsSyncing       bool
	TimeChartJSON   string
	Trim            int
	OGTitle         string
	OGDesc          string
	OGUrl           string
}
