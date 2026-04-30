package handlers

type SearchResult struct {
	Type         string // "repo", "user", "org"
	Name         string // display name
	FullName     string // owner/repo or login
	Description  string
	Stars        int
	AvatarURL    string
	Language     string
	MergedPRs    int
	AvgMergeTime int64
	SpeedRank    int
	IsCached     bool
}

type SearchData struct {
	Query   string
	Results []SearchResult
}
