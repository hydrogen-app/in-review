package handlers

import (
	"fmt"
	"net/http"
	"strconv"

	"inreview/internal/db"
)

const (
	dataDefaultLimit = 50
	dataMaxLimit     = 250
)

// DataExplorerData is returned by the data explorer JSON endpoint.
type DataExplorerData struct {
	BaseData
	ActiveTab     string
	Repos         []db.Repo
	ReposTotal    int
	PRs           []db.PullRequest
	PRsTotal      int
	Reviews       []db.Review
	ReviewsTotal  int
	Users         []db.User
	UsersTotal    int
	TotalIsApprox bool
	Page          int
	Offset        int
	Limit         int
	HasPrev       bool
	HasNext       bool
	PrevURL       string
	NextURL       string
	Search        string
	SortBy        string
	Status        string
	Author        string
	Reviewer      string
	State         string
	RepoFilter    string
	OGTitle       string
	OGDesc        string
	OGUrl         string
}

func parseDataQuery(r *http.Request) (page, offset, limit int, search, sortBy, status, author, reviewer, state, repo string) {
	page, _ = strconv.Atoi(r.URL.Query().Get("page"))
	if page < 0 {
		page = 0
	}
	limit, _ = strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = dataDefaultLimit
	}
	if limit > dataMaxLimit {
		limit = dataMaxLimit
	}
	offset = page * limit
	search = r.URL.Query().Get("search")
	sortBy = r.URL.Query().Get("sort")
	status = r.URL.Query().Get("status")
	author = r.URL.Query().Get("author")
	reviewer = r.URL.Query().Get("reviewer")
	state = r.URL.Query().Get("state")
	repo = r.URL.Query().Get("repo")
	return
}

func setPagination(d *DataExplorerData, baseURL string, total, page, offset, limit int, extra string) {
	d.HasPrev = page > 0
	d.HasNext = offset+limit < total
	if d.HasPrev {
		d.PrevURL = fmt.Sprintf("%s?page=%d%s", baseURL, page-1, extra)
	}
	if d.HasNext {
		d.NextURL = fmt.Sprintf("%s?page=%d%s", baseURL, page+1, extra)
	}
}
