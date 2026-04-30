package handlers

import (
	"log"
	"net/http"
	"strings"

	"inreview/internal/db"
)

// DashboardData is returned by the dashboard JSON endpoint.
type DashboardData struct {
	BaseData
	Login          string
	AvatarURL      string
	TrackedRepos   []db.Repo
	AvailableRepos []string // full_names from GitHub installation, not yet tracked
	HasInstall     bool
	InstallURL     string
	OGTitle        string
	OGDesc         string
	OGUrl          string
}

// AddRepo triggers a sync for a repo the user has access to via their installation.
func (h *Handler) AddRepo(w http.ResponseWriter, r *http.Request) {
	login := currentUser(r)
	fullName := r.FormValue("repo")
	if fullName == "" || !strings.Contains(fullName, "/") {
		http.Error(w, "invalid repo", http.StatusBadRequest)
		return
	}
	parts := strings.SplitN(fullName, "/", 2)
	owner, name := parts[0], parts[1]

	_ = h.db.UpsertRepo(db.Repo{
		FullName:   fullName,
		Owner:      owner,
		Name:       name,
		OrgName:    owner,
		SyncStatus: "pending",
	})
	if err := h.db.TrackRepoForUser(login, fullName); err != nil {
		log.Printf("dashboard: track repo for user %s: %v", login, err)
	}
	h.worker.Queue(fullName, true)
	log.Printf("dashboard: %s queued %s for sync", login, fullName)

	http.Redirect(w, r, "/dashboard", http.StatusFound)
}
