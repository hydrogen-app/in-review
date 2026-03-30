package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"inreview/internal/db"
)

// RecentlyViewedData is passed to the recently-viewed page template.
type RecentlyViewedData struct {
	BaseData
	Visits  []db.PageVisit
	OGTitle string
	OGDesc  string
	OGUrl   string
}

// RecentlyViewed renders the full recently-viewed page.
func (h *Handler) RecentlyViewed(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}

	visits, err := h.db.AllRecentVisits(limit)
	if err != nil {
		h.renderErrorReq(w, r, http.StatusInternalServerError, "DB Error", "Could not load recently viewed pages.")
		return
	}

	data := RecentlyViewedData{
		Visits:  visits,
		OGTitle: "Recently Viewed — ngmi",
		OGDesc:  "Recently viewed repos, users, and orgs on ngmi.",
		OGUrl:   "https://ngmi.review/recently-viewed",
	}
	data.BaseData = h.baseData(r)
	h.render(w, "recently_viewed", data)
}

// RecentlyViewedAPI returns the recently-viewed pages as JSON.
func (h *Handler) RecentlyViewedAPI(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}

	visits, err := h.db.AllRecentVisits(limit)
	if err != nil {
		http.Error(w, `{"error":"db error"}`, http.StatusInternalServerError)
		return
	}

	type visitJSON struct {
		Path        string `json:"path"`
		Kind        string `json:"kind"`
		Label       string `json:"label"`
		Count       int    `json:"count"`
		LastVisited string `json:"last_visited"`
	}

	out := make([]visitJSON, len(visits))
	for i, v := range visits {
		out[i] = visitJSON{
			Path:        v.Path,
			Kind:        v.Kind,
			Label:       v.Label,
			Count:       v.Count,
			LastVisited: v.LastVisited.UTC().Format("2006-01-02T15:04:05Z"),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
