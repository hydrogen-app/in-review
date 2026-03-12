package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"inreview/internal/db"
)

// PopularPagesData is the template data for the /popular page.
type PopularPagesData struct {
	BaseData
	Pages   []db.PageVisit
	OGTitle string
	OGDesc  string
	OGUrl   string
}

// PopularPages renders the most-visited pages as a full HTML page.
func (h *Handler) PopularPages(w http.ResponseWriter, r *http.Request) {
	pages, err := h.db.PopularVisits(100)
	if err != nil {
		h.renderErrorReq(w, r, http.StatusInternalServerError, "Error", "Could not load popular pages")
		return
	}
	h.render(w, "popular", PopularPagesData{
		BaseData: h.baseData(r),
		Pages:    pages,
		OGTitle:  "Popular Pages — ngmi",
		OGDesc:   "The most visited pages on ngmi.review",
		OGUrl:    "https://ngmi.review/popular",
	})
}

// popularPageItem is the JSON shape returned by the API.
type popularPageItem struct {
	Path  string `json:"path"`
	Kind  string `json:"kind"`
	Label string `json:"label"`
	Count int    `json:"count"`
}

// PopularPagesAPI returns a JSON array of the most-visited pages.
// Optional query param: limit (default 50, max 500).
func (h *Handler) PopularPagesAPI(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 500 {
		limit = n
	}

	pages, err := h.db.PopularVisits(limit)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal server error"}`))
		return
	}

	items := make([]popularPageItem, len(pages))
	for i, p := range pages {
		items[i] = popularPageItem{
			Path:  p.Path,
			Kind:  p.Kind,
			Label: p.Label,
			Count: p.Count,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(items)
}
