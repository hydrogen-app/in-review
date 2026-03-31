package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"inreview/internal/db"
)

// PopularData is passed to the popular pages template.
type PopularData struct {
	BaseData
	Popular []db.PageVisit
	Recent  []db.PageVisit
}

const popularCacheKey = "popular:v1"
const popularCacheTTL = 5 * time.Minute

type popularCached struct {
	Popular []db.PageVisit `json:"popular"`
	Recent  []db.PageVisit `json:"recent"`
}

func (h *Handler) Popular(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var c popularCached
	if raw, ok := h.cache.Get(ctx, popularCacheKey); ok {
		if err := json.Unmarshal(raw, &c); err == nil {
			h.render(w, "popular", PopularData{
				BaseData: h.baseData(r),
				Popular:  c.Popular,
				Recent:   c.Recent,
			})
			return
		}
	}

	popular, _ := h.db.PopularVisits(50)
	exclude := make([]string, len(popular))
	for i, v := range popular {
		exclude[i] = v.Path
	}
	recent, _ := h.db.RecentVisits(20, exclude)

	if raw, err := json.Marshal(popularCached{Popular: popular, Recent: recent}); err == nil {
		h.cache.Set(ctx, popularCacheKey, raw, popularCacheTTL)
	}

	h.render(w, "popular", PopularData{
		BaseData: h.baseData(r),
		Popular:  popular,
		Recent:   recent,
	})
}
