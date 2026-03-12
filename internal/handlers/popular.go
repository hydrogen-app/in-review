package handlers

import (
	"net/http"

	"inreview/internal/db"
)

type PopularData struct {
	BaseData
	Popular []db.PageVisit
	Recent  []db.PageVisit
}

func (h *Handler) Popular(w http.ResponseWriter, r *http.Request) {
	popular, _ := h.db.PopularVisits(100)
	var exclude []string
	for _, v := range popular {
		exclude = append(exclude, v.Path)
	}
	recent, _ := h.db.RecentVisits(50, exclude)

	data := PopularData{
		BaseData: h.baseData(r),
		Popular:  popular,
		Recent:   recent,
	}
	h.render(w, "popular", data)
}
