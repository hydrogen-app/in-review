package handlers

import (
	"encoding/json"
	"html/template"
	"net/http"

	"inreview/internal/db"
)

// StatsData is passed to the stats page template.
type StatsData struct {
	Overall       db.GlobalOverallStats
	SizeChartJSON template.JS
	OGTitle       string
	OGDesc        string
	OGUrl         string
}

// statsChartPayload is marshaled to JSON and embedded in the stats page.
type statsChartPayload struct {
	Labels               []string  `json:"labels"`
	PRCounts             []int     `json:"prCounts"`
	AvgHours             []float64 `json:"avgHours"`
	MedianHours          []float64 `json:"medianHours"`
	ApprovalRate         []float64 `json:"approvalRate"`
	ChangesRequestedRate []float64 `json:"changesRequestedRate"`
	AvgChangesRequested  []float64 `json:"avgChangesRequested"`
}

func (h *Handler) Stats(w http.ResponseWriter, r *http.Request) {
	overall, _ := h.db.GlobalOverallStats()
	buckets, _ := h.db.GlobalSizeChartData()

	data := StatsData{
		Overall: overall,
		OGTitle: "Global PR Stats — ngmi",
		OGDesc:  "How PR size affects review time and changes requested, across all repos tracked on ngmi.",
		OGUrl:   "https://ngmi.review/stats",
	}

	if len(buckets) > 0 {
		payload := statsChartPayload{}
		for _, b := range buckets {
			payload.Labels = append(payload.Labels, b.Label)
			payload.PRCounts = append(payload.PRCounts, b.PRCount)
			payload.AvgHours = append(payload.AvgHours, roundTo1(b.AvgSecs/3600))
			payload.MedianHours = append(payload.MedianHours, roundTo1(b.MedianSecs/3600))
			payload.ApprovalRate = append(payload.ApprovalRate, roundTo1(b.ApprovalRate))
			payload.ChangesRequestedRate = append(payload.ChangesRequestedRate, roundTo1(b.ChangesRequestedRate))
			payload.AvgChangesRequested = append(payload.AvgChangesRequested, roundTo1(b.AvgChangesRequested))
		}
		if raw, err := json.Marshal(payload); err == nil {
			data.SizeChartJSON = template.JS(raw)
		}
	}

	h.render(w, "stats", data)
}
