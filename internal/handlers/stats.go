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
	TimeChartJSON template.JS
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

// timeChartPayload holds monthly time-series data for line charts.
type timeChartPayload struct {
	Labels               []string  `json:"labels"`
	PRCounts             []int     `json:"prCounts"`
	AvgSize              []float64 `json:"avgSize"`
	MedianSize           []float64 `json:"medianSize"`
	AvgHours             []float64 `json:"avgHours"`
	MedianHours          []float64 `json:"medianHours"`
	ChangesRequestedRate []float64 `json:"changesRequestedRate"`
}

func (h *Handler) Stats(w http.ResponseWriter, r *http.Request) {
	overall, _ := h.db.GlobalOverallStats()
	buckets, _ := h.db.GlobalSizeChartData()
	points, _ := h.db.GlobalTimeSeriesData()

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

	if len(points) > 0 {
		tp := timeChartPayload{}
		for _, p := range points {
			tp.Labels = append(tp.Labels, p.Label)
			tp.PRCounts = append(tp.PRCounts, p.PRCount)
			tp.AvgSize = append(tp.AvgSize, roundTo1(p.AvgSize))
			tp.MedianSize = append(tp.MedianSize, roundTo1(p.MedianSize))
			tp.AvgHours = append(tp.AvgHours, roundTo1(p.AvgSecs/3600))
			tp.MedianHours = append(tp.MedianHours, roundTo1(p.MedianSecs/3600))
			tp.ChangesRequestedRate = append(tp.ChangesRequestedRate, roundTo1(p.ChangesRequestedRate))
		}
		if raw, err := json.Marshal(tp); err == nil {
			data.TimeChartJSON = template.JS(raw)
		}
	}

	h.render(w, "stats", data)
}
