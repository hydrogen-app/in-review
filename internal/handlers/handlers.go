package handlers

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"path/filepath"
	"time"

	"inreview/internal/config"
	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/rdb"
	"inreview/internal/worker"
)

// Handler holds all dependencies and parsed templates.
type Handler struct {
	db      *db.DB
	gh      *github.Client
	worker  *worker.Worker
	cache   *rdb.Client
	cfg     *config.Config
	tmpls   map[string]*template.Template
	funcMap template.FuncMap
}

func New(database *db.DB, gh *github.Client, w *worker.Worker, cache *rdb.Client, cfg *config.Config) *Handler {
	h := &Handler{
		db:     database,
		gh:     gh,
		worker: w,
		cache:  cache,
		cfg:    cfg,
	}
	h.funcMap = template.FuncMap{
		"formatDuration":      formatDuration,
		"formatDurationShort": formatDurationShort,
		"timeAgo":             timeAgo,
		"formatNumber":        formatNumber,
		"rankBadge":           rankBadge,
		"rankClass":           rankClass,
		"percent": func(a, b int) int {
			if b == 0 {
				return 0
			}
			return (a * 100) / b
		},
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
		"deref64": func(p *int64) int64 {
			if p == nil {
				return 0
			}
			return *p
		},
	}
	h.loadTemplates()
	return h
}

func (h *Handler) loadTemplates() {
	h.tmpls = make(map[string]*template.Template)

	pages := []string{"home", "repo", "user", "org", "leaderboard_page", "error", "hi_wall"}
	for _, page := range pages {
		tmpl := template.Must(
			template.New("").Funcs(h.funcMap).ParseFiles(
				filepath.Join("templates", "layout.html"),
				filepath.Join("templates", page+".html"),
			),
		)
		h.tmpls[page] = tmpl
	}

	partials := []string{"search_results", "leaderboard", "leaderboard_rows", "leaderboard_search"}
	for _, partial := range partials {
		tmpl := template.Must(
			template.New(partial).Funcs(h.funcMap).ParseFiles(
				filepath.Join("templates", "partials", partial+".html"),
			),
		)
		h.tmpls[partial] = tmpl
	}
}

// ErrorData is passed to the error template.
type ErrorData struct {
	Code     int
	Title    string
	Message  string
	Detail   string
	RetryURL string
	OGTitle  string
	OGDesc   string
	OGUrl    string
}

// renderError renders the error page with the given HTTP status code.
func (h *Handler) renderError(w http.ResponseWriter, code int, title, message string) {
	tmpl, ok := h.tmpls["error"]
	if !ok {
		http.Error(w, message, code)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	if err := tmpl.ExecuteTemplate(w, "layout", ErrorData{
		Code:    code,
		Title:   title,
		Message: message,
	}); err != nil {
		log.Printf("error template error: %v", err)
	}
}

// render executes the full layout template for a page.
func (h *Handler) render(w http.ResponseWriter, name string, data interface{}) {
	tmpl, ok := h.tmpls[name]
	if !ok {
		http.Error(w, fmt.Sprintf("template %q not found", name), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "layout", data); err != nil {
		log.Printf("template %q error: %v", name, err)
	}
}

// renderPartial executes a named partial template (for HTMX responses).
func (h *Handler) renderPartial(w http.ResponseWriter, name string, data interface{}) {
	tmpl, ok := h.tmpls[name]
	if !ok {
		http.Error(w, fmt.Sprintf("partial %q not found", name), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		log.Printf("partial %q error: %v", name, err)
	}
}

// ── Template helpers ───────────────────────────────────────────────────────────

func formatDuration(secs int64) string {
	if secs <= 0 {
		return "—"
	}
	d := time.Duration(secs) * time.Second
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day"
	}
	if days < 30 {
		return fmt.Sprintf("%d days", days)
	}
	months := days / 30
	if months == 1 {
		return "1 month"
	}
	if months < 12 {
		return fmt.Sprintf("%d months", months)
	}
	years := days / 365
	if years == 1 {
		return "1 year"
	}
	return fmt.Sprintf("%d years", years)
}

func formatDurationShort(secs int64) string {
	if secs <= 0 {
		return "—"
	}
	d := time.Duration(secs) * time.Second
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func timeAgo(t *time.Time) string {
	if t == nil {
		return "never"
	}
	d := time.Since(*t)
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day ago"
	}
	if days < 30 {
		return fmt.Sprintf("%d days ago", days)
	}
	months := days / 30
	if months == 1 {
		return "1 month ago"
	}
	return fmt.Sprintf("%d months ago", months)
}

func formatNumber(n int) string {
	if n < 1_000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	}
	return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
}

func rankBadge(rank int) string {
	switch rank {
	case 1:
		return "#1"
	case 2:
		return "#2"
	case 3:
		return "#3"
	default:
		return fmt.Sprintf("#%d", rank)
	}
}

func roundTo1(f float64) float64 {
	return float64(int(f*10+0.5)) / 10
}

func rankClass(rank int) string {
	switch rank {
	case 1:
		return "rank-gold"
	case 2:
		return "rank-silver"
	case 3:
		return "rank-bronze"
	default:
		return "rank-other"
	}
}
