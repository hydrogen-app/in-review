package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"inreview/internal/config"
	"inreview/internal/db"
	"inreview/internal/github"
	"inreview/internal/rdb"
)

// contextKey is a private type for context values to avoid collisions.
type contextKey string

const (
	userLoginKey      contextKey = "userLogin"
	installationIDKey contextKey = "installationID"
)

// BaseData is embedded in page data payloads so the Next app can render
// session-aware navigation without extra queries per handler.
type BaseData struct {
	CurrentUser string
}

// baseData builds a BaseData from the current request context.
func (h *Handler) baseData(r *http.Request) BaseData {
	return BaseData{CurrentUser: currentUser(r)}
}

// currentUser extracts the GitHub login from the request context.
// Returns "" when not authenticated.
func currentUser(r *http.Request) string {
	login, _ := r.Context().Value(userLoginKey).(string)
	return login
}

// installationID extracts the installation ID from the request context.
// Returns 0 when not present.
func installationID(r *http.Request) int64 {
	id, _ := r.Context().Value(installationIDKey).(int64)
	return id
}

// SessionLoader is a global middleware that loads session info from the
// session_id cookie and injects the GitHub login + installation ID into the
// request context.
func (h *Handler) SessionLoader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cookie, err := r.Cookie("session_id"); err == nil {
			if session, err := h.db.GetSession(cookie.Value); err == nil && session != nil {
				ctx := context.WithValue(r.Context(), userLoginKey, session.Login)
				if session.InstallationID != nil {
					ctx = context.WithValue(ctx, installationIDKey, *session.InstallationID)
				}
				r = r.WithContext(ctx)
			}
		}
		next.ServeHTTP(w, r)
	})
}

// RequireAuth wraps a handler and redirects unauthenticated users to /auth/github.
func (h *Handler) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if currentUser(r) == "" {
			http.Redirect(w, r, "/auth/github", http.StatusFound)
			return
		}
		next(w, r)
	}
}

// Queuer is the subset of worker.Worker used by HTTP handlers.
// Both the web-only and combined binaries satisfy this with a *worker.Worker;
// the difference is whether Worker.Start() has been called.
type Queuer interface {
	Queue(fullName string, force bool)
	IsSyncing(fullName string) bool
	QueuePosition(fullName string) int
}

// Handler holds backend dependencies shared by API endpoints.
type Handler struct {
	db     *db.DB
	gh     *github.Client
	worker Queuer
	cache  *rdb.Client
	cfg    *config.Config
}

func New(database *db.DB, gh *github.Client, w Queuer, cache *rdb.Client, cfg *config.Config) *Handler {
	return &Handler{
		db:     database,
		gh:     gh,
		worker: w,
		cache:  cache,
		cfg:    cfg,
	}
}

// ── Formatting helpers ────────────────────────────────────────────────────────

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

func roundTo1(f float64) float64 {
	return float64(int(f*10+0.5)) / 10
}
