package handlers

import (
	"fmt"
	"net/http"
	"strings"
)

type CommentsPageData struct {
	BaseData
	Comments interface{}
	OGTitle  string
	OGDesc   string
	OGUrl    string
}

// CommentsPage renders the comments page with a list of recent comments.
func (h *Handler) CommentsPage(w http.ResponseWriter, r *http.Request) {
	comments, err := h.db.CommentsGetAll(100)
	if err != nil {
		h.renderErrorReq(w, r, http.StatusInternalServerError, "Error", "Could not load comments")
		return
	}
	h.render(w, "comments", CommentsPageData{
		BaseData: h.baseData(r),
		Comments: comments,
		OGTitle:  "comments — ngmi",
		OGDesc:   "Leave a comment on ngmi.",
	})
}

// CommentPost handles comment submission and returns an HTMX fragment for the new comment.
func (h *Handler) CommentPost(w http.ResponseWriter, r *http.Request) {
	login := currentUser(r)
	if login == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	content := strings.TrimSpace(r.FormValue("content"))
	if content == "" {
		http.Error(w, "content required", http.StatusBadRequest)
		return
	}
	if len(content) > 2000 {
		http.Error(w, "comment too long (max 2000 chars)", http.StatusBadRequest)
		return
	}

	c, err := h.db.CommentCreate(login, content)
	if err != nil {
		http.Error(w, "could not save comment", http.StatusInternalServerError)
		return
	}

	// Return a single comment row fragment for HTMX to prepend.
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<div class="comment" id="comment-%d">
  <div class="comment-meta">
    <a href="/user/%s" class="comment-author">%s</a>
    <span class="comment-time muted">%s</span>
  </div>
  <p class="comment-body">%s</p>
</div>`,
		c.ID,
		htmlEscape(c.AuthorLogin),
		htmlEscape(c.AuthorLogin),
		timeAgo(&c.CreatedAt),
		htmlEscapeNL(c.Content),
	)
}

// htmlEscape escapes a string for safe HTML output.
func htmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&#34;")
	return s
}

// htmlEscapeNL escapes and preserves newlines as <br>.
func htmlEscapeNL(s string) string {
	s = htmlEscape(s)
	s = strings.ReplaceAll(s, "\n", "<br>")
	return s
}

