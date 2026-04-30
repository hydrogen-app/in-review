package handlers

import (
	"net/http"
	"strings"
)

const hiCookieName = "hiwall"

var hiReactions = []struct {
	Key   string
	Emoji string
}{
	{"wave", "👋"},
	{"thumbs", "👍"},
	{"heart", "❤️"},
	{"fire", "🔥"},
	{"gmi", "gmi"},
	{"ngmi", "ngmi"},
}

// alreadySaidHi checks whether the user has said hi on this path and returns
// the reaction they used. Entries in the old format (no "|") are treated as "wave".
func alreadySaidHi(r *http.Request, path string) (bool, string) {
	c, err := r.Cookie(hiCookieName)
	if err != nil {
		return false, ""
	}
	for _, entry := range strings.Split(c.Value, ",") {
		if strings.Contains(entry, "|") {
			parts := strings.SplitN(entry, "|", 2)
			if parts[0] == path {
				return true, parts[1]
			}
		} else if entry == path {
			return true, "wave"
		}
	}
	return false, ""
}

func markSaidHi(w http.ResponseWriter, r *http.Request, path, reaction string) {
	existing := ""
	if c, err := r.Cookie(hiCookieName); err == nil {
		existing = c.Value
	}
	entry := path + "|" + reaction
	val := entry
	if existing != "" {
		val = existing + "," + entry
	}
	http.SetCookie(w, &http.Cookie{
		Name:     hiCookieName,
		Value:    val,
		Path:     "/",
		MaxAge:   365 * 24 * 3600,
		SameSite: http.SameSiteLaxMode,
	})
}

type HiWallData struct {
	BaseData
	Pages   interface{}
	OGTitle string
	OGDesc  string
	OGUrl   string
}
