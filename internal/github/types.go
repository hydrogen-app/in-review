package github

import "time"

type GHRepo struct {
	ID          int    `json:"id"`
	FullName    string `json:"full_name"`
	Name        string `json:"name"`
	Owner       struct {
		Login string `json:"login"`
		Type  string `json:"type"`
	} `json:"owner"`
	Description string `json:"description"`
	Stars       int    `json:"stargazers_count"`
	Language    string `json:"language"`
	Private     bool   `json:"private"`
}

type GHPR struct {
	Number    int        `json:"number"`
	Title     string     `json:"title"`
	User      struct {
		Login string `json:"login"`
	} `json:"user"`
	State     string     `json:"state"`
	CreatedAt time.Time  `json:"created_at"`
	MergedAt  *time.Time `json:"merged_at"`
	ClosedAt  *time.Time `json:"closed_at"`
}

type GHReview struct {
	ID   int `json:"id"`
	User struct {
		Login     string `json:"login"`
		AvatarURL string `json:"avatar_url"`
	} `json:"user"`
	State       string    `json:"state"`
	SubmittedAt time.Time `json:"submitted_at"`
}

type GHUser struct {
	Login       string `json:"login"`
	Name        string `json:"name"`
	AvatarURL   string `json:"avatar_url"`
	Bio         string `json:"bio"`
	PublicRepos int    `json:"public_repos"`
	Followers   int    `json:"followers"`
	Company     string `json:"company"`
	Location    string `json:"location"`
	Type        string `json:"type"` // "User" or "Organization"
}
