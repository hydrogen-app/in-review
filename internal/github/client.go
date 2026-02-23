package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrRateLimited = errors.New("rate limited")
)

type Client struct {
	token      string
	httpClient *http.Client
}

func NewClient(token string) *Client {
	return &Client{
		token: token,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) get(ctx context.Context, path string, v interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.github.com"+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "inreview.dev/1.0")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusForbidden, http.StatusTooManyRequests:
		return ErrRateLimited
	case http.StatusUnauthorized:
		return errors.New("GitHub token invalid or expired")
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("GitHub API error: %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(v)
}

// GetRepo fetches repository metadata.
func (c *Client) GetRepo(ctx context.Context, owner, name string) (*GHRepo, error) {
	var r GHRepo
	if err := c.get(ctx, fmt.Sprintf("/repos/%s/%s", owner, name), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// GetMergedPRs fetches up to maxPRs merged pull requests (newest first).
func (c *Client) GetMergedPRs(ctx context.Context, owner, name string, maxPRs int) ([]GHPR, error) {
	var all []GHPR
	page := 1
	for len(all) < maxPRs {
		var prs []GHPR
		path := fmt.Sprintf("/repos/%s/%s/pulls?state=closed&per_page=100&page=%d&sort=updated&direction=desc", owner, name, page)
		if err := c.get(ctx, path, &prs); err != nil {
			return all, err
		}
		if len(prs) == 0 {
			break
		}
		for _, pr := range prs {
			if pr.MergedAt != nil {
				all = append(all, pr)
			}
		}
		if len(prs) < 100 {
			break
		}
		page++
	}
	if len(all) > maxPRs {
		all = all[:maxPRs]
	}
	return all, nil
}

// GetPRReviews fetches all reviews for a pull request.
func (c *Client) GetPRReviews(ctx context.Context, owner, name string, prNum int) ([]GHReview, error) {
	var reviews []GHReview
	if err := c.get(ctx, fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews", owner, name, prNum), &reviews); err != nil {
		return nil, err
	}
	return reviews, nil
}

// GetUser fetches a user or org by login.
func (c *Client) GetUser(ctx context.Context, login string) (*GHUser, error) {
	var u GHUser
	if err := c.get(ctx, fmt.Sprintf("/users/%s", login), &u); err != nil {
		return nil, err
	}
	return &u, nil
}

// GetOrgRepos fetches top public repos for an org sorted by stars.
func (c *Client) GetOrgRepos(ctx context.Context, org string, limit int) ([]GHRepo, error) {
	var repos []GHRepo
	path := fmt.Sprintf("/orgs/%s/repos?sort=stars&per_page=%d&type=public", org, limit)
	if err := c.get(ctx, path, &repos); err != nil {
		return nil, err
	}
	return repos, nil
}

// GetUserRepos fetches top public repos for a user sorted by stars.
func (c *Client) GetUserRepos(ctx context.Context, username string, limit int) ([]GHRepo, error) {
	var repos []GHRepo
	path := fmt.Sprintf("/users/%s/repos?sort=stars&per_page=%d&type=public", username, limit)
	if err := c.get(ctx, path, &repos); err != nil {
		return nil, err
	}
	return repos, nil
}

// SearchRepos searches GitHub for repositories.
func (c *Client) SearchRepos(ctx context.Context, query string, limit int) ([]GHRepo, error) {
	var result struct {
		Items []GHRepo `json:"items"`
	}
	path := fmt.Sprintf("/search/repositories?q=%s+fork:false&sort=stars&order=desc&per_page=%d",
		url.QueryEscape(query), limit)
	if err := c.get(ctx, path, &result); err != nil {
		return nil, err
	}
	return result.Items, nil
}

// SearchUsers searches GitHub for users and orgs.
func (c *Client) SearchUsers(ctx context.Context, query string, limit int) ([]GHUser, error) {
	var result struct {
		Items []GHUser `json:"items"`
	}
	path := fmt.Sprintf("/search/users?q=%s&per_page=%d", url.QueryEscape(query), limit)
	if err := c.get(ctx, path, &result); err != nil {
		return nil, err
	}
	return result.Items, nil
}
