package db

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	defaultGraphLimit = 360
	minGraphLimit     = 80
	maxGraphLimit     = 1200
)

type RelationGraphNode struct {
	ID     string
	Label  string
	Type   string
	Href   string
	Weight int
}

type RelationGraphEdge struct {
	Source string
	Target string
	Type   string
	Weight int
}

type RelationGraph struct {
	CenterID  string
	Nodes     []RelationGraphNode
	Edges     []RelationGraphEdge
	Truncated bool
}

type relationGraphBuilder struct {
	graph     RelationGraph
	nodeIndex map[string]int
	edgeIndex map[string]int
	maxNodes  int
	maxEdges  int
}

func newRelationGraphBuilder(centerID string, limit int) *relationGraphBuilder {
	limit = clampGraphLimit(limit)
	return &relationGraphBuilder{
		graph:     RelationGraph{CenterID: centerID},
		nodeIndex: make(map[string]int),
		edgeIndex: make(map[string]int),
		maxNodes:  limit,
		maxEdges:  limit * 5,
	}
}

func clampGraphLimit(limit int) int {
	if limit <= 0 {
		return defaultGraphLimit
	}
	if limit < minGraphLimit {
		return minGraphLimit
	}
	if limit > maxGraphLimit {
		return maxGraphLimit
	}
	return limit
}

func (b *relationGraphBuilder) addNode(id, label, nodeType, href string, weight int) bool {
	if id == "" || label == "" {
		return false
	}
	if idx, ok := b.nodeIndex[id]; ok {
		if weight > 0 {
			b.graph.Nodes[idx].Weight += weight
		}
		if b.graph.Nodes[idx].Href == "" && href != "" {
			b.graph.Nodes[idx].Href = href
		}
		return true
	}
	if len(b.graph.Nodes) >= b.maxNodes {
		b.graph.Truncated = true
		return false
	}
	if weight < 1 {
		weight = 1
	}
	b.nodeIndex[id] = len(b.graph.Nodes)
	b.graph.Nodes = append(b.graph.Nodes, RelationGraphNode{
		ID:     id,
		Label:  label,
		Type:   nodeType,
		Href:   href,
		Weight: weight,
	})
	return true
}

func (b *relationGraphBuilder) addEdge(source, target, edgeType string, weight int) bool {
	if source == "" || target == "" || source == target {
		return false
	}
	if _, ok := b.nodeIndex[source]; !ok {
		return false
	}
	if _, ok := b.nodeIndex[target]; !ok {
		return false
	}
	key := source + "|" + target + "|" + edgeType
	if idx, ok := b.edgeIndex[key]; ok {
		if weight > 0 {
			b.graph.Edges[idx].Weight += weight
		}
		return true
	}
	if len(b.graph.Edges) >= b.maxEdges {
		b.graph.Truncated = true
		return false
	}
	if weight < 1 {
		weight = 1
	}
	b.edgeIndex[key] = len(b.graph.Edges)
	b.graph.Edges = append(b.graph.Edges, RelationGraphEdge{
		Source: source,
		Target: target,
		Type:   edgeType,
		Weight: weight,
	})
	return true
}

func (b *relationGraphBuilder) build() RelationGraph {
	return b.graph
}

func userNodeID(login string) string { return "user:" + strings.ToLower(login) }
func orgNodeID(login string) string  { return "org:" + strings.ToLower(login) }
func repoNodeID(fullName string) string {
	return "repo:" + strings.ToLower(fullName)
}

func userHref(login string) string { return "/user/" + login }
func orgHref(login string) string  { return "/org/" + login }
func repoHref(fullName string) string {
	return "/repo/" + fullName
}

func ownerNode(owner string, knownOrg bool) (id, nodeType, href string) {
	if knownOrg {
		return orgNodeID(owner), "org", orgHref(owner)
	}
	return userNodeID(owner), "user", userHref(owner)
}

// RepoRelationGraph returns a compact graph centered on one repository.
func (d *DB) RepoRelationGraph(fullName string, limit int) (RelationGraph, error) {
	repo, err := d.GetRepo(fullName)
	if err != nil {
		return RelationGraph{}, err
	}

	centerID := repoNodeID(repo.FullName)
	b := newRelationGraphBuilder(centerID, limit)
	b.addNode(centerID, repo.FullName, "repo", repoHref(repo.FullName), repo.MergedPRCount)

	ownerIsOrg := repo.OrgName != "" && strings.EqualFold(repo.OrgName, repo.Owner)
	if owner, err := d.GetUser(repo.Owner); err == nil && owner.IsOrg {
		ownerIsOrg = true
	}
	ownerID, ownerType, ownerURL := ownerNode(repo.Owner, ownerIsOrg)
	if b.addNode(ownerID, repo.Owner, ownerType, ownerURL, repo.MergedPRCount) {
		b.addEdge(ownerID, centerID, "owns", 1)
	}

	perRoleLimit := clampGraphLimit(limit) / 2
	if perRoleLimit < 40 {
		perRoleLimit = 40
	}
	if err := d.addRepoAuthorEdges(b, repo.FullName, perRoleLimit); err != nil {
		return RelationGraph{}, err
	}
	if err := d.addRepoReviewerEdges(b, repo.FullName, perRoleLimit); err != nil {
		return RelationGraph{}, err
	}
	if err := d.addRepoReviewPairEdges(b, repo.FullName, clampGraphLimit(limit)); err != nil {
		return RelationGraph{}, err
	}

	return b.build(), nil
}

func (d *DB) addRepoAuthorEdges(b *relationGraphBuilder, fullName string, limit int) error {
	rows, err := d.conn.Query(`
		SELECT author_login, COUNT(*) AS cnt
		FROM pull_requests
		WHERE repo_full_name=$1 AND merged=TRUE
		GROUP BY author_login
		ORDER BY cnt DESC
		LIMIT $2
	`, fullName, limit)
	if err != nil {
		return fmt.Errorf("repo graph authors: %w", err)
	}
	defer rows.Close()

	repoID := repoNodeID(fullName)
	for rows.Next() {
		var login string
		var count int
		if err := rows.Scan(&login, &count); err != nil {
			continue
		}
		userID := userNodeID(login)
		if b.addNode(userID, login, "user", userHref(login), count) {
			b.addEdge(userID, repoID, "authored", count)
		}
	}
	return rows.Err()
}

func (d *DB) addRepoReviewerEdges(b *relationGraphBuilder, fullName string, limit int) error {
	rows, err := d.conn.Query(`
		SELECT reviewer_login, COUNT(*) AS cnt
		FROM reviews
		WHERE repo_full_name=$1
		GROUP BY reviewer_login
		ORDER BY cnt DESC
		LIMIT $2
	`, fullName, limit)
	if err != nil {
		return fmt.Errorf("repo graph reviewers: %w", err)
	}
	defer rows.Close()

	repoID := repoNodeID(fullName)
	for rows.Next() {
		var login string
		var count int
		if err := rows.Scan(&login, &count); err != nil {
			continue
		}
		userID := userNodeID(login)
		if b.addNode(userID, login, "user", userHref(login), count) {
			b.addEdge(userID, repoID, "reviewed", count)
		}
	}
	return rows.Err()
}

func (d *DB) addRepoReviewPairEdges(b *relationGraphBuilder, fullName string, limit int) error {
	rows, err := d.conn.Query(`
		SELECT rv.reviewer_login, pr.author_login, COUNT(*) AS cnt
		FROM reviews rv
		JOIN pull_requests pr
		  ON rv.repo_full_name = pr.repo_full_name AND rv.pr_number = pr.number
		WHERE rv.repo_full_name=$1 AND rv.reviewer_login != pr.author_login
		GROUP BY rv.reviewer_login, pr.author_login
		ORDER BY cnt DESC
		LIMIT $2
	`, fullName, limit)
	if err != nil {
		return fmt.Errorf("repo graph review pairs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var reviewer, author string
		var count int
		if err := rows.Scan(&reviewer, &author, &count); err != nil {
			continue
		}
		reviewerID := userNodeID(reviewer)
		authorID := userNodeID(author)
		b.addNode(reviewerID, reviewer, "user", userHref(reviewer), count)
		b.addNode(authorID, author, "user", userHref(author), count)
		b.addEdge(reviewerID, authorID, "reviewed-pr", count)
	}
	return rows.Err()
}

// UserRelationGraph returns repos and people connected to a user through PRs and reviews.
func (d *DB) UserRelationGraph(login string, limit int) (RelationGraph, error) {
	user, err := d.GetUser(login)
	if err != nil && err != sql.ErrNoRows {
		return RelationGraph{}, err
	}

	centerID := userNodeID(login)
	b := newRelationGraphBuilder(centerID, limit)
	weight := 1
	label := login
	if user != nil {
		label = user.Login
		weight = user.PublicRepos + user.Followers
	}
	b.addNode(centerID, label, "user", userHref(login), weight)

	limit = clampGraphLimit(limit)
	repoLimit := limit / 2
	collabLimit := limit / 3
	if repoLimit < 60 {
		repoLimit = 60
	}
	if collabLimit < 50 {
		collabLimit = 50
	}

	if err := d.addUserRepoEdges(b, login, "authored", repoLimit); err != nil {
		return RelationGraph{}, err
	}
	if err := d.addUserRepoEdges(b, login, "reviewed", repoLimit); err != nil {
		return RelationGraph{}, err
	}
	if err := d.addUserCollaboratorEdges(b, login, collabLimit); err != nil {
		return RelationGraph{}, err
	}

	return b.build(), nil
}

func (d *DB) addUserRepoEdges(b *relationGraphBuilder, login, edgeType string, limit int) error {
	var query string
	switch edgeType {
	case "authored":
		query = `
			SELECT r.full_name, r.owner, r.org_name, COUNT(*) AS cnt
			FROM pull_requests pr
			JOIN repos r ON r.full_name=pr.repo_full_name
			WHERE pr.author_login=$1 AND pr.merged=TRUE
			GROUP BY r.full_name, r.owner, r.org_name
			ORDER BY cnt DESC
			LIMIT $2
		`
	case "reviewed":
		query = `
			SELECT r.full_name, r.owner, r.org_name, COUNT(*) AS cnt
			FROM reviews rv
			JOIN repos r ON r.full_name=rv.repo_full_name
			WHERE rv.reviewer_login=$1
			GROUP BY r.full_name, r.owner, r.org_name
			ORDER BY cnt DESC
			LIMIT $2
		`
	default:
		return nil
	}

	rows, err := d.conn.Query(query, login, limit)
	if err != nil {
		return fmt.Errorf("user graph %s repos: %w", edgeType, err)
	}
	defer rows.Close()

	centerID := userNodeID(login)
	for rows.Next() {
		var fullName, owner, orgName string
		var count int
		if err := rows.Scan(&fullName, &owner, &orgName, &count); err != nil {
			continue
		}
		repoID := repoNodeID(fullName)
		ownerID, ownerType, ownerURL := ownerNode(owner, orgName != "" && strings.EqualFold(orgName, owner))
		if b.addNode(ownerID, owner, ownerType, ownerURL, count) {
			if b.addNode(repoID, fullName, "repo", repoHref(fullName), count) {
				b.addEdge(ownerID, repoID, "owns", 1)
			}
		} else {
			b.addNode(repoID, fullName, "repo", repoHref(fullName), count)
		}
		b.addEdge(centerID, repoID, edgeType, count)
	}
	return rows.Err()
}

func (d *DB) addUserCollaboratorEdges(b *relationGraphBuilder, login string, limit int) error {
	reviewers, authors, err := d.UserTopCollaborators(login, limit)
	if err != nil {
		return fmt.Errorf("user graph collaborators: %w", err)
	}

	centerID := userNodeID(login)
	for _, collaborator := range reviewers {
		userID := userNodeID(collaborator.Login)
		if b.addNode(userID, collaborator.Login, "user", userHref(collaborator.Login), collaborator.Count) {
			b.addEdge(userID, centerID, "reviewed-pr", collaborator.Count)
		}
	}
	for _, collaborator := range authors {
		userID := userNodeID(collaborator.Login)
		if b.addNode(userID, collaborator.Login, "user", userHref(collaborator.Login), collaborator.Count) {
			b.addEdge(centerID, userID, "reviewed-pr", collaborator.Count)
		}
	}
	return nil
}

// OrgRelationGraph returns repos, authors, and reviewers connected inside an org.
func (d *DB) OrgRelationGraph(orgName string, limit int) (RelationGraph, error) {
	org, err := d.GetUser(orgName)
	if err != nil && err != sql.ErrNoRows {
		return RelationGraph{}, err
	}

	centerID := orgNodeID(orgName)
	b := newRelationGraphBuilder(centerID, limit)
	label := orgName
	weight := 1
	if org != nil {
		label = org.Login
		weight = org.PublicRepos + org.Followers
	}
	b.addNode(centerID, label, "org", orgHref(orgName), weight)

	limit = clampGraphLimit(limit)
	repoLimit := limit / 2
	edgeLimit := limit * 2
	if repoLimit < 80 {
		repoLimit = 80
	}

	if err := d.addOrgRepos(b, orgName, repoLimit); err != nil {
		return RelationGraph{}, err
	}
	if err := d.addOrgRepoUserEdges(b, orgName, "reviewed", edgeLimit); err != nil {
		return RelationGraph{}, err
	}
	if err := d.addOrgRepoUserEdges(b, orgName, "authored", edgeLimit); err != nil {
		return RelationGraph{}, err
	}

	return b.build(), nil
}

func (d *DB) addOrgRepos(b *relationGraphBuilder, orgName string, limit int) error {
	rows, err := d.conn.Query(`
		SELECT full_name, merged_pr_count
		FROM repos
		WHERE org_name=$1
		ORDER BY merged_pr_count DESC
		LIMIT $2
	`, orgName, limit)
	if err != nil {
		return fmt.Errorf("org graph repos: %w", err)
	}
	defer rows.Close()

	orgID := orgNodeID(orgName)
	for rows.Next() {
		var fullName string
		var count int
		if err := rows.Scan(&fullName, &count); err != nil {
			continue
		}
		repoID := repoNodeID(fullName)
		if b.addNode(repoID, fullName, "repo", repoHref(fullName), count) {
			b.addEdge(orgID, repoID, "owns", 1)
		}
	}
	return rows.Err()
}

func (d *DB) addOrgRepoUserEdges(b *relationGraphBuilder, orgName, edgeType string, limit int) error {
	var query string
	switch edgeType {
	case "reviewed":
		query = `
			SELECT rv.reviewer_login, rv.repo_full_name, COUNT(*) AS cnt
			FROM reviews rv
			JOIN repos repo ON repo.full_name=rv.repo_full_name
			WHERE repo.org_name=$1
			GROUP BY rv.reviewer_login, rv.repo_full_name
			ORDER BY cnt DESC
			LIMIT $2
		`
	case "authored":
		query = `
			SELECT pr.author_login, pr.repo_full_name, COUNT(*) AS cnt
			FROM pull_requests pr
			JOIN repos repo ON repo.full_name=pr.repo_full_name
			WHERE repo.org_name=$1 AND pr.merged=TRUE
			GROUP BY pr.author_login, pr.repo_full_name
			ORDER BY cnt DESC
			LIMIT $2
		`
	default:
		return nil
	}

	rows, err := d.conn.Query(query, orgName, limit)
	if err != nil {
		return fmt.Errorf("org graph %s edges: %w", edgeType, err)
	}
	defer rows.Close()

	for rows.Next() {
		var login, fullName string
		var count int
		if err := rows.Scan(&login, &fullName, &count); err != nil {
			continue
		}
		userID := userNodeID(login)
		repoID := repoNodeID(fullName)
		if b.addNode(userID, login, "user", userHref(login), count) {
			b.addNode(repoID, fullName, "repo", repoHref(fullName), count)
			b.addEdge(userID, repoID, edgeType, count)
		}
	}
	return rows.Err()
}
