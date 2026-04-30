import type {
  LeaderboardEntry,
  OrgData,
  RelationGraphData,
  RelationGraphEdge,
  RelationGraphNode,
  Repo,
  RepoData,
  UserData
} from "@/types/api";

type PreviewBuilder = {
  data: RelationGraphData;
  nodes: Map<string, number>;
  edges: Set<string>;
};

function newPreview(centerID: string): PreviewBuilder {
  return {
    data: { CenterID: centerID, Nodes: [], Edges: [], Truncated: false },
    nodes: new Map(),
    edges: new Set()
  };
}

function userNodeID(login: string): string {
  return `user:${login.toLowerCase()}`;
}

function orgNodeID(login: string): string {
  return `org:${login.toLowerCase()}`;
}

function repoNodeID(fullName: string): string {
  return `repo:${fullName.toLowerCase()}`;
}

function repoOwner(fullName: string): string {
  return fullName.split("/")[0] || "";
}

function addNode(builder: PreviewBuilder, node: RelationGraphNode): void {
  if (!node.ID || !node.Label) return;
  const existing = builder.nodes.get(node.ID);
  if (existing !== undefined) {
    builder.data.Nodes[existing].Weight += Math.max(1, node.Weight || 1);
    return;
  }
  builder.nodes.set(node.ID, builder.data.Nodes.length);
  builder.data.Nodes.push({ ...node, Weight: Math.max(1, node.Weight || 1) });
}

function addEdge(builder: PreviewBuilder, edge: RelationGraphEdge): void {
  if (!edge.Source || !edge.Target || edge.Source === edge.Target) return;
  if (!builder.nodes.has(edge.Source) || !builder.nodes.has(edge.Target)) return;
  const key = `${edge.Source}|${edge.Target}|${edge.Type}`;
  if (builder.edges.has(key)) return;
  builder.edges.add(key);
  builder.data.Edges.push({ ...edge, Weight: Math.max(1, edge.Weight || 1) });
}

function addRepoOwner(builder: PreviewBuilder, repo: Pick<Repo, "FullName" | "Owner" | "OrgName" | "MergedPRCount">): void {
  const ownerID = repo.OrgName && repo.OrgName.toLowerCase() === repo.Owner.toLowerCase() ? orgNodeID(repo.Owner) : userNodeID(repo.Owner);
  addNode(builder, {
    ID: ownerID,
    Label: repo.Owner,
    Type: ownerID.startsWith("org:") ? "org" : "user",
    Href: ownerID.startsWith("org:") ? `/org/${repo.Owner}` : `/user/${repo.Owner}`,
    Weight: repo.MergedPRCount || 1
  });
  addEdge(builder, { Source: ownerID, Target: repoNodeID(repo.FullName), Type: "owns", Weight: 1 });
}

function addRepoByName(builder: PreviewBuilder, fullName: string, weight: number, edgeFrom: string, edgeType: RelationGraphEdge["Type"]): void {
  const owner = repoOwner(fullName);
  addNode(builder, {
    ID: repoNodeID(fullName),
    Label: fullName,
    Type: "repo",
    Href: `/repo/${fullName}`,
    Weight: weight
  });
  if (owner) {
    addNode(builder, {
      ID: userNodeID(owner),
      Label: owner,
      Type: "user",
      Href: `/user/${owner}`,
      Weight: 1
    });
    addEdge(builder, { Source: userNodeID(owner), Target: repoNodeID(fullName), Type: "owns", Weight: 1 });
  }
  addEdge(builder, { Source: edgeFrom, Target: repoNodeID(fullName), Type: edgeType, Weight: weight });
}

export function repoGraphPreview(data: RepoData): RelationGraphData {
  const repo = data.Repo;
  const centerID = repoNodeID(repo.FullName);
  const builder = newPreview(centerID);
  addNode(builder, { ID: centerID, Label: repo.FullName, Type: "repo", Href: `/repo/${repo.FullName}`, Weight: repo.MergedPRCount || 1 });
  addRepoOwner(builder, repo);

  data.TopReviewers?.slice(0, 40).forEach((reviewer) => {
    const id = userNodeID(reviewer.Login);
    addNode(builder, { ID: id, Label: reviewer.Login, Type: "user", Href: `/user/${reviewer.Login}`, Weight: reviewer.TotalReviews });
    addEdge(builder, { Source: id, Target: centerID, Type: "reviewed", Weight: reviewer.TotalReviews });
  });

  data.RecentPRs?.slice(0, 40).forEach((pr) => {
    const id = userNodeID(pr.AuthorLogin);
    addNode(builder, { ID: id, Label: pr.AuthorLogin, Type: "user", Href: `/user/${pr.AuthorLogin}`, Weight: 1 });
    addEdge(builder, { Source: id, Target: centerID, Type: "authored", Weight: 1 });
  });

  return builder.data;
}

export function userGraphPreview(data: UserData): RelationGraphData {
  const user = data.User;
  const centerID = userNodeID(user.Login);
  const builder = newPreview(centerID);
  addNode(builder, {
    ID: centerID,
    Label: user.Login,
    Type: "user",
    Href: `/user/${user.Login}`,
    Weight: (data.ReviewerStats?.TotalReviews || 0) + (data.AuthorStats?.MergedPRs || 0) || 1
  });

  data.ContributedRepos?.slice(0, 36).forEach((repo) => {
    addNode(builder, { ID: repoNodeID(repo.FullName), Label: repo.FullName, Type: "repo", Href: `/repo/${repo.FullName}`, Weight: repo.MergedPRCount || 1 });
    addRepoOwner(builder, repo);
    addEdge(builder, { Source: centerID, Target: repoNodeID(repo.FullName), Type: "authored", Weight: repo.MergedPRCount || 1 });
  });

  data.ReviewedRepos?.slice(0, 36).forEach((repo) => {
    addRepoByName(builder, repo.FullName, repo.Count, centerID, "reviewed");
  });

  data.ReviewersOfMe?.slice(0, 24).forEach((collab) => {
    const id = userNodeID(collab.Login);
    addNode(builder, { ID: id, Label: collab.Login, Type: "user", Href: `/user/${collab.Login}`, Weight: collab.Count });
    addEdge(builder, { Source: id, Target: centerID, Type: "reviewed-pr", Weight: collab.Count });
  });

  data.AuthorsIReview?.slice(0, 24).forEach((collab) => {
    const id = userNodeID(collab.Login);
    addNode(builder, { ID: id, Label: collab.Login, Type: "user", Href: `/user/${collab.Login}`, Weight: collab.Count });
    addEdge(builder, { Source: centerID, Target: id, Type: "reviewed-pr", Weight: collab.Count });
  });

  return builder.data;
}

export function orgGraphPreview(data: OrgData): RelationGraphData {
  const org = data.Org;
  const centerID = orgNodeID(org.Login);
  const builder = newPreview(centerID);
  addNode(builder, {
    ID: centerID,
    Label: org.Login,
    Type: "org",
    Href: `/org/${org.Login}`,
    Weight: data.TotalMergedPRs || data.Repos?.length || 1
  });

  data.Repos?.slice(0, 50).forEach((repo) => {
    const id = repoNodeID(repo.FullName);
    addNode(builder, { ID: id, Label: repo.FullName, Type: "repo", Href: `/repo/${repo.FullName}`, Weight: repo.MergedPRCount || 1 });
    addEdge(builder, { Source: centerID, Target: id, Type: "owns", Weight: repo.MergedPRCount || 1 });
  });

  addOrgLeaderboardUsers(builder, centerID, data.ReviewerBoard, "reviewed");
  addOrgLeaderboardUsers(builder, centerID, data.GatekeeperBoard, "reviewed-pr");

  return builder.data;
}

function addOrgLeaderboardUsers(
  builder: PreviewBuilder,
  centerID: string,
  entries: LeaderboardEntry[] | undefined,
  edgeType: RelationGraphEdge["Type"]
): void {
  entries?.slice(0, 30).forEach((entry) => {
    const id = userNodeID(entry.Name);
    addNode(builder, { ID: id, Label: entry.Name, Type: "user", Href: `/user/${entry.Name}`, Weight: entry.Count || entry.Value || 1 });
    addEdge(builder, { Source: id, Target: centerID, Type: edgeType, Weight: entry.Count || entry.Value || 1 });
  });
}
