export type BaseData = {
  CurrentUser?: string;
};

export type Repo = {
  FullName: string;
  Owner: string;
  Name: string;
  Description: string;
  Stars: number;
  Language: string;
  OrgName: string;
  LastSynced?: string | null;
  SyncStatus: string;
  PRCount: number;
  MergedPRCount: number;
  AvgMergeTimeSecs: number;
  MinMergeTimeSecs: number;
  MaxMergeTimeSecs: number;
};

export type PullRequest = {
  ID: string;
  RepoFullName: string;
  Number: number;
  Title: string;
  AuthorLogin: string;
  Merged: boolean;
  OpenedAt: string;
  MergedAt?: string | null;
  MergeTimeSecs?: number | null;
  ReviewCount: number;
  ChangesRequestedCount: number;
  Additions: number;
  Deletions: number;
  FirstReviewAt?: string | null;
};

export type Review = {
  ID: string;
  RepoFullName: string;
  PRNumber: number;
  ReviewerLogin: string;
  State: string;
  SubmittedAt: string;
};

export type User = {
  Login: string;
  Name: string;
  AvatarURL: string;
  Bio: string;
  PublicRepos: number;
  Followers: number;
  Company: string;
  Location: string;
  IsOrg: boolean;
  LastFetched?: string | null;
};

export type LeaderboardEntry = {
  Rank: number;
  Name: string;
  Value: number;
  Count: number;
  Extra: string;
};

export type ReviewerStats = {
  Login: string;
  AvatarURL: string;
  TotalReviews: number;
  Approvals: number;
  ChangesRequested: number;
  Comments: number;
  LastReviewedAt?: string | null;
};

export type AuthorStats = {
  Login: string;
  TotalPRs: number;
  MergedPRs: number;
  AvgMergeTimeSecs: number;
  TotalLinesWritten: number;
  AvgPRSize: number;
  CleanApprovalRate: number;
  AvgChangesRequested: number;
  AvgFirstReviewSecs: number;
  MedFirstReviewSecs: number;
};

export type PageVisit = {
  Path: string;
  Kind: string;
  Label: string;
  Count: number;
};

export type HomeData = BaseData & {
  TotalRepos: number;
  TotalPRs: number;
  TotalReviews: number;
  SpeedDemons: LeaderboardEntry[];
  PRGraveyard: LeaderboardEntry[];
  ReviewChamps: LeaderboardEntry[];
  Gatekeepers: LeaderboardEntry[];
  MergeMasters: LeaderboardEntry[];
  OneShot: LeaderboardEntry[];
  PopularVisits: PageVisit[];
  RecentVisits: PageVisit[];
};

export type SearchResult = {
  Type: "repo" | "user" | "org";
  Name: string;
  FullName: string;
  Description: string;
  Stars: number;
  AvatarURL: string;
  Language: string;
  MergedPRs: number;
  AvgMergeTime: number;
  SpeedRank: number;
  IsCached: boolean;
};

export type SearchData = {
  Query: string;
  Results: SearchResult[];
};

export type GlobalOverallStats = {
  TotalPRs: number;
  TotalRepos: number;
  AvgSecs: number;
  MedianSecs: number;
};

export type StatsData = BaseData & {
  Overall: GlobalOverallStats;
  SizeChartJSON: string;
  TimeChartJSON: string;
  Trim: number;
  MinStars: number;
  MinContribs: number;
};

export type RepoData = BaseData & {
  Repo: Repo;
  TopReviewers: ReviewerStats[];
  RecentPRs: PullRequest[];
  SpeedRank: number;
  IsSyncing: boolean;
  OwnerUser?: User | null;
  Trim: number;
  ShareURL: string;
};

export type RepoChartsData = {
  sizeChartJSON?: string;
  timeChartJSON?: string;
  SizeChartJSON?: string;
  TimeChartJSON?: string;
};

export type RelationGraphNode = {
  ID: string;
  Label: string;
  Type: "user" | "org" | "repo";
  Href: string;
  Weight: number;
};

export type RelationGraphEdge = {
  Source: string;
  Target: string;
  Type: "authored" | "reviewed" | "reviewed-pr" | "owns";
  Weight: number;
};

export type RelationGraphData = {
  CenterID: string;
  Nodes: RelationGraphNode[];
  Edges: RelationGraphEdge[];
  Truncated: boolean;
};

export type UserRecordPR = {
  Number: number;
  RepoFullName: string;
  MergeTimeSecs: number;
  Title: string;
};

export type CollabEntry = {
  Login: string;
  AvatarURL: string;
  Count: number;
};

export type UserRepoReview = {
  FullName: string;
  Count: number;
};

export type UserData = BaseData & {
  redirect?: string;
  User: User;
  ReviewerStats?: ReviewerStats | null;
  AuthorStats?: AuthorStats | null;
  ReviewerRank: number;
  GatekeeperRank: number;
  AuthorRank: number;
  ContributedRepos: Repo[];
  FastestPR?: UserRecordPR | null;
  SlowestPR?: UserRecordPR | null;
  ReviewedRepos: UserRepoReview[];
  ReviewersOfMe: CollabEntry[];
  AuthorsIReview: CollabEntry[];
  IsOrg: boolean;
  IsNGMI: boolean;
  ShareURL: string;
};

export type UserChartsData = {
  ActivityJSON: string;
  SizeBucketJSON: string;
  activityJSON?: string;
  sizeBucketJSON?: string;
};

export type OrgData = BaseData & {
  redirect?: string;
  Org: User;
  Repos: Repo[];
  ReviewerBoard: LeaderboardEntry[];
  GatekeeperBoard: LeaderboardEntry[];
  TotalMergedPRs: number;
  TotalReviews: number;
  IsSyncing: boolean;
  TimeChartJSON: string;
  Trim: number;
};

export type RepoLeaderboardRow = {
  Rank: number;
  FullName: string;
  AvgSecs: number;
  MinSecs: number;
  MaxSecs: number;
  PRCount: number;
};

export type UserLeaderboardRow = {
  Rank: number;
  Login: string;
  AvatarURL: string;
  Total: number;
  Approvals: number;
  ChangesRequested: number;
  MergedPRs: number;
  AvgMergeTimeSecs: number;
};

export type CleanLeaderboardRow = {
  Rank: number;
  FullName: string;
  CleanPct: number;
  Total: number;
  AvgSecs: number;
};

export type LeaderboardPageData = BaseData & {
  Category: string;
  Title: string;
  Description: string;
  RepoRows: RepoLeaderboardRow[];
  UserRows: UserLeaderboardRow[];
  CleanRows: CleanLeaderboardRow[];
  HasMore: boolean;
  NextOffset: number;
};

export type LeaderboardSearchData = {
  Category: string;
  Query: string;
  Empty: boolean;
  NotTracked: boolean;
  TrackURL: string;
  Login: string;
  AvatarURL: string;
  Rank: number;
  TotalReviews: number;
  Approvals: number;
  ChangesRequested: number;
  MergedPRs: number;
  AvgMergeTimeSecs: number;
  FullName: string;
  AvgSecs: number;
  MinSecs: number;
  MaxSecs: number;
  PRCount: number;
  SpeedRank: number;
  GraveyardRank: number;
};

export type DataExplorerData = BaseData & {
  ActiveTab: string;
  Repos: Repo[];
  ReposTotal: number;
  PRs: PullRequest[];
  PRsTotal: number;
  Reviews: Review[];
  ReviewsTotal: number;
  Users: User[];
  UsersTotal: number;
  TotalIsApprox?: boolean;
  Page: number;
  Offset: number;
  Limit: number;
  HasPrev: boolean;
  HasNext: boolean;
  PrevURL?: string;
  NextURL?: string;
  Search: string;
  SortBy: string;
  Status: string;
  Author: string;
  Reviewer: string;
  State: string;
  RepoFilter: string;
};

export type BlogData = BaseData & {
  LiveStats: GlobalOverallStats;
  TopReviewers: LeaderboardEntry[];
  TopSpeed: LeaderboardEntry[];
  TotalRepos: number;
  TotalPRs: number;
  TotalReviews: number;
};

export type DashboardData = BaseData & {
  Login: string;
  AvatarURL: string;
  TrackedRepos: Repo[];
  AvailableRepos: string[];
  HasInstall: boolean;
  InstallURL: string;
};

export type HiReaction = {
  key: string;
  emoji: string;
};

export type HiData = {
  reactions: HiReaction[];
  reactionCounts: Record<string, number>;
  total: number;
  todayCount: number;
  didHi: boolean;
  myReaction: string;
};

export type HiWallPage = {
  Path: string;
  Label: string;
  Kind: string;
  TotalCount: number;
  TodayCount: number;
};

export type HiWallData = BaseData & {
  Pages: HiWallPage[];
};
