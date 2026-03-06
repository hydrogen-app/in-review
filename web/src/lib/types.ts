// Go db structs serialize with PascalCase keys (no json tags).
// Chart payload structs have camelCase json tags.

export interface LeaderboardEntry {
	Rank: number;
	Name: string;
	Value: number;
	Count: number;
	Extra: string;
}

export interface PageVisit {
	Path: string;
	Kind: string;
	Label: string;
	Count: number;
}

export interface Repo {
	FullName: string;
	Owner: string;
	Name: string;
	Description: string;
	Stars: number;
	Language: string;
	OrgName: string;
	LastSynced: string | null;
	SyncStatus: string;
	PRCount: number;
	MergedPRCount: number;
	AvgMergeTimeSecs: number;
	MinMergeTimeSecs: number;
	MaxMergeTimeSecs: number;
}

export interface PullRequest {
	ID: string;
	RepoFullName: string;
	Number: number;
	Title: string;
	AuthorLogin: string;
	Merged: boolean;
	OpenedAt: string;
	MergedAt: string | null;
	MergeTimeSecs: number | null;
	ReviewCount: number;
	ChangesRequestedCount: number;
	Additions: number;
	Deletions: number;
	FirstReviewAt: string | null;
}

export interface Review {
	ID: string;
	RepoFullName: string;
	PRNumber: number;
	ReviewerLogin: string;
	State: string;
	SubmittedAt: string;
}

export interface User {
	Login: string;
	Name: string;
	AvatarURL: string;
	Bio: string;
	PublicRepos: number;
	Followers: number;
	Company: string;
	Location: string;
	IsOrg: boolean;
	LastFetched: string | null;
}

export interface ReviewerStats {
	Login: string;
	AvatarURL: string;
	TotalReviews: number;
	Approvals: number;
	ChangesRequested: number;
	Comments: number;
	LastReviewedAt: string | null;
}

export interface AuthorStats {
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
}

export interface RepoLeaderboardRow {
	Rank: number;
	FullName: string;
	AvgSecs: number;
	MinSecs: number;
	MaxSecs: number;
	PRCount: number;
}

export interface UserLeaderboardRow {
	Rank: number;
	Login: string;
	AvatarURL: string;
	Total: number;
	Approvals: number;
	ChangesRequested: number;
	MergedPRs: number;
	AvgMergeTimeSecs: number;
}

export interface CleanLeaderboardRow {
	Rank: number;
	FullName: string;
	CleanPct: number;
	Total: number;
	AvgSecs: number;
}

export interface CollabEntry {
	Login: string;
	AvatarURL: string;
	Count: number;
}

export interface UserRepoReview {
	FullName: string;
	Count: number;
}

export interface UserRecordPR {
	Number: number;
	RepoFullName: string;
	MergeTimeSecs: number;
	Title: string;
}

export interface GlobalOverallStats {
	TotalPRs: number;
	TotalRepos: number;
	AvgSecs: number;
	MedianSecs: number;
}

// Chart payloads — camelCase from Go json tags
export interface TimeChartData {
	labels: string[];
	prCounts: number[];
	openedCounts: number[];
	mergeVsOpenRate: number[];
	avgSize: number[];
	medianSize: number[];
	avgHours: number[];
	medianHours: number[];
	changesRequestedRate: number[];
	avgFirstReviewHours: number[];
	medFirstReviewHours: number[];
	unreviewedMergeRate: number[];
	linesPerContrib: number[];
}

export interface SizeChartData {
	labels: string[];
	prCounts: number[];
	avgHours: number[];
	approvalRate: number[];
}

export interface StatsChartData {
	labels: string[];
	prCounts: number[];
	avgHours: number[];
	medianHours: number[];
	approvalRate: number[];
	changesRequestedRate: number[];
	avgChangesRequested: number[];
}

export interface ActivityChartData {
	labels: string[];
	prCounts: number[];
	reviewCounts: number[];
	crRate: number[];
}

export interface SizeBucketChartData {
	labels: string[];
	prCounts: number[];
}

// Columnar chart data → row format for layerchart
export type ChartRow = Record<string, string | number>;

export function toRows(labels: string[], series: Record<string, number[]>): ChartRow[] {
	return labels.map((label, i) => {
		const row: ChartRow = { label };
		for (const [key, values] of Object.entries(series)) {
			row[key] = values[i] ?? 0;
		}
		return row;
	});
}

// Search result
export interface SearchResult {
	Type: string;
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
}
