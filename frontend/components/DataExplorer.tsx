"use client";

import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
  type VisibilityState
} from "@tanstack/react-table";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { formatDuration, formatNumber, timeAgo } from "@/lib/format";
import type { DataExplorerData, PullRequest, Repo, Review, User } from "@/types/api";

export type DataTab = "repos" | "prs" | "reviews" | "users";

type ExplorerRow = {
  id: string;
  repo?: string;
  repoHref?: string;
  number?: number;
  title?: string;
  login?: string;
  loginHref?: string;
  secondary?: string;
  secondaryHref?: string;
  state?: string;
  status?: string;
  language?: string;
  dateLabel?: string;
  dateSort?: number;
  stars?: number;
  mergedPRs?: number;
  avgSecs?: number;
  mergeSecs?: number | null;
  reviewCount?: number;
  size?: number;
  followers?: number;
  publicRepos?: number;
  userType?: "org" | "user";
  location?: string;
};

type ChartOption = {
  key: string;
  label: string;
  bucket: (row: ExplorerRow) => string;
  order?: string[];
};

type ChartRow = {
  label: string;
  value: number;
};

const tabs: Array<{ key: DataTab; label: string }> = [
  { key: "repos", label: "Repos" },
  { key: "prs", label: "Pull Requests" },
  { key: "reviews", label: "Reviews" },
  { key: "users", label: "Users" }
];

const durationOrder = ["under 1h", "1h to 1d", "1d to 1w", "1w to 1m", "over 1m", "unknown"];
const sizeOrder = ["under 50", "50 to 250", "250 to 1k", "1k to 5k", "over 5k", "unknown"];
const countOrder = ["0", "1", "2 to 5", "6 to 20", "21 to 100", "over 100", "unknown"];

export default function DataExplorer({ data, tab }: { data: DataExplorerData; tab: DataTab }) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({});
  const rows = useMemo(() => normalizeRows(tab, data), [tab, data]);
  const columns = useMemo(() => columnsFor(tab), [tab]);
  const chartOptions = useMemo(() => chartOptionsFor(tab), [tab]);
  const [chartKey, setChartKey] = useState(chartOptions[0]?.key ?? "");

  useEffect(() => {
    setSorting([]);
    setColumnVisibility({});
    setChartKey(chartOptions[0]?.key ?? "");
  }, [chartOptions, tab]);

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting, columnVisibility },
    onSortingChange: setSorting,
    onColumnVisibilityChange: setColumnVisibility,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel()
  });

  const visibleRows = table.getSortedRowModel().rows.map((row) => row.original);
  const activeChart = chartOptions.find((option) => option.key === chartKey) || chartOptions[0];
  const chartRows = activeChart ? buildChartRows(visibleRows, activeChart) : [];
  const total = totalFor(tab, data);
  const start = rows.length ? data.Offset + 1 : 0;
  const end = data.Offset + rows.length;

  return (
    <div className="data-explorer">
      <nav className="tab-nav" aria-label="Data sections">
        {tabs.map((item) => (
          <Link key={item.key} href={`/data?tab=${item.key}`} className={`tab-btn ${tab === item.key ? "active" : ""}`}>
            {item.label}
          </Link>
        ))}
      </nav>

      <div className="data-query-panel">
        <QueryForm tab={tab} data={data} />
        <div className="data-query-meta">
          <span>{totalLabel(total, data.TotalIsApprox)}</span>
          <span>{rows.length ? `${formatNumber(start)}-${formatNumber(end)} loaded` : "0 loaded"}</span>
        </div>
      </div>

      <div className="data-workspace">
        <section className="data-table-panel">
          <div className="data-table-toolbar">
            <div className="data-table-title">
              <span className="mono">{tabs.find((item) => item.key === tab)?.label}</span>
              <span className="muted">{rows.length} rows</span>
            </div>
            <div className="data-table-actions">
              <details className="data-columns-menu">
                <summary>Columns</summary>
                <div className="data-columns-list">
                  {table.getAllLeafColumns().map((column) => (
                    <label key={column.id}>
                      <input
                        type="checkbox"
                        checked={column.getIsVisible()}
                        disabled={!column.getCanHide()}
                        onChange={column.getToggleVisibilityHandler()}
                      />
                      <span>{columnLabel(column.id)}</span>
                    </label>
                  ))}
                </div>
              </details>
              <button type="button" className="page-btn" onClick={() => exportRows(tab, table.getVisibleLeafColumns(), visibleRows)}>
                Export CSV
              </button>
            </div>
          </div>

          <div className="pr-table-wrap data-table-wrap">
            <table className="pr-table data-table">
              <thead>
                {table.getHeaderGroups().map((headerGroup) => (
                  <tr key={headerGroup.id}>
                    {headerGroup.headers.map((header) => (
                      <th key={header.id}>
                        {header.isPlaceholder ? null : (
                          <button
                            type="button"
                            className="data-th-btn"
                            disabled={!header.column.getCanSort()}
                            onClick={header.column.getToggleSortingHandler()}
                          >
                            <span>{flexRender(header.column.columnDef.header, header.getContext())}</span>
                            <span className="data-sort-mark">{sortMark(header.column.getIsSorted())}</span>
                          </button>
                        )}
                      </th>
                    ))}
                  </tr>
                ))}
              </thead>
              <tbody>
                {table.getRowModel().rows.map((row) => (
                  <tr key={row.id}>
                    {row.getVisibleCells().map((cell) => (
                      <td key={cell.id} className={cellClass(cell.column.id)}>
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </td>
                    ))}
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td colSpan={table.getVisibleLeafColumns().length || 1} className="data-empty">
                      No rows match this query.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="pagination">
            {data.HasPrev ? <Link className="page-btn" href={pageHref(tab, data, data.Page - 1)}>Prev</Link> : null}
            <span className="muted">page {data.Page + 1}</span>
            {data.HasNext ? <Link className="page-btn" href={pageHref(tab, data, data.Page + 1)}>Next</Link> : null}
          </div>
        </section>

        <aside className="data-chart-panel">
          <div className="data-chart-head">
            <div>
              <h2 className="section-title">Chart</h2>
              <p className="chart-hint">Built from the loaded result set.</p>
            </div>
            <select className="data-filter-select" value={chartKey} onChange={(event) => setChartKey(event.target.value)}>
              {chartOptions.map((option) => (
                <option key={option.key} value={option.key}>{option.label}</option>
              ))}
            </select>
          </div>
          <div className="data-chart-bars">
            {chartRows.map((row, index) => (
              <div className="data-chart-row" key={row.label}>
                <span className="data-chart-label" title={row.label}>{row.label}</span>
                <span className="data-chart-track">
                  <span
                    className="data-chart-bar"
                    data-rank={(index % 5) + 1}
                    style={{ width: `${chartWidth(row.value, chartRows)}%` }}
                  />
                </span>
                <span className="data-chart-value">{formatNumber(row.value)}</span>
              </div>
            ))}
            {!chartRows.length ? <p className="data-empty">No chartable rows.</p> : null}
          </div>
        </aside>
      </div>
    </div>
  );
}

function QueryForm({ tab, data }: { tab: DataTab; data: DataExplorerData }) {
  return (
    <form className="data-filter-bar" action="/data">
      <input type="hidden" name="tab" value={tab} />
      {tab === "repos" ? (
        <>
          <input type="search" name="search" defaultValue={data.Search} placeholder="Search repos" className="data-filter-input" />
          <select name="sort" className="data-filter-select" defaultValue={data.SortBy}>
            <option value="">Most PRs</option>
            <option value="stars">Stars</option>
            <option value="speed">Fastest</option>
            <option value="slow">Slowest</option>
          </select>
          <select name="status" className="data-filter-select" defaultValue={data.Status || "done"}>
            <option value="done">done</option>
            <option value="syncing">syncing</option>
            <option value="pending">pending</option>
            <option value="all">all statuses</option>
          </select>
        </>
      ) : null}
      {tab === "prs" ? (
        <>
          <input type="search" name="repo" defaultValue={data.RepoFilter} placeholder="Repo prefix" className="data-filter-input" />
          <input type="search" name="author" defaultValue={data.Author} placeholder="Author prefix" className="data-filter-input" />
          <select name="sort" className="data-filter-select" defaultValue={data.SortBy}>
            <option value="">Recent</option>
            <option value="speed">Fastest</option>
            <option value="slow">Slowest</option>
            <option value="size">Largest</option>
          </select>
        </>
      ) : null}
      {tab === "reviews" ? (
        <>
          <input type="search" name="repo" defaultValue={data.RepoFilter} placeholder="Repo prefix" className="data-filter-input" />
          <input type="search" name="reviewer" defaultValue={data.Reviewer} placeholder="Reviewer prefix" className="data-filter-input" />
          <select name="state" className="data-filter-select" defaultValue={data.State}>
            <option value="">All states</option>
            <option value="APPROVED">APPROVED</option>
            <option value="CHANGES_REQUESTED">CHANGES_REQUESTED</option>
            <option value="COMMENTED">COMMENTED</option>
          </select>
        </>
      ) : null}
      {tab === "users" ? (
        <input type="search" name="search" defaultValue={data.Search} placeholder="Login or name" className="data-filter-input" />
      ) : null}
      <select name="limit" className="data-filter-select" defaultValue={String(data.Limit || 50)}>
        <option value="50">50 rows</option>
        <option value="100">100 rows</option>
        <option value="250">250 rows</option>
      </select>
      <button className="page-btn">Run query</button>
      <Link className="page-btn" href={`/data?tab=${tab}`}>Reset</Link>
    </form>
  );
}

function columnsFor(tab: DataTab): ColumnDef<ExplorerRow>[] {
  switch (tab) {
  case "repos":
    return [
      linkColumn("repo", "Repo", "repoHref", false),
      numberColumn("stars", "Stars"),
      textColumn("language", "Language"),
      numberColumn("mergedPRs", "Merged PRs"),
      durationColumn("avgSecs", "Avg time"),
      statusColumn("status", "Status"),
      dateColumn("Last synced")
    ];
  case "prs":
    return [
      linkColumn("repo", "Repo", "repoHref", false),
      numberColumn("number", "#"),
      textColumn("title", "Title"),
      linkColumn("login", "Author", "loginHref"),
      dateColumn("Merged"),
      durationColumn("mergeSecs", "Time"),
      numberColumn("reviewCount", "Reviews"),
      numberColumn("size", "Size")
    ];
  case "reviews":
    return [
      linkColumn("login", "Reviewer", "loginHref", false),
      linkColumn("repo", "Repo", "repoHref"),
      numberColumn("number", "PR #"),
      stateColumn("state", "State"),
      dateColumn("Submitted")
    ];
  case "users":
    return [
      linkColumn("login", "Login", "loginHref", false),
      textColumn("title", "Name"),
      textColumn("userType", "Type"),
      numberColumn("followers", "Followers"),
      numberColumn("publicRepos", "Repos"),
      textColumn("location", "Location")
    ];
  }
}

function linkColumn(id: keyof ExplorerRow, header: string, hrefKey: keyof ExplorerRow, lock = true): ColumnDef<ExplorerRow> {
  return {
    id: String(id),
    accessorFn: (row) => row[id] || "",
    header,
    enableHiding: lock,
    cell: ({ row, getValue }) => {
      const label = String(getValue() || "-");
      const href = row.original[hrefKey];
      return href ? <Link href={String(href)} className="link">{label}</Link> : <span className="muted">{label}</span>;
    }
  };
}

function textColumn(id: keyof ExplorerRow, header: string): ColumnDef<ExplorerRow> {
  return {
    id: String(id),
    accessorFn: (row) => row[id] || "",
    header,
    cell: ({ getValue }) => {
      const value = String(getValue() || "");
      return value ? value : <span className="muted">-</span>;
    }
  };
}

function numberColumn(id: keyof ExplorerRow, header: string): ColumnDef<ExplorerRow> {
  return {
    id: String(id),
    accessorFn: (row) => Number(row[id] || 0),
    header,
    cell: ({ getValue }) => <span className="data-num">{formatNumber(Number(getValue() || 0))}</span>
  };
}

function durationColumn(id: keyof ExplorerRow, header: string): ColumnDef<ExplorerRow> {
  return {
    id: String(id),
    accessorFn: (row) => Number(row[id] || 0),
    header,
    cell: ({ getValue }) => formatDuration(Number(getValue() || 0))
  };
}

function statusColumn(id: keyof ExplorerRow, header: string): ColumnDef<ExplorerRow> {
  return {
    id: String(id),
    accessorFn: (row) => row[id] || "",
    header,
    cell: ({ getValue }) => {
      const value = String(getValue() || "");
      const className = value === "done" ? "ok" : value === "syncing" ? "warn" : "muted";
      return <span className={className}>{value || "-"}</span>;
    }
  };
}

function stateColumn(id: keyof ExplorerRow, header: string): ColumnDef<ExplorerRow> {
  return {
    id: String(id),
    accessorFn: (row) => row[id] || "",
    header,
    cell: ({ getValue }) => {
      const value = String(getValue() || "");
      const className = value === "APPROVED" ? "ok" : value === "CHANGES_REQUESTED" ? "red" : "muted";
      return <span className={className}>{value || "-"}</span>;
    }
  };
}

function dateColumn(header: string): ColumnDef<ExplorerRow> {
  return {
    id: "date",
    accessorFn: (row) => row.dateSort || 0,
    header,
    cell: ({ row }) => <span className="muted">{row.original.dateLabel || "-"}</span>
  };
}

function normalizeRows(tab: DataTab, data: DataExplorerData): ExplorerRow[] {
  if (tab === "repos") return (data.Repos || []).map(repoRow);
  if (tab === "prs") return (data.PRs || []).map(prRow);
  if (tab === "reviews") return (data.Reviews || []).map(reviewRow);
  return (data.Users || []).map(userRow);
}

function repoRow(repo: Repo): ExplorerRow {
  return {
    id: repo.FullName,
    repo: repo.FullName,
    repoHref: `/repo/${repo.FullName}`,
    stars: repo.Stars,
    language: repo.Language || "unknown",
    mergedPRs: repo.MergedPRCount,
    avgSecs: repo.AvgMergeTimeSecs,
    status: repo.SyncStatus,
    dateLabel: timeAgo(repo.LastSynced),
    dateSort: dateSort(repo.LastSynced)
  };
}

function prRow(pr: PullRequest): ExplorerRow {
  return {
    id: pr.ID,
    repo: pr.RepoFullName,
    repoHref: `/repo/${pr.RepoFullName}`,
    number: pr.Number,
    title: pr.Title,
    login: pr.AuthorLogin,
    loginHref: `/user/${pr.AuthorLogin}`,
    dateLabel: timeAgo(pr.MergedAt),
    dateSort: dateSort(pr.MergedAt),
    mergeSecs: pr.MergeTimeSecs,
    reviewCount: pr.ReviewCount,
    size: pr.Additions + pr.Deletions
  };
}

function reviewRow(review: Review): ExplorerRow {
  return {
    id: `${review.ID}:${review.SubmittedAt}`,
    repo: review.RepoFullName,
    repoHref: `/repo/${review.RepoFullName}`,
    number: review.PRNumber,
    login: review.ReviewerLogin,
    loginHref: `/user/${review.ReviewerLogin}`,
    state: review.State,
    dateLabel: dateLabel(review.SubmittedAt),
    dateSort: dateSort(review.SubmittedAt)
  };
}

function userRow(user: User): ExplorerRow {
  return {
    id: user.Login,
    login: user.Login,
    loginHref: user.IsOrg ? `/org/${user.Login}` : `/user/${user.Login}`,
    title: user.Name,
    userType: user.IsOrg ? "org" : "user",
    followers: user.Followers,
    publicRepos: user.PublicRepos,
    location: user.Location
  };
}

function chartOptionsFor(tab: DataTab): ChartOption[] {
  switch (tab) {
  case "repos":
    return [
      { key: "language", label: "Language", bucket: (row) => clean(row.language) },
      { key: "status", label: "Sync status", bucket: (row) => clean(row.status) },
      { key: "merge-time", label: "Avg merge time", bucket: (row) => durationBucket(row.avgSecs), order: durationOrder },
      { key: "stars", label: "Stars", bucket: (row) => countBucket(row.stars), order: countOrder }
    ];
  case "prs":
    return [
      { key: "repo", label: "Repo", bucket: (row) => clean(row.repo) },
      { key: "author", label: "Author", bucket: (row) => clean(row.login) },
      { key: "merge-time", label: "Merge time", bucket: (row) => durationBucket(row.mergeSecs), order: durationOrder },
      { key: "size", label: "PR size", bucket: (row) => sizeBucket(row.size), order: sizeOrder }
    ];
  case "reviews":
    return [
      { key: "state", label: "State", bucket: (row) => clean(row.state) },
      { key: "reviewer", label: "Reviewer", bucket: (row) => clean(row.login) },
      { key: "repo", label: "Repo", bucket: (row) => clean(row.repo) },
      { key: "month", label: "Submitted month", bucket: (row) => monthBucket(row.dateSort) }
    ];
  case "users":
    return [
      { key: "type", label: "Type", bucket: (row) => clean(row.userType) },
      { key: "followers", label: "Followers", bucket: (row) => countBucket(row.followers), order: countOrder },
      { key: "repos", label: "Public repos", bucket: (row) => countBucket(row.publicRepos), order: countOrder },
      { key: "location", label: "Location", bucket: (row) => clean(row.location) }
    ];
  }
}

function buildChartRows(rows: ExplorerRow[], option: ChartOption): ChartRow[] {
  const counts = new Map<string, number>();
  rows.forEach((row) => {
    const key = option.bucket(row);
    counts.set(key, (counts.get(key) || 0) + 1);
  });
  const chartRows = Array.from(counts.entries()).map(([label, value]) => ({ label, value }));
  if (option.order) {
    const order = new Map(option.order.map((label, index) => [label, index]));
    chartRows.sort((a, b) => (order.get(a.label) ?? 999) - (order.get(b.label) ?? 999));
  } else {
    chartRows.sort((a, b) => b.value - a.value || a.label.localeCompare(b.label));
  }
  return chartRows.slice(0, 12);
}

function pageHref(tab: DataTab, data: DataExplorerData, page: number): string {
  const params = new URLSearchParams({ tab, page: String(page) });
  if (data.Limit) params.set("limit", String(data.Limit));

  if (tab === "repos") {
    setParam(params, "search", data.Search);
    setParam(params, "sort", data.SortBy);
    setParam(params, "status", data.Status);
  } else if (tab === "prs") {
    setParam(params, "repo", data.RepoFilter);
    setParam(params, "author", data.Author);
    setParam(params, "sort", data.SortBy);
  } else if (tab === "reviews") {
    setParam(params, "repo", data.RepoFilter);
    setParam(params, "reviewer", data.Reviewer);
    setParam(params, "state", data.State);
  } else {
    setParam(params, "search", data.Search);
  }

  return `/data?${params.toString()}`;
}

function totalFor(tab: DataTab, data: DataExplorerData): number {
  if (tab === "repos") return data.ReposTotal || 0;
  if (tab === "prs") return data.PRsTotal || 0;
  if (tab === "reviews") return data.ReviewsTotal || 0;
  return data.UsersTotal || 0;
}

function totalLabel(total: number, isApprox?: boolean): string {
  if (isApprox && total > 0) return `${formatNumber(Math.max(total - 1, 0))}+ total`;
  return `${formatNumber(total)} total`;
}

function exportRows(tab: DataTab, columns: Array<{ id: string; columnDef: { header?: unknown } }>, rows: ExplorerRow[]) {
  const headers = columns.map((column) => (typeof column.columnDef.header === "string" ? column.columnDef.header : columnLabel(column.id)));
  const body = rows.map((row) => columns.map((column) => csvCell(exportValue(row, column.id))).join(","));
  const csv = [headers.map(csvCell).join(","), ...body].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const href = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = `ngmi-${tab}.csv`;
  link.click();
  URL.revokeObjectURL(href);
}

function exportValue(row: ExplorerRow, id: string): unknown {
  if (id === "date") return row.dateLabel || "";
  return row[id as keyof ExplorerRow] || "";
}

function csvCell(value: unknown): string {
  const text = String(value ?? "");
  if (!/[",\n]/.test(text)) return text;
  return `"${text.replaceAll("\"", "\"\"")}"`;
}

function setParam(params: URLSearchParams, key: string, value?: string | null) {
  if (value) params.set(key, value);
}

function clean(value?: string | null): string {
  const trimmed = value?.trim();
  return trimmed || "unknown";
}

function durationBucket(secs?: number | null): string {
  if (!secs || secs <= 0) return "unknown";
  if (secs < 3600) return "under 1h";
  if (secs < 86400) return "1h to 1d";
  if (secs < 604800) return "1d to 1w";
  if (secs < 2592000) return "1w to 1m";
  return "over 1m";
}

function sizeBucket(value?: number | null): string {
  if (value === undefined || value === null) return "unknown";
  if (value < 50) return "under 50";
  if (value < 250) return "50 to 250";
  if (value < 1000) return "250 to 1k";
  if (value < 5000) return "1k to 5k";
  return "over 5k";
}

function countBucket(value?: number | null): string {
  if (value === undefined || value === null) return "unknown";
  if (value <= 0) return "0";
  if (value === 1) return "1";
  if (value <= 5) return "2 to 5";
  if (value <= 20) return "6 to 20";
  if (value <= 100) return "21 to 100";
  return "over 100";
}

function monthBucket(value?: number): string {
  if (!value) return "unknown";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "unknown";
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
}

function dateLabel(value?: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toISOString().slice(0, 10);
}

function dateSort(value?: string | null): number {
  if (!value) return 0;
  const time = new Date(value).getTime();
  return Number.isNaN(time) ? 0 : time;
}

function chartWidth(value: number, rows: ChartRow[]): number {
  const max = Math.max(...rows.map((row) => row.value), 1);
  return Math.max(4, Math.round((value / max) * 100));
}

function sortMark(sort: false | "asc" | "desc"): string {
  if (sort === "asc") return "asc";
  if (sort === "desc") return "desc";
  return "";
}

function cellClass(id: string): string | undefined {
  if (id === "title") return "data-pr-title";
  if (["stars", "mergedPRs", "avgSecs", "number", "mergeSecs", "reviewCount", "size", "followers", "publicRepos"].includes(id)) return "data-num-cell";
  return undefined;
}

function columnLabel(id: string): string {
  const labels: Record<string, string> = {
    repo: "Repo",
    stars: "Stars",
    language: "Language",
    mergedPRs: "Merged PRs",
    avgSecs: "Avg time",
    status: "Status",
    date: "Date",
    number: "#",
    title: "Title",
    login: "Login",
    mergeSecs: "Time",
    reviewCount: "Reviews",
    size: "Size",
    state: "State",
    userType: "Type",
    followers: "Followers",
    publicRepos: "Repos",
    location: "Location"
  };
  return labels[id] || id;
}
