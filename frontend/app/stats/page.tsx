import { TimeCharts, ChartBlock, type TimePayload } from "@/components/Charts";
import { apiGet, qs } from "@/lib/api";
import { formatDuration, formatNumber, jsonPayload } from "@/lib/format";
import type { StatsData } from "@/types/api";

type Props = {
  searchParams: Promise<{ trim?: string; min_stars?: string; min_contribs?: string }>;
};

type SizePayload = {
  labels?: string[];
  prCounts?: number[];
  avgHours?: number[];
  medianHours?: number[];
  approvalRate?: number[];
  changesRequestedRate?: number[];
  avgChangesRequested?: number[];
};

export default async function StatsPage({ searchParams }: Props) {
  const sp = await searchParams;
  const trim = Number(sp.trim || 0);
  const minStars = Number(sp.min_stars || 0);
  const minContribs = Number(sp.min_contribs || 0);
  const data = await apiGet<StatsData>(`/api/next/stats${qs({ trim, min_stars: minStars, min_contribs: minContribs })}`);
  const size = jsonPayload<SizePayload>(data.SizeChartJSON);
  const time = jsonPayload<TimePayload>(data.TimeChartJSON);

  return (
    <div className="page-wrap">
      <div className="breadcrumb">
        <a href="/" className="bc-link">
          ngmi
        </a>
        <span className="bc-sep">/</span>
        <span className="bc-current">stats</span>
      </div>

      <div className="repo-header">
        <div className="repo-title-row">
          <h1 className="page-title mono">Global PR Stats</h1>
        </div>
        <p className="repo-desc">Aggregate review data across all tracked repos.</p>
      </div>

      <div className="stats-grid">
        <Stat label="Merged PRs" value={formatNumber(data.Overall.TotalPRs)} />
        <Stat label="Repos Tracked" value={formatNumber(data.Overall.TotalRepos)} />
        <Stat label="Avg Review Time" value={formatDuration(data.Overall.AvgSecs)} />
        <Stat label="Median Review Time" value={formatDuration(data.Overall.MedianSecs)} className="highlight" />
      </div>

      <form className="chart-controls" action="/stats">
        <div className="trim-ctrl">
          <span>Trim top</span>
          <output>{trim}</output>
          <span>% outliers</span>
          <input type="range" name="trim" min="0" max="20" defaultValue={trim} />
        </div>
        <label className="filter-group">
          <span>Stars</span>
          <select name="min_stars" className="data-filter-select" defaultValue={minStars}>
            <option value="0">Any</option>
            <option value="100">100+</option>
            <option value="1000">1k+</option>
            <option value="10000">10k+</option>
          </select>
        </label>
        <label className="filter-group">
          <span>Contributors</span>
          <select name="min_contribs" className="data-filter-select" defaultValue={minContribs}>
            <option value="0">Any</option>
            <option value="5">5+</option>
            <option value="20">20+</option>
            <option value="100">100+</option>
          </select>
        </label>
        <button className="range-btn active" type="submit">
          Apply
        </button>
      </form>

      <TimeCharts payload={time} includeOpened />

      {size?.labels?.length ? (
        <section className="section">
          <h2 className="section-title">PR Size Buckets</h2>
          <div className="charts-grid charts-grid-2">
            <ChartBlock id="size-time" label="Review time by PR size (hrs)" labels={size.labels} ySuffix="h" series={[{ label: "Avg", data: size.avgHours || [], color: "#d29922" }, { label: "Median", data: size.medianHours || [], color: "#f0883e" }]} />
            <ChartBlock id="size-approval" label="Clean approval rate by PR size (%)" labels={size.labels} ySuffix="%" series={[{ label: "Clean approval", data: size.approvalRate || [], color: "#3fb950", fill: true }]} />
            <ChartBlock id="size-changes" label="Changes requested rate by PR size (%)" labels={size.labels} ySuffix="%" series={[{ label: "Changes requested", data: size.changesRequestedRate || [], color: "#f85149", fill: true }]} />
            <ChartBlock id="size-counts" label="PR count by size" labels={size.labels} type="bar" series={[{ label: "PRs", data: size.prCounts || [], color: "#58a6ff", fill: true }]} />
          </div>
        </section>
      ) : null}
    </div>
  );
}

function Stat({ label, value, className = "" }: { label: string; value: string; className?: string }) {
  return (
    <div className={`stat-card ${className}`}>
      <span className="stat-card-num">{value}</span>
      <span className="stat-card-label">{label}</span>
    </div>
  );
}
