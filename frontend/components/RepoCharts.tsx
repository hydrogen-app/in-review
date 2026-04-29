"use client";

import { useEffect, useState } from "react";

import { ChartBlock, TimeCharts, type TimePayload } from "@/components/Charts";
import { jsonPayload } from "@/lib/format";
import type { RepoChartsData } from "@/types/api";

type SizePayload = {
  labels?: string[];
  prCounts?: number[];
  avgHours?: number[];
  approvalRate?: number[];
};

export function RepoCharts({ owner, name, trim }: { owner: string; name: string; trim: number }) {
  const [data, setData] = useState<RepoChartsData | null>(null);

  useEffect(() => {
    fetch(`/api/next/repo/${owner}/${name}/charts?trim=${trim}`, { credentials: "include" })
      .then((res) => (res.ok ? res.json() : null))
      .then((body: RepoChartsData | null) => setData(body))
      .catch(() => setData(null));
  }, [owner, name, trim]);

  if (!data) return <div className="empty-state">Loading charts...</div>;

  const size = jsonPayload<SizePayload>(data.sizeChartJSON || data.SizeChartJSON);
  const time = jsonPayload<TimePayload>(data.timeChartJSON || data.TimeChartJSON);

  return (
    <>
      {size?.labels?.length ? (
        <section className="section">
          <h2 className="section-title">PR Size</h2>
          <div className="charts-grid charts-grid-2">
            <ChartBlock id="repo-size-time" label="Avg merge time by PR size (hrs)" labels={size.labels} ySuffix="h" series={[{ label: "Avg hours", data: size.avgHours || [], color: "#d29922", fill: true }]} />
            <ChartBlock id="repo-size-approval" label="Clean approval rate by PR size (%)" labels={size.labels} ySuffix="%" series={[{ label: "Approval rate", data: size.approvalRate || [], color: "#3fb950", fill: true }]} />
          </div>
        </section>
      ) : null}
      <TimeCharts payload={time} />
    </>
  );
}
