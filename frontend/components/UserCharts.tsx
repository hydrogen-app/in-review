"use client";

import { useEffect, useState } from "react";

import { ChartBlock } from "@/components/Charts";
import { jsonPayload } from "@/lib/format";
import type { UserChartsData } from "@/types/api";

type ActivityPayload = {
  labels?: string[];
  prCounts?: number[];
  reviewCounts?: number[];
  crRate?: number[];
};

type SizePayload = {
  labels?: string[];
  prCounts?: number[];
};

export function UserCharts({ username }: { username: string }) {
  const [data, setData] = useState<UserChartsData | null>(null);

  useEffect(() => {
    fetch(`/api/next/user/${username}/charts`, { credentials: "include" })
      .then((res) => (res.ok ? res.json() : null))
      .then((body: UserChartsData | null) => setData(body))
      .catch(() => setData(null));
  }, [username]);

  if (!data) return null;

  const activity = jsonPayload<ActivityPayload>(data.ActivityJSON || data.activityJSON);
  const sizes = jsonPayload<SizePayload>(data.SizeBucketJSON || data.sizeBucketJSON);
  if (!activity?.labels?.length && !sizes?.labels?.length) return null;

  return (
    <section className="section">
      <h2 className="section-title">Activity</h2>
      <div className="charts-grid charts-grid-2">
        {activity?.labels?.length ? (
          <>
            <ChartBlock
              id="user-activity"
              label="PRs and reviews over time"
              labels={activity.labels}
              series={[
                { label: "PRs", data: activity.prCounts || [], color: "#58a6ff", fill: true },
                { label: "Reviews", data: activity.reviewCounts || [], color: "#3fb950", fill: true }
              ]}
            />
            <ChartBlock id="user-crrate" label="Changes requested rate (%)" labels={activity.labels} ySuffix="%" series={[{ label: "Changes requested", data: activity.crRate || [], color: "#f85149", fill: true }]} />
          </>
        ) : null}
        {sizes?.labels?.length ? (
          <ChartBlock id="user-size" type="bar" label="Authored PR size distribution" labels={sizes.labels} series={[{ label: "PRs", data: sizes.prCounts || [], color: "#d29922", fill: true }]} />
        ) : null}
      </div>
    </section>
  );
}
