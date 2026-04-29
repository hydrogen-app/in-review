"use client";

import { useEffect, useMemo, useRef } from "react";

type ChartCtor = new (ctx: HTMLCanvasElement, config: Record<string, unknown>) => { destroy(): void };

declare global {
  interface Window {
    Chart?: ChartCtor;
  }
}

export type Series = {
  label: string;
  data: number[];
  color: string;
  fill?: boolean;
};

type ChartBlockProps = {
  id: string;
  label: string;
  labels: string[];
  series: Series[];
  ySuffix?: string;
  type?: "line" | "bar";
};

export function ChartBlock({ id, label, labels, series, ySuffix = "", type = "line" }: ChartBlockProps) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const configKey = useMemo(() => JSON.stringify({ labels, series, ySuffix, type }), [labels, series, ySuffix, type]);

  useEffect(() => {
    const canvas = canvasRef.current;
    const Chart = window.Chart;
    if (!canvas || !Chart || labels.length === 0) return;

    const chart = new Chart(canvas, {
      type,
      data: {
        labels,
        datasets: series.map((item) => ({
          label: item.label,
          data: item.data,
          borderColor: item.color,
          backgroundColor: `${item.color}22`,
          borderWidth: 2,
          fill: item.fill ?? false,
          tension: 0.3
        }))
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            display: series.length > 1,
            labels: { color: "#848d97", font: { size: 11 }, boxWidth: 12 }
          }
        },
        scales: {
          x: {
            ticks: { color: "#848d97", font: { size: 11 }, maxRotation: 45 },
            grid: { color: "#30363d" }
          },
          y: {
            beginAtZero: true,
            ticks: {
              color: "#848d97",
              font: { size: 11 },
              callback: (value: string | number) => `${value}${ySuffix}`
            },
            grid: { color: "#30363d" }
          }
        }
      }
    });

    return () => chart.destroy();
  }, [configKey, labels, series, type, ySuffix]);

  if (!labels.length) return null;

  return (
    <div className="chart-card">
      <div className="chart-label">{label}</div>
      <div className="chart-wrap">
        <canvas id={id} ref={canvasRef} />
      </div>
    </div>
  );
}

export type TimePayload = {
  labels?: string[];
  prCounts?: number[];
  openedCounts?: number[];
  mergeVsOpenRate?: number[];
  avgSize?: number[];
  medianSize?: number[];
  avgHours?: number[];
  medianHours?: number[];
  changesRequestedRate?: number[];
  avgFirstReviewHours?: number[];
  medFirstReviewHours?: number[];
  unreviewedMergeRate?: number[];
  linesPerContrib?: number[];
};

export function TimeCharts({ payload, includeOpened = false }: { payload: TimePayload | null; includeOpened?: boolean }) {
  if (!payload?.labels?.length) return null;
  const labels = payload.labels;

  return (
    <section className="section">
      <h2 className="section-title">Trends Over Time</h2>
      <p className="chart-hint">Monthly aggregates across tracked repos.</p>
      <div className="charts-grid charts-grid-2">
        <ChartBlock
          id="tc-size"
          label="PR size over time (lines changed)"
          labels={labels}
          series={[
            { label: "Avg", data: payload.avgSize || [], color: "#d29922" },
            { label: "Median", data: payload.medianSize || [], color: "#f0883e" }
          ]}
        />
        <ChartBlock
          id="tc-time"
          label="Review time over time (hrs)"
          labels={labels}
          ySuffix="h"
          series={[
            { label: "Avg", data: payload.avgHours || [], color: "#d29922" },
            { label: "Median", data: payload.medianHours || [], color: "#f0883e" }
          ]}
        />
        <ChartBlock
          id="tc-crrate"
          label="Changes requested rate (%)"
          labels={labels}
          ySuffix="%"
          series={[{ label: "Changes requested", data: payload.changesRequestedRate || [], color: "#f85149", fill: true }]}
        />
        <ChartBlock
          id="tc-count"
          label="Merged PRs per month"
          labels={labels}
          series={[{ label: "Merged PRs", data: payload.prCounts || [], color: "#58a6ff", fill: true }]}
        />
        {includeOpened ? (
          <>
            <ChartBlock
              id="tc-opened"
              label="PRs opened per month"
              labels={labels}
              series={[{ label: "Opened PRs", data: payload.openedCounts || [], color: "#3fb950", fill: true }]}
            />
            <ChartBlock
              id="tc-merge-rate"
              label="Merge rate - merged / opened (%)"
              labels={labels}
              ySuffix="%"
              series={[{ label: "Merge rate", data: payload.mergeVsOpenRate || [], color: "#e3b341", fill: true }]}
            />
          </>
        ) : null}
        <ChartBlock
          id="tc-first-review"
          label="Time to first review (hrs)"
          labels={labels}
          ySuffix="h"
          series={[
            { label: "Avg", data: payload.avgFirstReviewHours || [], color: "#bc8cff" },
            { label: "Median", data: payload.medFirstReviewHours || [], color: "#a371f7" }
          ]}
        />
        <ChartBlock
          id="tc-unreviewed"
          label="Unreviewed merge rate (%)"
          labels={labels}
          ySuffix="%"
          series={[{ label: "Unreviewed", data: payload.unreviewedMergeRate || [], color: "#ffa657", fill: true }]}
        />
        <ChartBlock
          id="tc-loc"
          label="Lines of code per contributor"
          labels={labels}
          series={[{ label: "Lines", data: payload.linesPerContrib || [], color: "#39c5cf", fill: true }]}
        />
      </div>
    </section>
  );
}
