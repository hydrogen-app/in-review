"use client";

import { Graph } from "@cosmos.gl/graph";
import type { GraphConfigInterface } from "@cosmos.gl/graph";
import { useEffect, useMemo, useRef, useState } from "react";

import type { RelationGraphData, RelationGraphEdge, RelationGraphNode } from "@/types/api";

type RelationGraphProps = {
  src: string;
  title?: string;
  limit?: number;
  initialData?: RelationGraphData;
};

type GraphArrays = {
  positions: Float32Array;
  links: Float32Array;
  pointColors: Float32Array;
  pointSizes: Float32Array;
  linkColors: Float32Array;
  linkWidths: Float32Array;
};

const GOLDEN_ANGLE = Math.PI * (3 - Math.sqrt(5));

const nodeColors: Record<RelationGraphNode["Type"], [number, number, number, number]> = {
  user: rgba(88, 166, 255, 1),
  repo: rgba(63, 185, 80, 1),
  org: rgba(245, 158, 11, 1)
};

const linkColors: Record<RelationGraphEdge["Type"], [number, number, number, number]> = {
  authored: rgba(63, 185, 80, 0.62),
  reviewed: rgba(88, 166, 255, 0.5),
  "reviewed-pr": rgba(248, 81, 73, 0.46),
  owns: rgba(245, 158, 11, 0.58)
};

type IdleWindow = Window & {
  requestIdleCallback?: (callback: () => void, options?: { timeout: number }) => number;
  cancelIdleCallback?: (id: number) => void;
};

export function RelationGraph({ src, title = "Relation Graph", limit = 260, initialData }: RelationGraphProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const graphRef = useRef<Graph | null>(null);
  const [data, setData] = useState<RelationGraphData | null>(initialData || null);
  const [loading, setLoading] = useState(!initialData);
  const [error, setError] = useState("");
  const [hovered, setHovered] = useState<RelationGraphNode | null>(null);
  const [selected, setSelected] = useState<RelationGraphNode | null>(null);
  const [shouldFetch, setShouldFetch] = useState(false);

  useEffect(() => {
    setData(initialData || null);
    setLoading(!initialData);
    setError("");
    setSelected(null);
    setHovered(null);
    setShouldFetch(false);
  }, [initialData, limit, src]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let cancelled = false;
    let timer: number | undefined;
    let idleID: number | undefined;
    const idleWindow = window as IdleWindow;

    const scheduleFetch = () => {
      if (cancelled) return;
      if (idleWindow.requestIdleCallback) {
        idleID = idleWindow.requestIdleCallback(() => !cancelled && setShouldFetch(true), { timeout: 1800 });
      } else {
        timer = window.setTimeout(() => !cancelled && setShouldFetch(true), 700);
      }
    };

    if (!("IntersectionObserver" in window)) {
      scheduleFetch();
      return () => {
        cancelled = true;
        if (timer) window.clearTimeout(timer);
        if (idleID !== undefined) idleWindow.cancelIdleCallback?.(idleID);
      };
    }

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          observer.disconnect();
          scheduleFetch();
        }
      },
      { rootMargin: "360px" }
    );
    observer.observe(container);

    return () => {
      cancelled = true;
      observer.disconnect();
      if (timer) window.clearTimeout(timer);
      if (idleID !== undefined) idleWindow.cancelIdleCallback?.(idleID);
    };
  }, [limit, src]);

  useEffect(() => {
    if (!shouldFetch) return;

    const controller = new AbortController();
    const url = withLimit(src, limit);

    setLoading(!initialData);
    setError("");

    fetch(url, {
      signal: controller.signal,
      headers: { accept: "application/json" }
    })
      .then(async (response) => {
        if (!response.ok) {
          const body = (await response.json().catch(() => null)) as { message?: string; error?: string } | null;
          throw new Error(body?.message || body?.error || `Graph request failed: ${response.status}`);
        }
        return response.json() as Promise<RelationGraphData>;
      })
      .then((payload) => {
        setData(payload);
        setSelected(null);
        setHovered(null);
      })
      .catch((err: Error) => {
        if (err.name !== "AbortError") {
          setError(err.message);
          setData((current) => current);
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });

    return () => controller.abort();
  }, [initialData, limit, shouldFetch, src]);

  const arrays = useMemo(() => (data ? buildGraphArrays(data) : null), [data]);
  const activeNode = selected || hovered;

  useEffect(() => {
    if (!containerRef.current || !data || !arrays || data.Nodes.length === 0) {
      return;
    }

    const container = containerRef.current;
    container.replaceChildren();

    const config: GraphConfigInterface = {
      attribution: "",
      backgroundColor: "#16181d",
      curvedLinks: true,
      curvedLinkSegments: 8,
      enableDrag: true,
      enableRightClickRepulsion: false,
      enableSimulation: false,
      fitViewDelay: 450,
      fitViewDuration: 500,
      fitViewOnInit: true,
      fitViewPadding: 0.18,
      focusedPointRingColor: "#f0f6fc",
      hoveredPointCursor: "pointer",
      hoveredPointRingColor: "#e6edf3",
      linkDefaultColor: "#6e7681",
      linkDefaultWidth: 0.4,
      linkOpacity: data.Nodes.length > 700 ? 0.48 : 0.68,
      pixelRatio: Math.min(window.devicePixelRatio || 1, 2),
      pointDefaultColor: "#58a6ff",
      pointDefaultSize: 4,
      pointOpacity: 1,
      randomSeed: data.CenterID,
      renderHoveredPointRing: true,
      rescalePositions: false,
      scaleLinksOnZoom: false,
      scalePointsOnZoom: false,
      spaceSize: 8192,
      onBackgroundClick: () => {
        setSelected(null);
        graphRef.current?.setConfig({ focusedPointIndex: undefined });
      },
      onPointClick: (index) => {
        const node = data.Nodes[index];
        if (!node) return;
        setSelected(node);
        graphRef.current?.setConfig({ focusedPointIndex: index });
      },
      onPointMouseOut: () => setHovered(null),
      onPointMouseOver: (index) => {
        const node = data.Nodes[index];
        if (node) setHovered(node);
      }
    };

    const graph = new Graph(container, config);
    graphRef.current = graph;
    graph.setPointPositions(arrays.positions);
    graph.setPointColors(arrays.pointColors);
    graph.setPointSizes(arrays.pointSizes);
    graph.setLinks(arrays.links);
    graph.setLinkColors(arrays.linkColors);
    graph.setLinkWidths(arrays.linkWidths);
    graph.render(0);

    return () => {
      graph.destroy();
      if (graphRef.current === graph) {
        graphRef.current = null;
      }
      container.replaceChildren();
    };
  }, [arrays, data]);

  return (
    <section className="section relation-graph-section">
      <div className="relation-graph-head">
        <h2 className="section-title">{title}</h2>
        <div className="relation-graph-actions">
          {data ? (
            <span className="relation-graph-count">
              {compact(data.Nodes.length)} nodes / {compact(data.Edges.length)} edges{data.Truncated ? " / capped" : ""}
            </span>
          ) : null}
          <button type="button" className="btn btn-sm btn-outline" onClick={() => graphRef.current?.fitView(400, 0.18)} disabled={!data || loading}>
            Fit
          </button>
        </div>
      </div>

      <div className="relation-graph-frame">
        <div ref={containerRef} className="relation-graph-canvas" aria-label={title} />
        <div className="relation-graph-panel">
          {loading ? <span className="muted">Loading graph...</span> : null}
          {error ? <span className="warn">{error}</span> : null}
          {!loading && !error && activeNode ? <ActiveNode node={activeNode} /> : null}
          {!loading && !error && !activeNode ? <span className="muted">No node selected</span> : null}
        </div>
        <div className="relation-graph-legend" aria-hidden="true">
          <span className="legend-item">
            <span className="legend-dot user" /> users
          </span>
          <span className="legend-item">
            <span className="legend-dot repo" /> repos
          </span>
          <span className="legend-item">
            <span className="legend-dot org" /> orgs
          </span>
        </div>
      </div>
    </section>
  );
}

function ActiveNode({ node }: { node: RelationGraphNode }) {
  const label = node.Type === "user" ? `@${node.Label}` : node.Label;
  return (
    <>
      {node.Href ? (
        <a href={node.Href} className="relation-graph-node-link mono">
          {label}
        </a>
      ) : (
        <span className="relation-graph-node-link mono">{label}</span>
      )}
      <span className={`relation-graph-type ${node.Type}`}>{node.Type}</span>
      <span className="muted">{compact(node.Weight)} weight</span>
    </>
  );
}

function buildGraphArrays(data: RelationGraphData): GraphArrays {
  const nodeIndex = new Map<string, number>();
  data.Nodes.forEach((node, index) => nodeIndex.set(node.ID, index));

  const edges = data.Edges.filter((edge) => nodeIndex.has(edge.Source) && nodeIndex.has(edge.Target));
  const positions = new Float32Array(data.Nodes.length * 2);
  const pointColors = new Float32Array(data.Nodes.length * 4);
  const pointSizes = new Float32Array(data.Nodes.length);
  const links = new Float32Array(edges.length * 2);
  const linkColorValues = new Float32Array(edges.length * 4);
  const linkWidths = new Float32Array(edges.length);
  const typeCounts: Record<RelationGraphNode["Type"], number> = { user: 0, repo: 0, org: 0 };

  data.Nodes.forEach((node, index) => {
    const position = node.ID === data.CenterID ? [0, 0] : initialPosition(node, typeCounts[node.Type]++);
    positions[index * 2] = position[0];
    positions[index * 2 + 1] = position[1];

    const color = nodeColors[node.Type] || nodeColors.user;
    pointColors.set(color, index * 4);
    pointSizes[index] = node.ID === data.CenterID ? 13 : nodeSize(node);
  });

  edges.forEach((edge, index) => {
    links[index * 2] = nodeIndex.get(edge.Source) || 0;
    links[index * 2 + 1] = nodeIndex.get(edge.Target) || 0;
    linkColorValues.set(linkColors[edge.Type] || linkColors.reviewed, index * 4);
    linkWidths[index] = Math.min(3.5, 0.35 + Math.log1p(edge.Weight) * 0.38);
  });

  return {
    positions,
    links,
    pointColors,
    pointSizes,
    linkColors: linkColorValues,
    linkWidths
  };
}

function initialPosition(node: RelationGraphNode, index: number): [number, number] {
  const typeOffset = node.Type === "org" ? 0.6 : node.Type === "repo" ? 2.2 : 4.1;
  const baseRadius = node.Type === "org" ? 130 : node.Type === "repo" ? 210 : 300;
  const spread = node.Type === "org" ? 36 : node.Type === "repo" ? 31 : 38;
  const angle = typeOffset + index * GOLDEN_ANGLE + (hashString(node.ID) % 360) * (Math.PI / 720);
  const radius = baseRadius + Math.sqrt(index + 1) * spread;
  return [Math.cos(angle) * radius, Math.sin(angle) * radius];
}

function rgba(red: number, green: number, blue: number, alpha: number): [number, number, number, number] {
  return [red / 255, green / 255, blue / 255, alpha];
}

function nodeSize(node: RelationGraphNode): number {
  const base = node.Type === "org" ? 7 : node.Type === "repo" ? 5.5 : 4.5;
  return Math.min(12, base + Math.log1p(Math.max(1, node.Weight)) * 1.05);
}

function hashString(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function compact(value: number): string {
  return new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function withLimit(src: string, limit: number): string {
  const separator = src.includes("?") ? "&" : "?";
  return `${src}${separator}limit=${encodeURIComponent(limit)}`;
}
