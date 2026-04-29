import Link from "next/link";

import { apiGet } from "@/lib/api";
import type { HiWallData } from "@/types/api";

export default async function HiWallPage() {
  const data = await apiGet<HiWallData>("/api/next/hi-wall");

  return (
    <div>
      <div className="breadcrumb">
        <Link href="/" className="bc-link">ngmi</Link>
        <span className="bc-sep">/</span>
        <span className="bc-current">hi wall</span>
      </div>

      <div className="hi-wall-header">
        <h1>hi wall</h1>
        <p className="muted">pages where people said hi</p>
      </div>

      {data.Pages?.length ? (
        <div className="hi-wall-list">
          {data.Pages.map((page, index) => (
            <Link href={page.Path} className="hi-wall-row" key={page.Path}>
              <span className="hi-wall-rank muted">{index + 1}</span>
              <span className="hi-wall-label">{page.Label}</span>
              <span className="hi-wall-stats muted">
                {page.TotalCount} hi
                {page.TodayCount ? <span className="green">&nbsp;·&nbsp;+{page.TodayCount} today</span> : null}
              </span>
            </Link>
          ))}
        </div>
      ) : (
        <p className="muted" style={{ padding: "2rem 0" }}>no hi&apos;s yet. be the first on any page!</p>
      )}
    </div>
  );
}
