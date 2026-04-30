import Link from "next/link";

import DataExplorer, { type DataTab } from "@/components/DataExplorer";
import { apiGet, qs } from "@/lib/api";
import type { DataExplorerData } from "@/types/api";

type Props = {
  searchParams: Promise<Record<string, string | undefined>>;
};

const validTabs = new Set<DataTab>(["repos", "prs", "reviews", "users"]);

export default async function DataPage({ searchParams }: Props) {
  const sp = await searchParams;
  const requestedTab = sp.tab || "repos";
  const tab: DataTab = validTabs.has(requestedTab as DataTab) ? (requestedTab as DataTab) : "repos";

  const data = await apiGet<DataExplorerData>(
    `/api/next/data/${tab}${qs({
      page: sp.page || 0,
      limit: sp.limit,
      search: sp.search,
      sort: sp.sort,
      status: sp.status,
      author: sp.author,
      reviewer: sp.reviewer,
      state: sp.state,
      repo: sp.repo
    })}`
  );

  return (
    <div className="page-wrap">
      <div className="breadcrumb">
        <Link href="/" className="bc-link">ngmi</Link>
        <span className="bc-sep">/</span>
        <span className="bc-current">data</span>
      </div>

      <div className="repo-header">
        <div className="repo-title-row">
          <h1 className="page-title mono">Data Explorer</h1>
        </div>
        <p className="repo-desc">Query tracked repos, pull requests, reviews, and users.</p>
      </div>

      <DataExplorer data={data} tab={tab} />
    </div>
  );
}
