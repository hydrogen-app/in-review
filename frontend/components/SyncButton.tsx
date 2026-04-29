"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

export function SyncButton({ owner, name }: { owner: string; name: string }) {
  const router = useRouter();
  const [busy, setBusy] = useState(false);

  async function sync() {
    setBusy(true);
    await fetch(`/api/sync/${owner}/${name}`, { method: "POST", credentials: "include" }).catch(() => undefined);
    setBusy(false);
    router.refresh();
  }

  return (
    <button className="btn btn-sm" onClick={sync} disabled={busy}>
      {busy ? "Syncing..." : "↻ Sync Now"}
    </button>
  );
}
