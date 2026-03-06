# SvelteKit Migration — Gap Analysis

## Route Coverage

All HTMX routes have corresponding SvelteKit pages:

| Route | File | Status |
|---|---|---|
| `/` | `+page.svelte` | Needs style + icon fixes |
| `/repo/[owner]/[name]` | `+page.svelte` | Needs style + icon fixes |
| `/user/[username]` | `+page.svelte` | Needs style + icon fixes |
| `/org/[org]` | `+page.svelte` | Needs style + icon fixes |
| `/leaderboard/[category]` | `+page.svelte` | Needs style + icon fixes |
| `/stats` | `+page.svelte` | **Major — incomplete (see below)** |
| `/data` | `+page.svelte` | Needs style fixes |
| `/blog` | `+page.svelte` | Empty content sections |
| `/dashboard` | `+page.svelte` | Needs style fixes |
| `/hi-wall` | `+page.svelte` | Needs style fixes |

---

## 1. Styles — Critical (everything looks wrong)

`web/src/routes/layout.css` is the default shadcn/Tailwind theme. The original `static/css/app.css` uses a minimal terminal aesthetic. Must fix:

- Remap CSS vars to terminal palette:
  - `--bg: #1c1c1e`, `--fg: #e6edf3`, `--border: #30363d`
  - `--muted: #848d97`, `--link: #58a6ff`
  - `--green: #3fb950`, `--red: #f85149`, `--orange: #d29922`
- Set `--radius: 0` — eliminates all rounded corners
- Font: `ui-monospace, 'Cascadia Code', Menlo, Consolas, monospace` everywhere
- Body max-width: `860px` (currently `max-w-4xl` = 896px — close, but use explicit value)
- Hard `1px solid` borders everywhere, no shadows, no card backgrounds

---

## 2. Stats Page — Major Incomplete

`web/src/routes/stats/+page.svelte` currently has 2 charts and basic filter inputs.
The original has 15 charts + interactive controls.

### Missing: Controls bar
- Range buttons: All / 3M / 6M / 1Y / 2Y / 5Y / 10Y (client-side slice — no server fetch)
- Avg toggle button (show/hide avg datasets across charts that have avg+median)
- Trim slider (0–20% outlier trim, triggers server refetch via `goto`)
- Stars filter buttons: Any / 100+ / 1k+ / 10k+
- Contributors filter buttons: Any / 5+ / 20+ / 100+

### Missing: Time-series section (9 charts)
Each chart has a stat sidebar panel next to it (latest value + trend vs. first period).

| Chart | Series | Colors |
|---|---|---|
| PR size over time (lines) | avg, median | `#d29922`, `#f0883e` |
| Review time over time (hrs) | avg, median | `#d29922`, `#f0883e` |
| Changes requested rate (%) | single | `#f85149` |
| Merged PRs per month | single | `#58a6ff` |
| PRs opened per month | single | `#3fb950` |
| Merge rate — merged/opened (%) | single | `#e3b341` |
| Time to first review (hrs) | avg, median | `#bc8cff`, `#a371f7` |
| Unreviewed merge rate (%) | single | `#ffa657` |
| Lines per contributor (monthly) | single | `#39c5cf` |

Stat sidebar: each chart gets 2–3 stat cards (latest value, first-period value, trend arrow + %).
Trend format: `▲ +N% avg` / `▼ N% median` / `→ 0%` since first label.

### Missing: Size-bucket section (6 charts)
Each has a bucket table sidebar (label | value).

| Chart | Data field | Color |
|---|---|---|
| Avg review time by size (hrs) | `avgHours` | `#d29922` |
| Median review time by size (hrs) | `medianHours` | `#f0883e` |
| PRs by size bucket | `prCounts` | `#58a6ff` |
| Changes requested rate by size (%) | `changesRequestedRate` | `#f85149` |
| Avg changes requested per PR | `avgChangesRequested` | `#da3633` |
| Clean approval rate by size (%) | `approvalRate` | `#3fb950` |

### Chart library
The original uses Chart.js via CDN. In SvelteKit, use the already-installed `layerchart` with `LineChart` / `BarChart`, or switch to a direct Chart.js import. The layerchart approach is already wired up for the 2 existing charts — extend it.

---

## 3. Icons — Replace Emojis with Lucide

Use `lucide-svelte` (install if not present: `pnpm add lucide-svelte`). No emoji should appear anywhere. The fire icon on home quick pills should simply be **removed** (no replacement).

### Replacements by page

**`/repo/[owner]/[name]`** (`+page.svelte`):
- `⟳ Syncing…` → `<RefreshCw class="size-3 animate-spin" /> Syncing`
- `⟳ Queue #N` → `<RefreshCw class="size-3 animate-spin" /> Queue #N`
- `✓ Synced` → `<Check class="size-3" /> Synced`
- `⏳ Pending` → `<Clock class="size-3" /> Pending`
- `↻ Sync Now` button text → keep text, optionally add `<RefreshCw />` icon
- PR table: `✓` (clean PR) → `<Check class="size-3" />`
- PR table: `N×` changes requested badge → keep as text, no icon needed

**`/user/[username]`** (`+page.svelte`):
- Approvals badge: `✓ N` → `<Check class="size-3" /> N`
- Blocks badge: `↺ N` → `<RefreshCcw class="size-3" /> N`

**`/leaderboard/[category]`** (`+page.svelte`):
- Approval column: `✓` → `<Check class="size-3" />`

**`/` home** (`+page.svelte`):
- Remove `🔥` from popular visit pills entirely (no replacement)
- Search result `★` for stars count — replace with `<Star class="size-3" />` or use plain text `stars`

---

## 4. Blog Page — Empty Content

`web/src/routes/blog/+page.svelte` has placeholder `<p>` tags with no text:
- "The Question" section
- "What the Data Shows" section
- "The AI Inflection Point" section
- "What This Means for Review Time" section

These need actual written content. The methodology section is already filled in.

---

## Priority Order

1. **Fix `layout.css`** — terminal palette, zero radius, monospace font
2. **Complete `/stats`** — controls + 9 time-series charts + 6 size charts + sidebars
3. **Replace emojis with lucide icons** across all pages
4. **Fill blog content**
