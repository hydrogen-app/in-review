# Product Strategy — Company Value Proposition

## The Core Moat

ngmi has data from thousands of public repos. LinearB, Swarmia, and Jellyfish only benchmark
customers against each other — a biased sample of paying enterprise teams. ngmi can say:

> "Your average review time is 3.2 days, which is slower than 70% of Go repos with 1k+ stars."

No other tool can make that statement with public data as the reference. This is the differentiator
to build on.

---

## High-Value Features for Companies

### 1. Industry Benchmarking — TODO
Show a company how they compare to public repos filtered by language, size tier, and star count.
"Your team reviews PRs in 4.1 days. The median for your language/size tier is 1.8 days."
This is the hook. It's immediately legible to an engineering manager and requires zero setup for
public repos.

### 2. Bottleneck Identification — TODO
Surface which individuals are the single point of failure for approvals.
"3 people approve 80% of your PRs. If any one is unavailable, velocity drops significantly."
GitHub doesn't surface this. Engineering managers care about review bus factor, not just code
ownership. Pair with reviewer load distribution (see below).

### 3. Reviewer Load Distribution — TODO
Visualize how evenly review work is spread across the team. A heavily skewed distribution is
both a burnout risk and a knowledge-silo risk. Show the Gini coefficient or a simple bar chart
of review counts per person. Immediately actionable — reassign review assignments or use
CODEOWNERS to rebalance.

### 4. Review Quality Signals — TODO
Track rubber-stamp rate: approvals with zero review comments. A team with 0% changes-requested
rate and zero comments per review is not reviewing — they're clicking approve. Surface this
alongside changes-requested rate so managers can distinguish healthy rigor from checkbox culture.

### 5. First Response Time — TODO
Time from PR open to first review comment or approval, separate from total merge time.
This is the metric engineering managers actually track for SLAs ("no PR sits unreviewed for
more than 24 hours"). Splitting it out lets teams diagnose whether the bottleneck is
responsiveness or iteration cycles.

### 6. PR Size Coaching — TODO
"Your org's PRs average 800 lines. Review time drops 60% for PRs under 200 lines."
Benchmarked against the public dataset, this is a data-backed recommendation, not an opinion.
Actionable at the team level without requiring process changes from leadership.

### 7. Day-of-Week / Time Patterns — TODO
Show when PRs are opened vs. when they receive first review. PRs opened Friday afternoon
and reviewed Monday is a process problem, not a people problem. Surfacing this separates the
tool from pure blame dashboards and makes it useful for process improvement conversations.

### 8. Slack / Email Digest — TODO
Weekly summary of org PR stats sent to a channel or inbox. Frictionless — no one logs into
dashboards voluntarily. Engineers read Slack. GitHub sends nothing like this. Digest should
include: avg review time this week vs. last week, top reviewer, any PRs that sat unreviewed
>48h, and a link to the full dashboard.

### 9. Review Debt View — TODO
A single list of PRs open for more than N days with no review. The thing a team lead checks
Monday morning. Currently requires filtering GitHub's PR list manually across each repo.
Cross-repo in a single view is the value.

### 10. Embeddable Widget / API — TODO
Let teams pull data into internal wikis, Notion pages, or custom dashboards. An iframe embed
or simple JSON API endpoint requires low engineering effort but has high perceived value for
companies that want to centralize tooling. Also useful as a marketing surface — embedded
badges in READMEs already exist, expand to full widget embeds.

---

## Private Repo Data Privacy — Critical Blocker

**STATUS: IMPLEMENTED** (2026-03-05)

~~**Right now, all synced data is public.**~~ Private repo data is now gated. Repos added via the
GitHub App dashboard are marked `is_private=true` and are excluded from all public-facing queries.

What was shipped:
- `is_private` column on repos table; repos added via dashboard are marked private on insert
- All leaderboard queries (speed, reviewers, gatekeepers, authors, clean approvals) filter private repos via `AND NOT is_private` or JOIN
- All global stats/benchmarks (GlobalSizeChartData, GlobalOverallStats, time series) filter private repos
- TotalStats, SearchRepos, Data Explorer (repos/PRs/reviews) all exclude private repos
- Org queries (OrgRepos, OrgTimeSeriesData, org leaderboards) exclude private repos
- User profile contributed-repos list only shows public repo activity to anonymous visitors
- All rank computations exclude private repos so rankings reflect the public dataset
- Repo page gates access: unauthenticated users → redirect to login; authenticated users checked against `user_tracked_repos` + `app_installations` before serving data
- `UserCanAccessRepo()` DB method checks both direct tracking and installation org membership
- Leaderboard search treats private repos as not found for unauthenticated queries

What is NOT yet done:
- User profile: authenticated users with shared org access do not yet see combined public+private stats
- Org page: does not yet require auth when all repos are private; currently just shows empty repo list

### What needs to change

**Access control on repo data:**
Private repos should only be visible to users who have authenticated via GitHub OAuth and have
read access to that repo (i.e., are a member of the org or have been granted access). The
simplest check: call the GitHub API with the user's token to verify they have access before
serving any data. Cache the result for a short TTL (e.g., 5 minutes).

**Repo visibility flag in the DB:**
Store a `is_private` boolean on each synced repo. Any route that serves repo data, user data,
or leaderboard data must filter out private repos for unauthenticated requests. Private repo
data should never appear in global leaderboards, the data explorer, or user profiles for
anonymous visitors.

**Leaderboard isolation:**
A private repo's PR times, reviewer stats, and author stats must not bleed into global
leaderboards or the public stats page. If a company syncs 500 private repos, those should
not skew the public "average review time across all repos" number.

**User profile pages:**
If a user has reviewed PRs in both public and private repos, their public profile should only
show activity from public repos to anonymous visitors. Authenticated users who share org access
should see the combined view.

**Org pages:**
An org page for a company with private repos should require authentication. Showing a company's
repo list, even without PR details, leaks information about their internal project structure.

### Implementation sketch

1. Add `is_private` and `owner_github_id` columns to the repos table
2. GitHub App webhook on `installation` events — mark all installed repos as private
3. Middleware check on all data routes: if `repo.is_private`, verify the requesting user's
   GitHub token has repo access before returning data
4. Strip private repos from all aggregate queries (stats, leaderboards, data explorer) for
   unauthenticated requests
5. Add a `private_orgs` set to the session — once a user authenticates, resolve which orgs
   they belong to and cache it; use this to gate access to org-level pages

### Trust and messaging

Even after the technical fix, companies will ask about data handling. Be explicit on the
landing page: private repo data is never shown to users who don't have GitHub access to that
repo, is never included in public leaderboards, and can be deleted on request. This is table
stakes for any B2B tool touching code.

---

## Pricing / Go-to-Market Angle

Do not try to compete with LinearB, Swarmia, or Jellyfish on features. They have DORA metrics,
deployment tracking, Jira integration, SSO, compliance certifications, and enterprise sales teams.

The winning angle: **free, fast, zero-setup, benchmarked against the real world.**

- Free for public repos — no login, no install, paste a repo name and see data immediately
- Paid for private repos + Slack digest + team grouping
- Flat pricing (not per-seat) — this is explicitly what engineers hate about the incumbents

The hook is the public repo path. A team lead searches their own org, sees how they compare
to industry, shares it with their manager. That's the viral loop. The paid conversion is
"now show me the private repos too."

---

## What to Avoid

- Do not build DORA metrics (deployment frequency, change failure rate) — requires CI/CD
  integration, not just GitHub data, and puts you directly against funded competitors
- Do not add per-seat pricing — the incumbents charge $20-40/user/month and it's a common
  complaint; flat pricing is a positioning statement
- Do not require GitHub App install for the initial hook — the zero-friction public repo path
  is the differentiator, protect it
