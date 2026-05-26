# MomentScience — Project Context

Stacks on top of global `~/.claude/CLAUDE.md`.

## Role
- **Head of CS / PP&SE** at MomentScience. Manages 1 CSM + 1 SE (Chris Teceno).
- Owns: partner strategy, new platform products (WiWo, MoMoney/Sudoku), supply-side analytics, API/SDK ecosystem, demo pipeline oversight.
- **Not the CEO.** No company-wide strategy / fundraising / org decisions unless asked.

## Chris Teceno (SE, direct report)
- Strong at: building demos from tight briefs, SDK config, technical partner onboarding.
- Needs: clear, scoped briefs — exact screens, partner hesitation, the belief to leave with.
- **Not client-ready** for discovery calls. Route to execution, never relationship. Sidd reviews output before partner sees it.
- Build systems Chris can run alone. Templates + checklists beat supervision.

## Active Product Builds
- **WiWo** (`wiwo/`) — location-based rewards mobile (Flutter + Node.js server). Background geofencing, WonderPush, Mapbox. Flutter code → `flutter-reviewer`. Library docs → context7 MCP.
- **MoMoney** (`site-momoney/`) — developer platform (Vite + Svelte 5 runes + Tailwind 3). Docs/API/platform examples (Unity, RN, Godot, Solar2D). `frontend-patterns` for site.
- **Sudoku Perks** (`Sudoku-Unity-Game/`) — Unity 6 + Firebase. SDK fires on level-complete and consolation. Treat as vanilla (no test framework). `renaissance-architecture` for systems, `security-review` for Firebase rules.

## About MomentScience
- Ad-tech SDK for post-transaction monetization. Products: Moments SDK, Perkswall, PWaaS.
- Partners = **Publishers** (integrate SDK on confirmation pages).
- Repo layout: `demos/demo-[partner]`, `tools/`, `wiwo/`, `_templates/` (in `demos/_templates/`, never edit), `knowledge/`.

## Work Modes (priority order)
1. Product strategy & partnership leadership
2. New product builds (WiWo, Sudoku, MoMoney)
3. Strategic demos Sidd owns directly (OfferUp-level, new categories). Chris handles routine demos.
4. Supply-side analytics (ClickHouse, partner perf, Scout)
5. Team enablement (briefs, templates for Chris + CSM)
6. Partner pipeline (call prep, follow-ups, onboarding)
7. Content & distribution
8. Marketing/landing pages

## Skill Routing (mosci-specific)

| Task | Skill |
|---|---|
| SDK/embed config for publisher | `ms-partner-config` |
| Integration docs for new partner | `ms-integration-guide` |
| Security/compliance questionnaire | `ms-enterprise-review` |
| Product feedback memo | `ms-product-feedback` |
| Post-call follow-up (Slack + email + summary) | `ms-partner-follow-up` |
| ClickHouse analytics | use `mcp__ClickHouse_Analytics__MomentScience__*` — ignore generic `mcp-clickhouse`. See `~/.claude/playbooks/clickhouse-schema.md` |
| Building a demo | see `~/.claude/playbooks/demo-anatomy.md` |
| Python code (Scout, scrapers, beverly) | `python-patterns` |
| Scout bot work | `continuous-agent-loop` + `tools/offer-scraper/CLAUDE.md` |
| Scrapers / data pipelines | `data-scraper-agent` |
| Schedule recurring report | `autonomous-loops` + `mcp__scheduled-tasks__*` |

## Demo Workflow
**New demo (`demos/demo-*`)** — hook enforced. Before code, answer the partner brief:
1. What stage is [partner] at?
2. What's their primary hesitation?
3. What's the ONE thing they need to leave believing?

Then route: `ui-ux-pro-max` → `frontend-slides`. Save to claude-mem tagged with partner name.

**Returning to existing demo** — run `mem-search [partner] brief` first. If no prior brief, answer the brief.

**Verification** — Sidd reviews Chris's output before any partner sees it. Use Claude Preview to spot-check before the call.

## Demo Stack Defaults
- **Vanilla HTML + CSS + JS only.** No build, no npm, no framework. GSAP via CDN. **No tests, no coverage.** ECC rules do NOT apply to demos.
- Always start from a template — `cp -r demos/_templates/moments-checkout demos/demo-[partner]`.
- Structural gold standard: `demos/_templates/moments-checkout` (uses `demo.config.js`).
- Visual gold standard: `demos/demo-txb` (older structure, design reference only).
- Full anatomy (iPhone bezel CSS, SDK integration, animation curves) → `~/.claude/playbooks/demo-anatomy.md`.
- Deploy: `render.yaml` → Render.

## Other Stack Defaults
- **Websites / marketing** — Next.js + Tailwind + Framer Motion + shadcn/ui.
- **Internal tools** — Next.js + Tailwind + shadcn/ui. `security-review` for any auth.
- **WiWo** — Flutter, stay the course. Don't suggest React Native.
- **Video** — Remotion.
- **Animation rule:** demos → GSAP; React → Framer Motion; video → Remotion; mobile → Flutter animations.

## MCP Tools (mosci)

| Tool | When |
|---|---|
| Slack `mcp__18ab42c2-*` | revenue-operations channel, Scout traffic, partner comms |
| Notion `mcp__33bb34b4-*` | Beverly, analytics context |
| Gmail `mcp__d4cf6c1a-*` | partner follow-up, advertiser outreach — MomentScience account |
| Google Drive `mcp__c1fc4002-*` | contracts, creatives, SDK docs |
| Google Calendar `mcp__5c63523a-*` | partner calls, demo meetings |
| Clarify CRM `mcp__b6600653-*` | leads, lists, campaigns |
| Scheduled Tasks `mcp__scheduled-tasks__*` | digests, recurring reports |
| Gamma `mcp__3fdca5ac-*` | quick decks, briefs |
| Claude Preview `mcp__Claude_Preview__*` | demo QA in browser |
| ClickHouse `mcp__ClickHouse_Analytics__MomentScience__*` | all analytics |

## Linear Ticket Discipline
- **Never create Linear issues without explicit approval.** Show draft table (Title | Priority | Platform | desc) first, wait for "looks good"/"ship it"/"create them", then API call.
- iOS + Android = ONE ticket with platform labels unless fix paths genuinely differ.
- Fewer, higher-signal tickets over comprehensive coverage. If unsure, ask.

## Partner Pipeline

| Task | Skill |
|---|---|
| Prospect research | `apollo:enrich-lead` + `common-room:account-research` + `market-research` |
| Sourcing new partners | `apollo:prospect` + `common-room:prospect` |
| Call prep | `sales:call-prep` |
| Post-call summary | `sales:call-summary` + `ms-partner-follow-up` |
| CRM hygiene | Clarify MCP |
| Competitive landscape | `market-research` → `deep-research` |
| Partner-facing GTM copy | `marketing:draft-content` |
| Onboarding playbook | `ms-integration-guide` + knowledge vault |
| CSM/SE tool or template | document in `tools/` or `knowledge/`, optimize for self-serve |

## Session-end
Run `/dream` before closing long Flutter / Svelte / Unity / Scout sessions — captures mid-build judgment calls into CLAUDE.md.

## Batch Playbook
- Partner call/demo prep → `/partner-prep [name]` (account + call-prep + ClickHouse + competitive)
- 3+ partners in pipeline sprint → `/batch`
- Building a demo → fan out demo build + ClickHouse pull + offer catalog in parallel
- **Never batch:** demo iteration (builds on prior), Scout debugging (shared signal state), ClickHouse tuning (shared investigation).

## Scout Quickstart
Starting any Scout session:
1. `/mem-search scout` for prior context
2. Read `tools/offer-scraper/CLAUDE.md` Engineering Principles + Known Debt
3. Read `scout_agent.py` SYSTEM_PROMPT + TOOLS + TOOL_MAP if planning anything new
4. Run `python3 smoke_test.py` to confirm baseline — "it worked last time" is not a baseline

## Scout Response Patterns

Scout uses ScoutKit (`scout_ui_kit.py`) for all Slack output. Every response maps to one of six patterns. Match pattern → surface → severity before writing new handlers.

| Pattern | Surface | Severity | Max blocks | Buttons | When |
|---|---|---|---|---|---|
| `ALERT` | `MONITOR_ALARM` | WARN / CRITICAL | per budget | 0 | monitor alarm fires |
| `ANSWER` | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO | per budget | ≤3 | ask() reply |
| `STATUS` | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO / WARN | per budget | ≤3 | `@Scout status` |
| `CONFIRM` | `EPHEMERAL` | POSITIVE | per budget | 0 | action acknowledged |
| `EMPTY` | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO | per budget | 0 | no data found |
| `ERROR` | `EPHEMERAL` | CRITICAL | per budget | 0 | ClickHouse failure |

**Actionability rule:** every ALERT and ANSWER must give the reader one next action. No number without context.

### Code example (ANSWER pattern)

```python
from scout_ui_kit import Card, Severity, Surface, ResponsePattern, wrap_response

card = Card(severity=Severity.INFO, headline="Revenue MTD", body="$847K / $1M · 71%")
_, blocks = wrap_response(card=card, surface=Surface.CHANNEL_ROOT, pattern=ResponsePattern.ANSWER)
web.chat_postMessage(channel=channel, text="Revenue MTD", blocks=blocks)
```

### Pattern enforcement

Passing `pattern=` raises `ValueError` at call time if the surface is wrong:

```python
# This raises ValueError — ALERT requires MONITOR_ALARM, not CHANNEL_ROOT
wrap_response(card=card, surface=Surface.CHANNEL_ROOT, pattern=ResponsePattern.ALERT)
```

Existing callers that don't pass `pattern=` are unaffected.

## Scout Slash Commands

Registered at api.slack.com/apps — each must be added to the Slack app manifest.

| Command | What it does |
|---|---|
| `/scout-cap` | Force-run cap signal now; results post in the channel |
| `/scout-vel` | Force-run velocity signal now |
| `/scout-ghost` | Force-run ghost (zero-conversion) signal now |
| `/scout-fill` | Force-run fill-rate signal now |
| `/scout-signal-status` | List signals currently firing (reads alert_registry) |
| `/scout-revenue` | Prompts user to ask `@Scout revenue` for full response |
| `/scout-pub [publisher]` | Revenue health card for a publisher |
| `/scout-enter [advertiser]` | Campaign entry card for the MS platform |
| `/scout-queue` | Current demand queue with Notion links |
| `/scout-status` | System health + benchmark freshness |
| `/scout-help` | Full reference card (ephemeral) |

All `/scout-cap/vel/ghost/fill` commands route to `_FORCE_MONITOR_FNS` — the same path as `@Scout force <signal>`. Requires the demand-feed service to be running with monitors initialized.
