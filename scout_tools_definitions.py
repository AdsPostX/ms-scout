"""
Scout tool schema definitions — static tool list passed to the Anthropic API.

Extracted from scout_agent.py (Phase 13-04) to keep the orchestration file lean.
No runtime logic lives here: just SUPPORTED_NETWORKS and the TOOLS list.

After filtering _INTERNAL_TOOLS, scout_agent.py reassigns TOOLS to the public
subset. Consumers that need the full unfiltered list should import TOOLS before
that reassignment — but in practice only scout_agent.py itself does this.
"""

from __future__ import annotations

# ── PR 17c / PR 18: SUPPORTED_NETWORKS — single source ───────────────────────
# ACTIVE networks only (creds present on Render → scraper actually returns offers).
# PR 18 trimmed this from 9 → 4: ShareASale, Rakuten, AWIN, Tune, Everflow all
# silently no-op when their API credentials aren't set on Render. Listing them as
# "supported" was misleading because the digest header showed them but the scraper
# returned []. See Known Debt in CLAUDE.md for the credential checklist.
#
# When credentials are added on Render, append the network name here AND to
# _DIGEST_NETWORKS_FALLBACK in scout_digest.py.
#
# Used in tool description strings and function docstrings ONLY (not the
# SYSTEM_PROMPT body — converting that to an f-string would require escaping
# every {} in the SQL/JSON examples and risks silent format breakage).
SUPPORTED_NETWORKS: tuple[str, ...] = (
    "Impact", "FlexOffers", "MaxBounty", "CJ",
)


TOOLS = [
    {
        "name": "search_offers",
        "description": (
            "Full-text search across advertiser name and description. "
            "Use for specific advertiser lookups or keyword searches. "
            "Leave query empty ('') to browse all offers with only filters applied. "
            "Optional filters: network, category, min_payout, max_payout, ms_status. "
            "Returns results ranked by Scout Score (estimated RPM), not raw payout."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search term — advertiser name or keyword. Use '' to browse all offers."},
                "network": {"type": "string", "description": "Optional network filter — pass whatever network name the user mentioned (e.g. 'cj', 'impact'). Fuzzy matching handles normalization. Check available_networks in get_scout_status() to see what's in inventory."},
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness, Retail"},
                "min_payout": {"type": "number", "description": "Minimum payout amount (floor)"},
                "max_payout": {"type": "number", "description": "Maximum payout amount (ceiling), e.g. 0.05 for ≤$0.05"},
                "ms_status": {"type": "string", "description": "Live, In System, or Not in System"},
                "limit": {"type": "integer", "description": "Max results (default 5)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_top_opportunities",
        "description": (
            "Returns best untapped offers MS is NOT running (MS Status = Not in System), "
            "ranked by Scout Score (estimated RPM = payout × predicted CVR). "
            "Use for prospecting: 'what should we go after?', 'best opportunities in X vertical'. "
            "Optional filters: category, geo."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness"},
                "geo": {"type": "string", "description": "e.g. US Only, Global"},
                "limit": {"type": "integer", "description": "Max results (default 5)"},
            },
        },
    },
    {
        "name": "get_running_offers",
        "description": (
            "Returns offers MS is currently running (MS Status = Live) with real CVR + RPM data where available. "
            "Use to benchmark payouts, see what verticals are covered, check if MS has an offer from a specific advertiser, "
            "or understand what's actually performing. Optional filter: category."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness"},
            },
        },
    },
    {
        "name": "get_category_performance",
        "description": (
            "Returns real CVR and RPM benchmarks from MS's live ClickHouse data, by category and by specific offer. "
            "Use this to answer questions about what performs well for MS, "
            "to contextualize a new offer's expected value, or to compare verticals. "
            "This is the most data-driven signal available — prioritize it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Optional: filter to a specific category"},
            },
        },
    },
    {
        "name": "get_offer_stats",
        "description": (
            "Returns aggregate inventory stats: count and avg Scout Score by network and category, "
            "MS Status breakdown, and top 5 highest-value offers. "
            "Use for strategic / high-level questions about the inventory."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_publisher_competitive_landscape",
        "description": (
            "Query ClickHouse for what's currently running on a specific publisher (e.g. AT&T, TXB, MLB, or partner ID 6103), "
            "ranked by RPM. Answers questions like: 'would a higher payout help us win more AT&T impressions?', "
            "'how competitive is the TurboTax offer on AT&T?', 'how many impressions would we get?', "
            "'what does partner 6103 run?'. "
            "Supply offer_name + hypothetical_payout to get a rank-change + impression share projection. "
            "Use publisher_id when a numeric partner ID is given (e.g. 6103); use publisher_name otherwise."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {"type": "string", "description": "Publisher name (partial match OK) e.g. 'AT&T', 'TXB'. Omit if using publisher_id."},
                "publisher_id": {"type": "integer", "description": "Numeric publisher/partner ID (e.g. 6103). Use when the user provides a partner number."},
                "offer_name": {"type": "string", "description": "Optional: offer/advertiser to rank in the competitive set e.g. 'TurboTax'"},
                "hypothetical_payout": {"type": "number", "description": "Optional: new payout to test (e.g. 40.0 for $40 CPA)"},
                "weeks": {"type": "integer", "description": "Optional: projection window in weeks (default 2)"},
            },
        },
    },
    {
        "name": "get_fallback_candidates",
        "description": (
            "When an offer might go dark (budget cap, advertiser pause, network issue), find the best replacement. "
            "Returns (1) same advertiser on a different network — plug-and-play swap, "
            "and (2) top category substitutes not currently live in MS, ranked by Scout Score. "
            "Use for: 'what's our fallback if X goes dark?', 'backup for Y', 'if X hits cap what do we run?', "
            "'what do we replace X with?', 'contingency plan for Y'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "offer_name": {"type": "string", "description": "The offer that may go dark"},
                "category": {"type": "string", "description": "Override category if needed"},
                "payout_type": {"type": "string", "description": "Optional: filter subs by payout type (CPA, CPL, etc.)"},
            },
            "required": ["offer_name"],
        },
    },
    {
        "name": "draft_campaign_brief",
        "description": (
            "Fetch all offer details needed to generate a campaign brief: tracking URL, payout, geo, "
            "description, network, offer ID, and real MS performance data. "
            "Use when asked to 'build', 'create a brief for', 'I like [offer], build it', or similar. "
            "Returns structured data — you then generate the copy, titles, and CTAs."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser": {"type": "string", "description": "Advertiser name (partial match OK)"},
                "network": {"type": "string", "description": "Optional network filter — pass whatever network name the user mentioned (e.g. 'cj', 'impact'). Fuzzy matching handles normalization."},
            },
            "required": ["advertiser"],
        },
    },
    {
        "name": "get_queue_status",
        "description": (
            "Fetch the offer pipeline queue from Notion and return a Slack Block Kit card. "
            "Grouped by pipeline stage: Awaiting Entry → In Platform → Test Offer ON → Live. "
            "Use for: 'what's in the queue?', 'what's pending?', 'queue status', 'pipeline', "
            "'what's been approved?', 'what's waiting to go live?'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_demand_queue_status",
        "description": (
            "Read the MS Demand Queue — cross-references ClickHouse to detect if any queued offer "
            "is already live (impressions > 0 since approval date). "
            "Use for impression-based queries: 'is X live?', 'how many impressions since X was approved?'. "
            "For general queue visibility use get_queue_status() instead."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "mark_offer_launched",
        "description": (
            "Mark an approved offer as live. Updates queue state and triggers a notification "
            "to the person who approved it + AdOps. Thread-only — no channel noise. "
            "Use when: 'TurboTax is live', 'confirm X is live', 'mark X as launched', "
            "'X went live', 'X is running'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser": {"type": "string", "description": "Advertiser name (partial match OK)"},
            },
            "required": ["advertiser"],
        },
    },
    {
        "name": "get_advertiser_revenue_projection",
        "description": (
            "Project gross revenue for a specific advertiser across ALL MS publisher partners for a target month. "
            "Uses last 30 days as the baseline (avg daily revenue × days in month). "
            "Checks campaign end dates (warns if campaigns end before month-end) and monthly budget caps. "
            "Returns: projected total revenue, breakdown by publisher, cap warnings, end-date warnings. "
            "Use for: 'projected revenue for Disney+ in April', 'how much will TurboTax generate this month', "
            "'gross revenue forecast for X across all partners', 'what's the April projection for X'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser_name": {"type": "string", "description": "Advertiser/offer name (partial match OK) e.g. 'Disney+', 'TurboTax'"},
                "month": {"type": "string", "description": "Target month e.g. 'April 2026' or '2026-04'. Defaults to next calendar month."},
            },
            "required": ["advertiser_name"],
        },
    },
    {
        "name": "get_publisher_health",
        "description": (
            "Full publisher health analysis: sessions, impressions, clicks, conversions, revenue, RPM, CTR, and CVR. "
            "Breaks down by placement (e.g. FuelHub vs TransactionReceipt) and OS (iOS/Android). "
            "Includes click position data (which carousel slot gets clicked). "
            "Use for: 'how is [publisher] doing', 'performance for [publisher]', "
            "'breakdown by placement', 'full funnel for [publisher]', "
            "'[publisher] placement performance', 'what placement drives most revenue on [publisher]'. "
            "This is the default tool for any publisher performance query — always call this before offer-level analysis."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {"type": "string", "description": "Publisher name (partial match OK) e.g. '7-Eleven', 'AT&T'. Omit if using publisher_id."},
                "publisher_id": {"type": "integer", "description": "Numeric publisher ID. Use when user provides a partner number."},
                "days": {"type": "integer", "description": "Lookback window in days (default 14)"},
                "geo_state": {"type": "string", "description": "Optional: filter to a US state e.g. 'California', 'TX'"},
            },
        },
    },
    {
        "name": "get_campaign_status",
        "description": (
            "Check if an advertiser's campaigns are active or paused, and see recent changes from the audit log. "
            "Use for: 'is [offer] paused?', 'confirm [offer] is paused', 'is [offer] still live?', "
            "'what happened to [offer]?', 'when was [offer] paused?', 'confirm all [offer] campaigns are killed'. "
            "Returns current is_active status for each publisher campaign + last 30 days of change history."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser_name": {"type": "string", "description": "Advertiser/offer name (partial match OK) e.g. 'TurboTax', 'Hulu'"},
            },
            "required": ["advertiser_name"],
        },
    },
    {
        "name": "get_perkswall_engagement",
        "description": (
            "Perkswall offer selection analytics — which offers do loyalty members actually pick? "
            "Queries user_selected_perks to show offer selections, unique members engaged, and selection rates. "
            "Use for: 'which perks are [publisher] users picking?', 'Perkswall engagement for [publisher]', "
            "'what do loyalty members select on [publisher]?', 'top selected perks on [publisher]'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {"type": "string", "description": "Publisher name (partial match OK)"},
                "publisher_id": {"type": "integer", "description": "Numeric publisher ID"},
                "days": {"type": "integer", "description": "Lookback window in days (default 30)"},
            },
        },
    },
    {
        "name": "get_supply_demand_gaps",
        "description": (
            "Identify supply-demand gaps: which advertisers are performing on other publishers but missing from a given publisher, "
            "or which publishers an advertiser is not running in. Also surfaces dead weight (provisioned but zero impressions in 30 days). "
            "Provide publisher_name OR advertiser_name, not both."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {
                    "type": "string",
                    "description": "Publisher to analyze (e.g. 'TextNow', 'Pinger'). Leave blank if using advertiser_name."
                },
                "advertiser_name": {
                    "type": "string",
                    "description": "Advertiser to analyze (e.g. 'Scrambly', 'BLD'). Leave blank if using publisher_name."
                }
            },
            "required": []
        },
    },
    {
        "name": "run_sql_query",
        "description": (
            "Execute an arbitrary ClickHouse SELECT query for questions not covered by other tools. "
            "Use the DATA DICTIONARY in your context to write correct SQL. "
            "Use for: any novel analytical question, multi-table joins, custom date ranges, "
            "cap/schedule config inspection, per-campaign payout lookups, "
            "serving group analysis, custom report recreation, or any query not covered by existing tools. "
            "Safety: SELECT-only, 500 row max by default. Always include a description of what you're querying. "
            "After getting results, present them clearly and add a sourcing callout: "
            "'> Queried: [description] — live ClickHouse'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Valid ClickHouse SQL SELECT statement. Use the DATA DICTIONARY for table/column names.",
                },
                "description": {
                    "type": "string",
                    "description": "One-line description of what this query retrieves, e.g. 'TurboTax campaign end dates and cap configs'",
                },
                "max_rows": {
                    "type": "integer",
                    "description": "Max rows to return (default 500, max 2000)",
                },
            },
            "required": ["sql", "description"],
        },
    },
    {
        "name": "get_ghost_campaigns",
        "description": (
            "Return the full list of ghost campaigns: active campaigns with high impressions + clicks "
            "but near-zero revenue (< $5 in last 7 days), older than 7 days. Includes per-campaign "
            "pixel/postback diagnosis. "
            "Use for: 'ghost brief', 'ghost campaigns', 'what campaigns are earning nothing', "
            "'campaigns with no revenue', 'show me the ghosts', 'zero revenue campaigns'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_low_fill_publishers",
        "description": (
            "Return publishers on post-transaction placements (checkout confirmation, order receipt, "
            "thank you pages, etc.) where fill rate is below 15% — meaning more than 85% of "
            "checkout sessions are receiving no offer. Uses a 7-day lookback window with a minimum of "
            "2,500 sessions over 7 days to filter out low-volume noise. "
            "Includes missed session count and revenue-at-risk estimate. "
            "Use for: 'fill rate', 'low fill rate', 'which publishers have low fill', 'sessions not getting offers', "
            "'offer fill', 'checkout fill', 'confirmation page fill', 'publishers underserving'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_top_revenue_opportunities",
        "description": (
            "Return top cross-publisher revenue gap opportunities: high-performing advertisers "
            "(active on 2+ publishers, >$10K/30d revenue) that are NOT yet active in high-volume publishers "
            "(>100K sessions/30d). Ranked by estimated monthly revenue. Shows total revenue at risk. "
            "Use for: 'revenue opportunities', 'what are we missing', 'what should we add', 'net-new revenue', "
            "'supply gaps', 'where should we add advertisers', 'uncaptured revenue', 'largest gaps'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_scout_status",
        "description": (
            "Return a system health snapshot: benchmark freshness, offer inventory count, "
            "queue depth, ClickHouse connectivity, and any data quality warnings. "
            "Use for: '@Scout status', 'how are you doing?', 'is Scout healthy?', "
            "'benchmark freshness', 'system check'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_revenue_today",
        "description": (
            "Return today's intraday revenue vs 30-day daily average, broken down by publisher. "
            "Use ONLY for 'how is revenue today / right now / so far'. "
            "Do NOT use for 'project / estimate / forecast / EOD / end of day / how will today land / "
            "after it ends' — use get_revenue_today_projection for those. "
            "Use for: 'how is revenue today', 'how are we doing today', 'how we looking', "
            "'today's revenue', 'revenue so far today', 'what's revenue at', 'how we doing'. "
            "Do NOT use run_sql_query for today's revenue — this tool exists specifically for this question. "
            "Deliver the value of the 'formatted' key in the result verbatim."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_revenue_today_projection",
        "description": (
            "Project today's END-OF-DAY revenue using a 90-day hour-of-day arrival curve and "
            "8-week same-weekday median baseline. Leads with one number plus a confidence range, "
            "pace vs typical at this hour, and typical same-weekday EOD comparison. "
            "Refuses to project before 10am CT "
            "or when the curve sample is thin. "
            "Use for: 'project today's revenue', 'estimate today's revenue', 'EOD revenue', "
            "'what will today land at', 'how much will we make today', 'forecast today', "
            "'after it ends', 'how much do you estimate our revenue for today'. "
            "Do NOT use for 'revenue so far today' — that's get_revenue_today. "
            "Deliver the value of the 'formatted' key in the result verbatim."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "run_offer_scraper",
        "description": (
            "Trigger an immediate offer inventory refresh from affiliate networks "
            f"({', '.join(SUPPORTED_NETWORKS)}). Takes ~2 minutes. Run when offer inventory "
            "is empty or stale. Updates offers_latest.json and posts the Scout Signal digest. "
            "Use for: 'refresh offers', 'run scraper', 'update offer inventory', "
            "'load benchmarks', 'inventory is empty', 'reload offers', 'fetch latest offers'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_pipeline_health",
        "description": (
            "Report on the Scout offer approval pipeline: total approved offers, "
            "stale offers (>7 days without Live/Done status), and oldest pending. "
            "Use for: 'pipeline health', 'how many offers went live', 'what is stuck in the queue', "
            "'pipeline status', 'are we launching offers', 'offer queue status'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_usage_report",
        "description": (
            "Return Scout usage statistics: queries per period, top users, most-used tools, avg response time. "
            "Admin-only — requires SCOUT_ADMIN_USER_ID env var match. "
            "Use for: 'scout usage', 'usage report', 'who uses scout', 'usage stats', "
            "'how often is scout used', 'who asks the most questions'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "requesting_user_id": {
                    "type": "string",
                    "description": "Slack user ID of the person asking — for admin authorization check.",
                }
            },
        },
    },
    {
        "name": "export_usage_log",
        "description": (
            "Dump raw (query → tools fired) pairs from usage_log so an admin can audit "
            "whether Scout's tool routing matched user intent. Admin-only. "
            "Use for: 'export usage', 'dump usage log', 'show usage entries', 'audit tool routing', "
            "'what tools fired for recent queries'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days":  {"type": "integer", "description": "Lookback window in days (default 30, clamped to 1..365).", "minimum": 1, "maximum": 365},
                "limit": {"type": "integer", "description": "Max entries to return (default 200, newest last; clamped to 1..500).", "minimum": 1, "maximum": 500},
                "requesting_user_id": {"type": "string", "description": "Slack user ID for admin gate."}
            },
        },
    },
    {
        "name": "record_entity_note",
        "description": (
            "Record publisher or advertiser knowledge in Scout's persistent learning store. "
            "Use when a team member explains a publisher's integration quirk, SDK limitation, or signal distortion, "
            "OR an advertiser's budget cap pattern, seasonality, attribution issue, or payout reliability. "
            "Publisher notes: set exclude_from_fill_rate=True when high session count with low fill is expected behavior "
            "(e.g., pre-purchase SDK calls, non-standard integration). "
            "Advertiser notes: budget caps, seasonal patterns, attribution quirks, campaign status context. "
            "Use for: '[entity] has a known limitation', 'note that [entity] does X', "
            "'log this about [entity]', 'exclude [publisher] from fill rate', "
            "'[advertiser] caps every [month]', '[advertiser] has attribution issues', "
            "'remember that [entity]...', 'scout, [entity] does X because...'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "entity_name": {
                    "type": "string",
                    "description": "Publisher or advertiser name (e.g., 'Button', 'TurboTax').",
                },
                "entity_type": {
                    "type": "string",
                    "enum": ["publisher", "advertiser"],
                    "description": "'publisher' for SDK/platform integrators, 'advertiser' for campaign/budget owners.",
                },
                "note": {
                    "type": "string",
                    "description": "The knowledge to record — what the team knows about this entity.",
                },
                "exclude_from_fill_rate": {
                    "type": "boolean",
                    "description": "Publishers only: True to suppress from Pulse fill rate signals (pre-purchase or non-standard integrations).",
                },
            },
            "required": ["entity_name", "entity_type", "note"],
        },
    },
    {
        "name": "forget_entity_note",
        "description": (
            "Drop a previously-recorded publisher or advertiser fact. "
            "Use when a team member tells Scout to forget, retract, or remove a learned note. "
            "Triggers: 'forget that about [entity]', 'scout, that was wrong about [entity]', "
            "'remove the note about [entity]', 'scratch that for [entity]', "
            "'unlearn [entity]', 'never mind about [entity]'. "
            "Idempotent — friendly message if no note exists."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "entity_name": {
                    "type": "string",
                    "description": "Publisher or advertiser name whose note should be dropped.",
                },
                "entity_type": {
                    "type": "string",
                    "enum": ["publisher", "advertiser"],
                    "description": "'publisher' or 'advertiser'.",
                },
            },
            "required": ["entity_name", "entity_type"],
        },
    },
    {
        "name": "why_entity_note",
        "description": (
            "Explain where a stored publisher/advertiser fact came from — returns the note, "
            "who taught Scout (Slack user_id), when, and the Slack permalink if available. "
            "Use when a team member challenges or audits Scout's beliefs about an entity. "
            "Triggers: 'why do you think [X] about [entity]', 'where did you learn that about [entity]', "
            "'who told you [entity] [does X]', 'source for [entity]', 'scout, justify [entity]'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "entity_name": {
                    "type": "string",
                    "description": "Publisher or advertiser name to audit.",
                },
                "entity_type": {
                    "type": "string",
                    "enum": ["publisher", "advertiser"],
                    "description": "Optional. Omit to search both sections.",
                },
            },
            "required": ["entity_name"],
        },
    },
    {
        "name": "get_offers_for_publisher",
        "description": (
            f"Return top affiliate offers (from {', '.join(SUPPORTED_NETWORKS)} inventory) that are "
            "a good fit for a specific publisher but not yet provisioned in their campaign set. "
            "Scored by estimated RPM using real MS conversion benchmarks. "
            "DIFFERENT from get_supply_demand_gaps — this surfaces net-new affiliate inventory, "
            "not advertisers already on the MS platform. "
            "Use for: 'offers for [partner]', 'what should we add to [partner]', "
            "'recommend offers for [partner]', 'what can we run on [partner]', "
            "'what's a good fit for [partner]', 'pitch ideas for [partner]', "
            "'affiliate offers for [partner]', 'new offers for [partner]'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {
                    "type": "string",
                    "description": "The publisher/partner name (e.g., 'TextNow', 'PCH', 'Metropolis').",
                }
            },
            "required": ["publisher_name"],
        },
    },
    {
        "name": "run_self_qa",
        "description": (
            "Run Scout's full self-QA suite — 15 representative questions covering every major intent "
            "(system status, ghost campaigns, offer search, revenue analysis, publisher health, "
            "campaign status, revenue projection, supply gaps, perkswall, pipeline health, "
            "multi-part question protocol, and data boundary tests). "
            "Each question is evaluated for pass/fail by checking expected content signals. "
            "Use when the user says: 'QA yourself', 'self test', 'run QA', 'test yourself', "
            "'run the QA suite', 'scout QA', 'run self-qa', 'check yourself'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_pulse_summary",
        "description": (
            "Returns which monitoring signals have fired today (cap, velocity, ghost, fill, CVR anomaly, expiration). "
            "Use for: 'what did Scout flag today', 'what alerted this morning', 'what signals fired', "
            "'did anything fire', 'any alerts today', 'recent Scout signals', 'monitor recap'. "
            "Returns fired_today dict with per-signal booleans and currently_active list. "
            "Returns has_pulse=False with a message when no signals have fired yet today."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_scout_config",
        "description": (
            "Return Scout's current active configuration: scoring thresholds, signal thresholds, "
            "health-check intervals, supported networks, and Pulse schedule. "
            "Use when the team asks: 'what are Scout's thresholds', 'what's the fill rate cutoff', "
            "'how does Scout decide which offers to surface', 'what's the RPM floor', "
            "'what networks does Scout support', 'show me Scout's config', 'what are the velocity "
            "thresholds', 'when does the pulse run', 'health check settings'. "
            "Reads from config/scout_thresholds.json — always reflects current production values, "
            "no need to dig through source code."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_exposure_rate_anomalies",
        "description": (
            "Find publisher-campaign pairs where yesterday's exposure conversion rate dropped significantly "
            "vs. the 7-day baseline. Exposure CVR = conversions / impressions (measures what fraction of "
            "ad exposures convert — intentionally uses impressions denominator for anomaly detection). "
            "NOTE: this is NOT the canonical CVR (conversions / clicks). Use run_sql_query with "
            "CVR = conversions/clicks when answering general CVR questions. "
            "Only surfaces high-value campaigns (avg payout >= $50) with enough volume "
            "(7d impressions >= 5000) to make the signal actionable. "
            "Use when the team asks: 'which campaigns dropped CVR', 'conversion rate anomalies', "
            "'why are conversions down for X', 'CVR drops', 'postback issues', "
            "'which campaigns stopped converting', 'CVR regression'. "
            "Returns publisher, campaign, exposure CVR yesterday vs 7d average, delta %, and payout."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "min_impressions_7d": {
                    "type": "integer",
                    "description": "Minimum 7d impressions to include a campaign. Default: 5000.",
                },
                "min_payout": {
                    "type": "number",
                    "description": "Minimum avg payout per conversion in USD. Default: 50.",
                },
                "drop_pct": {
                    "type": "number",
                    "description": "Minimum CVR drop percentage to flag. Default: 30.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_expiring_campaigns",
        "description": (
            "Find active campaigns expiring within the next N days (default 7). "
            "Includes last-7d impression volume, active publisher count, and revenue "
            "so you can distinguish high-impact expirations from low-traffic ones. "
            "Use when the team asks: 'what campaigns are expiring', 'upcoming campaign endings', "
            "'campaigns ending this week', 'expiration warnings', 'renewal needed', "
            "'which offers are about to expire', 'campaign end dates'. "
            "Returns campaign, advertiser, end date, days remaining, impression/publisher/revenue activity."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "warning_days": {
                    "type": "integer",
                    "description": "Look-ahead window in days. Default: 7.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_publisher_revenue_trends",
        "description": (
            "Publisher velocity: identifies publishers trending significantly up or down in revenue. "
            "Uses canonical annualized comparison: ((rev_7d / 7) × 30 − rev_30d) / rev_30d × 100. "
            "Fires for publishers with pct_delta < −25% (velocity down) or > +20% (velocity up), "
            "minimum $5K revenue over the past 30 days. "
            "Use when the team asks: 'which publishers are trending up/down', 'revenue trends', "
            "'publisher velocity', 'who dropped revenue', 'which publishers improved this week', "
            "'publisher performance trends'. "
            "Returns publisher_name, rev_7d, rev_30d, pct_delta, direction ('up' or 'down')."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Ignored — always uses 7d/30d canonical window.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_advertiser_revenue_trends",
        "description": (
            "Advertiser revenue trends: compare each advertiser's actual revenue over the last N days "
            "against their historical median for the same period length (8 prior periods). "
            "Aggregated across all publishers — cross-publisher view of advertiser performance. "
            "Use when the team asks: 'which advertisers are trending up/down', 'advertiser revenue trends', "
            "'Capital One revenue vs historical', 'which advertisers improved this week', "
            "'advertiser performance compared to baseline', 'who dropped advertiser-side revenue'. "
            "Returns advertiser name, actual revenue, expected revenue, delta %, and trend direction."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Period length in days. Default: 7.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_publisher_fleet_health",
        "description": (
            "Fleet-level publisher health using statistical (σ-based) baseline. "
            "Classifies publishers into Act Now (>=2σ drop, >=$500 gap) and Watch (>=1.5σ, >=$200). "
            "Use for: 'how are all publishers doing?', 'Monday health report', 'fleet health', "
            "'which publishers need attention?', 'publisher overview'. "
            "Optional: days (default 7, max 90)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 7, "minimum": 1, "maximum": 90},
            },
            "required": [],
        },
    },
    {
        "name": "list_thresholds",
        "description": (
            "Return all active Scout monitor thresholds plus override metadata. "
            "Use when: 'what are the current thresholds', 'list scout thresholds', "
            "'show me threshold overrides', 'which thresholds have been changed', "
            "'what's the current cap_alert_pct', 'show monitor settings'."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_threshold_history",
        "description": (
            "Return recent threshold-change events from the changelog. "
            "Optional 'key' filter (e.g. 'signals.cap_alert_pct') and 'limit' (default 50). "
            "Use when: 'who changed the threshold', 'threshold history', 'why is X set to Y', "
            "'when did we tune the cap alert', 'show changelog for fill_rate_min_sessions_7d'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "description": "Optional filter like 'signals.cap_alert_pct'. Omit for all changes.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max entries to return, newest first. Default 50, max 500.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "set_threshold",
        "description": (
            "Admin-only — requires SCOUT_THRESHOLD_ADMINS env match. "
            "Write a runtime override for one Scout threshold; persisted to "
            "data/threshold_overrides.json and reloaded immediately. Always require a reason. "
            "Use when an admin says: 'change cap_alert_pct to 80', 'set the fill rate threshold to 3000', "
            "'tune ghost_recency_hours to 72', 'lower velocity_down_threshold_pct to -30'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "section": {
                    "type": "string",
                    "description": "Top-level section in scout_thresholds.json (e.g. 'signals', 'digest', 'health').",
                },
                "key": {
                    "type": "string",
                    "description": "Threshold name within the section (e.g. 'cap_alert_pct').",
                },
                "value": {
                    "description": "New value (number for numeric thresholds; type matches the config schema).",
                },
                "reason": {
                    "type": "string",
                    "description": "Why the threshold is changing — recorded permanently in the changelog.",
                },
            },
            "required": ["section", "key", "value", "reason"],
        },
    },
    {
        "name": "force_run_monitor",
        "description": (
            "Admin-only — requires SCOUT_THRESHOLD_ADMINS env match. "
            "Run a silent-monitor signal immediately and post results to #scout-qa. "
            "Use when an admin says: 'force run the cap monitor', 'rerun ghost detection now', "
            "'test the fill rate alert', 'fire velocity monitor on demand', 'force run cvr'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "monitor": {
                    "type": "string",
                    "description": "Which monitor to fire (e.g. cap, velocity, ghost, fill, cvr, expiration, revenue).",
                },
            },
            "required": ["monitor"],
        },
    },
]
