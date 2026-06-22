# Scout — MCP Integrations

## ClickHouse (`mcp__mcp-clickhouse__`)
**Status:** Active  
**Use for:** Ad-hoc queries, schema exploration, verifying SQL shape before writing handler code.  
**Key tables:** `ms_events.conversions`, `ms_events.clicks`, `from_airbyte_campaigns`  
**Pattern:** `run_query` → verify shape (column names, types, row grain) → write query fn in `queries_*.py`  
**Note:** Run the raw query here first before touching any handler. Most bugs are data bugs.

## Slack (`mcp__18ab42c2-6196-4409-a18c-7eb96900aafe__`)
**Status:** Active  
**Bot scopes:** `channels:read`, `chat:write`, `files:read`, `users:read`, `app_mentions:read`  
**Use for:** Reading threads for context, searching for prior questions, checking channel names.  
**Test channel:** `#bot-qa` — all Scout card renders verified here before production.

## Notion (`mcp__notion-mosci__`)
**Status:** Active, read-only for Scout  
**Use for:** Reading offer pipeline, demand queue — never write-back from Scout handlers.  
**Note:** Scout handlers must not call Notion write tools. `scout_notion.py` is the read-only boundary.
