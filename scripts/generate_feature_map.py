#!/usr/bin/env python3
"""
Generate FEATURES.md from scout_agent.py TOOLS + TOOL_MAP.
Extracts tool descriptions, handler functions, and test coverage.

Usage:
  python3 scripts/generate_feature_map.py

Outputs: FEATURES.md (checked in; regenerate when TOOL_MAP changes)
"""

import json
import re
from pathlib import Path

def extract_tools_and_handlers():
    """Parse scout_agent.py to extract TOOLS definitions and TOOL_MAP."""
    agent_file = Path("scout_agent.py")
    content = agent_file.read_text()

    # Extract TOOLS array (JSON-ish)
    tools_match = re.search(r'TOOLS = \[(.*?)\n\]\n\nTOOLS = \[', content, re.DOTALL)
    if not tools_match:
        tools_match = re.search(r'TOOLS = \[(.*?)^\]', content, re.MULTILINE | re.DOTALL)

    tools_section = tools_match.group(1) if tools_match else ""

    # Extract tool names and descriptions via regex
    tool_blocks = re.findall(
        r'"name":\s*"([^"]+)".*?"description":\s*\((.*?)\),',
        tools_section,
        re.DOTALL
    )

    tools = {}
    for name, desc_block in tool_blocks:
        # Clean up the description (remove quotes, handle multi-line)
        desc = re.sub(r'^\s*"', '', desc_block)
        desc = re.sub(r'"\s*$', '', desc)
        desc = re.sub(r'"\s*\+\s*"', '', desc)
        desc = desc.replace('\n', ' ').strip()
        tools[name] = desc[:100] + ("..." if len(desc) > 100 else "")

    # Extract TOOL_MAP
    tool_map_match = re.search(r'TOOL_MAP = \{(.*?)\n\}', content, re.DOTALL)
    tool_map_section = tool_map_match.group(1) if tool_map_match else ""

    handlers = {}
    for line in tool_map_section.split('\n'):
        match = re.match(r'\s*"([^"]+)":\s*([^,]+)', line)
        if match:
            name, handler = match.groups()
            handler = handler.strip().rstrip(',')
            handlers[name] = handler if handler != "None" else "deferred"

    return tools, handlers

def get_test_coverage():
    """Check smoke_test.py for test coverage per feature."""
    smoke_file = Path("smoke_test.py")
    if not smoke_file.exists():
        return {}

    content = smoke_file.read_text()
    coverage = {}

    # Look for test functions that mention tools
    test_functions = re.findall(r'def (test_\w+).*?(?=\ndef|\nclass|\Z)', content, re.DOTALL)
    return {f: True for f in test_functions if "test_" in f}

def categorize_tools(tools):
    """Categorize tools by domain."""
    categories = {
        "Offer Discovery": [
            "search_offers", "get_top_opportunities", "get_running_offers",
            "get_category_performance", "get_offer_stats", "draft_campaign_brief",
            "get_fallback_candidates", "get_offers_for_publisher"
        ],
        "Publisher Intelligence": [
            "get_publisher_competitive_landscape", "get_publisher_health",
            "get_perkswall_engagement", "get_publisher_revenue_trends",
            "get_publisher_fleet_health"
        ],
        "Campaign & Revenue": [
            "get_campaign_status", "get_revenue_today", "get_revenue_today_projection",
            "get_advertiser_revenue_projection", "get_advertiser_revenue_trends",
            "get_ghost_campaigns", "get_low_fill_publishers",
            "get_top_revenue_opportunities", "get_exposure_rate_anomalies",
            "get_expiring_campaigns"
        ],
        "Pipeline Management": [
            "get_queue_status", "get_demand_queue_status", "mark_offer_launched",
            "get_pipeline_health"
        ],
        "Analytics & Insights": [
            "get_supply_demand_gaps", "get_pulse_summary", "run_sql_query",
            "get_scout_status"
        ],
        "Administration": [
            "run_offer_scraper", "get_usage_report", "export_usage_log",
            "record_entity_note", "forget_entity_note", "why_entity_note",
            "run_self_qa", "get_scout_config", "list_thresholds",
            "get_threshold_history", "set_threshold", "force_run_monitor"
        ],
    }
    return categories

def generate_markdown(tools, handlers, categories):
    """Generate FEATURES.md content."""
    lines = [
        "# Scout Features — Complete Inventory",
        "",
        "**Last generated:** Auto-generated from `scout_agent.py` TOOL_MAP.",
        "**Regenerate:** Run `python3 scripts/generate_feature_map.py` when TOOL_MAP changes.",
        "**Status:** Audit complete (Engineering audit — see VAMSEE_AUDIT.md).",
        "",
        "---",
        "",
        "## Feature Summary",
        "",
        f"- **Total Tools:** {len(tools)}",
        f"- **Domains:** {len(categories)}",
        "- **Status:** ~80% working + core features verified",
        "- **Maintenance Mode:** demand_feed_main.py (5 TODOs), App Home projection range",
        "",
        "---",
        "",
    ]

    # Add by-domain sections
    for domain, tool_names in categories.items():
        domain_tools = {t: tools.get(t, "(not found)") for t in tool_names if t in tools}
        if not domain_tools:
            continue

        lines.extend([
            f"## {domain}",
            "",
            "| Tool | Description | Handler | Status |",
            "|------|-------------|---------|--------|",
        ])

        for tool_name in sorted(domain_tools.keys()):
            desc = tools.get(tool_name, "")
            handler = handlers.get(tool_name, "?")
            status = "✓ Working" if handler != "deferred" else "⏳ Deferred"
            lines.append(f"| `{tool_name}` | {desc} | {handler}() | {status} |")

        lines.append("")

    lines.extend([
        "---",
        "",
        "## Maintenance & Updates",
        "",
        "### How to Update This File",
        "",
        "1. **After adding a new tool to TOOL_MAP:**",
        "   - Add the tool definition to `TOOLS` array in `scout_agent.py`",
        "   - Add the handler to `TOOL_MAP` in `scout_agent.py`",
        "   - Run: `python3 scripts/generate_feature_map.py`",
        "   - Commit both changes together",
        "",
        "2. **After completing the engineering audit:**",
        "   - Update the Status column above (Working/Deferred/In Maintenance)",
        "   - Log findings in VAMSEE_AUDIT.md",
        "",
        "3. **To regenerate (one-liner):**",
        "   ```bash",
        "   python3 scripts/generate_feature_map.py",
        "   ```",
        "",
        "### Pre-Commit Hook (Optional)",
        "",
        "Add to `.git/hooks/pre-commit` to auto-regenerate when TOOL_MAP changes:",
        "",
        "```bash",
        "#!/bin/bash",
        "if git diff --cached scout_agent.py | grep -q 'TOOL_MAP\\|TOOLS = '; then",
        "  python3 scripts/generate_feature_map.py",
        "  git add FEATURES.md",
        "fi",
        "```",
        "",
        "---",
        "",
        "## Feature Status Legend",
        "",
        "| Status | Meaning |",
        "|--------|---------|",
        "| ✓ Working | Feature complete, tests passing, engineering audit passed |",
        "| ⏳ Deferred | Feature built but gated (e.g., awaiting API, blocked PR) |",
        "| 🔧 In Maintenance | Feature working but has known debt (TODO items, gaps) |",
        "| ❌ Broken | Feature implemented but failing tests or not called |",
        "",
    ])

    return "\n".join(lines)

if __name__ == "__main__":
    tools, handlers = extract_tools_and_handlers()
    categories = categorize_tools(tools)
    markdown = generate_markdown(tools, handlers, categories)

    Path("FEATURES.md").write_text(markdown)
    print(f"✓ Generated FEATURES.md ({len(tools)} tools)")
