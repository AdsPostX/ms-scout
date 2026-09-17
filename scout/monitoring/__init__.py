"""scout.monitoring — the demand-feed background alert daemons.

Extracted out of demand_feed_main.py (which had accreted this alongside the
scraper it was actually named for) into its own module boundary, per
DESIGN.md's Service Topology decision: this is a module split within the
same ms-demand-feed process, not a new deployed service — the coupling
DESIGN.md flagged (a scraper hang sharing failure blast radius with revenue
alerting) is a code-organization problem, not a traffic/isolation one.
"""
