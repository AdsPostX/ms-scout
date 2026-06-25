---
globs: offer_scraper.py
---

# Rules for offer_scraper.py

- Network-specific parsers are brittle — flag any change that could silently swallow offers (empty list returns, exception swallowing without logging).
- `_NETWORK_ENDPOINTS` is the canonical URL registry (14 entries). Do not hardcode URLs in function bodies.
- Scraper runs every 6h via the demand-feed service. A parser regression goes unnoticed for up to 6h — always add a smoke assertion when adding a new parser.
