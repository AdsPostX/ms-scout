---
globs: scout_digest.py
---

# Rules for scout_digest.py

- Scoring and dedup logic is sensitive — any change to scoring weights or dedup windows must be explicitly called out in the PR description with before/after examples.
- The payout type normalization map here intentionally diverges from `offer_scraper.py` — they serve different purposes. Do not unify them without a separate investigation.
