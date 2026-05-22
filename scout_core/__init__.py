"""scout_core — shared contracts and utilities imported by both ms-scout
(the Slack worker) and ms-demand-feed (the data/jobs service).

Phase 2 foundation. Additive: existing dict-based code paths in scout_agent,
offer_scraper, scout_digest continue to work unchanged. New code in
ms-demand-feed and forthcoming queue/campaign endpoints uses the typed
dataclasses defined here.
"""

from scout_core.contracts import (
    Approval,
    CampaignRequest,
    DigestCandidate,
    NormalizedOffer,
    QueueDraft,
    RawOffer,
    SlackDigestBlock,
)

__all__ = [
    "Approval",
    "CampaignRequest",
    "DigestCandidate",
    "NormalizedOffer",
    "QueueDraft",
    "RawOffer",
    "SlackDigestBlock",
]
