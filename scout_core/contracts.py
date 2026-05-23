"""Typed data contracts shared by ms-scout and ms-demand-feed.

These dataclasses define the canonical shapes that flow through the offer
pipeline: scraped raw payload → normalized offer → digest candidate →
queue draft → campaign request. Slack rendering is represented as a
serializable JSON payload (SlackDigestBlock), keeping the Slack SDK out
of the data layer.

Design rules:
  * Additive only. Existing dict-based callers (scout_types.Offer,
    FormattedOffer, Brief) continue to work; these dataclasses are new
    consumers and producers, not a replacement.
  * No Slack SDK imports here. SlackDigestBlock is pure JSON.
  * No ClickHouse or HTTP imports here. Contracts must be import-cheap.
  * `from_dict` / `to_dict` helpers exist so the boundary with legacy
    dict-shaped code stays explicit.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

# Geo normalization is injected by the producer (offer_scraper registers
# itself at import time). Keeping the dependency one-way means scout_core
# stays import-cheap and free of producer-side imports.
_geo_normalizer: Optional[Callable[[str], str]] = None


def set_geo_normalizer(fn: Callable[[str], str]) -> None:
    """Register the canonical geo-normalization function.

    ms-scout / ms-demand-feed call this once at startup with
    `offer_scraper.normalize_geo`. Tests can register a stub.
    """
    global _geo_normalizer
    _geo_normalizer = fn


# ── RawOffer ──────────────────────────────────────────────────────────
# What offer_scraper.py produces per row, before normalization. Keep
# this loose — scrapers add network-specific fields that downstream
# code ignores. The required fields are the bare minimum needed to
# normalize and dedupe.

@dataclass
class RawOffer:
    network: str
    offer_id: str
    advertiser: str = ""
    title: str = ""
    description: str = ""
    payout: str = ""
    payout_type: str = ""
    currency: str = "USD"
    geo_raw: str = ""
    category: str = ""
    icon_url: str = ""
    hero_url: str = ""
    banner_url: str = ""
    tracking_url: str = ""
    preview_url: str = ""
    status: str = ""
    date_scraped: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RawOffer":
        known = {f.name for f in cls.__dataclass_fields__.values()} - {"extra"}
        extra = {k: v for k, v in d.items() if k not in known}
        kwargs = {k: d[k] for k in known if k in d}
        return cls(extra=extra, **kwargs)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        extra = d.pop("extra") or {}
        d.update(extra)
        return d


# ── NormalizedOffer ───────────────────────────────────────────────────
# Geo collapsed to the canonical Notion-select set, payout typed,
# network tagged. This is the shape downstream ranking and rendering
# code should consume.

@dataclass
class NormalizedOffer:
    network: str
    offer_id: str
    advertiser: str
    title: str = ""
    description: str = ""
    payout_num: Optional[float] = None
    payout_type: str = ""           # "CPA" | "CPL" | "CPI" | "RevShare" | ...
    payout_raw: str = ""
    currency: str = "USD"
    geo: str = "Unknown"            # canonical: "US Only" | "US + CA" | "EU" | ...
    geo_raw: str = ""
    category: str = ""
    icon_url: str = ""
    hero_url: str = ""
    banner_url: str = ""
    tracking_url: str = ""
    status: str = ""
    date_scraped: str = ""

    @property
    def uid(self) -> str:
        """Stable cross-network identifier."""
        return f"{self.network}:{self.offer_id}"

    @classmethod
    def normalize_geo(cls, raw: str) -> str:
        """Canonical geo string. Delegates to the producer-registered
        normalizer (see `set_geo_normalizer`). Producers (ms-scout,
        ms-demand-feed) must register `offer_scraper.normalize_geo` at
        startup; tests may register a stub."""
        if _geo_normalizer is None:
            raise RuntimeError(
                "scout_core.contracts: no geo normalizer registered. "
                "Call scout_core.contracts.set_geo_normalizer(fn) at startup."
            )
        return _geo_normalizer(raw)

    @classmethod
    def from_raw(cls, raw: RawOffer) -> "NormalizedOffer":
        try:
            payout_num: Optional[float] = float(raw.payout) if raw.payout else None
        except (TypeError, ValueError):
            payout_num = None
        return cls(
            network=raw.network,
            offer_id=raw.offer_id,
            advertiser=raw.advertiser,
            title=raw.title,
            description=raw.description,
            payout_num=payout_num,
            payout_type=raw.payout_type,
            payout_raw=raw.payout,
            currency=raw.currency or "USD",
            geo=cls.normalize_geo(raw.geo_raw),
            geo_raw=raw.geo_raw,
            category=raw.category,
            icon_url=raw.icon_url,
            hero_url=raw.hero_url,
            banner_url=raw.banner_url,
            tracking_url=raw.tracking_url,
            status=raw.status,
            date_scraped=raw.date_scraped,
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "NormalizedOffer":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: d[k] for k in known if k in d})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── DigestCandidate ───────────────────────────────────────────────────
# A NormalizedOffer plus the scoring/ranking signals the digest pipeline
# attaches. `why` is human-readable rationale shown in Slack.

@dataclass
class DigestCandidate:
    offer: NormalizedOffer
    score: float = 0.0
    fit_tier: str = ""              # "S" | "A" | "B" | "C" | ""
    why: str = ""
    performance_context: str = ""
    risk_flag: str = ""             # non-empty suppresses "Add to Queue" button

    def to_dict(self) -> dict[str, Any]:
        return {
            "offer": self.offer.to_dict(),
            "score": self.score,
            "fit_tier": self.fit_tier,
            "why": self.why,
            "performance_context": self.performance_context,
            "risk_flag": self.risk_flag,
        }


# ── SlackDigestBlock ──────────────────────────────────────────────────
# Pure JSON payload — what gets POSTed to Slack. No SDK imports. This
# is the boundary between ms-demand-feed (produces blocks) and ms-scout
# (POSTs them).

@dataclass
class SlackDigestBlock:
    blocks: list[dict[str, Any]]
    text: str = ""                  # fallback for push previews
    channel: Optional[str] = None
    thread_ts: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"blocks": self.blocks, "text": self.text}
        if self.channel is not None:
            d["channel"] = self.channel
        if self.thread_ts is not None:
            d["thread_ts"] = self.thread_ts
        return d


# ── QueueDraft ────────────────────────────────────────────────────────
# A NormalizedOffer plus the AI-generated campaign fragment, sitting in
# the draft/approval state that the Vamsee platform queue page reads.

@dataclass
class Approval:
    state: str = "pending"          # "pending" | "approved" | "rejected"
    approver: str = ""
    approved_at: Optional[str] = None  # ISO-8601 UTC
    note: str = ""


@dataclass
class QueueDraft:
    draft_id: str
    offer: NormalizedOffer
    ai_copy: dict[str, Any] = field(default_factory=dict)
    # ai_copy keys: headline, short_headline, description, short_desc,
    # cta_yes, cta_no, goal_title — matches Brief.ai_copy today.
    estimated_rpm: Optional[float] = None
    perf_ctx: str = ""
    risk_flag: str = ""
    approval: Approval = field(default_factory=Approval)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "draft_id": self.draft_id,
            "offer": self.offer.to_dict(),
            "ai_copy": self.ai_copy,
            "estimated_rpm": self.estimated_rpm,
            "perf_ctx": self.perf_ctx,
            "risk_flag": self.risk_flag,
            "approval": asdict(self.approval),
            "created_at": self.created_at,
        }


# ── CampaignRequest ───────────────────────────────────────────────────
# What ms-demand-feed hands to the MS Platform once a QueueDraft is
# approved. The platform owns campaign creation; this is the contract.

@dataclass
class CampaignRequest:
    draft_id: str
    offer: NormalizedOffer
    ai_copy: dict[str, Any]
    approver: str
    approved_at: str                # ISO-8601 UTC
    dry_run: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "draft_id": self.draft_id,
            "offer": self.offer.to_dict(),
            "ai_copy": self.ai_copy,
            "approver": self.approver,
            "approved_at": self.approved_at,
            "dry_run": self.dry_run,
        }

    @classmethod
    def from_approved_draft(cls, draft: QueueDraft, dry_run: bool = False) -> "CampaignRequest":
        if draft.approval.state != "approved":
            raise ValueError(f"draft {draft.draft_id} is not approved (state={draft.approval.state})")
        if not draft.approval.approved_at:
            raise ValueError(f"approved draft {draft.draft_id} missing approved_at")
        if not (draft.approval.approver or "").strip():
            raise ValueError(f"approved draft {draft.draft_id} missing approver")
        return cls(
            draft_id=draft.draft_id,
            offer=draft.offer,
            ai_copy=draft.ai_copy,
            approver=draft.approval.approver,
            approved_at=draft.approval.approved_at,
            dry_run=dry_run,
        )
