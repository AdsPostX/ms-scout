"""Tests for scout_core.contracts — additive typed contracts for the
offer → normalized → digest → queue → campaign pipeline.

These tests pin behavior we'll rely on across services:
  * RawOffer round-trips through a dict (with unknown keys preserved in `extra`)
  * NormalizedOffer.from_raw parses payout and calls the canonical normalize_geo
  * NormalizedOffer.uid is stable and network-scoped
  * QueueDraft → CampaignRequest requires explicit approval
"""

from __future__ import annotations

import pytest

from scout_core.contracts import (
    Approval,
    CampaignRequest,
    DigestCandidate,
    NormalizedOffer,
    QueueDraft,
    RawOffer,
    SlackDigestBlock,
)


def test_raw_offer_round_trip_preserves_unknown_fields():
    src = {
        "network": "Impact",
        "offer_id": "abc123",
        "advertiser": "Acme",
        "payout": "12.50",
        "geo_raw": "United States",
        "weird_network_field": "carry me through",
    }
    raw = RawOffer.from_dict(src)
    assert raw.network == "Impact"
    assert raw.advertiser == "Acme"
    assert raw.extra == {"weird_network_field": "carry me through"}
    flat = raw.to_dict()
    assert flat["weird_network_field"] == "carry me through"
    assert flat["network"] == "Impact"


def test_normalized_offer_from_raw_parses_payout_and_geo():
    raw = RawOffer(
        network="CJ",
        offer_id="999",
        advertiser="Beta LLC",
        payout="8.00",
        payout_type="CPA",
        geo_raw="United States",
    )
    norm = NormalizedOffer.from_raw(raw)
    assert norm.payout_num == 8.0
    assert norm.payout_raw == "8.00"
    assert norm.geo == "US Only"
    assert norm.uid == "CJ:999"


def test_normalized_offer_handles_non_numeric_payout():
    raw = RawOffer(network="X", offer_id="1", advertiser="A", payout="see terms")
    norm = NormalizedOffer.from_raw(raw)
    assert norm.payout_num is None
    assert norm.payout_raw == "see terms"


def test_normalized_offer_empty_geo_is_unknown():
    raw = RawOffer(network="X", offer_id="1", advertiser="A", geo_raw="")
    norm = NormalizedOffer.from_raw(raw)
    assert norm.geo == "Unknown"


def test_digest_candidate_to_dict_embeds_offer():
    offer = NormalizedOffer(network="Impact", offer_id="1", advertiser="Acme")
    cand = DigestCandidate(offer=offer, score=42.0, fit_tier="A", why="strong fit")
    d = cand.to_dict()
    assert d["score"] == 42.0
    assert d["offer"]["advertiser"] == "Acme"


def test_slack_digest_block_omits_optional_channel():
    block = SlackDigestBlock(blocks=[{"type": "section"}], text="hi")
    d = block.to_dict()
    assert "channel" not in d
    assert d["text"] == "hi"


def test_queue_draft_default_approval_is_pending():
    offer = NormalizedOffer(network="N", offer_id="1", advertiser="A")
    draft = QueueDraft(draft_id="d1", offer=offer)
    assert draft.approval.state == "pending"
    assert draft.created_at  # ISO timestamp populated


def test_campaign_request_requires_approval():
    offer = NormalizedOffer(network="N", offer_id="1", advertiser="A")
    draft = QueueDraft(draft_id="d1", offer=offer)
    with pytest.raises(ValueError):
        CampaignRequest.from_approved_draft(draft)


def test_campaign_request_from_approved_draft():
    offer = NormalizedOffer(network="N", offer_id="1", advertiser="A")
    draft = QueueDraft(
        draft_id="d1",
        offer=offer,
        ai_copy={"headline": "Test"},
        approval=Approval(state="approved", approver="sidd", approved_at="2026-05-22T12:00:00+00:00"),
    )
    req = CampaignRequest.from_approved_draft(draft, dry_run=True)
    assert req.draft_id == "d1"
    assert req.approver == "sidd"
    assert req.dry_run is True
    assert req.ai_copy == {"headline": "Test"}
