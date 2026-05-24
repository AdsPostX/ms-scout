"""Tests for P3.2 — MS Platform campaign creation stub.

Covers:
  T1:  GET  /queue/config → 200 with mode=dry_run when WEBHOOK_URL not set
  T2:  GET  /queue/config → reports webhook_url_set=true when env set (still dry_run)
  T3:  GET  /queue/config → mode=live when WEBHOOK_URL set + DRY_RUN=false
  T4:  GET  /queue/config → queue_depth counts pending/approved/rejected
  T5:  POST /queue/approve (dry_run) → campaign.would_send contains offer + ai_copy
  T6:  POST /queue/approve (dry_run) → campaign.would_send.draft_id matches draft
"""

from __future__ import annotations

import importlib
import json
import os
import socketserver
import sys
import threading
import types
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

_WT_ROOT = Path(__file__).parent.parent
if str(_WT_ROOT) not in sys.path:
    sys.path.insert(0, str(_WT_ROOT))


# ---------------------------------------------------------------------------
# Stub heavy deps
# ---------------------------------------------------------------------------

def _stub(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


try:
    importlib.import_module("anthropic")
except ImportError:
    from unittest.mock import MagicMock
    _ant = _stub("anthropic")
    _ant.Anthropic = MagicMock
    _ant.types = _stub("anthropic.types")
    sys.modules["anthropic.types"] = _ant.types

for _dep in ("clickhouse_connect", "queries", "scout_types"):
    try:
        importlib.import_module(_dep)
    except ImportError:
        _stub(_dep)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _start_server(tmp_dir: Path):
    import demand_feed_main as dm
    importlib.reload(dm)
    dm._DATA_DIR      = tmp_dir
    dm._OFFERS_FILE   = tmp_dir / "offers_latest.json"
    dm._SCRAPER_STATE = tmp_dir / "scraper_state.json"
    dm._QUEUE_FILE    = tmp_dir / "queue.json"
    server = socketserver.TCPServer(("", 0), dm._OffersHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port, dm


def _post(port: int, path: str, body: dict) -> tuple[int, dict]:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://localhost:{port}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read())
        except Exception:
            return exc.code, {}


def _get(port: int, path: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(f"http://localhost:{port}{path}", timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read())
        except Exception:
            return exc.code, {}


_VALID_OFFER = {
    "network":    "impact",
    "offer_id":   "offer_xyz",
    "advertiser": "Acme Corp",
    "title":      "Acme Widget — $20 CPA",
    "payout_num": 20.0,
    "payout_type": "CPA",
}

_VALID_AI_COPY = {
    "headline":    "Shop Acme Today",
    "description": "Get $20 back on your first Acme order.",
    "cta_yes":     "Claim Offer",
    "cta_no":      "Maybe Later",
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestQueueConfig(unittest.TestCase):
    """GET /queue/config — platform integration status endpoint.

    The handler reads env vars at *request* time (not server-start time), so
    patch.dict must wrap the actual HTTP call, not the server construction.
    """

    def setUp(self):
        import tempfile
        self._tmp_obj = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_obj.name)
        self.server, self.port, self.dm = _start_server(self.tmp)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self._tmp_obj.cleanup()

    def test_config_dry_run_default(self):
        """GET /queue/config → mode=dry_run when WEBHOOK_URL not set."""
        with patch.dict(os.environ, {
            "CAMPAIGN_CREATE_WEBHOOK_URL": "",
            "CAMPAIGN_CREATE_DRY_RUN":     "true",
        }):
            status, body = _get(self.port, "/queue/config")
        self.assertEqual(status, 200)
        cc = body.get("campaign_creation", {})
        self.assertEqual(cc.get("mode"), "dry_run")
        self.assertFalse(cc.get("webhook_url_set"))
        self.assertFalse(cc.get("api_key_set"))
        self.assertTrue(cc.get("dry_run_flag"))

    def test_config_webhook_url_set_still_dry_run(self):
        """GET /queue/config → webhook_url_set=true but mode=dry_run when flag=true."""
        with patch.dict(os.environ, {
            "CAMPAIGN_CREATE_WEBHOOK_URL": "https://platform.example.com/campaigns",
            "CAMPAIGN_CREATE_API_KEY":     "",
            "CAMPAIGN_CREATE_DRY_RUN":     "true",
        }):
            status, body = _get(self.port, "/queue/config")
        self.assertEqual(status, 200)
        cc = body.get("campaign_creation", {})
        self.assertTrue(cc.get("webhook_url_set"))
        self.assertEqual(cc.get("mode"), "dry_run")   # still dry_run

    def test_config_mode_live_when_both_set(self):
        """GET /queue/config → mode=live when WEBHOOK_URL set and DRY_RUN=false."""
        with patch.dict(os.environ, {
            "CAMPAIGN_CREATE_WEBHOOK_URL": "https://platform.example.com/campaigns",
            "CAMPAIGN_CREATE_API_KEY":     "secret-token",
            "CAMPAIGN_CREATE_DRY_RUN":     "false",
        }):
            status, body = _get(self.port, "/queue/config")
        self.assertEqual(status, 200)
        cc = body.get("campaign_creation", {})
        self.assertEqual(cc.get("mode"), "live")
        self.assertFalse(cc.get("dry_run_flag"))
        self.assertTrue(cc.get("api_key_set"))

    def test_config_queue_depth_counts(self):
        """GET /queue/config → queue_depth reflects actual draft states."""
        # Create two drafts, approve one, reject one
        _, b1 = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        _, b2 = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        d1, d2 = b1["draft_id"], b2["draft_id"]

        with patch.object(self.dm, "_fire_campaign_creation",
                          return_value={"status": "dry_run"}):
            _post(self.port, "/queue/approve", {"draft_id": d1, "approver": "sidd"})
        _post(self.port, "/queue/reject", {"draft_id": d2, "approver": "sidd"})

        # Create a third that stays pending
        _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})

        status, body = _get(self.port, "/queue/config")
        self.assertEqual(status, 200)
        depth = body.get("queue_depth", {})
        self.assertEqual(depth.get("pending"),  1)
        self.assertEqual(depth.get("approved"), 1)
        self.assertEqual(depth.get("rejected"), 1)


class TestDryRunPayloadPreview(unittest.TestCase):
    """Verify that dry_run mode returns would_send with the full CampaignRequest shape."""

    def setUp(self):
        import tempfile
        self._tmp_obj = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_obj.name)
        self.server, self.port, self.dm = _start_server(self.tmp)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self._tmp_obj.cleanup()

    def test_approve_dry_run_includes_would_send(self):
        """POST /queue/approve (dry_run) → campaign.would_send contains offer + ai_copy."""
        _, cb = _post(self.port, "/queue/draft",
                      {"offer": _VALID_OFFER, "ai_copy": _VALID_AI_COPY})
        draft_id = cb["draft_id"]

        with patch.dict(os.environ, {
            "CAMPAIGN_CREATE_WEBHOOK_URL": "",
            "CAMPAIGN_CREATE_DRY_RUN":     "true",
        }):
            status, body = _post(self.port, "/queue/approve",
                                  {"draft_id": draft_id, "approver": "sidd"})

        self.assertEqual(status, 200)
        campaign = body.get("campaign", {})
        self.assertEqual(campaign.get("status"), "dry_run")
        ws = campaign.get("would_send", {})
        self.assertEqual(ws.get("draft_id"), draft_id)
        self.assertEqual(ws.get("offer", {}).get("offer_id"), "offer_xyz")
        self.assertEqual(ws.get("ai_copy", {}).get("headline"), "Shop Acme Today")
        self.assertEqual(ws.get("approver"), "sidd")
        self.assertFalse(ws.get("dry_run"))   # real payload has dry_run=False

    def test_approve_dry_run_draft_id_matches(self):
        """POST /queue/approve → campaign.would_send.draft_id matches the approved draft."""
        _, cb = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        draft_id = cb["draft_id"]

        with patch.dict(os.environ, {
            "CAMPAIGN_CREATE_WEBHOOK_URL": "",
            "CAMPAIGN_CREATE_DRY_RUN":     "true",
        }):
            _, body = _post(self.port, "/queue/approve",
                             {"draft_id": draft_id, "approver": "vamsee"})

        ws = body.get("campaign", {}).get("would_send", {})
        self.assertEqual(ws.get("draft_id"), draft_id)
        self.assertEqual(ws.get("approver"), "vamsee")


if __name__ == "__main__":
    unittest.main()
