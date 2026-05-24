"""Tests for P3.1 — queue endpoints on demand-feed.

Covers:
  T1:  POST /queue/draft → 201 with draft_id when valid offer body
  T2:  POST /queue/draft → 400 when offer field missing
  T3:  POST /queue/draft → 400 when network/offer_id missing
  T4:  GET  /queue/pending → 200 with empty list initially
  T5:  GET  /queue/pending → 200 with draft after create
  T6:  GET  /queue/<id>   → 200 with draft dict
  T7:  GET  /queue/<id>   → 404 for unknown id
  T8:  POST /queue/approve → 200; draft transitions to approved
  T9:  POST /queue/approve → 404 for unknown draft
  T10: POST /queue/approve → 409 when already approved
  T11: POST /queue/reject  → 200; draft transitions to rejected
  T12: POST /queue/reject  → 409 when already rejected
  T13: POST /campaigns/create → 200 in dry-run mode (default)
  T14: POST /campaigns/create → 404 for unknown draft_id
  T15: POST /queue/approve fires _fire_campaign_creation once
"""

from __future__ import annotations

import importlib
import json
import socketserver
import sys
import threading
import types
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    dm._DATA_DIR    = tmp_dir
    dm._OFFERS_FILE = tmp_dir / "offers_latest.json"
    dm._SCRAPER_STATE = tmp_dir / "scraper_state.json"
    dm._QUEUE_FILE  = tmp_dir / "queue.json"
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
    "network": "impact",
    "offer_id": "offer_123",
    "advertiser": "Acme Corp",
    "title": "Acme Widget — $20 CPA",
    "payout_num": 20.0,
    "payout_type": "CPA",
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestQueueEndpoints(unittest.TestCase):

    def setUp(self):
        import tempfile
        self._tmp_obj = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_obj.name)
        self.server, self.port, self.dm = _start_server(self.tmp)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self._tmp_obj.cleanup()

    # ── T1-T3: create draft ──────────────────────────────────────────────────

    def test_create_draft_returns_201(self):
        """POST /queue/draft with valid offer → 201 + draft_id."""
        status, body = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        self.assertEqual(status, 201)
        self.assertIn("draft_id", body)
        self.assertEqual(body.get("status"), "pending")

    def test_create_draft_missing_offer_field(self):
        """POST /queue/draft without offer key → 400."""
        status, body = _post(self.port, "/queue/draft", {"ai_copy": {}})
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_create_draft_missing_network_or_offer_id(self):
        """POST /queue/draft with offer missing network → 400."""
        status, body = _post(self.port, "/queue/draft",
                             {"offer": {"title": "No network"}})
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    # ── T4-T5: list pending ──────────────────────────────────────────────────

    def test_list_pending_empty_initially(self):
        """GET /queue/pending → 200, empty list before any draft."""
        status, body = _get(self.port, "/queue/pending")
        self.assertEqual(status, 200)
        self.assertEqual(body.get("drafts"), [])
        self.assertEqual(body.get("count"), 0)

    def test_list_pending_after_create(self):
        """GET /queue/pending → 200, one draft after POST /queue/draft."""
        _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        status, body = _get(self.port, "/queue/pending")
        self.assertEqual(status, 200)
        self.assertEqual(body.get("count"), 1)
        drafts = body.get("drafts", [])
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0]["offer"]["offer_id"], "offer_123")

    # ── T6-T7: get draft by id ───────────────────────────────────────────────

    def test_get_draft_by_id(self):
        """GET /queue/<id> → 200 with draft dict."""
        _, create_body = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        draft_id = create_body["draft_id"]
        status, body = _get(self.port, f"/queue/{draft_id}")
        self.assertEqual(status, 200)
        self.assertEqual(body["draft_id"], draft_id)
        self.assertEqual(body["approval"]["state"], "pending")

    def test_get_draft_unknown_id(self):
        """GET /queue/nonexistent → 404."""
        status, body = _get(self.port, "/queue/nonexistent-id")
        self.assertEqual(status, 404)
        self.assertIn("error", body)

    # ── T8-T10: approve ──────────────────────────────────────────────────────

    def test_approve_transitions_to_approved(self):
        """POST /queue/approve → 200; draft state becomes approved."""
        _, create_body = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        draft_id = create_body["draft_id"]

        with patch.object(self.dm, "_fire_campaign_creation",
                          return_value={"status": "dry_run", "draft_id": draft_id}):
            status, body = _post(self.port, "/queue/approve",
                                  {"draft_id": draft_id, "approver": "sidd"})

        self.assertEqual(status, 200)
        self.assertEqual(body.get("status"), "approved")
        self.assertIn("approved_at", body)
        self.assertIn("campaign", body)

        # Verify persistence
        _, draft_body = _get(self.port, f"/queue/{draft_id}")
        self.assertEqual(draft_body["approval"]["state"], "approved")
        self.assertEqual(draft_body["approval"]["approver"], "sidd")

    def test_approve_unknown_draft(self):
        """POST /queue/approve with unknown draft_id → 404."""
        with patch.object(self.dm, "_fire_campaign_creation",
                          return_value={"status": "dry_run"}):
            status, body = _post(self.port, "/queue/approve",
                                  {"draft_id": "no-such-id", "approver": "sidd"})
        self.assertEqual(status, 404)

    def test_approve_idempotent_conflict(self):
        """POST /queue/approve a second time → 409."""
        _, create_body = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        draft_id = create_body["draft_id"]

        with patch.object(self.dm, "_fire_campaign_creation",
                          return_value={"status": "dry_run"}):
            _post(self.port, "/queue/approve",
                  {"draft_id": draft_id, "approver": "sidd"})
            status, body = _post(self.port, "/queue/approve",
                                  {"draft_id": draft_id, "approver": "sidd"})
        self.assertEqual(status, 409)

    # ── T11-T12: reject ──────────────────────────────────────────────────────

    def test_reject_transitions_to_rejected(self):
        """POST /queue/reject → 200; draft state becomes rejected."""
        _, create_body = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        draft_id = create_body["draft_id"]

        status, body = _post(self.port, "/queue/reject",
                              {"draft_id": draft_id, "approver": "sidd",
                               "note": "not a fit"})
        self.assertEqual(status, 200)
        self.assertEqual(body.get("status"), "rejected")

        _, draft_body = _get(self.port, f"/queue/{draft_id}")
        self.assertEqual(draft_body["approval"]["state"], "rejected")
        self.assertEqual(draft_body["approval"]["note"], "not a fit")

    def test_reject_already_rejected(self):
        """POST /queue/reject a second time → 409."""
        _, create_body = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        draft_id = create_body["draft_id"]
        _post(self.port, "/queue/reject", {"draft_id": draft_id, "approver": "sidd"})
        status, _ = _post(self.port, "/queue/reject",
                           {"draft_id": draft_id, "approver": "sidd"})
        self.assertEqual(status, 409)

    # ── T13-T14: campaigns/create ────────────────────────────────────────────

    def test_campaigns_create_dry_run(self):
        """POST /campaigns/create → 200 with status=dry_run (default)."""
        _, create_body = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        draft_id = create_body["draft_id"]

        with patch.dict("os.environ", {"CAMPAIGN_CREATE_DRY_RUN": "true"}):
            status, body = _post(self.port, "/campaigns/create",
                                  {"draft_id": draft_id})
        self.assertEqual(status, 200)
        self.assertEqual(body.get("status"), "dry_run")

    def test_campaigns_create_unknown_draft(self):
        """POST /campaigns/create with unknown draft_id → 404."""
        status, body = _post(self.port, "/campaigns/create",
                              {"draft_id": "no-such-id"})
        self.assertEqual(status, 404)

    # ── T15: approve fires _fire_campaign_creation ───────────────────────────

    def test_approve_fires_campaign_creation_once(self):
        """Approving a draft calls _fire_campaign_creation exactly once."""
        _, create_body = _post(self.port, "/queue/draft", {"offer": _VALID_OFFER})
        draft_id = create_body["draft_id"]

        mock_fire = MagicMock(return_value={"status": "dry_run"})
        with patch.object(self.dm, "_fire_campaign_creation", mock_fire):
            _post(self.port, "/queue/approve",
                  {"draft_id": draft_id, "approver": "sidd"})

        mock_fire.assert_called_once()
        called_draft = mock_fire.call_args[0][0]
        self.assertEqual(called_draft["draft_id"], draft_id)


if __name__ == "__main__":
    unittest.main()
