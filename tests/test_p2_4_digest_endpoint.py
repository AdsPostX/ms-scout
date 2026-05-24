"""Tests for P2.4 — digest pipeline extraction and /digest/blocks endpoint.

Covers:
  T1: build_digest_payload() returns None when event gate fires
  T2: build_digest_payload() returns correct shape when gate is bypassed
  T3: GET /digest/blocks → 204 when payload is None
  T4: GET /digest/blocks → 200 with blocks JSON when payload is returned
  T5: GET /digest/blocks → 500 when build_digest_payload raises
  T6: GET /digest/blocks?force=1 passes is_force=True
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
# Stub heavy deps that aren't available in the test venv
# Only stub packages that DON'T exist in the test venv — leave real ones alone
# so downstream test files (test_slack_safe, test_save_on_empty) aren't polluted.
# ---------------------------------------------------------------------------

def _stub(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# anthropic — not in test venv; scout_agent imports it at module level
if "anthropic" not in sys.modules:
    _ant = _stub("anthropic")
    _ant.Anthropic = MagicMock
    _ant.types = _stub("anthropic.types")
    sys.modules["anthropic.types"] = _ant.types

# clickhouse_connect / queries / scout_types — not needed in test venv
for _dep in ("clickhouse_connect", "queries", "scout_types"):
    if _dep not in sys.modules:
        _stub(_dep)


# ---------------------------------------------------------------------------
# Minimal digest payload fixture
# ---------------------------------------------------------------------------

_FAKE_PAYLOAD = {
    "blocks":          [{"type": "section", "text": {"type": "mrkdwn", "text": "Test digest"}}],
    "fallback":        "🎯 Scout Signal — Jun 3: 2 offers across 1 networks",
    "total_selected":  2,
    "new_offer_count": 2,
    "networks_active": 1,
    "run_date":        "Jun 3",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _start_server(tmp_dir: Path):
    import demand_feed_main as dm
    importlib.reload(dm)
    dm._DATA_DIR    = tmp_dir
    dm._OFFERS_FILE = tmp_dir / "offers_latest.json"
    dm._SCRAPER_STATE = tmp_dir / "scraper_state.json"
    server = socketserver.TCPServer(("", 0), dm._OffersHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port


# ---------------------------------------------------------------------------
# T1–T2: build_digest_payload() unit tests
# ---------------------------------------------------------------------------

class TestBuildDigestPayload(unittest.TestCase):

    def _import_sd(self):
        import scout_digest
        importlib.reload(scout_digest)
        return scout_digest

    def test_returns_none_when_event_gate_fires(self):
        """Event gate (no new offers, not Monday, not forced) → returns None."""
        sd = self._import_sd()
        with patch.object(sd, "select_offers", return_value=({}, {"total_selected": 0, "total_offers": 0, "skipped_in_ms": 0, "skipped_no_score": 0})):
            result = sd.build_digest_payload(is_force=False)
        self.assertIsNone(result)

    def test_returns_payload_dict_when_gate_bypassed(self):
        """skip_event_gate=True → returns dict with expected keys."""
        sd = self._import_sd()

        fake_offers_by_network = {
            "impact": [(9.5, {"offer_name": "Acme", "_unique_key": "k1", "network": "impact"})],
        }
        fake_sel_meta = {
            "total_selected": 1, "total_offers": 5,
            "skipped_in_ms": 0, "skipped_no_score": 4,
            "per_network": {}, "no_score_reasons": {},
        }

        with patch.object(sd, "select_offers", return_value=(fake_offers_by_network, fake_sel_meta)), \
             patch.object(sd, "_prefetch_offer_images", return_value={}), \
             patch.object(sd, "build_digest_blocks", return_value=[{"type": "section"}]), \
             patch.object(sd, "_load_offers", return_value=[]), \
             patch.object(sd, "_run_sourcing_signals", return_value=[]), \
             patch.object(sd, "_build_sourcing_intel_blocks", return_value=[]), \
             patch.object(sd, "get_active_ms_campaigns", return_value=[]):
            result = sd.build_digest_payload(is_force=False, skip_event_gate=True)

        self.assertIsNotNone(result)
        for key in ("blocks", "fallback", "total_selected", "new_offer_count", "networks_active", "run_date"):
            self.assertIn(key, result, f"missing key: {key}")
        self.assertIsInstance(result["blocks"], list)
        self.assertIsInstance(result["fallback"], str)


# ---------------------------------------------------------------------------
# T3–T6: GET /digest/blocks endpoint tests
# ---------------------------------------------------------------------------

class TestDigestBlocksEndpoint(unittest.TestCase):

    def setUp(self):
        import tempfile
        self._tmp_obj = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_obj.name)

    def tearDown(self):
        self._tmp_obj.cleanup()

    def test_204_when_payload_is_none(self):
        """build_digest_payload returns None → endpoint responds 204."""
        import scout_digest
        server, port = _start_server(self.tmp)
        try:
            with patch.object(scout_digest, "build_digest_payload", return_value=None):
                with urllib.request.urlopen(
                    f"http://localhost:{port}/digest/blocks", timeout=5
                ) as resp:
                    self.assertEqual(resp.status, 204)
        finally:
            server.shutdown()

    def test_200_with_payload_json(self):
        """build_digest_payload returns payload → endpoint responds 200 + JSON."""
        import scout_digest
        server, port = _start_server(self.tmp)
        try:
            with patch.object(scout_digest, "build_digest_payload", return_value=_FAKE_PAYLOAD):
                with urllib.request.urlopen(
                    f"http://localhost:{port}/digest/blocks", timeout=5
                ) as resp:
                    status = resp.status
                    body = json.loads(resp.read())
        finally:
            server.shutdown()

        self.assertEqual(status, 200)
        self.assertIn("blocks", body)
        self.assertIn("fallback", body)

    def test_500_when_build_raises(self):
        """build_digest_payload raises → endpoint returns 500."""
        import scout_digest
        server, port = _start_server(self.tmp)
        try:
            with patch.object(scout_digest, "build_digest_payload", side_effect=RuntimeError("boom")):
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(
                        f"http://localhost:{port}/digest/blocks", timeout=5
                    )
                self.assertEqual(ctx.exception.code, 500)
        finally:
            server.shutdown()

    def test_force_param_forwarded(self):
        """?force=1 query param → build_digest_payload called with is_force=True."""
        import scout_digest
        server, port = _start_server(self.tmp)
        try:
            captured = {}

            def _fake_build(is_force=False):
                captured["is_force"] = is_force
                return _FAKE_PAYLOAD

            with patch.object(scout_digest, "build_digest_payload", side_effect=_fake_build):
                urllib.request.urlopen(
                    f"http://localhost:{port}/digest/blocks?force=1", timeout=5
                )
        finally:
            server.shutdown()

        self.assertTrue(captured.get("is_force"), "is_force should be True when ?force=1")


if __name__ == "__main__":
    unittest.main()
