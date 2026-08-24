"""Integration tests for demand_feed_main.py HTTP /offers endpoint and
scout_agent._load_offers() URL fetch behaviour (Part B — PR 27).

Tests spin a real TCPServer on an OS-assigned port and exercise the live handler;
no external deps required beyond stdlib + the repo's own modules.

scout_agent.py imports `anthropic`, `clickhouse_connect`, `queries`, and
`scout_types` at module level — none of which are present in the lightweight
test venv.  We stub them into sys.modules *before* the first import so
importlib.reload() can safely re-execute the module body without touching any
heavy runtime dep.  Only _load_offers() (stdlib-only) is exercised here.
"""

import importlib
import json
import os
import socketserver
import sys
import tempfile
import threading
import types
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Ensure the worktree root is on sys.path for both modules under test
# ---------------------------------------------------------------------------
_WT_ROOT = Path(__file__).parent.parent
if str(_WT_ROOT) not in sys.path:
    sys.path.insert(0, str(_WT_ROOT))


# ---------------------------------------------------------------------------
# Stub heavy deps so scout_agent can be imported in the test venv
# ---------------------------------------------------------------------------

def _stub_module(name: str, **attrs) -> types.ModuleType:
    """Create a minimal fake module and register it in sys.modules."""
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# anthropic — only the top-level package needs to exist; scout_agent uses
# `anthropic.Anthropic(...)` and a handful of exception types.
if "anthropic" not in sys.modules:
    _ant = _stub_module("anthropic")
    _ant.Anthropic = MagicMock
    # Stub commonly referenced sub-attributes
    _ant.types = _stub_module("anthropic.types")
    sys.modules["anthropic.types"] = _ant.types

# clickhouse_connect — imported lazily inside _get_ch_client(); stub it anyway
# so any top-level reference is safe.
if "clickhouse_connect" not in sys.modules:
    _stub_module("clickhouse_connect")

# queries — local module with ClickHouse SQL strings; not needed for _load_offers
if "queries" not in sys.modules:
    _stub_module("queries")

# scout_types — just type aliases; stub FormattedOffer, Brief, Offer.
# Offer is needed because scout_agent.py now pulls in offer_scraper.py at
# module level (via scout_tools_offers's canonical-function imports), and
# offer_scraper.py imports scout_types.Offer.
if "scout_types" not in sys.modules:
    _stub_module("scout_types", FormattedOffer=dict, Brief=dict, Offer=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_OFFERS = [
    {"advertiser": "Acme Corp", "payout": 12.5, "network": "Impact"},
    {"advertiser": "Beta LLC", "payout": 8.0,  "network": "CJ"},
]
_FAKE_OFFERS_BYTES = json.dumps(_FAKE_OFFERS).encode()


def _start_server(tmp_dir: Path):
    """
    Reload demand_feed_main, redirect _OFFERS_FILE into *tmp_dir*, start the
    TCP server on an OS-assigned port, and return (server, port).

    Uses port 0 so the OS picks a free port — no collisions between tests.
    """
    import demand_feed_main as dm
    importlib.reload(dm)

    dm._DATA_DIR   = tmp_dir
    dm._OFFERS_FILE = tmp_dir / "offers_latest.json"
    dm._SCRAPER_STATE = tmp_dir / "scraper_state.json"

    server = socketserver.TCPServer(("", 0), dm._OffersHandler)
    port = server.server_address[1]

    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

    return server, port


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestOffersHTTPEndpoint(unittest.TestCase):

    def setUp(self):
        self._tmp_obj = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_obj.name)

    def tearDown(self):
        self._tmp_obj.cleanup()

    # ------------------------------------------------------------------
    # T1: GET /offers → 200 + JSON when file exists (>100 bytes)
    # ------------------------------------------------------------------
    def test_get_offers_returns_200_with_json(self):
        """Writes a valid offers file (>100 bytes) and checks the endpoint."""
        # Guarantee >100 bytes — write enough entries
        offers_data = json.dumps(_FAKE_OFFERS + [{"advertiser": f"Filler-{i}", "payout": i}
                                                   for i in range(10)]).encode()
        self.assertGreater(len(offers_data), 100, "fixture must be >100 bytes for this test to be meaningful")

        server, port = _start_server(self.tmp)
        try:
            (self.tmp / "offers_latest.json").write_bytes(offers_data)

            with urllib.request.urlopen(f"http://localhost:{port}/offers", timeout=5) as resp:
                status = resp.status
                content_type = resp.headers.get("Content-Type", "")
                body = resp.read()
        finally:
            server.shutdown()

        self.assertEqual(status, 200)
        self.assertIn("application/json", content_type)
        parsed = json.loads(body)
        self.assertIsInstance(parsed, list)
        self.assertTrue(len(parsed) >= 2)

    # ------------------------------------------------------------------
    # T2: GET /offers → 503 when file is missing
    # ------------------------------------------------------------------
    def test_get_offers_returns_503_when_file_missing(self):
        """No offers file written — endpoint must return 503."""
        server, port = _start_server(self.tmp)
        try:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(f"http://localhost:{port}/offers", timeout=5)
        finally:
            server.shutdown()

        self.assertEqual(ctx.exception.code, 503)

    # ------------------------------------------------------------------
    # T3: GET /unknown → 404
    # ------------------------------------------------------------------
    def test_unknown_path_returns_404(self):
        """Any path other than /offers must return 404."""
        server, port = _start_server(self.tmp)
        try:
            for path in ["/", "/offers/", "/foo"]:
                with self.subTest(path=path):
                    with self.assertRaises(urllib.error.HTTPError) as ctx:
                        urllib.request.urlopen(f"http://localhost:{port}{path}", timeout=5)
                    self.assertEqual(ctx.exception.code, 404)
        finally:
            server.shutdown()


    # ------------------------------------------------------------------
    # T3b: GET /health → 200 with uptime + status
    # ------------------------------------------------------------------
    def test_health_endpoint_returns_200(self):
        server, port = _start_server(self.tmp)
        try:
            with urllib.request.urlopen(f"http://localhost:{port}/health", timeout=5) as resp:
                status = resp.status
                body = json.loads(resp.read())
        finally:
            server.shutdown()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        self.assertIn("uptime_secs", body)
        self.assertIsInstance(body["uptime_secs"], int)

    # ------------------------------------------------------------------
    # T3c: GET /last-run → 200 with empty-state shape when no run yet
    # ------------------------------------------------------------------
    def test_last_run_endpoint_empty_state(self):
        server, port = _start_server(self.tmp)
        try:
            with urllib.request.urlopen(f"http://localhost:{port}/last-run", timeout=5) as resp:
                status = resp.status
                body = json.loads(resp.read())
        finally:
            server.shutdown()

        self.assertEqual(status, 200)
        for key in ("last_run_date", "last_success_ts", "last_failure_ts",
                    "last_failure_reason", "offers_mtime", "offers_size"):
            self.assertIn(key, body)
        self.assertIsNone(body["last_run_date"])
        self.assertIsNone(body["offers_mtime"])

    # ------------------------------------------------------------------
    # T3d: GET /last-run → reflects state file + offers file metadata
    # ------------------------------------------------------------------
    def test_last_run_endpoint_with_state(self):
        import demand_feed_main as dm
        server, port = _start_server(self.tmp)
        try:
            # Seed scraper_state.json and offers_latest.json
            dm._save_state({
                "last_run_date":       "2026-05-22",
                "last_success_ts":     "2026-05-22T06:00:00+00:00",
                "last_failure_ts":     None,
                "last_failure_reason": None,
            })
            (self.tmp / "offers_latest.json").write_bytes(_FAKE_OFFERS_BYTES)

            with urllib.request.urlopen(f"http://localhost:{port}/last-run", timeout=5) as resp:
                body = json.loads(resp.read())
        finally:
            server.shutdown()

        self.assertEqual(body["last_run_date"], "2026-05-22")
        self.assertEqual(body["last_success_ts"], "2026-05-22T06:00:00+00:00")
        self.assertIsNone(body["last_failure_reason"])
        self.assertEqual(body["offers_size"], len(_FAKE_OFFERS_BYTES))
        self.assertIsNotNone(body["offers_mtime"])


# ---------------------------------------------------------------------------
# Tests for scout_agent._load_offers()
# ---------------------------------------------------------------------------

class TestLoadOffers(unittest.TestCase):

    def setUp(self):
        self._tmp_obj = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_obj.name)
        # Remove DEMAND_FEED_URL from env before each test
        os.environ.pop("DEMAND_FEED_URL", None)

    def tearDown(self):
        self._tmp_obj.cleanup()
        os.environ.pop("DEMAND_FEED_URL", None)

    # ------------------------------------------------------------------
    # T4: _load_offers() fetches from DEMAND_FEED_URL when env var is set
    # ------------------------------------------------------------------
    def test_load_offers_fetches_from_url_when_env_var_set(self):
        """When DEMAND_FEED_URL is set, _load_offers() calls urllib.request.urlopen.

        Retargeted from scout_tools_offers._load_offers (dead code, deleted —
        scout_agent.py's copy is the only one TOOL_MAP ever calls; see
        KNOWN_DEBT.md / DESIGN.md for the duplicate-function cleanup)."""
        import scout_agent

        fake_url = "http://fake-demand-feed-host"

        # Build a fake HTTP response object that urllib.request.urlopen returns
        fake_response = MagicMock()
        fake_response.read.return_value = json.dumps(_FAKE_OFFERS).encode()
        fake_response.__enter__ = lambda s: s
        fake_response.__exit__ = MagicMock(return_value=False)

        # _CFG is a frozen dataclass computed once at import — patch the whole
        # object rather than an attribute (scout_agent._load_offers reads
        # _CFG.demand_feed_url at call time, so this takes effect immediately).
        fake_cfg = scout_agent._ScoutCfg(demand_feed_url=fake_url)
        with patch.object(scout_agent, "_CFG", fake_cfg):
            with patch("urllib.request.urlopen", return_value=fake_response) as mock_urlopen:
                result = scout_agent._load_offers()

        # urlopen must have been called with the correct URL
        mock_urlopen.assert_called_once()
        called_url = mock_urlopen.call_args[0][0]
        self.assertIn("/offers", called_url)
        self.assertIn("fake-demand-feed-host", called_url)

        # Returned data must match the fake payload
        self.assertEqual(result, _FAKE_OFFERS)

    # ------------------------------------------------------------------
    # T5: _load_offers() falls back to disk when DEMAND_FEED_URL is unset
    # ------------------------------------------------------------------
    def test_load_offers_falls_back_to_disk_when_url_unset(self):
        """When DEMAND_FEED_URL is absent, _load_offers() reads from SNAPSHOT_PATH."""
        import scout_agent

        # Write a fake offers file to the temp dir
        fake_snapshot = self.tmp / "offers_latest.json"
        fake_snapshot.write_text(json.dumps(_FAKE_OFFERS))

        fake_cfg = scout_agent._ScoutCfg(demand_feed_url="")
        with patch.object(scout_agent, "_CFG", fake_cfg), \
             patch("urllib.request.urlopen") as mock_urlopen, \
             patch.object(scout_agent, "SNAPSHOT_PATH", fake_snapshot):
            result = scout_agent._load_offers()

        # urllib must NOT have been called
        mock_urlopen.assert_not_called()

        # Data must come from the disk file
        self.assertEqual(result, _FAKE_OFFERS)

    # ------------------------------------------------------------------
    # T6: _load_offers() falls back to disk when HTTP fetch fails
    # ------------------------------------------------------------------
    def test_load_offers_falls_back_to_disk_when_http_fails(self):
        """When DEMAND_FEED_URL is set but the fetch raises, _load_offers() reads disk."""
        import scout_agent

        fake_snapshot = self.tmp / "offers_latest.json"
        fake_snapshot.write_text(json.dumps(_FAKE_OFFERS))

        fake_cfg = scout_agent._ScoutCfg(demand_feed_url="http://broken-host")
        with patch.object(scout_agent, "_CFG", fake_cfg), \
             patch("urllib.request.urlopen", side_effect=urllib.error.URLError("nope")), \
             patch.object(scout_agent, "SNAPSHOT_PATH", fake_snapshot):
            result = scout_agent._load_offers()

        self.assertEqual(result, _FAKE_OFFERS)

    # ------------------------------------------------------------------
    # T7: _load_offers() returns [] when URL unset and disk missing
    # ------------------------------------------------------------------
    def test_load_offers_returns_empty_when_no_source(self):
        """When DEMAND_FEED_URL is unset and the disk snapshot is missing, return []."""
        import scout_agent

        missing = self.tmp / "does_not_exist.json"
        fake_cfg = scout_agent._ScoutCfg(demand_feed_url="")
        with patch.object(scout_agent, "_CFG", fake_cfg), \
             patch.object(scout_agent, "SNAPSHOT_PATH", missing):
            result = scout_agent._load_offers()

        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
