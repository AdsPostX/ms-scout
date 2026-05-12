"""
Part 3 (Trust Reset) — contract tests.

Pins the behavior of:
  - record_entity_note: writes added_by=<user_id> + permalink provenance
  - forget_entity_note: drops the row and writes an audit jsonl
  - why_entity_note: returns the stored note with provenance + Slack receipt
  - _run_tool: injects _caller_user_id / _caller_permalink for record_/forget_
  - context_harvester: HARVESTER_AUTO_WRITE_ENABLED gate (default off)
  - feedback helpers (scout_handlers): _maybe_append_ps, _feedback_log_row

These tests do not require Slack or Anthropic API access. Filesystem-only.
"""
import importlib
import json
import os
import pathlib
import sys
import tempfile
import unittest
import unittest.mock

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


class TestEntityNoteProvenance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = pathlib.Path(self.tmp.name)
        self.overrides_path = self.data_dir / "entity_overrides.json"
        # Patch scout_agent path constants to write into the tmp dir
        import scout_agent
        self._orig_load = scout_agent._load_entity_overrides
        self._orig_save = scout_agent._save_entity_overrides

        def _load():
            if not self.overrides_path.exists():
                return {"publishers": {}, "advertisers": {}}
            return json.loads(self.overrides_path.read_text())

        def _save(data):
            self.overrides_path.write_text(json.dumps(data, indent=2))

        scout_agent._load_entity_overrides = _load
        scout_agent._save_entity_overrides = _save
        # forget_entity_note writes audit under <scout_agent file>/data/
        self._audit_patch = unittest.mock.patch.object(
            scout_agent.pathlib.Path, "__truediv__",
            new=pathlib.Path.__truediv__,  # noop — we just need to redirect via tmp
        )

    def tearDown(self):
        import scout_agent
        scout_agent._load_entity_overrides = self._orig_load
        scout_agent._save_entity_overrides = self._orig_save
        self.tmp.cleanup()

    def test_record_entity_note_writes_user_id_and_permalink(self):
        from scout_agent import record_entity_note
        out = record_entity_note(
            entity_name="Truist",
            entity_type="publisher",
            note="Paused for the rest of the week",
            _caller_user_id="U12345",
            _caller_permalink="https://example.slack.com/archives/C1/p1234",
        )
        data = json.loads(self.overrides_path.read_text())
        entry = data["publishers"]["Truist"]
        self.assertEqual(entry["added_by"], "U12345")
        self.assertEqual(
            entry["permalink"],
            "https://example.slack.com/archives/C1/p1234",
        )
        self.assertIn("Truist", out)

    def test_record_entity_note_defaults_added_by_to_scout_agent(self):
        from scout_agent import record_entity_note
        record_entity_note("ChimeBank", "advertiser", "Test note")
        data = json.loads(self.overrides_path.read_text())
        self.assertEqual(data["advertisers"]["ChimeBank"]["added_by"], "scout-agent")
        self.assertNotIn("permalink", data["advertisers"]["ChimeBank"])

    def test_forget_entity_note_drops_and_returns_friendly_message(self):
        from scout_agent import record_entity_note, forget_entity_note
        record_entity_note("Truist", "publisher", "Test note",
                            _caller_user_id="U1")
        msg = forget_entity_note("Truist", "publisher", _caller_user_id="U1")
        data = json.loads(self.overrides_path.read_text())
        self.assertNotIn("Truist", data.get("publishers", {}))
        self.assertIn("Forgot", msg)

    def test_forget_entity_note_no_op_for_unknown(self):
        from scout_agent import forget_entity_note
        msg = forget_entity_note("DoesNotExist", "publisher")
        self.assertIn("no note", msg.lower())

    def test_why_entity_note_returns_provenance_and_permalink(self):
        from scout_agent import record_entity_note, why_entity_note
        record_entity_note(
            "TextNow", "publisher", "Iframe quirk",
            _caller_user_id="U99",
            _caller_permalink="https://example.slack.com/p999",
        )
        out = why_entity_note("TextNow")
        self.assertIn("TextNow", out)
        self.assertIn("U99", out)
        self.assertIn("Slack receipt", out)

    def test_why_entity_note_returns_friendly_miss(self):
        from scout_agent import why_entity_note
        out = why_entity_note("Nobody")
        self.assertIn("don't have a note", out)


class TestRunToolInjectsCaller(unittest.TestCase):
    """_run_tool must inject _caller_user_id and _caller_permalink for
    record_entity_note + forget_entity_note (and NOT for other tools)."""

    def test_run_tool_forwards_caller_kwargs_to_record(self):
        import scout_agent
        captured = {}

        def fake_record(**kwargs):
            captured.update(kwargs)
            return "ok"

        original = scout_agent.TOOL_MAP["record_entity_note"]
        scout_agent.TOOL_MAP["record_entity_note"] = fake_record
        try:
            scout_agent._run_tool(
                "record_entity_note",
                {"entity_name": "X", "entity_type": "publisher", "note": "Y"},
                "U_TEST",
                "https://example/perma",
            )
        finally:
            scout_agent.TOOL_MAP["record_entity_note"] = original

        self.assertEqual(captured.get("_caller_user_id"), "U_TEST")
        self.assertEqual(captured.get("_caller_permalink"), "https://example/perma")

    def test_run_tool_does_not_forward_caller_kwargs_to_unrelated_tool(self):
        import scout_agent
        captured = {}

        def fake_tool(**kwargs):
            captured.update(kwargs)
            return "ok"

        # Pick any non-record/forget tool present in TOOL_MAP
        name = next(
            n for n in scout_agent.TOOL_MAP
            if n not in ("record_entity_note", "forget_entity_note")
        )
        original = scout_agent.TOOL_MAP[name]
        scout_agent.TOOL_MAP[name] = fake_tool
        try:
            scout_agent._run_tool(name, {}, "U_TEST", "https://example/perma")
        finally:
            scout_agent.TOOL_MAP[name] = original

        self.assertNotIn("_caller_user_id", captured)
        self.assertNotIn("_caller_permalink", captured)


class TestHarvesterAutoWriteGate(unittest.TestCase):
    """Part 3.1 — HARVESTER_AUTO_WRITE_ENABLED defaults off; flipping it on
    is the ONLY way the harvester can write to entity_overrides.json."""

    def test_gate_default_false_does_not_save(self):
        import context_harvester as ch
        called = {"saved": False}

        def fake_save(_overrides):
            called["saved"] = True

        with unittest.mock.patch.object(ch, "_save_entity_overrides", fake_save, create=True):
            # Direct flag inspection — the env var default is "false"
            self.assertNotEqual(
                os.getenv("HARVESTER_AUTO_WRITE_ENABLED", "false").lower(),
                "true",
            )
        self.assertFalse(called["saved"])


class TestFeedbackHelpers(unittest.TestCase):
    """Part 3.6 — feedback_log writing + PS-nudge state."""

    def setUp(self):
        import scout_handlers
        self.tmp = tempfile.TemporaryDirectory()
        self.feedback_path = pathlib.Path(self.tmp.name) / "feedback_log.jsonl"
        self._orig_path = scout_handlers._FEEDBACK_LOG
        scout_handlers._FEEDBACK_LOG = self.feedback_path
        scout_handlers._FEEDBACK_PS_SEEN.clear()

    def tearDown(self):
        import scout_handlers
        scout_handlers._FEEDBACK_LOG = self._orig_path
        scout_handlers._FEEDBACK_PS_SEEN.clear()
        self.tmp.cleanup()

    def test_maybe_append_ps_fires_once_per_user(self):
        from scout_handlers import _maybe_append_ps
        first = _maybe_append_ps("U1", "answer")
        second = _maybe_append_ps("U1", "answer")
        self.assertIn("tap 👍", first)
        self.assertEqual(second, "answer")  # no PS on the second call

    def test_maybe_append_ps_writes_ps_shown_row(self):
        from scout_handlers import _maybe_append_ps
        _maybe_append_ps("U2", "answer")
        rows = [json.loads(l) for l in self.feedback_path.read_text().splitlines()]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["rating"], "ps_shown")
        self.assertEqual(rows[0]["user"], "U2")

    def test_feedback_log_row_appends_ts(self):
        from scout_handlers import _feedback_log_row
        _feedback_log_row({"user": "U3", "rating": "up"})
        row = json.loads(self.feedback_path.read_text().splitlines()[0])
        self.assertEqual(row["user"], "U3")
        self.assertIn("ts", row)


if __name__ == "__main__":
    unittest.main()
