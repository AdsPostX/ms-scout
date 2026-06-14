"""Tests for the agent-blocks feature (Phase 11-02).

Covers: AgentStep dataclass, _agent_plan_block() renderer, wrap_response()
agent_steps integration, and AskResult.agent_steps field.
"""
import os
import pathlib
import sys
import unittest

_REPO = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(_REPO))

# Force feature flag ON for these tests regardless of environment
os.environ["SCOUT_AGENT_BLOCKS"] = "1"

# Re-import after env set so the module-level constant picks it up
import importlib
import scout_ui_kit
importlib.reload(scout_ui_kit)

from scout_ui_kit import (
    AgentStep, _agent_plan_block, wrap_response,
    Card, Severity, Surface, ResponsePattern,
)


class TestAgentStep(unittest.TestCase):
    def test_valid_step_constructs(self):
        s = AgentStep(label="Cap check", status="pass", finding="87% of cap")
        self.assertEqual(s.label, "Cap check")
        self.assertEqual(s.status, "pass")
        self.assertEqual(s.finding, "87% of cap")

    def test_frozen(self):
        s = AgentStep(label="x", status="fail", finding="y")
        with self.assertRaises(Exception):
            s.label = "z"  # type: ignore[misc]

    def test_invalid_status_rejected(self):
        with self.assertRaises(TypeError):
            AgentStep(label="x", status="unknown", finding="y")  # type: ignore[arg-type]


class TestAgentPlanBlock(unittest.TestCase):
    def test_empty_steps_returns_empty(self):
        self.assertEqual(_agent_plan_block([]), [])

    def test_single_step_renders(self):
        steps = [AgentStep(label="Revenue check", status="pass", finding="$12K MTD")]
        blocks = _agent_plan_block(steps)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["type"], "section")
        text = blocks[0]["text"]["text"]
        self.assertIn("Revenue check", text)
        self.assertIn("$12K MTD", text)
        self.assertIn("✅", text)

    def test_status_emoji_mapping(self):
        steps = [
            AgentStep(label="A", status="pass", finding="ok"),
            AgentStep(label="B", status="fail", finding="bad"),
            AgentStep(label="C", status="warn", finding="borderline"),
            AgentStep(label="D", status="skip", finding="n/a"),
        ]
        blocks = _agent_plan_block(steps)
        text = blocks[0]["text"]["text"]
        self.assertIn("✅", text)
        self.assertIn("❌", text)
        self.assertIn("⚠️", text)
        self.assertIn("⏭️", text)

    def test_multiple_steps_one_block(self):
        steps = [
            AgentStep(label="Cap", status="pass", finding="ok"),
            AgentStep(label="Ghost", status="warn", finding="2 campaigns"),
        ]
        blocks = _agent_plan_block(steps)
        self.assertEqual(len(blocks), 1)


class TestWrapResponseAgentSteps(unittest.TestCase):
    def _make_card(self, body="Revenue is $12K MTD"):
        return Card(Severity.INFO, "Revenue", body=body)

    def test_steps_inserted_when_flag_on(self):
        steps = [AgentStep(label="Cap check", status="pass", finding="87%")]
        card = self._make_card()
        _, blocks = wrap_response(
            card=card, surface=Surface.CHANNEL_ROOT,
            pattern=ResponsePattern.ANSWER,
            agent_steps=steps,
        )
        # At least one block should contain the step label
        texts = [
            b.get("text", {}).get("text", "")
            for b in blocks
            if b.get("type") == "section"
        ]
        self.assertTrue(any("Cap check" in t for t in texts))

    def test_no_steps_no_plan_block(self):
        card = self._make_card()
        _, blocks = wrap_response(
            card=card, surface=Surface.CHANNEL_ROOT,
            pattern=ResponsePattern.ANSWER,
            agent_steps=None,
        )
        texts = [
            b.get("text", {}).get("text", "")
            for b in blocks
            if b.get("type") == "section"
        ]
        # None of the blocks should contain step emoji patterns
        self.assertFalse(any("✅ *" in t or "❌ *" in t for t in texts))

    def test_steps_rendered_on_thread_surface(self):
        steps = [AgentStep(label="Cap check", status="pass", finding="87%")]
        card = self._make_card()
        _, blocks = wrap_response(
            card=card, surface=Surface.THREAD,
            pattern=ResponsePattern.ANSWER,
            agent_steps=steps,
        )
        texts = [
            b.get("text", {}).get("text", "")
            for b in blocks
            if b.get("type") == "section"
        ]
        # Steps render on all non-ephemeral surfaces when flag is on
        self.assertTrue(any("Cap check" in t for t in texts))


class TestAskResultAgentSteps(unittest.TestCase):
    def test_agent_steps_field_exists(self):
        from scout_agent import AskResult
        r = AskResult(text="hello", agent_steps=[{"label": "x", "status": "pass", "finding": "y"}])
        self.assertIsNotNone(r.agent_steps)
        self.assertEqual(r.agent_steps[0]["label"], "x")

    def test_agent_steps_defaults_none(self):
        from scout_agent import AskResult
        r = AskResult(text="hello")
        self.assertIsNone(r.agent_steps)


if __name__ == "__main__":
    unittest.main()
