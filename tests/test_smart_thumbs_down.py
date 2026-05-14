"""Tests for Part 9 — smart 👎 handler: clarification detection."""
from scout_handlers import _is_clarification_response


class TestIsClariificationResponse:
    def test_pure_clarification_question(self):
        assert _is_clarification_response("Do you mean RPM or RPC?")

    def test_clarification_with_context(self):
        assert _is_clarification_response(
            "Can you confirm — are you asking about AT&T or AT&T Buy Flow?"
        )

    def test_which_publisher_phrase(self):
        assert _is_clarification_response("Which publisher do you mean?")

    def test_factual_answer_no_trailing_q(self):
        assert not _is_clarification_response(
            "AT&T generated $3,400 today at 82% of 30-day avg."
        )

    def test_factual_answer_with_trailing_q_long(self):
        # False positive fix: long factual answer with trailing "?" should NOT classify as clarification.
        # This was the highest-risk edge case in the original plan — endswith("?") alone would catch it.
        text = (
            "AT&T generated $3,400 today, running at 82% of 30-day avg. "
            "Ifficient at $2,500. 4 others combined at $1,100. "
            "Is this the breakdown you needed?"
        )
        assert not _is_clarification_response(text)

    def test_short_answer_no_phrase(self):
        # Ends with ? but no clarification phrase — should NOT classify.
        assert not _is_clarification_response("Revenue looks good today?")

    def test_empty_string(self):
        assert not _is_clarification_response("")
