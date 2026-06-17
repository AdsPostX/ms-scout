import unittest
import dataclasses
from scout_response import ScoutResponse, Metric, Item


class TestScoutResponse(unittest.TestCase):

    # --- 1. Minimal construction ---
    def test_valid_construction_minimal(self):
        sr = ScoutResponse(
            status="ok",
            subject_type="monitor",
            subject_id=None,
            headline="x",
        )
        self.assertIsNotNone(sr)
        self.assertEqual(sr.confidence, "low")

    # --- 2. All valid status values ---
    def test_valid_status_all_values(self):
        for s in ("ok", "warn", "critical", "empty"):
            sr = ScoutResponse(
                status=s,
                subject_type="monitor",
                subject_id=None,
                headline="test",
            )
            self.assertEqual(sr.status, s)

    # --- 3. Invalid status raises ValueError ---
    def test_invalid_status_raises(self):
        with self.assertRaises(ValueError) as ctx:
            ScoutResponse(
                status="bad",
                subject_type="monitor",
                subject_id=None,
                headline="x",
            )
        self.assertIn("bad", str(ctx.exception))

    # --- 4. Invalid subject_type raises ValueError ---
    def test_invalid_subject_type_raises(self):
        with self.assertRaises(ValueError) as ctx:
            ScoutResponse(
                status="ok",
                subject_type="widget",
                subject_id=None,
                headline="x",
            )
        self.assertIn("widget", str(ctx.exception))

    # --- 5. More than 4 metrics raises ValueError ---
    def test_metrics_over_limit_raises(self):
        metrics = [Metric(f"label{i}", f"val{i}") for i in range(5)]
        with self.assertRaises(ValueError):
            ScoutResponse(
                status="ok",
                subject_type="monitor",
                subject_id=None,
                headline="x",
                metrics=metrics,
            )

    # --- 6. Exactly 4 metrics is fine ---
    def test_metrics_at_limit_ok(self):
        metrics = [Metric(f"label{i}", f"val{i}") for i in range(4)]
        sr = ScoutResponse(
            status="ok",
            subject_type="monitor",
            subject_id=None,
            headline="x",
            metrics=metrics,
        )
        self.assertEqual(len(sr.metrics), 4)

    # --- 7. More than 2 suggestions raises ValueError ---
    def test_suggestions_over_limit_raises(self):
        with self.assertRaises(ValueError):
            ScoutResponse(
                status="ok",
                subject_type="monitor",
                subject_id=None,
                headline="x",
                suggestions=["a", "b", "c"],
            )

    # --- 8. Exactly 2 suggestions is fine ---
    def test_suggestions_at_limit_ok(self):
        sr = ScoutResponse(
            status="ok",
            subject_type="monitor",
            subject_id=None,
            headline="x",
            suggestions=["do this", "or that"],
        )
        self.assertEqual(len(sr.suggestions), 2)

    # --- 9. projection_n=None → confidence low ---
    def test_confidence_low_when_projection_n_none(self):
        sr = ScoutResponse(
            status="ok",
            subject_type="monitor",
            subject_id=None,
            headline="x",
            projection_n=None,
        )
        self.assertEqual(sr.confidence, "low")

    # --- 10. projection_n=2 → confidence low ---
    def test_confidence_low_when_projection_n_2(self):
        sr = ScoutResponse(
            status="ok",
            subject_type="monitor",
            subject_id=None,
            headline="x",
            projection_n=2,
        )
        self.assertEqual(sr.confidence, "low")

    # --- 11. projection_n=3 → confidence medium ---
    def test_confidence_medium_when_projection_n_3(self):
        sr = ScoutResponse(
            status="ok",
            subject_type="monitor",
            subject_id=None,
            headline="x",
            projection_n=3,
        )
        self.assertEqual(sr.confidence, "medium")

    # --- 12. projection_n=5 → confidence medium ---
    def test_confidence_medium_when_projection_n_5(self):
        sr = ScoutResponse(
            status="ok",
            subject_type="monitor",
            subject_id=None,
            headline="x",
            projection_n=5,
        )
        self.assertEqual(sr.confidence, "medium")

    # --- 13. projection_n=6 → confidence high ---
    def test_confidence_high_when_projection_n_6(self):
        sr = ScoutResponse(
            status="ok",
            subject_type="monitor",
            subject_id=None,
            headline="x",
            projection_n=6,
        )
        self.assertEqual(sr.confidence, "high")

    # --- 14. confidence field has init=False ---
    def test_confidence_not_an_init_param(self):
        f = dataclasses.fields(ScoutResponse)
        conf_field = next(x for x in f if x.name == "confidence")
        self.assertFalse(conf_field.init)

    # --- 15. Metric delta is optional ---
    def test_metric_has_delta_optional(self):
        m = Metric("label", "val")
        self.assertIsNone(m.delta)

    # --- 16. Item rank is optional ---
    def test_item_has_rank_optional(self):
        item = Item("label", "val")
        self.assertIsNone(item.rank)

    # --- 17. Full construction with all fields ---
    def test_full_response_all_fields(self):
        metrics = [
            Metric("Revenue", "$100K", delta="+10%"),
            Metric("Fills", "95%", delta="+2%"),
        ]
        items = [
            Item("Publisher A", "$50K", rank=1),
            Item("Publisher B", "$30K", rank=2),
        ]
        sr = ScoutResponse(
            status="warn",
            subject_type="publisher",
            subject_id="pub-123",
            headline="Revenue is down",
            body="Something is off with fill rates.",
            metrics=metrics,
            items=items,
            suggestions=["Check cap settings", "Review demand queue"],
            projection_n=7,
        )
        self.assertEqual(sr.status, "warn")
        self.assertEqual(sr.subject_type, "publisher")
        self.assertEqual(sr.subject_id, "pub-123")
        self.assertEqual(len(sr.metrics), 2)
        self.assertEqual(len(sr.items), 2)
        self.assertEqual(len(sr.suggestions), 2)
        self.assertEqual(sr.confidence, "high")


if __name__ == "__main__":
    unittest.main()
