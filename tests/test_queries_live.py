"""
Live ClickHouse smoke harness for risky query functions.

Five fixes in the last 24h targeted HAVING/dateDiff/alias-shadow bugs in
cvr_anomaly + expiring_campaigns. ~12 other HAVING clauses across queries.py
have the same shape and could carry the same bugs. This test fires each
risky function against live ClickHouse and asserts no SQL error.

Skips automatically when CH_HOST is unset (CI without creds). Run locally
before Monday's battle-test:

    CH_HOST=... CH_USER=... CH_PASSWORD=... python -m pytest tests/test_queries_live.py -v

Does NOT validate result shape — only that the SQL parses + executes.
Result correctness is the agent's job; this catches the syntax/scope
regression class.
"""
import os
import sys
import pathlib
import unittest

_REPO = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(_REPO))

# Load .env so creds populate when running locally. python-dotenv is a
# required dependency (see requirements.txt); load_dotenv() returns False
# (no exception) when the .env file is absent, so no try/except needed
# for the missing-file case. CI without a .env silently skips below
# because CH_HOST will be unset.
from dotenv import load_dotenv
load_dotenv(_REPO / ".env", override=True)

_HAS_CH = bool(os.getenv("CH_HOST"))


@unittest.skipUnless(_HAS_CH, "CH_HOST not set — skipping live ClickHouse tests")
class TestQueriesLive(unittest.TestCase):
    """Each test calls a query function and asserts it returns a list without
    raising. The query must execute end-to-end — a SQL parse/scope error
    raises clickhouse_connect.driver.exceptions.DatabaseError."""

    @classmethod
    def setUpClass(cls):
        from scout_ch import _get_ch_client
        cls.ch = _get_ch_client()

    def test_ghost_campaigns(self):
        import queries
        result = queries.ghost_campaigns(self.ch, recency_hours=48)
        self.assertIsInstance(result, list)

    def test_revenue_opportunities(self):
        """Three HAVING clauses in nested CTEs. Highest risk."""
        import queries
        result = queries.revenue_opportunities(self.ch)
        self.assertIsInstance(result, list)

    def test_supply_dead_weight(self):
        """HAVING in nested CTE. Needs a real pub_id; we sample one cheaply."""
        import queries
        # Sample any active publisher to drive the query — we just need the
        # SQL to parse + execute, not specific data.
        # Use default.adpx_sdk_sessions (user_id UInt64) + string(user_id) as pub_pid stand-in.
        rows = self.ch.query(
            "SELECT user_id FROM default.adpx_sdk_sessions "
            "WHERE user_id != 0 LIMIT 1"
        ).result_rows
        if not rows:
            self.skipTest("No publishers in sessions table to sample")
        pub_id = int(rows[0][0])
        result = queries.supply_dead_weight(self.ch, pub_id, str(pub_id))
        self.assertIsInstance(result, list)

    def test_publisher_health_sessions(self):
        import queries
        from datetime import date, timedelta
        rows = self.ch.query(
            "SELECT user_id FROM default.adpx_sdk_sessions "
            "WHERE user_id != 0 LIMIT 1"
        ).result_rows
        if not rows:
            self.skipTest("No publishers to sample")
        pid = int(rows[0][0])
        days = 30
        start = date.today() - timedelta(days=days)
        partition = start.year * 100 + start.month
        result = queries.publisher_health_sessions(self.ch, pid, partition, days)
        self.assertIsNotNone(result)

    def test_publisher_health_ad_metrics(self):
        import queries
        from datetime import date, timedelta
        rows = self.ch.query(
            "SELECT user_id FROM default.adpx_sdk_sessions "
            "WHERE user_id != 0 LIMIT 1"
        ).result_rows
        if not rows:
            self.skipTest("No publishers to sample")
        pid = int(rows[0][0])
        pid_str = str(pid)
        days = 30
        start = date.today() - timedelta(days=days)
        partition = start.year * 100 + start.month
        ext_start = date.today() - timedelta(days=days + 14)
        extended_partition = ext_start.year * 100 + ext_start.month
        result = queries.publisher_health_ad_metrics(
            self.ch, pid, pid_str, partition, extended_partition, days
        )
        self.assertIsNotNone(result)

    def test_publisher_health_click_metrics(self):
        import queries
        from datetime import date, timedelta
        rows = self.ch.query(
            "SELECT user_id FROM default.adpx_sdk_sessions "
            "WHERE user_id != 0 LIMIT 1"
        ).result_rows
        if not rows:
            self.skipTest("No publishers to sample")
        pid = int(rows[0][0])
        days = 30
        start = date.today() - timedelta(days=days)
        partition = start.year * 100 + start.month
        result = queries.publisher_health_click_metrics(self.ch, pid, partition, days)
        self.assertIsNotNone(result)

    def test_cvr_anomaly(self):
        """Just fixed (352c604) — HAVING → final CTE + WHERE. Regression guard."""
        import queries
        result = queries.cvr_anomaly(self.ch)
        self.assertIsInstance(result, list)

    def test_expiring_campaigns(self):
        """Just fixed (a560394) — split CTE for end_date_dt. Regression guard."""
        import queries
        result = queries.expiring_campaigns(self.ch, warning_days=7)
        self.assertIsInstance(result, list)

    def test_low_fill_publishers(self):
        import queries
        # Empty list is a valid call shape — function handles it
        result = queries.low_fill_publishers(self.ch, [])
        self.assertIsInstance(result, list)

    def test_publisher_revenue_trends(self):
        import queries
        result = queries.publisher_revenue_trends(self.ch, days=7, min_periods=4)
        self.assertIsInstance(result, list)

    def test_advertiser_revenue_trends(self):
        import queries
        result = queries.advertiser_revenue_trends(self.ch, days=7, min_periods=4)
        self.assertIsInstance(result, list)


if __name__ == "__main__":
    unittest.main()
