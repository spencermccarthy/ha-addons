import unittest
import collector
from tests import fixtures


class TestApi(unittest.TestCase):
    def setUp(self):
        self.conn = collector.init_db(":memory:")
        collector.ingest_log(self.conn, fixtures.LOG_ENTRIES, now_ts=1000)
        collector.insert_context(self.conn, 1000, {"outsidetemp": 30.1})

    def test_health(self):
        st, body = collector.api_response(self.conn, "/health")
        self.assertEqual(st, 200)
        self.assertEqual(body["telegram_rows"], 2)

    def test_keys(self):
        st, body = collector.api_response(self.conn, "/keys")
        self.assertEqual(st, 200)
        self.assertIn("08b511:07", [k["key"] for k in body])

    def test_history(self):
        st, body = collector.api_response(self.conn, "/history/08b511:07?limit=10")
        self.assertEqual(st, 200)
        self.assertEqual(body[0]["payload"], "003d00c0003900000000")

    def test_context(self):
        st, body = collector.api_response(self.conn, "/context")
        self.assertEqual(body[0]["outsidetemp"], 30.1)

    def test_unknown_path_404(self):
        st, _ = collector.api_response(self.conn, "/nope")
        self.assertEqual(st, 404)


class TestPollOnce(unittest.TestCase):
    def test_poll_once_ingests_and_contexts(self):
        conn = collector.init_db(":memory:")
        cfg = {"ha_entities": [("outsidetemp", "sensor.o")]}
        n, ctx = collector.poll_once(
            conn, cfg,
            get_log=lambda: fixtures.LOG_ENTRIES,
            get_state=lambda e: "30.1",
            now=2000)
        self.assertEqual(n, 2)
        self.assertEqual(ctx["outsidetemp"], 30.1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM ha_context").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
