import sqlite3, unittest
import collector
from tests import fixtures


class TestIngest(unittest.TestCase):
    def setUp(self):
        self.conn = collector.init_db(":memory:")

    def test_parse_unknown_line(self):
        r = collector.parse_unknown_line("ebusd: received unknown 7108b5110107 / 0a003d00c0003900000000")
        self.assertEqual(r["key"], "08b511:07")
        self.assertEqual(r["payload"], "003d00c0003900000000")

    def test_ingest_dedupes(self):
        n1 = collector.ingest_log(self.conn, fixtures.LOG_ENTRIES, now_ts=1000)
        n2 = collector.ingest_log(self.conn, fixtures.LOG_ENTRIES, now_ts=1001)
        rows = self.conn.execute("SELECT key,payload,wall_ts FROM telegram").fetchall()
        self.assertEqual(n1, 2)          # 08b511 + 76b512 distinct
        self.assertEqual(n2, 0)          # re-poll inserts nothing
        keys = sorted(r[0] for r in rows)
        self.assertEqual(keys, ["08b511:07", "76b512:130013000006"])


if __name__ == "__main__":
    unittest.main()
