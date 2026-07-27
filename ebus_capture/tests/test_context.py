import unittest
import collector


class TestContext(unittest.TestCase):
    def setUp(self):
        self.conn = collector.init_db(":memory:")

    def test_fetch_parses_numeric_and_text(self):
        fake = {"sensor.o": "30.1", "sensor.s": "standby", "sensor.bad": "unknown"}
        ents = [("outsidetemp", "sensor.o"), ("statuscode", "sensor.s"),
                ("flowtemp", "sensor.bad")]
        ctx = collector.fetch_ha_context(ents, lambda e: fake[e])
        self.assertEqual(ctx["outsidetemp"], 30.1)
        self.assertEqual(ctx["statuscode"], "standby")
        self.assertIsNone(ctx["flowtemp"])   # "unknown" -> None for numeric col

    def test_insert_context_row(self):
        collector.insert_context(self.conn, 1000, {"outsidetemp": 30.1, "statuscode": "standby"})
        row = self.conn.execute(
            "SELECT wall_ts,outsidetemp,statuscode FROM ha_context").fetchone()
        self.assertEqual(row, (1000, 30.1, "standby"))


if __name__ == "__main__":
    unittest.main()
