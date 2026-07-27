#!/usr/bin/env python3
"""ebus_capture — poll the C6 /log + HA context into SQLite, expose a read API."""
import json, os, re, sqlite3, time, urllib.request, urllib.error, urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

_UNKNOWN_RE = re.compile(r"received unknown ([0-9a-fA-F]+)\s*/\s*([0-9a-fA-F]*)")


def load_config():
    ents = [e for e in os.environ.get("HA_ENTITIES", "").split(",") if ":" in e]
    return {
        "ebus_host": os.environ.get("EBUS_HOST", "172.16.12.216"),
        "poll_secs": int(os.environ.get("POLL_SECS", "60")),
        "api_port": int(os.environ.get("API_PORT", "9099")),
        "ha_entities": [tuple(e.split(":", 1)) for e in ents],
        "db_path": os.environ.get("EBUS_DB", "/data/telegrams.sqlite"),
    }


def init_db(path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS telegram (
        wall_ts INTEGER, device_time INTEGER, key TEXT,
        qq TEXT, zz TEXT, pb TEXT, sb TEXT, mdata TEXT, payload TEXT,
        UNIQUE(device_time, key, payload))""")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_telegram_key ON telegram(key, wall_ts)")
    conn.execute("""CREATE TABLE IF NOT EXISTS ha_context (
        wall_ts INTEGER PRIMARY KEY, outsidetemp REAL, flowtemp REAL,
        returntemp REAL, roomtemp REAL, compressor_rps REAL,
        power_kw REAL, statuscode TEXT)""")
    conn.commit()
    return conn


def parse_unknown_line(text):
    m = _UNKNOWN_RE.search(text)
    if not m:
        return None
    master, slave = m.group(1).lower(), m.group(2).lower()
    if len(master) < 10:
        return None
    qq, zz, pb, sb = master[0:2], master[2:4], master[4:6], master[6:8]
    nn = int(master[8:10], 16)
    mdata = master[10:10 + nn * 2]
    payload = slave[2:] if len(slave) >= 2 else ""
    return {"qq": qq, "zz": zz, "pb": pb, "sb": sb, "nn": nn,
            "mdata": mdata, "payload": payload, "key": f"{zz}{pb}{sb}:{mdata}"}


def ingest_log(conn, entries, now_ts):
    n = 0
    for e in entries:
        r = parse_unknown_line(e.get("log", ""))
        if not r:
            continue
        cur = conn.execute(
            "INSERT OR IGNORE INTO telegram"
            "(wall_ts,device_time,key,qq,zz,pb,sb,mdata,payload)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (now_ts, e.get("time", 0), r["key"], r["qq"], r["zz"],
             r["pb"], r["sb"], r["mdata"], r["payload"]))
        n += cur.rowcount
    conn.commit()
    return n


_CONTEXT_COLS = {"outsidetemp", "flowtemp", "returntemp", "roomtemp",
                 "compressor_rps", "power_kw", "statuscode"}


def fetch_ha_context(entities, get_state):
    ctx = {}
    for col, entity in entities:
        if col not in _CONTEXT_COLS:
            continue
        try:
            raw = get_state(entity)
        except Exception:
            raw = None
        if col == "statuscode":
            ctx[col] = raw
        else:
            try:
                ctx[col] = float(raw)
            except (TypeError, ValueError):
                ctx[col] = None
    return ctx


def insert_context(conn, now_ts, ctx):
    cols = ["wall_ts"] + list(ctx.keys())
    vals = [now_ts] + list(ctx.values())
    ph = ",".join("?" * len(cols))
    conn.execute(f"INSERT OR REPLACE INTO ha_context ({','.join(cols)}) VALUES ({ph})", vals)
    conn.commit()


def _supervisor_get_state(entity_id):
    base = "http://supervisor/core/api/states/"
    token = os.environ.get("SUPERVISOR_TOKEN", "")
    req = urllib.request.Request(base + entity_id, headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode()).get("state")


def _rows(conn, sql, args=()):
    cur = conn.execute(sql, args)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def api_response(conn, path):
    parts = urllib.parse.urlsplit(path)
    p = parts.path
    q = urllib.parse.parse_qs(parts.query)
    if p == "/health":
        tr = conn.execute("SELECT COUNT(*) FROM telegram").fetchone()[0]
        cr = conn.execute("SELECT COUNT(*) FROM ha_context").fetchone()[0]
        since = conn.execute("SELECT MIN(wall_ts) FROM telegram").fetchone()[0]
        return 200, {"ok": True, "telegram_rows": tr, "context_rows": cr, "since_ts": since}
    if p == "/keys":
        return 200, _rows(conn,
            "SELECT key, COUNT(*) count, MIN(wall_ts) first_ts, MAX(wall_ts) last_ts"
            " FROM telegram GROUP BY key ORDER BY count DESC")
    if p.startswith("/history/"):
        key = urllib.parse.unquote(p[len("/history/"):])
        limit = int(q.get("limit", ["500"])[0])
        return 200, _rows(conn,
            "SELECT wall_ts, device_time, payload FROM telegram"
            " WHERE key=? ORDER BY wall_ts DESC LIMIT ?", (key, limit))
    if p == "/context":
        frm = int(q.get("from", ["0"])[0])
        to = int(q.get("to", ["9999999999"])[0])
        return 200, _rows(conn,
            "SELECT * FROM ha_context WHERE wall_ts BETWEEN ? AND ? ORDER BY wall_ts", (frm, to))
    return 404, {"error": "not found"}


def make_handler(db_path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            conn = sqlite3.connect(db_path)
            try:
                status, body = api_response(conn, self.path)
            finally:
                conn.close()
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *a):
            pass
    return Handler


def serve(db_path, port):
    HTTPServer(("0.0.0.0", port), make_handler(db_path)).serve_forever()


import threading


def _c6_get_log(host):
    with urllib.request.urlopen(f"http://{host}/api/v1/log", timeout=10) as r:
        return json.loads(r.read().decode()).get("entries", [])


def poll_once(conn, cfg, get_log, get_state, now):
    n = ingest_log(conn, get_log(), now)
    ctx = fetch_ha_context(cfg["ha_entities"], get_state)
    insert_context(conn, now, ctx)
    return n, ctx


def main():
    cfg = load_config()
    conn = init_db(cfg["db_path"])
    threading.Thread(target=serve, args=(cfg["db_path"], cfg["api_port"]),
                     daemon=True).start()
    print(f"ebus_capture: polling {cfg['ebus_host']} every {cfg['poll_secs']}s; "
          f"API on :{cfg['api_port']}", flush=True)
    while True:
        try:
            n, _ = poll_once(conn, cfg,
                             lambda: _c6_get_log(cfg["ebus_host"]),
                             _supervisor_get_state, int(time.time()))
            if n:
                print(f"ebus_capture: +{n} telegram row(s)", flush=True)
        except Exception as e:
            print(f"ebus_capture: poll error: {e}", flush=True)
        time.sleep(cfg["poll_secs"])


if __name__ == "__main__":
    main()
