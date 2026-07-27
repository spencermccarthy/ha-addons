#!/usr/bin/env python3
"""ebus_capture — poll the C6 /log + HA context into SQLite, expose a read API."""
import json, os, re, sqlite3, time, urllib.request, urllib.error

_UNKNOWN_RE = re.compile(r"received unknown ([0-9a-fA-F]+)\s*/\s*([0-9a-fA-F]*)")


def load_config():
    ents = [e for e in os.environ.get("HA_ENTITIES", "").split(",") if ":" in e]
    return {
        "ebus_host": os.environ.get("EBUS_HOST", "172.16.12.216"),
        "poll_secs": int(os.environ.get("POLL_SECS", "60")),
        "api_port": int(os.environ.get("API_PORT", "8099")),
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
