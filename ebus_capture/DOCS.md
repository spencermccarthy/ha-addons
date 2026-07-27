# eBUS Capture

Durably captures the **undecoded** eBUS telegrams the C6 sees (which otherwise
live only in the C6's short rolling `/log`) plus a compact Home Assistant sensor
context, so heat-pump telegram fields can be identified by correlation over time.

It is **read-only toward the heat pump** — it only issues `GET /api/v1/log` to
the C6 and reads HA sensor states. It never writes to the C6 or the heat pump.

## What it does

Every `poll_secs` (default 60) it:
1. `GET http://<ebus_host>/api/v1/log`, extracts `received unknown` telegrams,
   and appends new `(key, payload)` rows to SQLite (`/data/telegrams.sqlite`),
   de-duplicated.
2. Reads the configured HA entities via the Supervisor proxy and stores one
   context row (outside/flow/return/room temp, compressor rps, power, status).
3. Serves a read-only JSON API for the `ebus` skill.

## Options

| Option | Default | Meaning |
|---|---|---|
| `ebus_host` | `172.16.12.216` | C6 adapter IP/host |
| `poll_secs` | `60` | poll interval |
| `api_port` | `8099` | read API port (host-mapped) |
| `ha_entities` | see below | `col:entity_id` list captured each poll |

Default `ha_entities` map to columns `outsidetemp, flowtemp, returntemp,
roomtemp, compressor_rps, power_kw, statuscode`. Numeric columns are stored as
floats; `statuscode` as text.

## Read API (`:8099`, JSON, LAN-only)

| Endpoint | Returns |
|---|---|
| `GET /health` | row counts + earliest timestamp |
| `GET /keys` | distinct telegram keys with counts + span |
| `GET /history/<key>?limit=N` | payload time-series for a key (newest first) |
| `GET /context?from=&to=` | HA context rows in a time range |

Consumed by the `ebus` skill: `ebus keys`, `ebus history <key>`,
`ebus correlate <key>` (set `EBUS_CAPTURE_URL` to `http://<ha-host>:8099`).
