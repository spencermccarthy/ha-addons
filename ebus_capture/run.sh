#!/usr/bin/with-contenv bashio
export EBUS_HOST="$(bashio::config 'ebus_host')"
export POLL_SECS="$(bashio::config 'poll_secs')"
export API_PORT="$(bashio::config 'api_port')"
export HA_ENTITIES="$(bashio::config 'ha_entities | join(",")')"
export EBUS_DB="/data/telegrams.sqlite"
exec python3 /collector.py
