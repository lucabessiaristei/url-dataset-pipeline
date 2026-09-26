#!/usr/bin/with-contenv bashio
# The service keeps the generator running, serves the sidebar panel and posts the daily digest.
bashio::log.info "URL Dataset Pipeline: runs continuously, digest at $(bashio::config 'digest_time'), panel in the sidebar"
exec /opt/venv/bin/python /app/service.py
