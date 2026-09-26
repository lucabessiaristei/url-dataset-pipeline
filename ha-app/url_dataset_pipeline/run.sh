#!/usr/bin/with-contenv bashio
# The service schedules the daily run, serves the sidebar panel and posts the notifications.
bashio::log.info "URL Dataset Pipeline: daily start $(bashio::config 'daily_start'), panel in the sidebar"
exec /opt/venv/bin/python /app/service.py
