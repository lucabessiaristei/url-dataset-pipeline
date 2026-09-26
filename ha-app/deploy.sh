#!/usr/bin/env bash
# Copies the app (plus the current generator and rules) to Home Assistant and rebuilds it.
# Usage: ha-app/deploy.sh [ssh-target]   (default root@homeassistant.local)
set -euo pipefail
TARGET="${1:-root@homeassistant.local}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP=url_dataset_pipeline

tar -C "${ROOT}" -czf - \
    -s ",^ha-app/${APP},${APP}," \
    -s ",^tools,${APP}/tools," \
    -s ",^ai_rules.txt,${APP}/ai_rules.txt," \
    "ha-app/${APP}" tools/MULTI-PROVIDER_output_generator_API_v6.py ai_rules.txt \
  | ssh "${TARGET}" "mkdir -p /local_apps && tar -xzf - -C /local_apps && ls -R /local_apps/${APP}"
echo "Copied. Reload the store and (re)build with:"
echo "  ssh ${TARGET} 'ha store reload && (ha apps rebuild local_${APP} || ha apps install local_${APP})'"
