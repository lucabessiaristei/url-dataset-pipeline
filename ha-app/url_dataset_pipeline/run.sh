#!/usr/bin/with-contenv bashio
# Runs the generator once a day after the free quotas reset, then posts a summary notification.

DATA=/share/url-dataset-pipeline
export PIPELINE_DATA_DIR="${DATA}/in_out-s"
export PIPELINE_LOG_FILE="${DATA}/generator.log"
export COLUMNS=160
GENERATOR=/app/tools/MULTI-PROVIDER_output_generator_API_v6.py

for provider in gemini groq openrouter nvidia deepseek; do
    value="$(bashio::config "${provider}_api_key")"
    if [ -n "${value}" ] && [ "${value}" != "null" ]; then
        export "${provider^^}_API_KEY=${value}"
    fi
done

START="$(bashio::config 'daily_start')"
MAX_HOURS="$(bashio::config 'max_hours')"
MODELS="$(bashio::config 'models')"

notify() {
    local summary="${PIPELINE_DATA_DIR}/last_run.json" message payload
    [ -f "${summary}" ] || return 0
    message="$(jq -r '
        "\(.done) files done in \(.minutes) min, \(.files_left) left.\n"
        + (.done_by_model | to_entries | map("• \(.key): \(.value)") | join("\n"))
        + (if (.retired | length) > 0
           then "\n\nStopped: " + (.retired | to_entries | map("\(.key) (\(.value))") | join("; "))
           else "" end)' "${summary}")"
    payload="$(jq -n --arg m "${message}" \
        '{title: "URL dataset pipeline", message: $m, notification_id: "url_dataset_pipeline"}')"
    curl -sS -o /dev/null -X POST \
        -H "Authorization: Bearer ${SUPERVISOR_TOKEN}" -H "Content-Type: application/json" \
        -d "${payload}" http://supervisor/core/api/services/persistent_notification/create \
        || bashio::log.warning "Could not post the notification"
}

run_once() {
    if [ ! -d "${PIPELINE_DATA_DIR}" ]; then
        bashio::log.error "No dataset at ${PIPELINE_DATA_DIR}: copy in_out-s there first"
        return
    fi
    local args=(--dir all --headless --max-hours "${MAX_HOURS}")
    if [ -n "${MODELS}" ] && [ "${MODELS}" != "null" ]; then
        args+=(--models "${MODELS}")
    fi
    bashio::log.info "Starting daily run"
    /opt/venv/bin/python "${GENERATOR}" "${args[@]}" || bashio::log.warning "Generator exited with an error"
    /opt/venv/bin/python "${GENERATOR}" --stats || true
    notify
}

if bashio::config.true 'run_on_start'; then
    run_once
fi

while true; do
    now="$(date +%s)"
    target="$(date -d "${START}" +%s)"
    if [ "${target}" -le "${now}" ]; then
        target=$((target + 86400))
    fi
    bashio::log.info "Next run at $(date -d "@${target}" '+%Y-%m-%d %H:%M %Z')"
    sleep $((target - now))
    run_once
done
