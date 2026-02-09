#!/usr/bin/env bash
set -euo pipefail

run_with_unity() {
  if [[ -z "${UNITY_PROJECT_PATH:-}" ]]; then
    echo "UNITY_PROJECT_PATH is required for Unity-based validation." >&2
    exit 2
  fi
  if [[ -z "${UNITY_EXECUTE_METHOD:-}" ]]; then
    echo "UNITY_EXECUTE_METHOD is required for Unity-based validation." >&2
    exit 2
  fi

  local unity_bin="${UNITY_EXECUTABLE:-Unity}"
  local log_file=()

  if [[ -n "${UNITY_LOG_FILE:-}" ]]; then
    log_file=(-logFile "${UNITY_LOG_FILE}")
  fi

  "${unity_bin}" \
    -batchmode \
    -nographics \
    -quit \
    -projectPath "${UNITY_PROJECT_PATH}" \
    -executeMethod "${UNITY_EXECUTE_METHOD}" \
    "${log_file[@]}"
}

start_time=$(date +%s)
if [[ -n "${VISUAL_VALIDATION_CMD:-}" ]]; then
  if [[ -n "${CICD_LOG_PATH:-}" ]]; then
    eval "${VISUAL_VALIDATION_CMD}" | tee "${CICD_LOG_PATH}"
  else
    eval "${VISUAL_VALIDATION_CMD}"
  fi
else
  run_with_unity
fi
end_time=$(date +%s)

echo "Visual validation duration: $((end_time - start_time))s"
