#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${BOARD_FLASH_CMD:-}" ]]; then
  echo "BOARD_FLASH_CMD is required to flash the board." >&2
  echo "Example: export BOARD_FLASH_CMD='python tools/flash.py --target ${BOARD_TARGET} --image ${FIRMWARE_PATH}'" >&2
  exit 2
fi

if [[ -n "${BOARD_TARGET:-}" ]]; then
  echo "Board target: ${BOARD_TARGET}"
fi

if [[ -n "${FIRMWARE_PATH:-}" ]]; then
  echo "Firmware path: ${FIRMWARE_PATH}"
fi

start_time=$(date +%s)
if [[ -n "${CICD_LOG_PATH:-}" ]]; then
  eval "${BOARD_FLASH_CMD}" | tee "${CICD_LOG_PATH}"
else
  eval "${BOARD_FLASH_CMD}"
fi
end_time=$(date +%s)

echo "Flash duration: $((end_time - start_time))s"
