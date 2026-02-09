#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${BOARD_TEST_CMD:-}" ]]; then
  echo "BOARD_TEST_CMD is required to run board tests." >&2
  echo "Example: export BOARD_TEST_CMD='python tools/run_board_tests.py --target ${BOARD_TARGET}'" >&2
  exit 2
fi

if [[ -n "${BOARD_TARGET:-}" ]]; then
  echo "Board target: ${BOARD_TARGET}"
fi

start_time=$(date +%s)
if [[ -n "${BOARD_TEST_OUTPUT:-}" ]]; then
  eval "${BOARD_TEST_CMD}" | tee "${BOARD_TEST_OUTPUT}"
else
  eval "${BOARD_TEST_CMD}"
fi
end_time=$(date +%s)

echo "Board test duration: $((end_time - start_time))s"
