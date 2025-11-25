#!/usr/bin/env bash
set -euo pipefail

# Watch RayJob until it reaches SUCCEEDED or FAILED.
# Usage: scripts/watch_job.sh [timeout_seconds]

NAMESPACE=${NAMESPACE:-wiki-tokenizer}
JOB_NAME=${JOB_NAME:-wiki-rayjob-sample}
TIMEOUT=${1:-600}
POLL_INTERVAL=5

echo "Watching $JOB_NAME in namespace $NAMESPACE (timeout: ${TIMEOUT}s)..."

start_time=$(date +%s)
while true; do
  STATUS=$(kubectl get rayjob "$JOB_NAME" -n "$NAMESPACE" -o jsonpath='{.status.jobStatus}' 2>/dev/null || echo "PENDING")
  
  elapsed=$(( $(date +%s) - start_time ))
  echo "[${elapsed}s] Job status: $STATUS"
  
  if [[ "$STATUS" == "SUCCEEDED" ]]; then
    echo "Job completed successfully!"
    exit 0
  elif [[ "$STATUS" == "FAILED" ]]; then
    echo "Job failed!" >&2
    exit 1
  fi
  
  if (( elapsed >= TIMEOUT )); then
    echo "Timeout after ${TIMEOUT}s" >&2
    exit 2
  fi
  
  sleep "$POLL_INTERVAL"
done

