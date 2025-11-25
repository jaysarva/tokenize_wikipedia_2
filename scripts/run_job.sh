#!/usr/bin/env bash
set -euo pipefail

# Full dev cycle: deploy job, wait for completion, retrieve output.
# Usage: scripts/run_job.sh [timeout_seconds]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMEOUT=${1:-600}

echo "=== Deploying toy job ==="
"$SCRIPT_DIR/deploy_toy_job.sh"

echo ""
echo "=== Waiting for job completion ==="
"$SCRIPT_DIR/watch_job.sh" "$TIMEOUT"

echo ""
echo "=== Retrieving output ==="
"$SCRIPT_DIR/get_job_output.sh"

