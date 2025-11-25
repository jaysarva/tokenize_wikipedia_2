#!/usr/bin/env bash
set -euo pipefail

# Copy per-page token files from the Ray head/driver pod to a local directory.
# Usage: LOCAL_DIR=./tokens scripts/get_tokens.sh

NAMESPACE=${NAMESPACE:-wiki-tokenizer}
LOCAL_DIR=${LOCAL_DIR:-./tokens}

HEAD_POD=$(kubectl get pods -n "$NAMESPACE" -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
if [[ -z "${HEAD_POD}" ]]; then
  echo "No head pod found in namespace $NAMESPACE" >&2
  exit 1
fi

mkdir -p "$LOCAL_DIR"
echo "Copying tokens from $HEAD_POD:/tmp/tokens to $LOCAL_DIR"
kubectl cp -n "$NAMESPACE" "$HEAD_POD":/tmp/tokens "$LOCAL_DIR"
echo "Done. Local tokens dir: $LOCAL_DIR/tokens"
