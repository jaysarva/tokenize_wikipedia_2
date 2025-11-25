#!/usr/bin/env bash
set -euo pipefail

# Port-forward the Ray dashboard (8265) from the first head service in the namespace.
NAMESPACE=${NAMESPACE:-wiki-tokenizer}
LOCAL_PORT=${LOCAL_PORT:-8265}

SVC_NAME=$(kubectl get svc -n "$NAMESPACE" -l ray.io/cluster -o jsonpath='{.items[0].metadata.name}')
if [[ -z "$SVC_NAME" ]]; then
  echo "No Ray head service found in namespace $NAMESPACE" >&2
  exit 1
fi

echo "Port-forwarding svc/$SVC_NAME in $NAMESPACE to localhost:${LOCAL_PORT} ..."
kubectl port-forward -n "$NAMESPACE" "svc/${SVC_NAME}" "${LOCAL_PORT}:8265"
