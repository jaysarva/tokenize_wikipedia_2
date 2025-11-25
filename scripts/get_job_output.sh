#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${NAMESPACE:-wiki-tokenizer}

HEAD_POD=$(kubectl get pods -n "$NAMESPACE" -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
if [[ -z "${HEAD_POD}" ]]; then
  echo "No head pod found in namespace $NAMESPACE" >&2
  exit 1
fi

echo "Head pod: $HEAD_POD"
kubectl exec -n "$NAMESPACE" "$HEAD_POD" -- cat /tmp/token_counts.jsonl
