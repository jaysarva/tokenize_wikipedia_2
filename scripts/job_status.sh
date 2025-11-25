#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${NAMESPACE:-wiki-tokenizer}

kubectl get rayjobs -n "$NAMESPACE"
kubectl get pods -n "$NAMESPACE"
echo
echo "Recent events:"
kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp | tail -n 20
