#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${NAMESPACE:-wiki-tokenizer}

kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || kubectl create ns "$NAMESPACE"

# Refresh the sample pages ConfigMap to pick up changes.
kubectl apply -n "$NAMESPACE" -f k8s/pages-configmap.yaml
kubectl delete rayjob wiki-rayjob-sample -n "$NAMESPACE" --ignore-not-found
kubectl apply -n "$NAMESPACE" -f k8s/rayjob-toy.yaml

echo "Pods:"
kubectl get pods -n "$NAMESPACE"
