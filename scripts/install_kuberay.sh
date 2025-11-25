#!/usr/bin/env bash
set -euo pipefail

# Install the KubeRay operator (CRDs + controller) into the cluster.
NAMESPACE=${NAMESPACE:-ray-system}
RELEASE=${RELEASE:-kuberay-operator}

kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || kubectl create ns "$NAMESPACE"
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ >/dev/null 2>&1
helm repo update >/dev/null 2>&1
helm upgrade --install "$RELEASE" kuberay/kuberay-operator -n "$NAMESPACE"
kubectl get pods -n "$NAMESPACE"
