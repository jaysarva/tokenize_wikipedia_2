#!/usr/bin/env bash
set -euo pipefail

# Deploy the Wikipedia tokenizer RayJob to the K3s cluster.
#
# Prerequisites:
#   1. K3s cluster running with kubectl access
#   2. KubeRay operator installed (./scripts/install_kuberay.sh)
#   3. NFS storage configured (./scripts/setup_nfs_server.sh + ./scripts/setup_nfs_csi.sh)
#   4. Image pushed to registry and k8s/rayjob-toy.yaml updated with image path
#
# Usage:
#   ./scripts/deploy_toy_job.sh

NAMESPACE=${NAMESPACE:-wiki-tokenizer}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/../k8s"

echo "=== Deploying Wikipedia Tokenizer to K3s ==="

# Verify kubectl connectivity
if ! kubectl cluster-info &>/dev/null; then
    echo "ERROR: Cannot connect to Kubernetes cluster. Check your kubeconfig."
    exit 1
fi

# Check if KubeRay is installed
if ! kubectl get crd rayjobs.ray.io &>/dev/null; then
    echo "ERROR: KubeRay CRDs not found. Run: ./scripts/install_kuberay.sh"
    exit 1
fi

# Check if NFS StorageClass exists
if ! kubectl get storageclass nfs-csi &>/dev/null; then
    echo "WARNING: nfs-csi StorageClass not found. Run: ./scripts/setup_nfs_csi.sh"
    echo "         Or modify k8s/pvc.yaml to use a different storage class."
fi

# Create namespace
kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || kubectl create ns "$NAMESPACE"

# Create PVC for persistent output storage (idempotent)
echo "Creating PVC..."
kubectl apply -n "$NAMESPACE" -f "${K8S_DIR}/pvc.yaml"

# Apply the sample pages ConfigMap
echo "Applying ConfigMap..."
kubectl apply -n "$NAMESPACE" -f "${K8S_DIR}/pages-configmap.yaml"

# Delete existing job if present (to allow re-runs)
echo "Cleaning up previous job (if any)..."
kubectl delete rayjob wiki-rayjob-sample -n "$NAMESPACE" --ignore-not-found

# Deploy the RayJob
echo "Deploying RayJob..."
kubectl apply -n "$NAMESPACE" -f "${K8S_DIR}/rayjob-toy.yaml"

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Monitor with:"
echo "  kubectl get pods -n $NAMESPACE -w"
echo "  kubectl logs -n $NAMESPACE -l ray.io/node-type=head -f"
echo ""
echo "Dashboard (port-forward):"
echo "  kubectl port-forward -n $NAMESPACE svc/wiki-rayjob-sample-head-svc 8265:8265"
echo "  Open: http://localhost:8265"
echo ""
kubectl get pods -n "$NAMESPACE"
