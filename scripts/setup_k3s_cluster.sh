#!/usr/bin/env bash
set -euo pipefail

# Master setup script for K3s cluster.
# This script orchestrates the full cluster setup for the Wikipedia tokenizer.
#
# Prerequisites:
#   - K3s cluster running with all nodes joined
#   - kubectl access configured
#   - Helm installed
#   - Docker installed (for building/pushing images)
#   - Docker Hub account (or other registry)
#
# Usage:
#   # Set required environment variables
#   export NFS_SERVER=10.19.49.195           # Master node internal IP
#   export IMAGE_REGISTRY=docker.io/myuser   # Your Docker Hub username
#
#   # Run the setup
#   ./scripts/setup_k3s_cluster.sh

echo "=== K3s Cluster Setup for Wikipedia Tokenizer ==="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

# Validate environment
NFS_SERVER=${NFS_SERVER:?"ERROR: Set NFS_SERVER to master node's internal IP (e.g., 10.19.49.195)"}
IMAGE_REGISTRY=${IMAGE_REGISTRY:?"ERROR: Set IMAGE_REGISTRY (e.g., docker.io/yourusername)"}
IMAGE_TAG=${IMAGE_TAG:-v1}

echo "Configuration:"
echo "  NFS_SERVER:     ${NFS_SERVER}"
echo "  IMAGE_REGISTRY: ${IMAGE_REGISTRY}"
echo "  IMAGE_TAG:      ${IMAGE_TAG}"
echo ""

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v kubectl &> /dev/null; then
    echo "ERROR: kubectl not found"
    exit 1
fi

if ! command -v helm &> /dev/null; then
    echo "ERROR: helm not found. Install with:"
    echo "  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"
    exit 1
fi

if ! kubectl cluster-info &>/dev/null; then
    echo "ERROR: Cannot connect to Kubernetes cluster"
    exit 1
fi

echo "  kubectl: OK"
echo "  helm: OK"
echo "  cluster: OK"
echo ""

# Step 1: Install KubeRay operator
echo "=== Step 1: Installing KubeRay Operator ==="
"${SCRIPT_DIR}/install_kuberay.sh"
echo ""

# Step 2: Install NFS CSI driver and StorageClass
echo "=== Step 2: Setting up NFS CSI Driver ==="
NFS_SERVER="${NFS_SERVER}" "${SCRIPT_DIR}/setup_nfs_csi.sh"
echo ""

# Step 3: Update manifests with image registry
echo "=== Step 3: Updating Manifests ==="
FULL_IMAGE="${IMAGE_REGISTRY}/tokenize-wiki:${IMAGE_TAG}"

# Update rayjob-toy.yaml
sed -i.bak "s|docker.io/DOCKERHUB_USERNAME/tokenize-wiki:v1|${FULL_IMAGE}|g" "${PROJECT_DIR}/k8s/rayjob-toy.yaml"
rm -f "${PROJECT_DIR}/k8s/rayjob-toy.yaml.bak"

# Update raycluster.yaml
sed -i.bak "s|docker.io/DOCKERHUB_USERNAME/tokenize-wiki:v1|${FULL_IMAGE}|g" "${PROJECT_DIR}/k8s/raycluster.yaml"
rm -f "${PROJECT_DIR}/k8s/raycluster.yaml.bak"

echo "Updated manifests to use image: ${FULL_IMAGE}"
echo ""

echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo ""
echo "1. Set up NFS server on master node (run on 64-181-251-75):"
echo "   sudo ./scripts/setup_nfs_server.sh"
echo ""
echo "2. Install NFS client on worker nodes:"
echo "   ssh user@163-192-5-72 'sudo apt install -y nfs-common'"
echo "   ssh user@64-181-226-252 'sudo apt install -y nfs-common'"
echo ""
echo "3. Build and push the Docker image:"
echo "   IMAGE_REGISTRY=${IMAGE_REGISTRY} ./scripts/build_and_push.sh"
echo ""
echo "4. Deploy the job:"
echo "   ./scripts/deploy_toy_job.sh"
echo ""

