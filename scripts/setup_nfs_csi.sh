#!/usr/bin/env bash
set -euo pipefail

# Install NFS CSI driver and create StorageClass for K3s.
# Run this from a machine with kubectl access to the cluster.
#
# Usage:
#   NFS_SERVER=10.19.49.195 ./scripts/setup_nfs_csi.sh
#
# Environment variables:
#   NFS_SERVER - IP address of the NFS server (master node's internal IP)

NFS_SERVER=${NFS_SERVER:?"ERROR: Set NFS_SERVER to the master node's internal IP (e.g., 10.19.49.195)"}
NFS_SHARE=${NFS_SHARE:-/srv/nfs/wiki-output}

echo "=== Installing NFS CSI Driver ==="

# Check if Helm is available
if ! command -v helm &> /dev/null; then
    echo "ERROR: Helm is not installed. Install it first:"
    echo "  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"
    exit 1
fi

# Add and update Helm repo
echo "Adding NFS CSI driver Helm repo..."
helm repo add csi-driver-nfs https://raw.githubusercontent.com/kubernetes-csi/csi-driver-nfs/master/charts
helm repo update

# Install NFS CSI driver
echo "Installing NFS CSI driver..."
helm upgrade --install csi-driver-nfs csi-driver-nfs/csi-driver-nfs \
    --namespace kube-system \
    --set driver.name=nfs.csi.k8s.io \
    --wait

echo "Waiting for CSI driver pods to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=csi-driver-nfs -n kube-system --timeout=120s

# Create StorageClass
echo "Creating NFS StorageClass..."
cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: nfs-csi
provisioner: nfs.csi.k8s.io
parameters:
  server: "${NFS_SERVER}"
  share: "${NFS_SHARE}"
reclaimPolicy: Retain
volumeBindingMode: Immediate
mountOptions:
  - nfsvers=4.1
EOF

echo ""
echo "=== NFS CSI Setup Complete ==="
echo ""
echo "StorageClass created: nfs-csi"
echo "NFS Server: ${NFS_SERVER}"
echo "NFS Share: ${NFS_SHARE}"
echo ""
echo "Verify:"
kubectl get storageclass nfs-csi
kubectl get pods -n kube-system -l app.kubernetes.io/name=csi-driver-nfs

