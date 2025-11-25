#!/usr/bin/env bash
set -euo pipefail

# Setup NFS server on the master node for ReadWriteMany PVC support.
# Run this script on the K3s master node (64-181-251-75).
#
# Usage:
#   sudo ./scripts/setup_nfs_server.sh

NFS_EXPORT_PATH=${NFS_EXPORT_PATH:-/srv/nfs/wiki-output}

echo "=== Setting up NFS Server ==="

# Install NFS server
echo "Installing nfs-kernel-server..."
apt-get update
apt-get install -y nfs-kernel-server

# Create export directory
echo "Creating export directory: ${NFS_EXPORT_PATH}"
mkdir -p "${NFS_EXPORT_PATH}"
chown nobody:nogroup "${NFS_EXPORT_PATH}"
chmod 777 "${NFS_EXPORT_PATH}"

# Configure exports (allow all nodes in the cluster)
EXPORT_LINE="${NFS_EXPORT_PATH} *(rw,sync,no_subtree_check,no_root_squash)"
if ! grep -qF "${NFS_EXPORT_PATH}" /etc/exports 2>/dev/null; then
    echo "Adding export to /etc/exports..."
    echo "${EXPORT_LINE}" >> /etc/exports
else
    echo "Export already exists in /etc/exports"
fi

# Apply exports and restart NFS
echo "Applying exports..."
exportfs -ra

echo "Restarting NFS server..."
systemctl restart nfs-kernel-server
systemctl enable nfs-kernel-server

echo ""
echo "=== NFS Server Setup Complete ==="
echo ""
echo "Export path: ${NFS_EXPORT_PATH}"
echo "Status:"
systemctl status nfs-kernel-server --no-pager || true
echo ""
echo "Verify exports:"
exportfs -v
echo ""
echo "Next steps:"
echo "  1. Install NFS client on worker nodes: sudo apt install -y nfs-common"
echo "  2. Install NFS CSI driver: ./scripts/setup_nfs_csi.sh"

