#!/usr/bin/env bash
set -euo pipefail

# Setup NVIDIA GPU support for K3s nodes.
# Run this script on each node that has an NVIDIA GPU.
#
# Prerequisites:
#   - NVIDIA GPU hardware installed
#   - NVIDIA drivers installed (nvidia-smi should work)
#   - K3s already installed and running
#
# Usage:
#   sudo ./scripts/setup_nvidia_k3s.sh

echo "=== NVIDIA GPU Setup for K3s ==="

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "ERROR: This script must be run as root (sudo)"
    exit 1
fi

# Check if NVIDIA driver is working
if ! command -v nvidia-smi &> /dev/null; then
    echo "ERROR: nvidia-smi not found. Install NVIDIA drivers first:"
    echo "  sudo apt-get install -y nvidia-driver-535"
    echo "  sudo reboot"
    exit 1
fi

echo "Checking NVIDIA driver..."
if ! nvidia-smi &> /dev/null; then
    echo "ERROR: nvidia-smi failed. You may need to reboot after driver installation."
    echo "  sudo reboot"
    exit 1
fi

nvidia-smi --query-gpu=name,driver_version --format=csv
echo ""

# Detect OS
distribution=$(. /etc/os-release; echo $ID$VERSION_ID)
echo "Detected OS: $distribution"

# Install NVIDIA Container Toolkit
echo ""
echo "=== Installing NVIDIA Container Toolkit ==="

# Add NVIDIA GPG key and repo
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
    gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg 2>/dev/null || true

curl -s -L https://nvidia.github.io/libnvidia-container/$distribution/libnvidia-container.list | \
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
    tee /etc/apt/sources.list.d/nvidia-container-toolkit.list > /dev/null

apt-get update
apt-get install -y nvidia-container-toolkit

echo ""
echo "=== Configuring K3s containerd for NVIDIA ==="

# K3s uses its own containerd config location
K3S_CONTAINERD_CONFIG="/var/lib/rancher/k3s/agent/etc/containerd/config.toml"

# Create the directory if it doesn't exist
mkdir -p "$(dirname $K3S_CONTAINERD_CONFIG)"

# Configure nvidia-ctk for K3s containerd
nvidia-ctk runtime configure --runtime=containerd --config="$K3S_CONTAINERD_CONFIG"

echo ""
echo "=== Restarting K3s ==="

# Detect if this is master or agent
if systemctl is-active --quiet k3s; then
    echo "Restarting k3s (master node)..."
    systemctl restart k3s
elif systemctl is-active --quiet k3s-agent; then
    echo "Restarting k3s-agent (worker node)..."
    systemctl restart k3s-agent
else
    echo "WARNING: Could not detect k3s service. Please restart manually:"
    echo "  Master: sudo systemctl restart k3s"
    echo "  Worker: sudo systemctl restart k3s-agent"
fi

echo ""
echo "=== NVIDIA GPU Setup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Run this script on all other GPU nodes"
echo "  2. Install NVIDIA device plugin (run once from master):"
echo "     kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.14.1/nvidia-device-plugin.yml"
echo "  3. Verify GPUs are detected:"
echo "     kubectl get nodes -o json | jq '.items[].status.allocatable[\"nvidia.com/gpu\"]'"
echo ""

