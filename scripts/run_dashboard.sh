#!/usr/bin/env bash
#
# Ray Job Monitor Dashboard Launcher
#
# This script:
# 1. Sets up an SSH tunnel to the K3s master node
# 2. Configures kubectl to use the tunneled connection
# 3. Launches the Streamlit dashboard
#
# Usage:
#   ./scripts/run_dashboard.sh [master_host]
#
# Environment variables:
#   K3S_MASTER_HOST - Master node hostname/IP (default: ubuntu@64.181.251.75)
#   K3S_MASTER_USER - SSH user (default: ubuntu)
#   RAY_DASHBOARD_PORT - Ray dashboard port (default: 8265)
#   STREAMLIT_PORT - Local Streamlit port (default: 8501)

set -euo pipefail

# Configuration
K3S_MASTER_HOST="${K3S_MASTER_HOST:-64.181.251.75}"
K3S_MASTER_USER="${K3S_MASTER_USER:-ubuntu}"
RAY_DASHBOARD_PORT="${RAY_DASHBOARD_PORT:-8265}"
K8S_API_PORT="${K8S_API_PORT:-6443}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"

# Allow override via command line
if [[ $# -ge 1 ]]; then
    K3S_MASTER_HOST="$1"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
KUBECONFIG_LOCAL="$PROJECT_DIR/.kubeconfig-tunnel"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    
    # Kill SSH tunnel if running
    if [[ -n "${SSH_PID:-}" ]] && kill -0 "$SSH_PID" 2>/dev/null; then
        log_info "Stopping SSH tunnel (PID: $SSH_PID)"
        kill "$SSH_PID" 2>/dev/null || true
    fi
    
    # Remove temp kubeconfig
    if [[ -f "$KUBECONFIG_LOCAL" ]]; then
        rm -f "$KUBECONFIG_LOCAL"
    fi
    
    log_info "Cleanup complete"
}

trap cleanup EXIT INT TERM

# Check dependencies
check_dependencies() {
    log_info "Checking dependencies..."
    
    local missing=()
    
    if ! command -v ssh &>/dev/null; then
        missing+=("ssh")
    fi
    
    if ! command -v kubectl &>/dev/null; then
        missing+=("kubectl")
    fi
    
    if ! command -v streamlit &>/dev/null; then
        missing+=("streamlit (pip install streamlit)")
    fi
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing[*]}"
        log_info "Install with: pip install -r requirements-ui.txt"
        exit 1
    fi
    
    log_success "All dependencies found"
}

# Set up SSH tunnel
setup_ssh_tunnel() {
    log_info "Setting up SSH tunnel to ${K3S_MASTER_USER}@${K3S_MASTER_HOST}..."
    
    # Check if ports are already in use
    if lsof -i ":$RAY_DASHBOARD_PORT" &>/dev/null; then
        log_warn "Port $RAY_DASHBOARD_PORT already in use, assuming tunnel exists"
        return 0
    fi
    
    # Start SSH tunnel in background
    # -L local_port:remote_host:remote_port
    # -N no remote command
    # -f go to background
    ssh -o StrictHostKeyChecking=accept-new \
        -o ServerAliveInterval=60 \
        -o ServerAliveCountMax=3 \
        -L "${RAY_DASHBOARD_PORT}:localhost:${RAY_DASHBOARD_PORT}" \
        -L "${K8S_API_PORT}:localhost:${K8S_API_PORT}" \
        -N \
        "${K3S_MASTER_USER}@${K3S_MASTER_HOST}" &
    
    SSH_PID=$!
    
    # Wait a moment for tunnel to establish
    sleep 2
    
    if ! kill -0 "$SSH_PID" 2>/dev/null; then
        log_error "SSH tunnel failed to start"
        exit 1
    fi
    
    log_success "SSH tunnel established (PID: $SSH_PID)"
    log_info "  - Ray Dashboard: localhost:${RAY_DASHBOARD_PORT}"
    log_info "  - Kubernetes API: localhost:${K8S_API_PORT}"
}

# Fetch and configure kubeconfig
setup_kubeconfig() {
    log_info "Fetching kubeconfig from master node..."
    
    # Fetch kubeconfig from master
    ssh "${K3S_MASTER_USER}@${K3S_MASTER_HOST}" "sudo cat /etc/rancher/k3s/k3s.yaml" > "$KUBECONFIG_LOCAL"
    
    if [[ ! -s "$KUBECONFIG_LOCAL" ]]; then
        log_error "Failed to fetch kubeconfig"
        exit 1
    fi
    
    # Replace server URL to use local tunnel
    if [[ "$(uname)" == "Darwin" ]]; then
        # macOS sed
        sed -i '' "s|server: https://127.0.0.1:6443|server: https://localhost:${K8S_API_PORT}|g" "$KUBECONFIG_LOCAL"
        sed -i '' "s|server: https://[^:]*:6443|server: https://localhost:${K8S_API_PORT}|g" "$KUBECONFIG_LOCAL"
    else
        # Linux sed
        sed -i "s|server: https://127.0.0.1:6443|server: https://localhost:${K8S_API_PORT}|g" "$KUBECONFIG_LOCAL"
        sed -i "s|server: https://[^:]*:6443|server: https://localhost:${K8S_API_PORT}|g" "$KUBECONFIG_LOCAL"
    fi
    
    chmod 600 "$KUBECONFIG_LOCAL"
    
    log_success "Kubeconfig configured at $KUBECONFIG_LOCAL"
    
    # Test connection
    if KUBECONFIG="$KUBECONFIG_LOCAL" kubectl cluster-info &>/dev/null; then
        log_success "Kubernetes connection verified"
    else
        log_warn "Could not verify Kubernetes connection (may need to wait for tunnel)"
    fi
}

# Launch Streamlit dashboard
launch_dashboard() {
    log_info "Launching Streamlit dashboard..."
    
    cd "$PROJECT_DIR"
    
    # Export environment variables for the dashboard
    export KUBECONFIG="$KUBECONFIG_LOCAL"
    export RAY_DASHBOARD_URL="http://localhost:${RAY_DASHBOARD_PORT}"
    export PYTHONPATH="${PROJECT_DIR}:${PYTHONPATH:-}"
    
    echo ""
    echo "=============================================="
    echo "  🔮 Ray Job Monitor Dashboard"
    echo "=============================================="
    echo ""
    echo "  Dashboard URL: http://localhost:${STREAMLIT_PORT}"
    echo "  Ray Dashboard: http://localhost:${RAY_DASHBOARD_PORT}"
    echo ""
    echo "  Press Ctrl+C to stop"
    echo ""
    echo "=============================================="
    echo ""
    
    # Open browser (best effort)
    if command -v open &>/dev/null; then
        # macOS
        sleep 2 && open "http://localhost:${STREAMLIT_PORT}" &
    elif command -v xdg-open &>/dev/null; then
        # Linux
        sleep 2 && xdg-open "http://localhost:${STREAMLIT_PORT}" &
    fi
    
    # Run Streamlit
    streamlit run ui/dashboard.py \
        --server.port "$STREAMLIT_PORT" \
        --server.headless true \
        --browser.gatherUsageStats false \
        --theme.base dark
}

# Main
main() {
    echo ""
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║           Ray Job Monitor - Dashboard Launcher            ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo ""
    
    check_dependencies
    setup_ssh_tunnel
    setup_kubeconfig
    launch_dashboard
}

main "$@"


