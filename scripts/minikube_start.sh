#!/usr/bin/env bash
set -euo pipefail

# Start Minikube with a modest footprint suitable for the toy Ray job.
# Adjust --cpus/--memory if you need more capacity.

minikube start --driver=docker --cpus=4 --memory=8192
kubectl get nodes
