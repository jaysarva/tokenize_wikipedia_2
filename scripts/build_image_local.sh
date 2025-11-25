#!/usr/bin/env bash
set -euo pipefail

# Build the image inside Minikube's Docker daemon so no push is needed.
IMAGE_TAG=${IMAGE_TAG:-tokenize-wiki:local}

eval "$(minikube -p minikube docker-env)"
docker build -t "$IMAGE_TAG" .
docker images | grep "$IMAGE_TAG" || true
