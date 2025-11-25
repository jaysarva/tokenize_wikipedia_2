#!/usr/bin/env bash
set -euo pipefail

# Build and push the Docker image to a container registry.
# 
# Usage:
#   IMAGE_REGISTRY=docker.io/myusername ./scripts/build_and_push.sh
#   IMAGE_REGISTRY=docker.io/myusername IMAGE_TAG=v2 ./scripts/build_and_push.sh
#
# Environment variables:
#   IMAGE_REGISTRY  - Registry prefix (e.g., docker.io/username, ghcr.io/org)
#   IMAGE_TAG       - Image tag (default: v1)

IMAGE_REGISTRY=${IMAGE_REGISTRY:?"ERROR: Set IMAGE_REGISTRY (e.g., docker.io/myusername)"}
IMAGE_TAG=${IMAGE_TAG:-v1}
IMAGE_NAME="tokenize-wiki"
FULL_IMAGE="${IMAGE_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "Building image: ${FULL_IMAGE}"
docker build -t "${FULL_IMAGE}" .

echo "Pushing image: ${FULL_IMAGE}"
docker push "${FULL_IMAGE}"

echo ""
echo "Image pushed successfully: ${FULL_IMAGE}"
echo ""
echo "Update your K8s manifests with:"
echo "  image: \"${FULL_IMAGE}\""

