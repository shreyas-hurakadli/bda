#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME="${IMAGE_NAME:-student-warning-bda:local}"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-}"

if [ -z "${CONTAINER_RUNTIME}" ]; then
  if command -v podman >/dev/null 2>&1; then
    CONTAINER_RUNTIME="podman"
  elif command -v docker >/dev/null 2>&1; then
    CONTAINER_RUNTIME="docker"
  else
    echo "Install Docker or Podman only. Hadoop, Spark, and Hive stay inside the project container." >&2
    exit 1
  fi
fi

MOUNT_SUFFIX=""
if [ "${CONTAINER_RUNTIME}" = "podman" ]; then
  MOUNT_SUFFIX=":Z"
fi

cd "${PROJECT_ROOT}"

echo "Project root: ${PROJECT_ROOT}"
echo "Building project-scoped image: ${IMAGE_NAME}"
"${CONTAINER_RUNTIME}" build \
  -f infra/container/Containerfile \
  -t "${IMAGE_NAME}" \
  .

echo "Running demo inside container with workspace mounted at /workspace"
"${CONTAINER_RUNTIME}" run --rm \
  --name student-warning-bda-demo \
  -v "${PROJECT_ROOT}:/workspace${MOUNT_SUFFIX}" \
  -w /workspace \
  "${IMAGE_NAME}" \
  student-warning-run-demo
