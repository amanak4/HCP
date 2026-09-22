#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HCP_ROOT="${HCP_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
export PATH="${HOME}/.local/node/bin:/usr/bin:/usr/local/bin:${PATH}"
export DOCKER_HOST="${DOCKER_HOST:-unix:///var/run/docker.sock}"
export DOCKER_CONFIG="${HOME}/.docker-engine"
export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/k3s.yaml}"
export K8S_CONTEXT="${K8S_CONTEXT:-default}"
export HCP_API_HOST="${HCP_API_HOST:-0.0.0.0}"
export HCP_API_PORT="${HCP_API_PORT:-8080}"
export MONGODB_URI="${MONGODB_URI:-mongodb://127.0.0.1:27017/hcp}"
cd "${HCP_ROOT}/control-plane"
if curl -fsS "http://127.0.0.1:${HCP_API_PORT}/api/v1/health" >/dev/null 2>&1; then
  echo "already running"
  exit 0
fi
setsid node src/server.js >>/tmp/hcp-api.log 2>&1 </dev/null &
sleep 4
curl -fsS "http://127.0.0.1:${HCP_API_PORT}/api/v1/health"
echo
