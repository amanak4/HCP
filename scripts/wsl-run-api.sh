#!/usr/bin/env bash
# Run inside Ubuntu WSL after Docker Engine is up:
#   export DOCKER_HOST=unix:///run/hcp/docker.sock
set -euo pipefail
export PATH="${HOME}/.local/node/bin:/usr/bin:/usr/local/bin:${PATH}"
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/hcp/docker.sock}"
export DOCKER_CONFIG="${HOME}/.docker-engine"
export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/k3s.yaml}"
export K8S_CONTEXT="${K8S_CONTEXT:-default}"
export HCP_API_HOST="${HCP_API_HOST:-0.0.0.0}"
export HCP_API_PORT="${HCP_API_PORT:-8080}"
export MONGODB_URI="${MONGODB_URI:-mongodb://127.0.0.1:27017/hcp}"
mkdir -p "$DOCKER_CONFIG" "$HOME/.kube"
printf '%s\n' '{"auths":{}}' > "$DOCKER_CONFIG/config.json"
cd "${HCP_ROOT:-/home/amank/hcp}/control-plane"
npm install --omit=dev
exec node src/server.js
