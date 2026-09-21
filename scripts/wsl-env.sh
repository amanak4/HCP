#!/usr/bin/env bash
# Run this inside Ubuntu WSL. Starts a dedicated Docker engine (not Docker Desktop).
set -euo pipefail
SOCK=/run/hcp/docker.sock
if ! timeout 3 curl -sS --unix-socket "$SOCK" http://localhost/version >/dev/null 2>&1; then
  echo "HCP dockerd is not up. From Windows run:"
  echo "  wsl -d Ubuntu -u root -- bash /mnt/c/Users/amank/OneDrive/Desktop/new_ass/scripts/wsl-dockerd.sh"
  exit 1
fi
export DOCKER_HOST="unix://$SOCK"
export DOCKER_CONFIG="${HOME}/.docker-engine"
mkdir -p "$DOCKER_CONFIG"
printf '%s\n' '{"auths":{}}' > "$DOCKER_CONFIG/config.json"
