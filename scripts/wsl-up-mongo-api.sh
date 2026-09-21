#!/usr/bin/env bash
set -euo pipefail
export PATH="${HOME}/.local/node/bin:/usr/bin:/usr/local/bin:${PATH}"
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/hcp/docker.sock}"
export DOCKER_CONFIG="${HOME}/.docker-engine"
export KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/k3s.yaml}"
export K8S_CONTEXT="${K8S_CONTEXT:-default}"
export HCP_API_HOST="${HCP_API_HOST:-0.0.0.0}"
export HCP_API_PORT="${HCP_API_PORT:-8080}"
export MONGODB_URI="${MONGODB_URI:-mongodb://127.0.0.1:27017/hcp}"
SRC="${HCP_WIN_SRC:-/mnt/c/Users/amank/OneDrive/Desktop/new_ass}"
DST="${HCP_ROOT:-/home/amank/hcp}"

mkdir -p "$DOCKER_CONFIG"
printf '%s\n' '{"auths":{}}' > "$DOCKER_CONFIG/config.json"

rsync -a --exclude node_modules --exclude data "$SRC/control-plane/" "$DST/control-plane/"
cp "$SRC/docker-compose.mongo.yml" "$DST/docker-compose.mongo.yml"
rsync -a "$SRC/scripts/" "$DST/scripts/"

cd "$DST"
docker compose -f docker-compose.mongo.yml up -d
for _ in $(seq 1 30); do
  if docker exec hcp-mongo mongosh --quiet --eval "db.runCommand({ ping: 1 }).ok" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

cd "$DST/control-plane"
npm install --omit=dev

if pgrep -f "node src/server.js" >/dev/null 2>&1; then
  pkill -f "node src/server.js" || true
  sleep 1
fi
setsid node src/server.js >>/tmp/hcp-api.log 2>&1 </dev/null &
sleep 2
curl -fsS "http://127.0.0.1:${HCP_API_PORT}/api/v1/health"
echo
