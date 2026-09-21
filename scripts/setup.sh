#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing dependency: $1" >&2
    return 1
  fi
}

echo "==> checking prerequisites"
need docker
need node
need npm

# Git Bash on Windows cannot always exec docker-credential-desktop.
# Public image pulls do not need a creds store.
if [[ -n "${OS:-}" || "$(uname -s 2>/dev/null)" == MINGW* || "$(uname -s 2>/dev/null)" == MSYS* || "$(uname -s 2>/dev/null)" == CYGWIN* ]]; then
  export PATH="$HOME/bin:/c/Program Files/Docker/Docker/resources/bin:$PATH"
  TMPCFG="$(mktemp -d)"
  printf '%s\n' '{"auths":{}}' > "$TMPCFG/config.json"
  export DOCKER_CONFIG="$TMPCFG"
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker is installed but the daemon is not running. Start Docker Desktop and retry." >&2
  exit 1
fi

echo "==> starting MongoDB"
docker compose -f docker-compose.mongo.yml up -d

echo "==> starting Slurm cluster (controller + 2 compute nodes)"
docker compose -f docker-compose.slurm.yml up -d --build

echo "==> waiting for Slurm controller"
for _ in $(seq 1 40); do
  if docker exec hcp-slurmctld scontrol ping 2>/dev/null | grep -q UP; then
    break
  fi
  sleep 2
done
docker exec hcp-slurmctld sinfo || true
docker exec hcp-slurmctld bash -lc "scontrol update NodeName=c1 State=RESUME; scontrol update NodeName=c2 State=RESUME" || true

if command -v kind >/dev/null 2>&1 && command -v kubectl >/dev/null 2>&1; then
  echo "==> creating Kubernetes cluster with kind"
  if kind get clusters 2>/dev/null | grep -qx hcp; then
    echo "kind cluster 'hcp' already exists"
  else
    docker rm -f hcp-control-plane hcp-worker hcp-worker2 >/dev/null 2>&1 || true
    kind create cluster --config clusters/k8s/kind-config.yaml
  fi
  kubectl config use-context kind-hcp
  kubectl apply -f clusters/k8s/namespace.yaml
  kubectl get nodes
else
  echo "NOTE: kind/kubectl not found. Slurm will work; Kubernetes adapter will stay unhealthy until you install them:"
  echo "  https://kind.sigs.k8s.io/docs/user/quick-start/"
  echo "  https://kubernetes.io/docs/tasks/tools/"
fi

echo "==> installing Node.js dependencies"
(cd control-plane && npm install)

echo
echo "Clusters are up. Start the control plane with:"
echo "  bash scripts/run-api.sh"
echo "Then open http://127.0.0.1:8080"
echo "CLI: node control-plane/src/cli.js list"
