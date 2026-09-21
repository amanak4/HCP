#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> stopping Slurm cluster"
docker compose -f docker-compose.slurm.yml down -v || true

echo "==> stopping MongoDB"
docker compose -f docker-compose.mongo.yml down -v || true

if command -v kind >/dev/null 2>&1; then
  echo "==> deleting kind cluster hcp"
  kind delete cluster --name hcp || true
fi

echo "done"
