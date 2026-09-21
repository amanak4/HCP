#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/control-plane"
export HCP_API_HOST="${HCP_API_HOST:-127.0.0.1}"
export HCP_API_PORT="${HCP_API_PORT:-8080}"
export MONGODB_URI="${MONGODB_URI:-mongodb://127.0.0.1:27017/hcp}"
exec node src/server.js
