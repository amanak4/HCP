#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

API="${HCP_API:-http://127.0.0.1:8080}"
CLI=(node control-plane/src/cli.js --api "$API")

echo "==> cluster health"
"${CLI[@]}" clusters

echo "==> submit batch job (expected: Slurm)"
"${CLI[@]}" submit examples/slurm-batch-hello.yaml

echo "==> submit container job (expected: Kubernetes)"
"${CLI[@]}" submit examples/k8s-python-pi.yaml

echo "==> submit MPI-style job (expected: Slurm)"
"${CLI[@]}" submit examples/mpi-style.yaml

echo "==> submit failing job"
"${CLI[@]}" submit examples/failing-job.yaml

echo "==> current jobs"
"${CLI[@]}" list

echo
echo "Open $API to watch placement, status, and logs."
echo "After a job id appears, inspect it with:"
echo "  node control-plane/src/cli.js status <id>"
echo "  node control-plane/src/cli.js logs <id>"
echo "  node control-plane/src/cli.js cancel <id>"
