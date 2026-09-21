#!/usr/bin/env bash
# Start a dedicated Docker daemon in Ubuntu WSL (does not use Docker Desktop).
# Run as root: wsl -d Ubuntu -u root -- bash /mnt/c/Users/amank/OneDrive/Desktop/new_ass/scripts/wsl-dockerd.sh
set -euo pipefail
mkdir -p /var/lib/hcp-docker /run/hcp /var/run/hcp-docker
if timeout 3 curl -sS --unix-socket /run/hcp/docker.sock http://localhost/version >/dev/null 2>&1; then
  chmod 666 /run/hcp/docker.sock
  echo "HCP dockerd already running"
  exit 0
fi
rm -f /run/hcp/docker.sock /run/hcp/dockerd.pid
nohup dockerd \
  --host unix:///run/hcp/docker.sock \
  --pidfile /run/hcp/dockerd.pid \
  --data-root /var/lib/hcp-docker \
  --exec-root /var/run/hcp-docker \
  > /tmp/hcp-dockerd.log 2>&1 &
for _ in $(seq 1 20); do
  if timeout 2 curl -sS --unix-socket /run/hcp/docker.sock http://localhost/version >/dev/null 2>&1; then
    chmod 666 /run/hcp/docker.sock
    echo "HCP dockerd started on unix:///run/hcp/docker.sock"
    exit 0
  fi
  sleep 1
done
echo "failed to start HCP dockerd; see /tmp/hcp-dockerd.log" >&2
tail -40 /tmp/hcp-dockerd.log >&2
exit 1
