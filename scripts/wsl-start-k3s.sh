#!/usr/bin/env bash
# Recreate or start the k3s container used as the Kubernetes cluster.
set -euo pipefail
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/hcp/docker.sock}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s.yaml}"
export PATH="${HOME}/bin:${HOME}/.local/node/bin:/usr/bin:/usr/local/bin:${PATH}"

if docker inspect hcp-k3s >/dev/null 2>&1; then
  echo "starting existing hcp-k3s"
  docker start hcp-k3s >/dev/null
else
  echo "creating hcp-k3s"
  docker run -d --name hcp-k3s --privileged --restart=unless-stopped \
    -p 127.0.0.1:6443:6443 \
    rancher/k3s:v1.31.4-k3s1 \
    server --tls-san=127.0.0.1 --disable=traefik
fi

echo "waiting for k3s API"
for _ in $(seq 1 45); do
  if docker exec hcp-k3s kubectl get nodes >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec hcp-k3s kubectl get nodes

mkdir -p "$HOME/.kube"
docker exec hcp-k3s cat /etc/rancher/k3s/k3s.yaml > "$KUBECONFIG"
chmod 600 "$KUBECONFIG"

if ! curl -sk --max-time 3 https://127.0.0.1:6443/readyz >/dev/null 2>&1; then
  echo "host :6443 not listening yet; restarting k3s to recreate docker-proxy"
  docker restart hcp-k3s >/dev/null
  sleep 5
fi

kubectl --kubeconfig "$KUBECONFIG" get nodes
kubectl --kubeconfig "$KUBECONFIG" apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: hcp
  labels:
    app.kubernetes.io/part-of: hybrid-compute-platform
EOF
echo "k3s_ok"
