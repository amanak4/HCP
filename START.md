# Start clusters and panel (after reboot)

Use this after a Windows / WSL restart. Work in **Ubuntu WSL**. Docker Desktop is not required.

Startup order:

```text
1. WSL Docker engine
2. MongoDB          (job index for Compass)
3. Slurm cluster    (hcp-slurmctld + c1 + c2)
4. Kubernetes k3s   (hcp-k3s)
5. Control plane    (Node API + web panel)
```

---

## 0) Open Ubuntu and set env

From **Windows PowerShell / CMD** (once):

```bat
wsl -d Ubuntu
```

Then in Ubuntu:

```bash
export PATH="$HOME/.local/node/bin:/usr/bin:/usr/local/bin:$PATH"
export DOCKER_HOST=unix:///run/hcp/docker.sock
export DOCKER_CONFIG="$HOME/.docker-engine"
export KUBECONFIG="$HOME/.kube/k3s.yaml"
export K8S_CONTEXT=default
export MONGODB_URI=mongodb://127.0.0.1:27017/hcp
mkdir -p "$DOCKER_CONFIG"
printf '%s\n' '{"auths":{}}' > "$DOCKER_CONFIG/config.json"
cd ~/hcp
```

---

## 1) Start the Docker engine

You are already inside Ubuntu (`amank@HP14s:~/hcp$`). **Do not** run `wsl -d Ubuntu` from here — that is a Windows command and Ubuntu has a different `wsl` tool.

From this Ubuntu terminal:

```bash
sudo bash ~/hcp/scripts/wsl-dockerd.sh
```

If `~/hcp/scripts/wsl-dockerd.sh` is missing:

```bash
sudo bash /mnt/c/Users/amank/OneDrive/Desktop/new_ass/scripts/wsl-dockerd.sh
```

Only from **Windows PowerShell** (not from Ubuntu):

```bat
wsl -d Ubuntu -u root -- bash /mnt/c/Users/amank/OneDrive/Desktop/new_ass/scripts/wsl-dockerd.sh
```

Then in Ubuntu:

```bash
export DOCKER_HOST=unix:///run/hcp/docker.sock
docker info >/dev/null && echo docker_ok
```

---

## 2) Start MongoDB

```bash
cd ~/hcp
docker compose -f docker-compose.mongo.yml up -d
docker ps --filter name=hcp-mongo
```

Compass (from Windows):

```text
mongodb://172.28.204.225:27017
```

If that IP changed after reboot, run `hostname -I | awk '{print $1}'` in WSL and use that address. Database `hcp`, collection `jobs`. No username / password.

---

## 3) Start Slurm (3 containers)

```bash
cd ~/hcp
docker compose -f docker-compose.slurm.yml up -d
docker exec hcp-slurmctld scontrol ping
docker exec hcp-slurmctld bash -lc "scontrol update NodeName=c1 State=RESUME; scontrol update NodeName=c2 State=RESUME" || true
docker exec hcp-slurmctld sinfo
```

Ignore `Invalid node state specified` if nodes are already `idle`.

You want:

| Name | Role |
|---|---|
| `hcp-slurmctld` | controller (`sbatch` / `squeue`) |
| `hcp-c1` | compute node |
| `hcp-c2` | compute node |

---

## 4) Start Kubernetes (k3s)

If the container already exists:

```bash
docker start hcp-k3s
export KUBECONFIG="$HOME/.kube/k3s.yaml"
kubectl get nodes
```

If you get `No such container: hcp-k3s` (common after a reboot that started a fresh Docker data dir), **create it**:

```bash
bash ~/hcp/scripts/wsl-start-k3s.sh
```

If `docker ps` shows `hcp-k3s` up but kubectl says `connection refused` on `127.0.0.1:6443`:

```bash
docker restart hcp-k3s
sleep 5
kubectl get nodes
```

The container name is **`hcp-k3s`**, not `hcp-k3`.

---

## 5) Start the panel (control plane)

Foreground (logs in this terminal):

```bash
bash ~/hcp/scripts/wsl-run-api.sh
```

Or background:

```bash
bash ~/hcp/scripts/wsl-start-api.sh
```

Check:

```bash
curl -sS http://127.0.0.1:8080/api/v1/health
node ~/hcp/control-plane/src/cli.js --api http://127.0.0.1:8080 clusters
```

Both cards should be **HEALTHY**.

WSL IP (for the browser on Windows):

```bash
hostname -I | awk '{print $1}'
```

Open:

```text
http://<WSL-IP>:8080
```

Example from this machine: `http://172.28.204.225:8080`

---

## 6) Verify everything

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Expected names:

- `hcp-mongo`
- `hcp-slurmctld`, `hcp-c1`, `hcp-c2`
- `hcp-k3s`
- Node process: `node src/server.js` on port `8080`

Useful checks:

```bash
# Slurm jobs
docker exec hcp-slurmctld squeue

# Kubernetes jobs
kubectl get jobs,pods -n hcp

# Panel jobs
node ~/hcp/control-plane/src/cli.js --api http://127.0.0.1:8080 list
```

---

## Stop / down

```bash
# Panel
pkill -f "node src/server.js" || true

# Kubernetes (keep the container)
docker stop hcp-k3s

# Slurm (keep volumes)
docker compose -f ~/hcp/docker-compose.slurm.yml stop

# Mongo (keep data)
docker compose -f ~/hcp/docker-compose.mongo.yml stop
```

Full wipe (deletes Slurm/Mongo volumes too):

```bash
cd ~/hcp
docker compose -f docker-compose.slurm.yml down -v
docker compose -f docker-compose.mongo.yml down -v
docker stop hcp-k3s
```

---

## If something is DOWN in the UI

| UI message | Fix |
|---|---|
| Slurm down / docker not responding | Step 1, then `export DOCKER_HOST=unix:///run/hcp/docker.sock` |
| Slurm container missing | Step 3 |
| Kubernetes `No such container: hcp-k3s` | `bash ~/hcp/scripts/wsl-start-k3s.sh` |
| Kubernetes `ECONNREFUSED 127.0.0.1:6443` | `docker restart hcp-k3s` then `kubectl get nodes` |
| Panel not loading | Step 5; confirm `curl http://127.0.0.1:8080/api/v1/health` |
| Compass empty / cannot connect | Step 2; use current WSL IP, not an old one |
| API cannot start (Mongo) | Mongo must be up before the panel |

Copy-paste block after reboot. First, in Ubuntu:

```bash
sudo bash ~/hcp/scripts/wsl-dockerd.sh
```

```bash
export PATH="$HOME/.local/node/bin:/usr/bin:$PATH"
export DOCKER_HOST=unix:///run/hcp/docker.sock
export DOCKER_CONFIG="$HOME/.docker-engine"
export KUBECONFIG="$HOME/.kube/k3s.yaml"
export K8S_CONTEXT=default
export MONGODB_URI=mongodb://127.0.0.1:27017/hcp
cd ~/hcp
docker compose -f docker-compose.mongo.yml up -d
docker compose -f docker-compose.slurm.yml up -d
docker exec hcp-slurmctld bash -lc "scontrol update NodeName=c1 State=RESUME; scontrol update NodeName=c2 State=RESUME" || true
bash scripts/wsl-start-k3s.sh
bash scripts/wsl-start-api.sh
hostname -I | awk '{print $1}'
```
