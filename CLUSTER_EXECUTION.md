# After submit: what Kubernetes and Slurm actually do

This note starts **after** placement has already chosen a cluster. The control plane has a `JobSpec` (name, command, image, CPU, memory, ntasks). An **adapter** translates that into the cluster’s native language and hands the work off. From this point the **cluster** owns execution.

The control plane does **not** start processes itself. It only:

1. Creates the native object (`batch/v1 Job` or `sbatch`) — or **queues** the job if both clusters are at capacity
2. Stores `native_id` (once dispatched)
3. Receives status via **events** (K8s Watch / Slurm job callbacks), plus `logs()` / `cancel()`

---

## Shared handoff

```text
JobSpec
  → place(): fit? → dispatch  OR  status=queued (202)
  → KubernetesAdapter.submit  OR  SlurmAdapter.submit
      → native_id  (hcp-<id>  or  14)
  → MongoDB: pending + scheduler + native_id
  → events: K8s Watch / Slurm /dev/tcp callback → update status
  → on terminal event: re-check capacity + drain control-plane queue
```

Code: `control-plane/src/server.js` (`dispatch`), `events.js`, `adapters/kubernetes.js`, `adapters/slurm.js`.

---

## Path A — Kubernetes (k3s)

We do **not** create a namespace per job. All HCP work lives in one namespace: **`hcp`**.

### 1. Ensure namespace `hcp`

On health check and on submit, the adapter calls `_ensureNamespace()`:

- `GET` namespace `hcp`
- If **404**, `POST` a Namespace named `hcp`
- If it already exists, continue

That is isolation for our jobs vs kube-system pods. It is created once, then reused.

### 2. Create a `batch/v1` Job (not a raw Pod)

`submit()` builds one API object and posts it:

```text
kind: Job
metadata:
  name: hcp-<jobId>          # this becomes native_id
  namespace: hcp
  labels:
    hcp.job-id: <jobId>
spec:
  backoffLimit: 1            # retry once if the pod fails
  ttlSecondsAfterFinished: 600
  template:                  # Pod template — Job controller uses this
    spec:
      restartPolicy: Never   # batch: do not restart a finished container
      containers:
        - name: main
          image: <spec.image>          # e.g. ubuntu:22.04
          command: ["sh", "-lc", "<spec.command>"]
          resources:
            requests/limits: cpu + memory from the form
```

`native_id` returned to Mongo is the Job name, e.g. `hcp-83724d770820`.

We create a **Job**, not a Pod, because a Job is the Kubernetes unit of “run this to completion.” The Job controller is what creates the Pod.

### 3. What Kubernetes does next (we do not code this)

| Step | Who | What |
|---|---|---|
| Job accepted | API server | Stores the Job in etcd |
| Pod created | Job controller | Creates a Pod from `spec.template`, name like `hcp-<id>-xxxxx` |
| Scheduling | kube-scheduler | Picks a node (this demo: the single k3s node) |
| Sandbox | kubelet | Creates a pod sandbox (pause container / network namespace) |
| Image | container runtime | Pulls `spec.image` if missing (`ErrImagePull` if the tag is wrong) |
| Container | kubelet | Starts container `main` with `sh -lc "<command>"` |
| Resources | kubelet / cgroups | Enforces CPU/memory **requests and limits** we set |
| Finish | container exit | Exit 0 → Job `succeeded`; non-zero → Job `failed` after backoff |

Object chain:

```text
Namespace hcp
  └── Job hcp-<id>
        └── Pod hcp-<id>-<random>
              └── Container "main"  (the image + command)
```

### 4. How we map that back to the UI

`status()` reads the **Job**, not the Pod:

- `active > 0` → `running` (pod running)
- `succeeded > 0` → `succeeded`
- `failed > 0` → `failed`
- none of those → `pending` (waiting for pod)

`logs()` lists pods with label `hcp.job-id=<id>` and reads the latest pod log.

`cancel()` **deletes the Job** with foreground propagation, which deletes the Pod and stops the container.

Inspect yourself:

```bash
kubectl get ns hcp
kubectl get jobs,pods -n hcp
kubectl describe job -n hcp hcp-<id>
kubectl logs -n hcp -l hcp.job-id=<id>
```

---

## Path B — Slurm (controller + c1 + c2)

Slurm has **no namespaces, no Jobs API, no pods, no containers for the user command** in this demo. The unit of work is a **batch script** and a numeric job id.

### 1. Write a batch script on the controller

The adapter `docker exec`s into `hcp-slurmctld` and:

1. `mkdir /data/jobs/<hcp-id>/`
2. Writes `job.sh` (decoded from base64)
3. `sbatch --parsable job.sh` → prints `14`

That number is `native_id`.

`job.sh` is Slurm’s language:

```bash
#SBATCH --job-name=demo-job
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=128M
#SBATCH --output=/data/jobs/<id>/slurm.out
#SBATCH --error=/data/jobs/<id>/slurm.err
<user command>
```

`--ntasks` > 1 is how MPI-style jobs ask for several tasks (Slurm placement bias).

### 2. How slurmctld is connected to c1 and c2

Already configured in `clusters/slurm/slurm.conf` (not created per job):

- Controller: `slurmctld` at `172.28.0.10:6817`
- Nodes: `c1` = `172.28.0.11`, `c2` = `172.28.0.12`, port **6818** (`slurmd`)
- Partition **`compute`** = `c1,c2` (default partition)
- Auth: shared **munge** key
- Plugin: `select/cons_tres` + `CR_CPU_Memory` (pack by CPU and memory)

At boot, each `slurmd` registers with `slurmctld`. That is the “namespace + node pool” equivalent.

### 3. What Slurm does next (we do not code this)

| Step | Who | What |
|---|---|---|
| `sbatch` | slurmctld | Parses `#SBATCH`, assigns job id, state `PENDING` |
| Fit | `select/cons_tres` | Finds a node in `compute` with enough idle CPU + memory |
| Assign | slurmctld | Sets `NodeList=c1` or `c2` (or both if ntasks needs it) |
| Launch | slurmd on that node | Starts the script as a Linux process (not a container) |
| Run | OS on c1/c2 | Runs `echo hello; sleep 20; …` |
| Output | slurmd | Writes stdout/err to the shared `/data/jobs/<id>/` volume |
| Finish | slurmctld | `COMPLETED` (exit 0) or `FAILED` (non-zero) |

Object chain:

```text
Partition compute
  ├── Node c1 (slurmd)
  └── Node c2 (slurmd)
        slurmctld
          └── Job 14
                └── process on c1 or c2  (the command)
```

There is **no** extra namespace and **no** pod. The “container” in Docker is only the fake machine (`hcp-c1`), not the user’s workload.

### 4. How we map that back to the UI

`status()` runs `scontrol show job <native_id>` and reads `JobState=`:

| Slurm | UI |
|---|---|
| PENDING, CONFIGURING | pending |
| RUNNING, COMPLETING | running |
| COMPLETED | succeeded |
| FAILED, TIMEOUT, OUT_OF_MEMORY | failed |
| CANCELLED | cancelled |

`logs()` cats `/data/jobs/<hcp-id>/slurm.out` and `slurm.err` on the controller (shared volume).

`cancel()` runs `scancel <native_id>`.

Inspect yourself:

```bash
docker exec hcp-slurmctld squeue
docker exec hcp-slurmctld scontrol show job <native_id>
docker exec hcp-slurmctld sinfo
docker exec hcp-slurmctld cat /data/jobs/<hcp-id>/slurm.out
```

---

## Side-by-side

| | Kubernetes | Slurm |
|---|---|---|
| Isolation we create | Namespace `hcp` (once) | Partition `compute` (config) |
| Object we create per submit | `batch/v1` Job | `sbatch` script + job id |
| Runtime object the cluster adds | Pod → container | Process on `c1`/`c2` |
| Image | Pulled and run | **Not used** (command on node OS) |
| Resource enforcement | requests/limits on the container | `#SBATCH --cpus-per-task` / `--mem` |
| Logs | `kubectl logs` on the pod | files under `/data/jobs/<id>/` |
| Cancel | Delete Job (kills pod) | `scancel` |

---

## What we deliberately do *not* do

- We do not create a new Kubernetes namespace per job.
- We do not create a Pod with `kubectl run`; the Job controller does.
- We do not SSH to `c1`/`c2`; only `sbatch` on the controller.
- We do not wrap the Slurm command in Docker/Pyxis in this demo.

That last point is the honest production gap: same OCI image on both backends would need extra Slurm container support.
