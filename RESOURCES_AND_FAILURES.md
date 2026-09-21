# Resources and failure handling (interview)

Say this out loud: **the control plane is a gateway, not a third scheduler.** Kubernetes and Slurm already queue, binpack, and enforce limits. We bias *where* the job goes; they decide *how* it fits.

---

## 1. Resource management

Two layers. Do not mix them up in the interview.

### Layer A — Placement (soft, in our API)

Before submit, each adapter reports **inventory**:

- healthy / not
- idle CPU, idle memory
- running + pending counts (queue pressure)

`placement.js` **scores** healthy clusters. Capacity is a bonus, not a hard gate:

- idle CPU ≥ request → **+15**
- idle memory ≥ request → **+10**
- `(running + pending) * 2`, capped at **−20**

Busy clusters can still win. If we rejected whenever idle CPU was low, we would hide the real scheduler’s queue and give a worse UX.

Other score inputs (workload shape, not live capacity):

| Signal | Effect |
|---|---|
| `workload_class=batch` | Slurm +40, Kubernetes +10 |
| `workload_class=service` | Kubernetes +40, Slurm +5 |
| Non-default image | Kubernetes +25 |
| `ntasks > 1` | Slurm +30 |
| GPU field > 0 | Kubernetes +10 (laptop has no GPUs; preference only) |

Unhealthy cluster = **ineligible**, even with a higher theoretical score.

Phrase: *“Soft placement, hard execution.”*

### Layer B — Execution (hard, on the cluster)

**Kubernetes**  
The Job container sets **requests and limits** to the form values (`cpu`, `memory_mb` → `128Mi`). kubelet applies cgroups. If the node cannot fit the **request**, the Pod stays `Pending` (our UI: `pending` / waiting for pod). If it exceeds **limit**, the runtime throttles CPU or OOM-kills (Job `failed`).

Idle CPU on the UI card is: node allocatable minus **requests** of non-terminal pods — the same numbers kube-scheduler uses to think about fit.

**Slurm**  
`#SBATCH --cpus-per-task` and `--mem=` are sent to `slurmctld`. `SelectType=cons_tres` + `CR_CPU_Memory` means: only assign `c1`/`c2` if that node has enough free CPU and memory. If not, the job stays `PENDING` in `squeue` (our UI: `pending`). We do not pick `c1` vs `c2`; Slurm does.

Slurm UI idle CPU comes from `sinfo %C` (allocated/idle/other/total). Idle memory from `%e`.

### What we do not do (and should say)

- No team quotas, QOS, or PriorityClass.
- No GPU device plugin / GRES (field exists, hardware does not).
- No “reject at API if cluster is 80% full.” Backends already queue.
- Production would add **admission** (quotas, max walltime, allowed images) on the gateway and still leave **fit** to Kubernetes/Slurm.

---

## 2. Failure handling

Walk these in order. They map 1:1 to code.

### Submit path

| Failure | What we do | HTTP |
|---|---|---|
| No healthy cluster | Do not submit. Job `failed`. | 503 |
| Hinted cluster down, peer healthy | Fail over, `fallback=true` | 201 |
| `adapter.submit` throws | Retry **once** on the other healthy cluster | 201 if peer works |
| Both submits fail | Job `failed` | 502 |
| Bad spec (empty command, bad name) | No cluster call | 422 |

`dispatch()` in `server.js` is the one-shot failover.

### Runtime path

Reconcile every ~3s calls `adapter.status(native_id)` for non-terminal jobs.

| Failure | What we do |
|---|---|
| Command exit ≠ 0 | Cluster says failed → UI `failed` |
| Image pull error (`ErrImagePull`) | Job `failed` on Kubernetes after backoff |
| Status blip / `UNKNOWN` | **Do not** overwrite `pending`/`running` |
| Control plane crash | Jobs keep running on the cluster. On restart, Mongo + `status()` rebuild the table |
| Node `c1` down | Slurm can still place on `c2` (partition has both) |

Phrase: *“Clusters are the source of truth; MongoDB is an index.”*

### Cancel path

| Case | What we do |
|---|---|
| Job already succeeded/failed/cancelled | **409**, button disabled. Do not lie. |
| Job pending/running | `kubectl delete job` or `scancel`, then mark `cancelled` |
| Backend cancel throws | **502**, message kept. Do not mark cancelled if the cluster said no |

### Demo-shaped gaps (say them; it scores well)

- Slurm does not run the OCI image (no Pyxis). Same command, different runtime.
- One-shot failover can surprise: a “batch” job might land on Kubernetes if Slurm `sbatch` fails.
- Mongo has no auth in the laptop demo.
- k3s is one node; Kind with 2 workers was the original laptop diagram.

---

## 3. 60-second spoken version

> Resource management is two layers. The control plane scores healthy clusters using workload shape and idle CPU/memory. That score is a hint, not admission. Kubernetes enforces requests/limits on the pod; Slurm enforces CPU/memory via `sbatch` and `cons_tres` on `c1`/`c2`. If a cluster is busy, we still submit and let it queue.
>
> Failures: unhealthy clusters are skipped. Submit error fails over once. Both down is 502. We never clobber running with unknown. Cancel of a finished job is 409. After an API crash, the job is still on the cluster and reconcile catches up.

---

## 4. If they push back

**Why not reject when idle CPU < request?**  
Because idle is a snapshot. Slurm/Kubernetes will start the job when a slot frees. Rejecting in the gateway duplicates the scheduler badly.

**Why fail over instead of returning an error?**  
Demo availability. I would make failover a policy flag in production (some users must stay on Slurm for licensing).

**Why not an LLM for placement?**  
Need an explainable reason string in the UI and a deterministic demo. Weights are documented in `placement.js`.
