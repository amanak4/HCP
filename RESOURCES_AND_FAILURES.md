# Resources and failure handling (interview)

Say this out loud: **the control plane is a gateway, not a third scheduler.** Kubernetes and Slurm already queue, binpack, and enforce limits. We bias *where* the job goes; they decide *how* it fits.

---

## 1. Resource management

Two layers. Do not mix them up in the interview.

### Layer A — Placement + control-plane queue

Before submit, each adapter reports **inventory** (on demand at submit / queue drain / cluster UI — not on a timer):

- healthy / not
- idle CPU, idle memory
- running + pending counts (queue pressure)

`placement.js` **scores** healthy clusters that **fit** the request (`canFit`: idle CPU/memory ≥ request). Capacity is a hard admission gate for *immediate* dispatch:

- idle CPU ≥ request and idle memory ≥ request → eligible to place (+ score bonuses)
- healthy but **no** cluster fits → job stays **`queued`** in Mongo (HTTP **202**), not rejected
- when a cluster emits a job lifecycle event (finished / failed / cancelled / updated), the control plane re-checks inventory and drains the queue

Other score inputs (workload shape, not live capacity):

| Signal | Effect |
|---|---|
| `workload_class=batch` | Slurm +40, Kubernetes +10 |
| `workload_class=service` | Kubernetes +40, Slurm +5 |
| Non-default image | Kubernetes +25 |
| `ntasks > 1` | Slurm +30 |
| GPU field > 0 | Kubernetes +10 (laptop has no GPUs; preference only) |

Unhealthy cluster = **ineligible**, even with a higher theoretical score. No healthy cluster = **503**.

Phrase: *“Hard fit at the gateway, event-driven drain — not a poll loop.”*

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
- Busy clusters → **control-plane queue** (`queued`), not HTTP 502 “no space”.
- Production would add **admission** (quotas, max walltime, allowed images) on the gateway and still leave **fit** to Kubernetes/Slurm for work already submitted.

---

## 2. Failure handling

Walk these in order. They map 1:1 to code.

### Submit path

| Failure | What we do | HTTP |
|---|---|---|
| No healthy cluster | Do not submit. Job `failed`. | 503 |
| Both healthy but no free capacity | Job `queued`; drain on cluster events | 202 |
| Hinted cluster down, peer healthy | Fail over, `fallback=true` | 201 |
| `adapter.submit` throws | Retry **once** on the other healthy cluster | 201 if peer works |
| Both submits fail | Job `failed` | 502 |
| Bad spec (empty command, bad name) | No cluster call | 422 |

`dispatch()` in `server.js` is the one-shot failover. `events.drainQueue()` runs when a job reaches a terminal state.

### Runtime path

Status is **event-driven** (no reconcile poll):

- **Kubernetes:** Watch API on `batch/v1` Jobs labeled `hcp.managed=true`
- **Slurm:** job script callbacks via bash `/dev/tcp` → `POST /api/v1/internal/events`
- **Startup:** one-shot `bootstrapStatuses()` then drain queued work

| Failure | What we do |
|---|---|
| Command exit ≠ 0 | Cluster event → UI `failed` |
| Image pull error (`ErrImagePull`) | Job `failed` on Kubernetes after backoff (watch) |
| Status blip / `UNKNOWN` | **Do not** overwrite `pending`/`running` |
| Control plane crash | Jobs keep running on the cluster. On restart, Mongo + one-shot status + watches rebuild the table |
| Node `c1` down | Slurm can still place on `c2` (partition has both) |

Phrase: *“Clusters are the source of truth; MongoDB is an index; events keep them aligned.”*

### Cancel path

| Case | What we do |
|---|---|
| Job already succeeded/failed/cancelled | **409**, button disabled. Do not lie. |
| Job queued | Mark `cancelled` in index (never reached a cluster) |
| Job pending/running | `kubectl delete job` or `scancel`, then mark `cancelled`, then drain queue |
| Backend cancel throws | **502**, message kept. Do not mark cancelled if the cluster said no |

### Demo-shaped gaps (say them; it scores well)

- Slurm does not run the OCI image (no Pyxis). Same command, different runtime.
- One-shot failover can surprise: a “batch” job might land on Kubernetes if Slurm `sbatch` fails.
- Mongo has no auth in the laptop demo.
- k3s is one node; Kind with 2 workers was the original laptop diagram.

---

## 3. 60-second spoken version

> Resource management is two layers. The control plane admits only to healthy clusters with enough idle CPU/memory. If both are busy, the job is queued in the API and drained when a cluster event says a job finished. Kubernetes enforces requests/limits on the pod; Slurm enforces CPU/memory via `sbatch` and `cons_tres` on `c1`/`c2`.
>
> Failures: unhealthy clusters are skipped. Busy → queue (202), not reject. Submit error fails over once. Both submit failures are 502. Status comes from watches/callbacks, not a poll loop. Cancel of a finished job is 409.

---

## 4. If they push back

**Why queue in the API when idle CPU < request?**  
So the user gets an accepted job immediately instead of a 502/“no space” error. Capacity is re-checked only when a job event frees resources — not by polling inventory on a timer.

**Why fail over instead of returning an error?**  
Demo availability. I would make failover a policy flag in production (some users must stay on Slurm for licensing).

**Why not an LLM for placement?**  
Need an explainable reason string in the UI and a deterministic demo. Weights are documented in `placement.js`.
