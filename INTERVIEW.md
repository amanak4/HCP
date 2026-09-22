# Interview prep

Target: a 20–30 minute demo plus architecture discussion. You are not being graded on knowing every `sbatch` flag. You are being graded on whether you built a real abstraction, can defend it, and know what is missing.

---

## 60-second opening

> “I built a hybrid compute control plane. Users submit a workload spec — command, image, CPU, memory, batch vs service. They never say Kubernetes or Slurm. A placement policy scores both clusters using workload shape and live capacity, then an adapter turns that into a Kubernetes Job or an `sbatch` script. Status, logs, and cancel all come back through the same API. I ran two real clusters on this laptop: Kind with a control-plane and two workers, and Slurm with a controller and two compute nodes.”

Then show [ARCHITECTURE.md](ARCHITECTURE.md) diagram 1.

---

## Live demo script (8 minutes)

1. Open the UI. Point at the two health cards. *“Separate clusters, both healthy, different inventories.”*
2. Submit the **batch** preset. Show it routed to **Slurm** and read the reason string.
3. Submit the **Python container** preset. Show it routed to **Kubernetes**.
4. `list` both jobs. Same columns, different schedulers.
5. Open logs on each. `HELLO_FROM_SLURM` vs `HELLO_FROM_K8S`.
6. Cancel one running job. Status becomes `cancelled`.
7. Optional: stop `hcp-c1` (`docker stop hcp-c1`) and submit again — still works on `c2`, or failover to Kubernetes if Slurm goes unhealthy.
8. Stop talking. Ask if they want failure handling or production next.

CLI backup if the UI misbehaves:

```bash
node control-plane/src/cli.js submit examples/slurm-batch-hello.yaml
node control-plane/src/cli.js submit examples/k8s-python-pi.yaml
node control-plane/src/cli.js list
```

---

## Likely questions and answers

### Why not one cluster? Why not run Slurm on Kubernetes or Kubernetes on Slurm?

Because the assignment said *hybrid* and *separate clusters*. Real labs already have both: Kubernetes for services and Slurm for batch/HPC. Wrapping them is the product. Collapsing them is a different project (Slinky/SUNK, Volcano, etc.). I would mention those as production options, not as this week’s scope.

### How does the user not know which platform ran the job?

The submit schema has no required scheduler field. Placement writes `scheduler` and `native_id` for operators. The user-facing fields are `id`, `status`, `logs`, `message`. I still *display* the scheduler in the demo so I can prove the abstraction works. In a real UI I would hide it behind an “advanced” toggle.

### What is your resource management approach?

Hard fit at submit, event-driven queue after that. The control plane asks each adapter for idle CPU/memory, scores clusters that **can fit**, and dispatches immediately. If both are healthy but full, the job is **`queued`** (HTTP 202) — not a 502. Capacity is re-checked only when a cluster event says a job finished/failed/cancelled, then the queue drains. Production would still add **quotas** (per team) on top.

### Who is the source of truth?

The clusters. MongoDB is an index. Status is event-driven: Kubernetes Watch on Jobs, Slurm job-script callbacks to `/api/v1/internal/events`. After a crash, a one-shot bootstrap refreshes active jobs, then watches/callbacks take over. I never treat the API process as the execution engine.

### How do you handle failure?

- Cluster unhealthy → ineligible for placement.
- Both busy → queue (`queued`), drain on events.
- Hinted cluster down → failover.
- `submit` throws → one retry on the other adapter.
- Both submit failures → 502, job marked failed. No healthy cluster → 503.
- Cancel fails on the backend → surface 502, do not silently claim success.
- Status `unknown` during a blip → do not overwrite `running`/`pending`.
- Job process exits non-zero → unified `failed`.

### Why adapters instead of a giant `if kubernetes: ... else: ...` in the API?

The API should not know `V1Job` or `sbatch` flags. New backends (cloud batch, another Slurm cluster, HPC site B) should be another class that implements `submit/status/cancel/logs/health`. That is the open/closed line in this design.

### Why Node.js/Express and MongoDB?

Laptop demo, one week, readable in an interview. Jobs have nested fields (`resources`, `placement.scores`), so a document store fits better than flattening JSON into SQLite. Production would still add auth, HA, and maybe Postgres if we needed strict relational reporting.

### How do containers work on Slurm in your demo?

They don’t, honestly. Kubernetes runs the image. Slurm runs the command on the node OS. That is a real product gap. Production Slurm would use Pyxis/Enroot or Singularity so the same OCI image runs on both. I would say this out loud — interviewers trust people who name the gap.

### What about GPUs?

The spec has a GPU field so the model is honest. This laptop has none, so the score only records a preference. Production needs device plugins on Kubernetes and GRES on Slurm, plus a control-plane inventory that knows which cluster actually has which GPU SKU.

### Did you look at existing products?

Yes, and I did not vendor them, because the assignment is to *build* the abstraction:

- **Slinky / SUNK (SchedMD)** — Slurm in/near Kubernetes.
- **Kueue + JobSet** — Kubernetes-native batch queues.
- **Armada (G-Research)** — multi-cluster Kubernetes batch.
- **Volcano / Yunikorn** — Kubernetes batch schedulers.

My control plane is closer to a thin **federation/gateway** than to a replacement scheduler.

---

## What I would change to make it production-ready

Say these in this order. It sounds senior.

1. **Identity and tenancy.** OIDC, RBAC, map org/team → Kubernetes namespace + Slurm account/QOS. No anonymous submit.
2. **A real database and HA control plane.** Postgres, migrations, leader election, at-least-once submit with idempotency keys.
3. **Admission, not just scoring.** Team quotas, max walltime, forbidden images, policy as config.
4. **Identical execution environment.** Shared POSIX/object storage, same OCI image on both backends (Pyxis), same secrets injection.
5. **Observability.** OpenTelemetry traces on submit, metrics for queue time / placement / failover, audit log.
6. **Multi-cluster.** N Kubernetes clusters + N Slurm partitions, not one of each.
7. **Preemption and priority.** Pass priority into both schedulers; do not invent a third fairshare algorithm in the gateway unless I must.
8. **No privileged Slurm containers.** Real nodes, real munge/key rotation, real accounting (`slurmdbd`).
9. **Data plane.** Jobs need datasets. Add a workspace/PVC/volume claim in the unified spec.
10. **Day-2.** Drain, version skew, canary of the control plane, backup of the job index.

---

## Design decisions to defend

| Decision | Why | Tradeoff |
|---|---|---|
| Separate clusters + gateway | Matches the assignment and real labs | Extra hop, eventual consistency |
| Score-based placement | Explainable in the UI | Heuristic, not optimal |
| Control-plane queue when busy | No 502/“no space”; drain on job events | Gateway holds work briefly |
| One-shot failover | High availability for a demo | A job might run in a surprising place |
| Event-driven status (watch/callbacks) | No standing poll loop | Needs reachable callback host for Slurm |
| Kind + Compose | Laptop, individual nodes | Not hardware-accurate |

---

## Questions you should ask *them*

Asking good questions is part of the assignment (“do not hesitate to ping me”). Even in the final interview, show you would have asked:

1. Who is the user — scientist, platform team, or CI?
2. Do we need the same container image on both backends in week one?
3. Is hiding the scheduler a hard requirement, or may power users pin it?
4. What is the SLOs — submit latency, or job start time?
5. Any shared filesystem already, or should jobs be assumed stateless?
6. Auth: do we inherit the company’s SSO?
7. Success metric for the week: working demo, or a design doc plus stubs?

If you did not get to ask during the week, say what you assumed:

- Stateless jobs, no shared data.
- Best-effort batch, not strict gang scheduling.
- Laptop-scale nodes.
- Scheduler visibility OK in the operator view.

---

## Cheatsheet: Kubernetes vs Slurm in this project

| | Kubernetes | Slurm |
|---|---|---|
| Cluster | Kind `hcp` | Compose `hcp-slurmctld`, `hcp-c1`, `hcp-c2` |
| Unit of work | `batch/v1 Job` | `sbatch` job |
| Identity of a running job | Job name `hcp-<id>` | Numeric job id |
| Logs | Pod log | `/data/jobs/<id>/slurm.out` |
| Cancel | Delete Job | `scancel` |
| Capacity | Node allocatable minus pod requests | `sinfo %C %e` |

---

## If something is down during the interview

- Only Slurm healthy: submit a container job anyway and show failover/placement still returning a unified job.
- Kind missing: say Kubernetes adapter reports unhealthy; placement only uses Slurm. That *is* failure handling.
- Image pull slow: use `ubuntu:22.04` which Kind often already has, or pre-pull `python:3.11-slim`.
- Never debug Docker in silence for more than 30 seconds. Switch to architecture and come back.

---

## Phrases that score well

- “Hard fit at submit; event-driven drain when a job finishes.”
- “Clusters are the source of truth; MongoDB is an index.”
- “Users submit intent; adapters submit mechanism.”
- “Busy clusters queue in the API — we do not return no-space errors.”
- “The production gap is identity, quotas, and a shared data plane — not more YAML.”
