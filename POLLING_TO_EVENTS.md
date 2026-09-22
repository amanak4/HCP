# From Polling to Queues & Events

A plain-language guide to how the Hybrid Compute Platform changed job **admission**, **status**, and **capacity checks** — from timer-based polling to an event-driven control-plane queue.

Use this when you demo or interview: it explains *why*, *what changed*, *how it works*, and *where the code lives*.

---

## 1. The problem we were solving

Originally the control plane did two things on a timer:

1. **Status reconcile (~every 3 seconds)**  
   For every non-finished job, call Kubernetes / Slurm and ask: “What is this job doing?” Then write the answer into MongoDB.

2. **Soft placement**  
   Look at idle CPU/memory as a *score bonus*, but still **submit even when clusters were full**. The real queue lived inside Kubernetes or Slurm. If submits failed badly, the API could return **502**.

That works for a small demo, but it has clear downsides:

| Issue | Why it hurts |
|---|---|
| Constant polling | API keeps waking clusters even when nothing changed |
| Lag | Status can be up to one poll interval stale |
| Busy = surprise | User may still get errors, or overload already-full clusters |
| Capacity checks on a clock | We re-read inventory whether or not a job finished |

**Goal of this change**

- When **both clusters are busy**, **do not fail** the user with a “no space” / 502 style response.
- Put the job in a **control-plane queue** (`queued`) and accept it (**HTTP 202**).
- When a job **finishes / fails / is cancelled**, *then* re-check free capacity and start the next queued job.
- For **job status**, stop polling both backends on a timer. Use **events** instead:
  - Kubernetes → **Watch**
  - Slurm → **job script callback**

---

## 2. Before vs after (one picture)

### Before (polling)

```text
User submits job
      │
      ▼
Place (soft score) ──► always try submit to a cluster
      │
      ▼
Mongo: pending / running / ...
      │
      ▼
Every ~3s: ask K8s + Slurm for every active job   ◄── POLL LOOP
      │
      ▼
Update Mongo → UI reads Mongo every 4s
```

### After (queue + events)

```text
User submits job
      │
      ▼
Place with HARD FIT (enough idle CPU + memory?)
      │
      ├── yes → submit to chosen cluster → pending
      │
      └── no (both busy but healthy) → status=queued (HTTP 202)
                                              │
                                              │  wait...
                                              ▼
Cluster event: job finished / failed / cancelled / updated
      │
      ├── update Mongo status
      └── if terminal → re-check capacity → drain queue → submit next jobs

Steady-state status:
  Kubernetes ── Watch stream ──────────────────┐
                                               ├──► event bus → Mongo
  Slurm job ── HTTP callback to control plane ─┘
```

**Important:** the browser still calls `GET /jobs` and `GET /clusters` every ~4 seconds. That is only the **UI refreshing the page**. It is **not** the old 3s cluster reconcile. Job truth is updated by events into Mongo; the UI just reloads the index.

---

## 3. New job states (what the user sees)

| Status | Meaning |
|---|---|
| `accepted` | Spec parsed, record created (brief) |
| `queued` | **New.** Healthy clusters exist, but none have free capacity *right now*. Waiting in our API queue. |
| `pending` | Submitted to K8s or Slurm; waiting to start on that cluster |
| `running` | Actually executing |
| `succeeded` / `failed` / `cancelled` | Finished (terminal) |

Flow:

```text
accepted ──► pending ──► running ──► succeeded / failed / cancelled
    │
    └──► queued ──(event frees capacity)──► pending ──► ...
              │
              └── cancel while queued → cancelled (never hit a cluster)
```

HTTP codes on submit:

| Situation | HTTP | Status in body |
|---|---|---|
| Placed and submitted | **201** | `pending` |
| Both busy → control-plane queue | **202** | `queued` |
| No healthy cluster | **503** | `failed` |
| Both adapters throw on submit | **502** | `failed` |
| Bad YAML/JSON | **422** | (no good job) |

---

## 4. Hard fit + control-plane queue (in depth)

### 4.1 What “busy” means here

We ask each adapter for a live **inventory** snapshot:

- healthy or not
- idle CPU
- idle memory
- rough running/pending counts

**Hard fit (`canFit`):**

```text
cluster is healthy
AND idle_cpu    >= job.cpu
AND idle_memory >= job.memory_mb
```

Only clusters that pass `canFit` are eligible for immediate placement. Among those, we still **score** (batch → Slurm bias, service/image → Kubernetes bias, etc.) and pick a winner.

### 4.2 If nobody fits

If at least one cluster is healthy, but **no** cluster has enough idle resources:

- Do **not** return 502 / “no space”
- Set `status = queued`
- Store a reason like: *“all healthy clusters are at capacity; queued until resources free”*
- Return **202 Accepted**

The job sits in Mongo until something frees capacity.

### 4.3 When do we look at capacity again?

**Not** on a timer.

We re-check inventory when:

1. A **new submit** arrives  
2. A job reaches a **terminal** state via an event (succeeded / failed / cancelled)  
3. User **cancels** a running/pending job (capacity may free)  
4. API **starts up** (one-shot bootstrap + drain)

That is the “event-driven drain”:

```text
terminal event → drainQueue()
                   │
                   ├─ load queued jobs (oldest first)
                   ├─ fetch inventories once
                   ├─ for each queued job: place() if it fits → dispatch
                   └─ reserve capacity in the local snapshot so we don’t over-admit in one burst
```

If a large queued job still doesn’t fit, we can skip to a smaller one behind it (backfill-style), instead of blocking the whole queue forever.

### 4.4 Where this code lives

| What | File |
|---|---|
| `canFit`, queue decision in `place()` | `control-plane/src/placement.js` |
| `QUEUED` status | `control-plane/src/models.js` |
| List queued jobs | `control-plane/src/store.js` → `queued()` |
| Enqueue + drain | `control-plane/src/events.js` |
| Submit returns 202 when queued | `control-plane/src/server.js` → `POST /api/v1/jobs` |

---

## 5. Event-driven status (replacing the 3s reconcile)

### 5.1 Why remove the reconcile loop?

The old loop:

```text
every 3 seconds:
  for each active job:
    adapter.status(job)   // kubectl read OR scontrol show job
    write Mongo
```

Problems: noisy, wasteful, and it mixes “are we free?” with “did status change?” on the same clock.

Now:

- Status changes arrive as **events**
- Mongo is updated from those events
- Queue drain runs when a job **ends**

On API restart we still do a **one-shot bootstrap** (ask once for active jobs) so we don’t lose the world after a crash — then Watch/callbacks take over. That is recovery, not a standing poll.

Bootstrap file: `control-plane/src/bootstrap.js`  
Old file removed: `control-plane/src/reconcile.js`

---

## 6. Kubernetes Watch (in depth)

### 6.1 What is a Watch?

Kubernetes stores objects (Jobs, Pods, …) in etcd. Clients can:

- **GET** one object (point read) — what polling did  
- **WATCH** a list — open a long-lived stream; the API server pushes changes

Watch events look like:

| Type | Meaning |
|---|---|
| `ADDED` | Object created |
| `MODIFIED` | Spec or status changed |
| `DELETED` | Object removed |

So instead of asking “any news?” every 3 seconds, we listen once and get news when it happens.

### 6.2 What we watch in HCP

- API path: `/apis/batch/v1/namespaces/hcp/jobs`
- Label filter: `hcp.managed=true` (only our jobs)
- Job id: label `hcp.job-id` (our Mongo id)
- Native name: e.g. `hcp-<id>`

When Watch fires:

1. Map Job fields (`active` / `succeeded` / `failed`) → unified status  
2. Call the shared event handler  
3. Update Mongo  
4. If terminal → drain the control-plane queue  

If the Watch connection drops, we reconnect with backoff.

### 6.3 Where the code lives

| Piece | Location |
|---|---|
| Labels on create | `adapters/kubernetes.js` → `submit()` |
| Status map helper | `mapJobStatus()` in same file |
| Watch loop | `startWatch(onEvent)` in same file |
| Started at boot | `server.js` after listen → `adapter.startWatch(...)` |

Library: `@kubernetes/client-node` → `k8s.Watch`.

### 6.4 Mental model

```text
kube-apiserver  ===== long HTTP stream =====►  KubernetesAdapter.startWatch
                                                      │
                                                      ▼
                                              onEvent({ job_id, status, ... })
                                                      │
                                                      ▼
                                              events.handleJobEvent → Mongo
```

---

## 7. Spurm… Slurm callbacks (in depth)

### 7.1 Why not Watch for Slurm?

Slurm’s control daemon (`slurmctld`) does **not** expose a Kubernetes-style Watch over HTTP. Typical tools (`squeue`, `scontrol show job`) are pull APIs.

So we push from the **job itself**.

### 7.2 What is a callback here?

When we `sbatch` a job, the generated script includes a small function `hcp_notify` that:

1. Builds a JSON body (`job_id`, `status`, `scheduler=slurm`, `native_id`, `token`)
2. Opens a TCP connection to the control plane (`host.docker.internal:8080` by default)
3. Sends `POST /api/v1/internal/events` using bash `/dev/tcp` (no curl required)

### 7.3 When does the script notify?

| When | Status |
|---|---|
| Script actually starts on a compute node | `running` |
| User command finishes successfully | `succeeded` |
| Command fails (`ERR` trap) | `failed` |
| Job killed (`TERM`/`INT`, e.g. cancel while running) | `cancelled` |

**Gap to be aware of:** while Slurm still has the job in its own PENDING state (waiting for a node), our script has not started, so there is **no** callback yet. Mongo stays `pending` from submit until the script runs. That is normal.

### 7.4 Network detail (demo)

Compute containers `hcp-c1` / `hcp-c2` must reach the API on the host. Compose adds:

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

Config knobs (`control-plane/src/config.js`):

- `HCP_CALLBACK_HOST` (default `host.docker.internal`)
- `HCP_CALLBACK_PORT` (default API port)
- `HCP_EVENT_TOKEN` (shared secret on the event endpoint)

### 7.5 Where the code lives

| Piece | Location |
|---|---|
| Script with `hcp_notify` + traps | `adapters/slurm.js` → `sbatchScript()` |
| Event HTTP API | `server.js` → `POST /api/v1/internal/events` |
| Same event bus as K8s | `events.js` |
| Compose host mapping | `docker-compose.slurm.yml` |
| `startWatch` for Slurm | no-op (callbacks replace Watch) |

### 7.6 Mental model

```text
sbatch script on c1/c2
   │
   ├─ start  → POST events { status: running }
   ├─ ok     → POST events { status: succeeded }
   ├─ error  → POST events { status: failed }
   └─ signal → POST events { status: cancelled }
                    │
                    ▼
         POST /api/v1/internal/events  (token checked)
                    │
                    ▼
         events.handleJobEvent → Mongo → maybe drainQueue()
```

---

## 8. Shared event bus (the glue)

Both backends end in the same place: `control-plane/src/events.js`.

Responsibilities:

1. **`handleJobEvent`** — validate event, load job, apply status  
2. **`applyStatus`** — write Mongo; ignore late events that would resurrect a finished job; ignore noisy `unknown` overwriting `pending`/`running`  
3. **`drainQueue`** — on terminal transition, try to dispatch queued work  
4. **`enqueue`** — mark a job queued with a human-readable reason  

Kubernetes Watch calls this **in-process** (no HTTP).  
Slurm calls it **over HTTP** at `/api/v1/internal/events`.

Same handler either way → one status model, one queue drain path.

---

## 9. End-to-end scenarios

### Scenario A — Cluster has space

1. User submits batch job  
2. Inventory shows Slurm (or K8s) can fit  
3. Score picks a winner → `submit` → `pending` → **201**  
4. Watch or callback moves status to `running` then `succeeded`  
5. Terminal event → drain queue (maybe nothing waiting)

### Scenario B — Both clusters full

1. User submits heavy job  
2. `canFit` fails on both → **`queued`**, **202**  
3. User sees status `queued` and a reason in “Why”  
4. Another job finishes → Watch/callback → terminal → `drainQueue`  
5. Inventory now fits → dispatch → `pending` on a cluster  

### Scenario C — Cancel while queued

1. Job never reached a cluster  
2. Cancel → mark `cancelled` in Mongo only (no `kubectl` / `scancel`)  

### Scenario D — Cancel while running

1. Adapter cancel on cluster  
2. Mark cancelled; drain queue (slot may free)  
3. K8s may also emit DELETED; Slurm script may emit cancelled via trap  

### Scenario E — API restarts

1. Jobs keep running on clusters  
2. On start: one-shot bootstrap refreshes active jobs  
3. Drain any `queued` jobs if capacity exists  
4. Start K8s Watch again; future Slurm scripts still callback  

---

## 10. File map (everything that changed for this design)

| File | Role |
|---|---|
| `control-plane/src/placement.js` | Hard fit + “queue instead of reject” |
| `control-plane/src/models.js` | `JobStatus.QUEUED` |
| `control-plane/src/store.js` | `queued()` query |
| `control-plane/src/events.js` | Event bus, enqueue, drain |
| `control-plane/src/bootstrap.js` | One-shot catch-up after restart |
| `control-plane/src/server.js` | 202 queue path, events endpoint, start Watch, no reconcile loop |
| `control-plane/src/adapters/kubernetes.js` | Job Watch |
| `control-plane/src/adapters/slurm.js` | Callbacks inside sbatch script |
| `control-plane/src/config.js` | Callback host/port/token (reconcile interval removed) |
| `docker-compose.slurm.yml` | `host.docker.internal` for callbacks |
| `web/index.html` | Show/cancel `queued` jobs |
| `control-plane/tests/placement.test.js` | Tests for queue-when-busy |
| *(deleted)* `control-plane/src/reconcile.js` | Old 3s poll loop |

Docs updated for the same story: `README.md`, `ARCHITECTURE.md`, `RESOURCES_AND_FAILURES.md`, `INTERVIEW.md`, `CLUSTER_EXECUTION.md`.

---

## 11. What the Network tab is *not*

If DevTools shows repeating:

```text
GET /api/v1/jobs
GET /api/v1/clusters
```

every few seconds — that is the **web UI** (`setInterval(refreshAll, 4000)` in `web/index.html`).

It refreshes cards and the table from Mongo (and live inventory for cluster cards). It does **not** mean the control plane is still reconciling every 3 seconds against every job.

---

## 12. Interview phrases (short)

- “We admit with a hard fit. If both clusters are full, we queue in the control plane with 202 — we don’t return no-space errors.”  
- “Capacity is re-checked when a job event frees resources, not on a poll timer.”  
- “Kubernetes status is a Watch on our labeled Jobs. Slurm has no Watch, so the job script callbacks into `/api/v1/internal/events`.”  
- “Mongo is an index. Clusters remain the source of truth. Events keep the index honest.”  
- “UI polling is only for the demo page refresh; backend status is event-driven.”

---

## 13. Honest demo gaps (say them)

- Slurm callbacks need the compute node to reach the API host (`host.docker.internal`). If compose wasn’t recreated after that change, callbacks may not arrive.  
- Inventory is a snapshot; races between two submits are possible (we mitigate during one drain pass by reserving capacity in memory).  
- K8s Watch DELETE after TTL could look like cancel if we never saw succeeded (we ignore status changes once already terminal).  
- While a Slurm job is pending *inside Slurm*, we won’t get a script callback until it starts.  

---

## 15. Live logs (SSE follow)

Status is event-driven; **logs** use a separate live stream (not polling `GET /logs`):

- Endpoint: `GET /api/v1/jobs/:id/logs/stream` (Server-Sent Events)
- Kubernetes: `Log` API with `follow: true` (like `kubectl logs -f`)
- Slurm: `tail -F` on `/data/jobs/<id>/slurm.out` and `.err`
- UI Logs drawer opens an `EventSource` and appends lines as they arrive; closing the drawer aborts the stream

Snapshot `GET /api/v1/jobs/:id/logs` still exists for CLI one-shot reads.

---

## 14. Quick checklist to verify locally

1. Clusters up (k3s + Slurm + Mongo).  
2. API started with `KUBECONFIG=$HOME/.kube/k3s.yaml`.  
3. Submit until both look full → next submit should be **`queued` / 202**.  
4. Let a job finish → queued job should move to **`pending`** without you resubmitting.  
5. K8s job status should flip without a 3s reconcile in the API logs.  
6. Slurm job should hit `POST /api/v1/internal/events` when it starts/finishes (API logs / Network or server logs).  
7. Open Logs on a running counting job → numbers should appear live (SSE), without repeated `GET /logs`.  

That is the full path: **from polling → control-plane queue + Kubernetes Watch + Slurm callbacks + live log follow**, start to finish.
