# Architecture

Use these diagrams at the start of the interview. Speak the first one for 60 seconds, then walk a submit through the sequence diagram.

---

## 1. System context — start here

```mermaid
flowchart LR
  subgraph Users
    CLI[CLI]
    UI[Web UI]
  end

  subgraph ControlPlane[Control plane]
    API[REST API<br/>submit / list / status / logs / cancel]
    Place[Placement policy]
    Rec[Reconcile loop]
    DB[(MongoDB index)]
    KAd[K8s adapter]
    SAd[Slurm adapter]
  end

  subgraph K8S[Kubernetes cluster - Kind]
    KCP[control-plane node]
    W1[worker]
    W2[worker]
  end

  subgraph SLURM[Slurm cluster - Docker]
    CT[slurmctld]
    C1[c1 slurmd]
    C2[c2 slurmd]
  end

  CLI --> API
  UI --> API
  API --> Place
  API --> DB
  Rec --> DB
  Place --> KAd
  Place --> SAd
  Rec --> KAd
  Rec --> SAd
  KAd --> KCP
  KCP --> W1
  KCP --> W2
  SAd --> CT
  CT --> C1
  CT --> C2
```

Talk track: *“Two independent clusters. One control plane. Adapters hide kubectl and sbatch. Placement picks a backend from the workload shape and live capacity. Reconcile keeps our index honest.”*

---

## 2. Submit sequence

```mermaid
sequenceDiagram
  actor User
  participant API as Control plane
  participant P as Placement
  participant A as Chosen adapter
  participant C as Cluster
  participant DB as MongoDB

  User->>API: POST /api/v1/jobs (JobSpec)
  API->>DB: persist status=accepted
  API->>A: health()
  API->>P: place(spec, inventories)
  P-->>API: scheduler + reason + scores
  API->>A: submit(job)
  alt submit succeeds
    A->>C: create K8s Job or sbatch
    C-->>A: native id
    API->>DB: pending + native_id + reason
    API-->>User: unified JobView
  else submit fails and peer is healthy
    API->>A: submit on fallback adapter
    API->>DB: pending (fallback=true)
    API-->>User: unified JobView
  else both fail
    API->>DB: failed
    API-->>User: 502
  end
```

---

## 3. Placement policy

```mermaid
flowchart TD
  spec[JobSpec]
  health{Any healthy cluster?}
  hint{Operator hint set?}
  hintOk{Hinted cluster healthy?}
  score[Score healthy clusters]
  pick[Highest score wins]
  fail[Reject 503]

  spec --> health
  health -->|no| fail
  health -->|yes| hint
  hint -->|yes| hintOk
  hintOk -->|yes| pin[Pin to hinted scheduler]
  hintOk -->|no| fo[Failover to the other]
  hint -->|no| score
  score --> batch{workload_class}
  batch -->|batch / ntasks greater than 1| slurmBias[Slurm +40 / +30]
  batch -->|service / custom image| k8sBias[Kubernetes +40 / +25]
  slurmBias --> cap[Add idle CPU/memory<br/>subtract queue pressure]
  k8sBias --> cap
  cap --> pick
  fo --> pick
  pin --> pick
```

Scoring is deliberate and explainable. The API stores the reason string on the job so the demo UI can show *why* a job landed on Slurm or Kubernetes.

| Signal | Effect |
|---|---|
| `workload_class=batch` | Slurm +40 |
| `workload_class=service` | Kubernetes +40 |
| Non-default container image | Kubernetes +25 |
| `ntasks > 1` | Slurm +30 |
| Enough idle CPU / memory | +15 / +10 |
| Running+pending pressure | up to -20 |
| `scheduler_hint` | pin, with failover if unhealthy |

---

## 4. Status model

Both backends are mapped into one enum. That is the abstraction.

```mermaid
stateDiagram-v2
  [*] --> accepted
  accepted --> pending: adapter.submit ok
  accepted --> failed: no cluster / submit error
  pending --> running: scheduler started work
  running --> succeeded
  running --> failed
  pending --> cancelled: user cancel
  running --> cancelled: user cancel
  pending --> failed: backend failure
```

| Unified | Kubernetes | Slurm |
|---|---|---|
| pending | Job created, no active pod | PENDING / CONFIGURING |
| running | `active > 0` | RUNNING / COMPLETING |
| succeeded | `succeeded > 0` | COMPLETED |
| failed | `failed > 0` | FAILED / TIMEOUT / OOM |
| cancelled | Job deleted | CANCELLED / scancel |

---

## 5. Failure handling

```mermaid
flowchart LR
  subgraph Submit
    S1[Preferred adapter error] --> S2[Retry once on the other cluster]
    S2 --> S3[If both fail, mark failed]
  end

  subgraph Runtime
    R1[Reconcile poll] --> R2[Map native state]
    R2 --> R3[Do not clobber terminal states with unknown]
  end

  subgraph ControlPlaneCrash
    C1[Jobs keep running on clusters] --> C2[MongoDB replay + status]
  end

  subgraph Cancel
    X1[Best-effort adapter.cancel] --> X2[Mark cancelled in index]
  end
```

---

## 6. Component view of the repo

```mermaid
flowchart TB
  subgraph repo
    web[web/index.html]
    cli[control-plane/src/cli.js]
    api[control-plane/src/server.js]
    models[models.js JobSpec]
    place[placement.js]
    store[store.js]
    k8s[adapters/kubernetes.js]
    slurm[adapters/slurm.js]
    kindcfg[clusters/k8s]
    slurmcfg[clusters/slurm + compose]
  end
  web --> api
  cli --> api
  api --> models
  api --> place
  api --> store
  api --> k8s
  api --> slurm
  k8s --> kindcfg
  slurm --> slurmcfg
```
