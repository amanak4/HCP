# Hybrid Compute Platform

One API for jobs that run on Kubernetes or Slurm. The caller submits a workload. Placement chooses a healthy cluster that can fit it. Status, logs, and cancel use the same interface either way.

```text
CLI / Web / REST
        |
  Control plane
  JobSpec → placement → dispatch or queue
        |                    |
  Kubernetes adapter     Slurm adapter
        |                    |
  Kind cluster           slurmctld + c1 + c2
```

The clusters stay separate. Kubernetes runs the container. Slurm runs the command on a compute node. MongoDB is the job index. The cluster is the source of truth for execution.

## Layout

```text
clusters/k8s/                 Kind config and namespace
clusters/slurm/               Slurm image, entrypoint, slurm.conf
control-plane/src/            API, placement, events, store
control-plane/src/adapters/   Kubernetes and Slurm adapters
control-plane/tests/          Placement tests
examples/                     Sample job specs
scripts/                      setup, run, demo, teardown
web/                          UI
```

## Run

Prerequisites: Docker, Node.js 18+, and [kind](https://kind.sigs.k8s.io/) plus kubectl if you want the Kubernetes cluster.

```bash
bash scripts/setup.sh
bash scripts/run-api.sh
```

Open http://127.0.0.1:8080

In another terminal:

```bash
bash scripts/demo.sh
```

That submits a batch job (Slurm), a container job (Kubernetes), a multi-task job (Slurm), and a job that exits non-zero.

CLI:

```bash
node control-plane/src/cli.js clusters
node control-plane/src/cli.js submit examples/slurm-batch-hello.yaml
node control-plane/src/cli.js submit examples/k8s-python-pi.yaml
node control-plane/src/cli.js list
node control-plane/src/cli.js status <id>
node control-plane/src/cli.js logs <id>
node control-plane/src/cli.js cancel <id>
```

Tear down:

```bash
bash scripts/teardown.sh
```

Tests, no clusters required:

```bash
cd control-plane
npm install
npm test
```

## Job spec

A job is a name, command, optional image, workload class (`batch` or `service`), and resources (`cpu`, `memory_mb`, `ntasks`). There is no required scheduler field. `scheduler_hint` is an optional pin.

```yaml
name: slurm-batch-hello
command: "echo HELLO_FROM_SLURM; sleep 15; echo SLURM_DONE"
workload_class: batch
resources:
  cpu: 1
  memory_mb: 128
  ntasks: 1
```

## Placement and status

Each adapter reports health and idle CPU and memory. A cluster is eligible when it is healthy and the request fits. Batch work and `ntasks > 1` score toward Slurm. A service or a non-default image scores toward Kubernetes. If the hinted cluster cannot take the job, submit is tried once on the other healthy cluster.

If both clusters are healthy and neither fits, the API returns 202 and stores the job as `queued`. The queue drains when a job reaches a terminal state. There is no standing capacity poll.

Status after dispatch comes from the cluster: a Kubernetes Watch on Jobs, and a callback from the Slurm job script to `POST /api/v1/internal/events`. On process restart, one bootstrap pass refreshes active jobs, then events take over again.

| | Kubernetes | Slurm |
|---|---|---|
| Submit | `batch/v1` Job in namespace `hcp` | `sbatch` with CPU, memory, and ntasks |
| Identity | Job name `hcp-<id>` | Numeric job id |
| Logs | Pod log | `/data/jobs/<id>/slurm.out` |
| Cancel | Delete the Job | `scancel` |
| Fit | Requests and limits on the container | `cons_tres` / `CR_CPU_Memory` on `c1` and `c2` |
