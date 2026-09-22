const { test } = require("node:test");
const assert = require("node:assert/strict");
const { parseJobSpec, SchedulerName, WorkloadClass } = require("../src/models");
const { PlacementError, place } = require("../src/placement");

function inv(name, healthy, cpu = 4, mem = 2048, running = 0) {
  return {
    scheduler: name,
    healthy,
    cpu_idle: cpu,
    memory_mb_idle: mem,
    running_jobs: running,
    pending_jobs: 0,
    message: healthy ? "ok" : "down",
  };
}

test("batch job goes to slurm", () => {
  const spec = parseJobSpec({ name: "train", command: "sleep 1", workload_class: WorkloadClass.BATCH });
  const decision = place(spec, [
    inv(SchedulerName.KUBERNETES, true),
    inv(SchedulerName.SLURM, true),
  ]);
  assert.equal(decision.scheduler, SchedulerName.SLURM);
  assert.match(decision.reason, /batch/);
});

test("container service goes to kubernetes", () => {
  const spec = parseJobSpec({
    name: "serve",
    command: "python app.py",
    image: "python:3.11-slim",
    workload_class: WorkloadClass.SERVICE,
  });
  const decision = place(spec, [
    inv(SchedulerName.KUBERNETES, true),
    inv(SchedulerName.SLURM, true),
  ]);
  assert.equal(decision.scheduler, SchedulerName.KUBERNETES);
  assert.match(decision.reason, /container image/);
});

test("hint pins scheduler", () => {
  const spec = parseJobSpec({
    name: "forced",
    command: "sleep 1",
    workload_class: WorkloadClass.BATCH,
    scheduler_hint: SchedulerName.KUBERNETES,
  });
  const decision = place(spec, [
    inv(SchedulerName.KUBERNETES, true),
    inv(SchedulerName.SLURM, true),
  ]);
  assert.equal(decision.scheduler, SchedulerName.KUBERNETES);
  assert.match(decision.reason, /hint/);
});

test("unhealthy hint fails over", () => {
  const spec = parseJobSpec({
    name: "forced",
    command: "sleep 1",
    scheduler_hint: SchedulerName.SLURM,
  });
  const decision = place(spec, [
    inv(SchedulerName.KUBERNETES, true),
    inv(SchedulerName.SLURM, false),
  ]);
  assert.equal(decision.scheduler, SchedulerName.KUBERNETES);
  assert.equal(decision.fallback, true);
});

test("no healthy cluster raises", () => {
  const spec = parseJobSpec({ name: "x", command: "true" });
  assert.throws(
    () => place(spec, [inv(SchedulerName.KUBERNETES, false), inv(SchedulerName.SLURM, false)]),
    PlacementError,
  );
});

test("parallel tasks prefer slurm", () => {
  const spec = parseJobSpec({
    name: "mpi-like",
    command: "echo hi",
    resources: { ntasks: 4 },
  });
  const decision = place(spec, [
    inv(SchedulerName.KUBERNETES, true, 4, 2048, 2),
    inv(SchedulerName.SLURM, true, 4, 2048, 0),
  ]);
  assert.equal(decision.scheduler, SchedulerName.SLURM);
});

test("both clusters busy returns queue decision instead of reject", () => {
  const spec = parseJobSpec({
    name: "wait",
    command: "sleep 1",
    resources: { cpu: 4, memory_mb: 1024 },
  });
  const decision = place(spec, [
    inv(SchedulerName.KUBERNETES, true, 0.5, 128, 3),
    inv(SchedulerName.SLURM, true, 1, 64, 2),
  ]);
  assert.equal(decision.queue, true);
  assert.equal(decision.scheduler, null);
  assert.match(decision.reason, /queued/i);
});

test("places on the cluster that still has capacity", () => {
  const spec = parseJobSpec({
    name: "fit",
    command: "sleep 1",
    workload_class: WorkloadClass.BATCH,
    resources: { cpu: 2, memory_mb: 256 },
  });
  const decision = place(spec, [
    inv(SchedulerName.KUBERNETES, true, 0.1, 64, 5),
    inv(SchedulerName.SLURM, true, 4, 2048, 0),
  ]);
  assert.equal(decision.queue, false);
  assert.equal(decision.scheduler, SchedulerName.SLURM);
});
