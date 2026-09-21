const crypto = require("crypto");

const WorkloadClass = {
  BATCH: "batch",
  SERVICE: "service",
};

const SchedulerName = {
  KUBERNETES: "kubernetes",
  SLURM: "slurm",
};

const JobStatus = {
  ACCEPTED: "accepted",
  PENDING: "pending",
  RUNNING: "running",
  SUCCEEDED: "succeeded",
  FAILED: "failed",
  CANCELLED: "cancelled",
  UNKNOWN: "unknown",
};

const TERMINAL_STATUSES = new Set([
  JobStatus.SUCCEEDED,
  JobStatus.FAILED,
  JobStatus.CANCELLED,
]);

class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = "ValidationError";
  }
}

function utcnow() {
  return new Date().toISOString();
}

function defaultResources() {
  return { cpu: 1, memory_mb: 128, gpu: 0, ntasks: 1 };
}

function parseResources(raw = {}) {
  const resources = {
    cpu: raw.cpu == null ? 1 : Number(raw.cpu),
    memory_mb: raw.memory_mb == null ? 128 : Number(raw.memory_mb),
    gpu: raw.gpu == null ? 0 : Number(raw.gpu),
    ntasks: raw.ntasks == null ? 1 : Number(raw.ntasks),
  };
  if (!(resources.cpu > 0)) throw new ValidationError("cpu must be greater than 0");
  if (!(resources.memory_mb > 0)) throw new ValidationError("memory_mb must be greater than 0");
  if (!(resources.gpu >= 0)) throw new ValidationError("gpu must be >= 0");
  if (!(resources.ntasks >= 1)) throw new ValidationError("ntasks must be >= 1");
  resources.memory_mb = Math.trunc(resources.memory_mb);
  resources.gpu = Math.trunc(resources.gpu);
  resources.ntasks = Math.trunc(resources.ntasks);
  return resources;
}

function sanitizeName(value) {
  const cleaned = String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9-_]/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 63);
  if (!cleaned) throw new ValidationError("name must contain alphanumeric characters");
  return cleaned;
}

function parseJobSpec(raw = {}) {
  if (!raw.command || !String(raw.command).trim()) {
    throw new ValidationError("command is required");
  }
  const workloadClass = raw.workload_class || WorkloadClass.BATCH;
  if (!Object.values(WorkloadClass).includes(workloadClass)) {
    throw new ValidationError("workload_class must be batch or service");
  }
  let schedulerHint = raw.scheduler_hint || null;
  if (schedulerHint === "") schedulerHint = null;
  if (schedulerHint && !Object.values(SchedulerName).includes(schedulerHint)) {
    throw new ValidationError("scheduler_hint must be kubernetes or slurm");
  }
  return {
    name: sanitizeName(raw.name),
    command: String(raw.command),
    image: raw.image || "ubuntu:22.04",
    workload_class: workloadClass,
    resources: parseResources(raw.resources),
    scheduler_hint: schedulerHint,
  };
}

function newJobRecord(spec) {
  const now = utcnow();
  return {
    id: crypto.randomUUID().replace(/-/g, "").slice(0, 12),
    spec,
    status: JobStatus.ACCEPTED,
    scheduler: null,
    native_id: null,
    placement: null,
    message: "",
    created_at: now,
    updated_at: now,
  };
}

function jobView(record) {
  return {
    id: record.id,
    name: record.spec.name,
    status: record.status,
    scheduler: record.scheduler,
    native_id: record.native_id,
    placement_reason: record.placement ? record.placement.reason : null,
    message: record.message || "",
    command: record.spec.command,
    image: record.spec.image,
    workload_class: record.spec.workload_class,
    resources: record.spec.resources,
    created_at: record.created_at,
    updated_at: record.updated_at,
  };
}

function clusterView(inv) {
  return {
    scheduler: inv.scheduler,
    healthy: inv.healthy,
    cpu_idle: inv.cpu_idle,
    memory_mb_idle: inv.memory_mb_idle,
    running_jobs: inv.running_jobs,
    pending_jobs: inv.pending_jobs,
    message: inv.message,
  };
}

function inventory(partial) {
  return {
    scheduler: partial.scheduler,
    healthy: Boolean(partial.healthy),
    cpu_idle: partial.cpu_idle || 0,
    memory_mb_idle: partial.memory_mb_idle || 0,
    running_jobs: partial.running_jobs || 0,
    pending_jobs: partial.pending_jobs || 0,
    message: partial.message || "",
  };
}

module.exports = {
  WorkloadClass,
  SchedulerName,
  JobStatus,
  TERMINAL_STATUSES,
  ValidationError,
  utcnow,
  defaultResources,
  parseJobSpec,
  newJobRecord,
  jobView,
  clusterView,
  inventory,
};
