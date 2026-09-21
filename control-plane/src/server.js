const fs = require("fs");
const path = require("path");
const express = require("express");
const { KubernetesAdapter } = require("./adapters/kubernetes");
const { SlurmAdapter } = require("./adapters/slurm");
const { API_HOST, API_PORT, RECONCILE_SECONDS, WEB_DIR, MONGODB_URI } = require("./config");
const {
  JobStatus,
  TERMINAL_STATUSES,
  ValidationError,
  parseJobSpec,
  newJobRecord,
  jobView,
  clusterView,
} = require("./models");
const { PlacementError, place, fallbackScheduler } = require("./placement");
const { reconcileOnce } = require("./reconcile");
const { connectStore } = require("./store");
const logger = require("./logger");

const app = express();
app.use(express.json({ limit: "1mb" }));

const adapters = {
  kubernetes: new KubernetesAdapter(),
  slurm: new SlurmAdapter(),
};
let store;

function httpError(status, detail) {
  const err = new Error(detail);
  err.status = status;
  err.detail = detail;
  return err;
}

async function inventories() {
  return Promise.all(Object.values(adapters).map((adapter) => adapter.health()));
}

function adapterFor(job) {
  if (!job.scheduler) return null;
  return adapters[job.scheduler] || null;
}

async function requireJob(jobId) {
  const job = await store.get(jobId);
  if (!job) throw httpError(404, "job not found");
  return job;
}

async function dispatch(job, scheduler, decision, clusterInventories) {
  const order = [scheduler];
  const fallback = fallbackScheduler(scheduler, clusterInventories);
  if (fallback) order.push(fallback);

  let lastError = null;
  for (let index = 0; index < order.length; index += 1) {
    const target = order[index];
    job.scheduler = target;
    job.placement = index === 0
      ? decision
      : {
          scheduler: target,
          fallback: true,
          reason: `primary submit to ${scheduler} failed; fallback to ${target}`,
          scores: decision.scores,
        };
    job.status = JobStatus.PENDING;
    job.message = "submitting";
    await store.upsert(job);
    try {
      job.native_id = await adapters[target].submit(job);
      job.message = job.placement.reason;
      await store.upsert(job);
      return;
    } catch (err) {
      lastError = err;
      logger.warn(`submit to ${target} failed: ${err.message}`);
    }
  }
  job.status = JobStatus.FAILED;
  job.message = `submit failed on all schedulers: ${lastError && lastError.message}`;
  await store.upsert(job);
  throw new Error(job.message);
}

async function refresh(job) {
  const adapter = adapterFor(job);
  if (!adapter || !job.native_id) return;
  if (TERMINAL_STATUSES.has(job.status)) return;
  try {
    const { status, message } = await adapter.status(job);
    if (status !== JobStatus.UNKNOWN) job.status = status;
    if (message) job.message = message;
    await store.upsert(job);
  } catch (err) {
    job.message = `status error: ${err.message}`;
    await store.upsert(job);
  }
}

app.get("/api/v1/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.get("/api/v1/clusters", async (_req, res, next) => {
  try {
    const inv = await inventories();
    const counts = {};
    for (const job of await store.list()) {
      counts[job.status] = (counts[job.status] || 0) + 1;
    }
    res.json({
      clusters: inv.map(clusterView),
      jobs: counts,
    });
  } catch (err) {
    next(err);
  }
});

app.post("/api/v1/jobs", async (req, res, next) => {
  let job;
  try {
    const spec = parseJobSpec(req.body || {});
    job = await store.upsert(newJobRecord(spec));
    const clusterInventories = await inventories();
    const decision = place(spec, clusterInventories);
    await dispatch(job, decision.scheduler, decision, clusterInventories);
    res.status(201).json(jobView(job));
  } catch (err) {
    if (job) {
      job.status = JobStatus.FAILED;
      job.message = err instanceof PlacementError ? String(err.message) : `submit failed: ${err.message}`;
      await store.upsert(job);
    }
    if (err instanceof ValidationError) return next(httpError(422, err.message));
    if (err instanceof PlacementError) return next(httpError(503, err.message));
    next(httpError(502, err.message));
  }
});

app.get("/api/v1/jobs", async (_req, res, next) => {
  try {
    res.json((await store.list()).map(jobView));
  } catch (err) {
    next(err);
  }
});

app.get("/api/v1/jobs/:jobId", async (req, res, next) => {
  try {
    const job = await requireJob(req.params.jobId);
    await refresh(job);
    res.json(jobView((await store.get(req.params.jobId)) || job));
  } catch (err) {
    next(err);
  }
});

app.delete("/api/v1/jobs/:jobId", async (req, res, next) => {
  try {
    const job = await requireJob(req.params.jobId);
    if (TERMINAL_STATUSES.has(job.status)) {
      throw httpError(409, `job already ${job.status}; cancel only works while pending or running`);
    }
    const adapter = adapterFor(job);
    try {
      if (adapter) await adapter.cancel(job);
    } catch (err) {
      job.message = `cancel requested, scheduler error: ${err.message}`;
      await store.upsert(job);
      throw httpError(502, err.message);
    }
    job.status = JobStatus.CANCELLED;
    job.message = "cancelled by user";
    await store.upsert(job);
    res.json(jobView(job));
  } catch (err) {
    next(err);
  }
});

app.get("/api/v1/jobs/:jobId/logs", async (req, res, next) => {
  try {
    const job = await requireJob(req.params.jobId);
    const adapter = adapterFor(job);
    const text = adapter ? await adapter.logs(job) : "";
    res.json({ id: job.id, scheduler: job.scheduler, logs: text });
  } catch (err) {
    next(err);
  }
});

if (fs.existsSync(WEB_DIR)) {
  app.use("/ui", express.static(WEB_DIR));
  app.get("/", (_req, res) => {
    res.sendFile(path.join(WEB_DIR, "index.html"));
  });
}

app.use((err, _req, res, _next) => {
  const status = err.status || 500;
  res.status(status).json({ detail: err.detail || err.message || "internal error" });
});

async function reconcileLoop() {
  while (true) {
    try {
      await reconcileOnce(store, adapters);
    } catch (err) {
      logger.warn(`reconcile loop error: ${err.message}`);
    }
    await new Promise((resolve) => setTimeout(resolve, RECONCILE_SECONDS * 1000));
  }
}

async function start() {
  store = await connectStore();
  logger.info(`connected to MongoDB ${MONGODB_URI}`);
  app.listen(API_PORT, API_HOST, () => {
    logger.info(`hybrid compute control plane started on ${API_HOST}:${API_PORT}`);
    reconcileLoop();
  });
}

if (require.main === module) {
  start().catch((err) => {
    logger.warn(`failed to start: ${err.message}`);
    process.exit(1);
  });
}

module.exports = { app };
