const fs = require("fs");
const path = require("path");
const express = require("express");
const { KubernetesAdapter } = require("./adapters/kubernetes");
const { SlurmAdapter } = require("./adapters/slurm");
const {
  API_HOST,
  API_PORT,
  WEB_DIR,
  MONGODB_URI,
  EVENT_TOKEN,
} = require("./config");
const {
  JobStatus,
  TERMINAL_STATUSES,
  ValidationError,
  parseJobSpec,
  newJobRecord,
  jobView,
  clusterView,
} = require("./models");
const { PlacementError, place, applyClaims, fallbackIfFits } = require("./placement");
const { bootstrapStatuses } = require("./bootstrap");
const { createEventBus } = require("./events");
const { connectStore } = require("./store");
const logger = require("./logger");

const app = express();
app.use(express.json({ limit: "1mb" }));

const adapters = {
  kubernetes: new KubernetesAdapter(),
  slurm: new SlurmAdapter(),
};
let store;
let events;
let admitChain = Promise.resolve();

/** Serialize admit so two POSTs in the same second cannot both see a full-idle snapshot. */
function withAdmitLock(fn) {
  const run = admitChain.then(fn, fn);
  admitChain = run.then(() => {}, () => {});
  return run;
}

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
  const fallback = fallbackIfFits(scheduler, job.spec, clusterInventories);
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

  const peer = scheduler === "kubernetes" ? "slurm" : "kubernetes";
  if (lastError && !fallback) {
    await events.enqueue(
      job,
      `primary submit to ${scheduler} failed; ${peer} has no free capacity; queued`,
    );
    return;
  }

  job.status = JobStatus.FAILED;
  job.message = `submit failed on all schedulers: ${lastError && lastError.message}`;
  await store.upsert(job);
  throw new Error(job.message);
}

app.get("/api/v1/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.get("/api/v1/clusters", async (_req, res, next) => {
  try {
    const inv = await inventories();
    const jobs = await store.list();
    const counts = {};
    const byScheduler = {};
    for (const job of jobs) {
      counts[job.status] = (counts[job.status] || 0) + 1;
      if (!job.scheduler) continue;
      if (!byScheduler[job.scheduler]) byScheduler[job.scheduler] = { running: 0, pending: 0 };
      if (job.status === JobStatus.RUNNING) byScheduler[job.scheduler].running += 1;
      if (job.status === JobStatus.PENDING || job.status === JobStatus.ACCEPTED) {
        byScheduler[job.scheduler].pending += 1;
      }
    }
    res.json({
      clusters: inv.map((item) => {
        const view = clusterView(item);
        const ours = byScheduler[view.scheduler];
        if (ours) {
          view.running_jobs = ours.running;
          view.pending_jobs = ours.pending;
        } else {
          view.running_jobs = 0;
          view.pending_jobs = 0;
        }
        return view;
      }),
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
    await withAdmitLock(async () => {
      const clusterInventories = await inventories();
      applyClaims(clusterInventories, await store.active());
      const decision = place(spec, clusterInventories);

      if (decision.queue) {
        await events.enqueue(job, decision.reason);
        return;
      }

      await dispatch(job, decision.scheduler, decision, clusterInventories);
    });
    if (job.status === JobStatus.QUEUED) {
      return res.status(202).json(jobView(job));
    }
    res.status(201).json(jobView(job));
  } catch (err) {
    if (job && job.status !== JobStatus.QUEUED) {
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
    // Status comes from cluster events into Mongo — no live poll on read.
    const job = await requireJob(req.params.jobId);
    res.json(jobView(job));
  } catch (err) {
    next(err);
  }
});

app.delete("/api/v1/jobs/:jobId", async (req, res, next) => {
  try {
    const job = await requireJob(req.params.jobId);
    if (TERMINAL_STATUSES.has(job.status)) {
      throw httpError(409, `job already ${job.status}; cancel only works while queued, pending or running`);
    }

    if (job.status === JobStatus.QUEUED) {
      job.status = JobStatus.CANCELLED;
      job.message = "cancelled while queued";
      await store.upsert(job);
      return res.json(jobView(job));
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
    // Capacity may have freed — drain control-plane queue.
    await events.drainQueue();
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

/**
 * Live log follow via Server-Sent Events (not client polling).
 * Kubernetes: pod log follow. Slurm: tail -F of sbatch output files.
 */
app.get("/api/v1/jobs/:jobId/logs/stream", async (req, res, next) => {
  let ac;
  try {
    const job = await requireJob(req.params.jobId);
    res.status(200);
    res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    if (typeof res.flushHeaders === "function") res.flushHeaders();

    const send = (payload) => {
      if (res.writableEnded) return;
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    };

    send({ type: "meta", id: job.id, scheduler: job.scheduler, status: job.status });

    if (job.status === JobStatus.QUEUED || !job.scheduler) {
      send({ type: "line", text: "(job is queued — logs appear after it is dispatched)\n" });
      send({ type: "end", reason: "queued" });
      return res.end();
    }

    const adapter = adapterFor(job);
    if (!adapter || typeof adapter.streamLogs !== "function") {
      send({ type: "line", text: "(log streaming not available for this scheduler)\n" });
      send({ type: "end", reason: "unsupported" });
      return res.end();
    }

    ac = new AbortController();
    const onClose = () => ac.abort();
    req.on("close", onClose);
    req.on("aborted", onClose);

    try {
      await adapter.streamLogs(job, (chunk) => {
        if (!chunk) return;
        send({ type: "line", text: String(chunk) });
      }, { signal: ac.signal });
      send({ type: "end", reason: ac.signal.aborted ? "client_closed" : "stream_closed" });
    } catch (err) {
      if (!ac.signal.aborted) {
        send({ type: "line", text: `(stream error) ${err.message || err}\n` });
        send({ type: "end", reason: "error" });
      }
    } finally {
      req.off("close", onClose);
      req.off("aborted", onClose);
      if (!res.writableEnded) res.end();
    }
  } catch (err) {
    if (res.headersSent) {
      try {
        res.write(`data: ${JSON.stringify({ type: "end", reason: "error", message: err.message })}\n\n`);
        res.end();
      } catch (_err) { /* ignore */ }
      return;
    }
    next(err);
  }
});

/** Cluster → control plane lifecycle events (K8s watch handler / Slurm job callback). */
app.post("/api/v1/internal/events", async (req, res, next) => {
  try {
    const body = req.body || {};
    const token = body.token || req.get("x-hcp-token") || "";
    if (token !== EVENT_TOKEN) {
      throw httpError(401, "invalid event token");
    }
    const job = await events.handleJobEvent(body);
    if (!job) throw httpError(404, "job not found");
    res.json(jobView(job));
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

async function start() {
  store = await connectStore();
  events = createEventBus({
    store,
    adapters,
    inventories,
    dispatch,
  });
  logger.info(`connected to MongoDB ${MONGODB_URI}`);

  app.listen(API_PORT, API_HOST, async () => {
    logger.info(`hybrid compute control plane started on ${API_HOST}:${API_PORT}`);

    // One-shot catch-up after restart, then event-driven only.
    try {
      await bootstrapStatuses(store, adapters, async () => {
        await events.drainQueue();
      });
      await events.drainQueue();
    } catch (err) {
      logger.warn(`bootstrap failed: ${err.message}`);
    }

    const onClusterEvent = (event) => events.handleJobEvent(event);
    for (const adapter of Object.values(adapters)) {
      if (typeof adapter.startWatch === "function") {
        adapter.startWatch(onClusterEvent);
      }
    }
  });
}

if (require.main === module) {
  start().catch((err) => {
    logger.warn(`failed to start: ${err.message}`);
    process.exit(1);
  });
}

module.exports = { app };
