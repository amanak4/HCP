const { JobStatus } = require("./models");
const logger = require("./logger");

async function reconcileOnce(store, adapters) {
  for (const job of await store.active()) {
    if (!job.scheduler || !job.native_id) continue;
    const adapter = adapters[job.scheduler];
    if (!adapter) continue;
    let status;
    let message;
    try {
      ({ status, message } = await adapter.status(job));
    } catch (err) {
      logger.warn(`reconcile failed for ${job.id}: ${err.message}`);
      job.message = `reconcile error: ${err.message}`;
      await store.upsert(job);
      continue;
    }
    if (status === JobStatus.UNKNOWN && (job.status === JobStatus.PENDING || job.status === JobStatus.RUNNING)) {
      job.message = message || job.message;
      await store.upsert(job);
      continue;
    }
    if (job.status !== status || (message && message !== job.message)) {
      job.status = status;
      if (message) job.message = message;
      await store.upsert(job);
    }
  }
}

module.exports = { reconcileOnce };
