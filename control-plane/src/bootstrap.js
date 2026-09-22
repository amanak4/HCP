const { JobStatus, TERMINAL_STATUSES } = require("./models");
const logger = require("./logger");

/**
 * One-shot catch-up after control-plane restart.
 * Continuous polling was removed; watches / job callbacks own steady-state updates.
 */
async function bootstrapStatuses(store, adapters, onTerminal) {
  for (const job of await store.active()) {
    if (job.status === JobStatus.QUEUED) continue;
    if (!job.scheduler || !job.native_id) continue;
    const adapter = adapters[job.scheduler];
    if (!adapter) continue;
    let status;
    let message;
    try {
      ({ status, message } = await adapter.status(job));
    } catch (err) {
      logger.warn(`bootstrap status failed for ${job.id}: ${err.message}`);
      continue;
    }
    if (status === JobStatus.UNKNOWN && (job.status === JobStatus.PENDING || job.status === JobStatus.RUNNING)) {
      continue;
    }
    const wasTerminal = TERMINAL_STATUSES.has(job.status);
    if (job.status !== status || (message && message !== job.message)) {
      job.status = status;
      if (message) job.message = message;
      await store.upsert(job);
    }
    if (!wasTerminal && TERMINAL_STATUSES.has(status) && onTerminal) {
      await onTerminal(job);
    }
  }
}

module.exports = { bootstrapStatuses };
