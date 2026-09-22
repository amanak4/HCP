const { JobStatus, TERMINAL_STATUSES } = require("./models");
const { place } = require("./placement");
const logger = require("./logger");

/**
 * Event-driven queue drain + status updates.
 * Capacity is re-checked only when a cluster reports a job lifecycle event
 * (or on submit / startup), never via a standing inventory poll loop.
 */
function createEventBus({ store, adapters, inventories, dispatch }) {
  let draining = false;
  let drainAgain = false;

  async function applyStatus(job, status, message) {
    if (!job) return null;
    if (TERMINAL_STATUSES.has(job.status) && status !== job.status) {
      // Do not resurrect terminal jobs from late events.
      return job;
    }
    if (status === JobStatus.UNKNOWN && (job.status === JobStatus.PENDING || job.status === JobStatus.RUNNING)) {
      if (message) {
        job.message = message;
        await store.upsert(job);
      }
      return job;
    }
    const becameTerminal = TERMINAL_STATUSES.has(status) && !TERMINAL_STATUSES.has(job.status);
    if (job.status !== status || (message && message !== job.message)) {
      job.status = status;
      if (message) job.message = message;
      await store.upsert(job);
    }
    if (becameTerminal) {
      await drainQueue();
    }
    return job;
  }

  async function handleJobEvent(event) {
    const { job_id: jobId, status, message, native_id: nativeId } = event;
    if (!jobId || !status) {
      throw new Error("event requires job_id and status");
    }
    const job = await store.get(jobId);
    if (!job) {
      logger.warn(`event for unknown job ${jobId}`);
      return null;
    }
    if (nativeId && !job.native_id) {
      job.native_id = String(nativeId);
    }
    return applyStatus(job, status, message || "");
  }

  /**
   * Reserve resources on a mutable inventory snapshot while draining so we do
   * not over-dispatch before the next real health() snapshot.
   */
  function reserve(capacity, scheduler, spec) {
    const inv = capacity[scheduler];
    if (!inv) return;
    inv.cpu_idle = Math.max(inv.cpu_idle - spec.resources.cpu, 0);
    inv.memory_mb_idle = Math.max(inv.memory_mb_idle - spec.resources.memory_mb, 0);
    inv.running_jobs = (inv.running_jobs || 0) + 1;
  }

  async function drainQueue() {
    if (draining) {
      drainAgain = true;
      return;
    }
    draining = true;
    try {
      do {
        drainAgain = false;
        const queued = await store.queued();
        if (!queued.length) continue;

        const live = await inventories();
        const capacity = {};
        for (const inv of live) {
          capacity[inv.scheduler] = { ...inv };
        }

        for (const job of queued) {
          // Re-read in case cancel raced.
          const current = await store.get(job.id);
          if (!current || current.status !== JobStatus.QUEUED) continue;

          let decision;
          try {
            decision = place(current.spec, Object.values(capacity));
          } catch (err) {
            logger.warn(`queue drain placement failed for ${current.id}: ${err.message}`);
            continue;
          }
          if (decision.queue) continue;

          try {
            await dispatch(current, decision.scheduler, decision, Object.values(capacity));
            reserve(capacity, decision.scheduler, current.spec);
            logger.info(`dequeued ${current.id} → ${decision.scheduler}`);
          } catch (err) {
            logger.warn(`dequeue submit failed for ${current.id}: ${err.message}`);
            // Leave as failed (dispatch marks it) and keep draining others.
          }
        }
      } while (drainAgain);
    } finally {
      draining = false;
    }
  }

  async function enqueue(job, reason) {
    job.status = JobStatus.QUEUED;
    job.scheduler = null;
    job.native_id = null;
    job.placement = {
      scheduler: null,
      reason,
      scores: {},
      fallback: false,
      queued: true,
    };
    job.message = reason;
    await store.upsert(job);
    return job;
  }

  return {
    handleJobEvent,
    applyStatus,
    drainQueue,
    enqueue,
  };
}

module.exports = { createEventBus };
