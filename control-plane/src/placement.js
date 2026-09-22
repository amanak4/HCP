const { SchedulerName, WorkloadClass } = require("./models");

const DEFAULT_IMAGES = new Set(["ubuntu:22.04", "ubuntu:24.04", "debian:bookworm-slim"]);

class PlacementError extends Error {
  constructor(message) {
    super(message);
    this.name = "PlacementError";
  }
}

/** Hard admission: cluster must be healthy and have idle CPU/memory for the request. */
function canFit(spec, inv) {
  if (!inv || !inv.healthy) return false;
  return inv.cpu_idle >= spec.resources.cpu
    && inv.memory_mb_idle >= spec.resources.memory_mb;
}

function place(spec, inventories) {
  const healthy = {};
  for (const inv of inventories) {
    if (inv.healthy) healthy[inv.scheduler] = inv;
  }
  if (!Object.keys(healthy).length) {
    throw new PlacementError("no healthy compute cluster is available");
  }

  const fitting = {};
  for (const [name, inv] of Object.entries(healthy)) {
    if (canFit(spec, inv)) fitting[name] = inv;
  }

  if (spec.scheduler_hint) {
    const hinted = spec.scheduler_hint;
    if (fitting[hinted]) {
      return {
        scheduler: hinted,
        reason: `operator hint pinned the job to ${hinted}`,
        scores: { [hinted]: 100 },
        fallback: false,
        queue: false,
      };
    }
    if (healthy[hinted] && !fitting[hinted]) {
      const other = otherScheduler(hinted);
      if (fitting[other]) {
        return {
          scheduler: other,
          reason: `hint ${hinted} has no free capacity; placing on ${other}`,
          scores: Object.fromEntries(Object.keys(fitting).map((name) => [name, 0])),
          fallback: true,
          queue: false,
        };
      }
      return queueDecision(healthy, `hint ${hinted} and peer have no free capacity; queued until a job finishes`);
    }
    if (!healthy[hinted]) {
      const other = otherScheduler(hinted);
      if (fitting[other]) {
        return {
          scheduler: other,
          reason: `hint ${hinted} is unhealthy; failing over to ${other}`,
          scores: Object.fromEntries(Object.keys(fitting).map((name) => [name, 0])),
          fallback: true,
          queue: false,
        };
      }
      if (healthy[other]) {
        return queueDecision(healthy, `hint ${hinted} unhealthy and ${other} has no free capacity; queued`);
      }
      throw new PlacementError(`hinted scheduler ${hinted} is unhealthy`);
    }
  }

  if (!Object.keys(fitting).length) {
    return queueDecision(healthy, "all healthy clusters are at capacity; queued until resources free");
  }

  const scores = {
    [SchedulerName.KUBERNETES]: 0,
    [SchedulerName.SLURM]: 0,
  };

  if (spec.workload_class === WorkloadClass.BATCH) {
    scores[SchedulerName.SLURM] += 40;
    scores[SchedulerName.KUBERNETES] += 10;
  } else {
    scores[SchedulerName.KUBERNETES] += 40;
    scores[SchedulerName.SLURM] += 5;
  }

  if (!DEFAULT_IMAGES.has(spec.image)) {
    scores[SchedulerName.KUBERNETES] += 25;
  }
  if (spec.resources.ntasks > 1) {
    scores[SchedulerName.SLURM] += 30;
  }
  if (spec.resources.gpu > 0) {
    scores[SchedulerName.KUBERNETES] += 10;
  }

  for (const [scheduler, inv] of Object.entries(fitting)) {
    if (inv.cpu_idle >= spec.resources.cpu) scores[scheduler] += 15;
    if (inv.memory_mb_idle >= spec.resources.memory_mb) scores[scheduler] += 10;
    const pressure = inv.running_jobs + inv.pending_jobs;
    scores[scheduler] -= Math.min(pressure * 2, 20);
  }

  const eligible = {};
  for (const [name, score] of Object.entries(scores)) {
    if (fitting[name]) eligible[name] = score;
  }
  const winner = Object.entries(eligible).sort((a, b) => b[1] - a[1])[0][0];
  return {
    scheduler: winner,
    reason: reasonText(spec, winner, fitting[winner], eligible),
    scores: eligible,
    fallback: false,
    queue: false,
  };
}

function queueDecision(healthy, reason) {
  return {
    scheduler: null,
    reason,
    scores: Object.fromEntries(Object.keys(healthy).map((name) => [name, 0])),
    fallback: false,
    queue: true,
  };
}

function fallbackScheduler(current, inventories) {
  const other = otherScheduler(current);
  for (const inv of inventories) {
    if (inv.scheduler === other && inv.healthy) return other;
  }
  return null;
}

function otherScheduler(name) {
  return name === SchedulerName.KUBERNETES ? SchedulerName.SLURM : SchedulerName.KUBERNETES;
}

function reasonText(spec, winner, inv, scores) {
  const bits = [];
  if (spec.workload_class === WorkloadClass.BATCH && winner === SchedulerName.SLURM) {
    bits.push("batch workload");
  }
  if (spec.workload_class === WorkloadClass.SERVICE && winner === SchedulerName.KUBERNETES) {
    bits.push("long-running/service workload");
  }
  if (!DEFAULT_IMAGES.has(spec.image) && winner === SchedulerName.KUBERNETES) {
    bits.push(`container image ${spec.image}`);
  }
  if (spec.resources.ntasks > 1 && winner === SchedulerName.SLURM) {
    bits.push(`${spec.resources.ntasks} parallel tasks`);
  }
  bits.push(`capacity ${Number(inv.cpu_idle).toFixed(1)} CPU idle`);
  const scoreTxt = Object.entries(scores)
    .map(([name, score]) => `${name}=${score.toFixed(0)}`)
    .join(", ");
  return `routed to ${winner} (${bits.join("; ")}; scores ${scoreTxt})`;
}

module.exports = {
  PlacementError,
  place,
  canFit,
  fallbackScheduler,
};
