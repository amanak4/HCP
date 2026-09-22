const Docker = require("dockerode");
const { SLURM_CONTAINER, CALLBACK_HOST, CALLBACK_PORT, EVENT_TOKEN } = require("../config");
const { JobStatus, SchedulerName, inventory } = require("../models");

const STATE_MAP = {
  PENDING: JobStatus.PENDING,
  CONFIGURING: JobStatus.PENDING,
  RUNNING: JobStatus.RUNNING,
  COMPLETING: JobStatus.RUNNING,
  SUSPENDED: JobStatus.RUNNING,
  COMPLETED: JobStatus.SUCCEEDED,
  FAILED: JobStatus.FAILED,
  NODE_FAIL: JobStatus.FAILED,
  TIMEOUT: JobStatus.FAILED,
  OUT_OF_MEMORY: JobStatus.FAILED,
  BOOT_FAIL: JobStatus.FAILED,
  DEADLINE: JobStatus.FAILED,
  CANCELLED: JobStatus.CANCELLED,
  PREEMPTED: JobStatus.CANCELLED,
};

function dockerOptions() {
  const host = process.env.DOCKER_HOST;
  if (!host) return { timeout: 3000 };
  if (host.startsWith("unix://")) {
    return { socketPath: host.slice("unix://".length), timeout: 3000 };
  }
  if (host.startsWith("npipe://")) {
    return { socketPath: host.slice("npipe://".length), timeout: 3000 };
  }
  try {
    const url = new URL(host);
    return {
      host: url.hostname,
      port: Number(url.port) || 2375,
      protocol: (url.protocol || "http").replace(":", ""),
      timeout: 3000,
    };
  } catch (_err) {
    return { timeout: 3000 };
  }
}

function withTimeout(promise, ms, message) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}

class SlurmAdapter {
  constructor() {
    this.name = SchedulerName.SLURM;
    this.containerName = SLURM_CONTAINER;
    this.client = null;
  }

  async _ensureClient() {
    if (this.client) return this.client;
    const docker = new Docker(dockerOptions());
    await withTimeout(docker.ping(), 4000, "docker is not responding");
    this.client = docker;
    return docker;
  }

  async _exec(command) {
    const client = await this._ensureClient();
    const container = client.getContainer(this.containerName);
    let inspect;
    try {
      inspect = await container.inspect();
    } catch (err) {
      if (err.statusCode === 404) {
        throw new Error(`slurm controller ${this.containerName} is not running`);
      }
      throw err;
    }
    if (!inspect) throw new Error(`slurm controller ${this.containerName} is not running`);

    const exec = await container.exec({
      Cmd: ["bash", "-lc", command],
      AttachStdout: true,
      AttachStderr: true,
    });
    const stream = await exec.start({ hijack: true, stdin: false });
    const output = await new Promise((resolve, reject) => {
      const chunks = [];
      client.modem.demuxStream(
        stream,
        { write(chunk) { chunks.push(Buffer.from(chunk)); } },
        { write(chunk) { chunks.push(Buffer.from(chunk)); } },
      );
      stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
      stream.on("error", reject);
    });
    const result = await exec.inspect();
    return { code: result.ExitCode, output: (output || "").trim() };
  }

  async health() {
    try {
      await this._ensureClient();
    } catch (err) {
      return inventory({
        scheduler: SchedulerName.SLURM,
        healthy: false,
        message: String(err.message || err),
      });
    }
    try {
      const ping = await this._exec("scontrol ping");
      if (ping.code !== 0 || !ping.output.toUpperCase().includes("UP")) {
        return inventory({
          scheduler: SchedulerName.SLURM,
          healthy: false,
          message: ping.output || "slurmctld not UP",
        });
      }
      const info = await this._exec("sinfo -h -o '%C %e'");
      let cpuIdle = 0;
      let memIdle = 0;
      if (info.code === 0 && info.output) {
        const first = info.output.split("\n")[0];
        const parts = first.split(/\s+/);
        if (parts[0] && parts[0].includes("/")) {
          const [, idle] = parts[0].split("/");
          cpuIdle = Number(idle);
        }
        if (parts[1] && !["N/A", "n/a"].includes(parts[1])) {
          const parsed = Number(parts[1]);
          memIdle = Number.isNaN(parsed) ? 0 : Math.trunc(parsed);
        }
      }
      const queue = await this._exec("squeue -h -o '%T'");
      let running = 0;
      let pending = 0;
      if (queue.code === 0 && queue.output) {
        for (const state of queue.output.split("\n")) {
          if (state.trim() === "RUNNING") running += 1;
          else if (state.trim() === "PENDING") pending += 1;
        }
      }
      return inventory({
        scheduler: SchedulerName.SLURM,
        healthy: true,
        cpu_idle: cpuIdle,
        memory_mb_idle: memIdle,
        running_jobs: running,
        pending_jobs: pending,
        message: ping.output.split("\n")[0] || "UP",
      });
    } catch (err) {
      return inventory({
        scheduler: SchedulerName.SLURM,
        healthy: false,
        message: String(err.message || err),
      });
    }
  }

  async submit(job) {
    const script = sbatchScript(job);
    const encoded = Buffer.from(script, "utf8").toString("base64");
    const command = [
      `mkdir -p /data/jobs/${job.id} &&`,
      `echo ${encoded} | base64 -d > /data/jobs/${job.id}/job.sh &&`,
      `chmod +x /data/jobs/${job.id}/job.sh &&`,
      `sbatch --parsable /data/jobs/${job.id}/job.sh`,
    ].join(" ");
    const { code, output } = await this._exec(command);
    if (code !== 0) throw new Error(output || "sbatch failed");
    const nativeId = output.split(";")[0].trim();
    if (!/^\d+$/.test(nativeId)) throw new Error(`unexpected sbatch output: ${output}`);
    return nativeId;
  }

  async status(job) {
    if (!job.native_id) return { status: JobStatus.UNKNOWN, message: "missing native id" };
    const { code, output } = await this._exec(`scontrol show job ${job.native_id}`);
    if (code !== 0 || !output) {
      return { status: JobStatus.UNKNOWN, message: output || "job not found in slurm" };
    }
    const match = output.match(/JobState=([A-Z_]+)/);
    if (!match) return { status: JobStatus.UNKNOWN, message: output.slice(0, 200) };
    const state = match[1];
    const mapped = STATE_MAP[state] || JobStatus.UNKNOWN;
    const reasonMatch = output.match(/Reason=(\S+)/);
    let reason = "";
    if (reasonMatch && !["None", "None,"].includes(reasonMatch[1])) reason = reasonMatch[1];
    return { status: mapped, message: reason || state.toLowerCase() };
  }

  async cancel(job) {
    if (!job.native_id) return;
    await this._exec(`scancel ${job.native_id}`);
  }

  async logs(job) {
    try {
      const { output } = await this._exec(
        `cat /data/jobs/${job.id}/slurm.out /data/jobs/${job.id}/slurm.err 2>/dev/null || true`,
      );
      return output;
    } catch (err) {
      return `(log error) ${err.message || err}`;
    }
  }

  /**
   * Follow Slurm stdout/stderr files (tail -F). Stops when signal aborts or
   * the Slurm job leaves active states (then a short grace flush).
   */
  async streamLogs(job, onChunk, { signal } = {}) {
    const client = await this._ensureClient();
    const container = client.getContainer(this.containerName);
    const jobDir = `/data/jobs/${job.id}`;
    const nativeId = job.native_id || "";
    const cmd = [
      `mkdir -p ${jobDir}`,
      `touch ${jobDir}/slurm.out ${jobDir}/slurm.err`,
      `tail -n +1 -F ${jobDir}/slurm.out ${jobDir}/slurm.err &`,
      "TAIL_PID=$!",
      "trap 'kill $TAIL_PID 2>/dev/null || true' EXIT",
      nativeId
        ? [
          `while true; do`,
          `  state=$(scontrol show job ${nativeId} 2>/dev/null | tr ' ' '\\n' | awk -F= '/^JobState=/{print $2; exit}')`,
          `  case "$state" in`,
          `    PENDING|CONFIGURING|RUNNING|COMPLETING|SUSPENDED|"") sleep 1 ;;`,
          `    *) sleep 1; break ;;`,
          `  esac`,
          `  kill -0 $TAIL_PID 2>/dev/null || break`,
          `done`,
        ].join(" ")
        : "while kill -0 $TAIL_PID 2>/dev/null; do sleep 1; done",
      "kill $TAIL_PID 2>/dev/null || true",
      "wait $TAIL_PID 2>/dev/null || true",
    ].join("; ");

    let inspect;
    try {
      inspect = await container.inspect();
    } catch (err) {
      if (err.statusCode === 404) throw new Error(`slurm controller ${this.containerName} is not running`);
      throw err;
    }
    if (!inspect) throw new Error(`slurm controller ${this.containerName} is not running`);

    const exec = await container.exec({
      Cmd: ["bash", "-lc", cmd],
      AttachStdout: true,
      AttachStderr: true,
    });
    const stream = await exec.start({ hijack: true, stdin: false });

    const abort = () => {
      try { stream.destroy(); } catch (_err) { /* ignore */ }
    };
    if (signal) {
      if (signal.aborted) {
        abort();
        return;
      }
      signal.addEventListener("abort", abort, { once: true });
    }

    await new Promise((resolve, reject) => {
      const writeChunk = {
        write(chunk) {
          try { onChunk(Buffer.from(chunk).toString("utf8")); } catch (_err) { /* ignore */ }
        },
      };
      client.modem.demuxStream(stream, writeChunk, writeChunk);
      stream.on("end", resolve);
      stream.on("close", resolve);
      stream.on("error", (err) => {
        if (signal?.aborted) resolve();
        else reject(err);
      });
    });
  }

  /** Slurm has no Watch API; job scripts push lifecycle events via /dev/tcp callbacks. */
  startWatch(_onEvent) {
    return () => {};
  }
}

function sbatchScript(job) {
  const res = job.spec.resources;
  // Event callback from the compute node — no polling. Uses bash /dev/tcp (no curl required).
  return `#!/bin/bash
#SBATCH --job-name=${job.spec.name}
#SBATCH --ntasks=${res.ntasks}
#SBATCH --cpus-per-task=${Math.max(Math.trunc(res.cpu), 1)}
#SBATCH --mem=${res.memory_mb}M
#SBATCH --output=/data/jobs/${job.id}/slurm.out
#SBATCH --error=/data/jobs/${job.id}/slurm.err
set -uo pipefail
HCP_JOB_ID='${job.id}'
HCP_EVENT_TOKEN='${EVENT_TOKEN}'
HCP_CALLBACK_HOST='${CALLBACK_HOST}'
HCP_CALLBACK_PORT='${CALLBACK_PORT}'
hcp_notify() {
  local st="$1"
  local msg="\${2:-}"
  local body
  body=$(printf '{"job_id":"%s","status":"%s","scheduler":"slurm","native_id":"%s","message":"%s","token":"%s"}' \\
    "$HCP_JOB_ID" "$st" "\${SLURM_JOB_ID:-}" "$msg" "$HCP_EVENT_TOKEN")
  {
    printf 'POST /api/v1/internal/events HTTP/1.0\\r\\n'
    printf 'Host: %s\\r\\n' "$HCP_CALLBACK_HOST"
    printf 'Content-Type: application/json\\r\\n'
    printf 'Content-Length: %d\\r\\n' "\${#body}"
    printf '\\r\\n'
    printf '%s' "$body"
  } >/dev/tcp/"$HCP_CALLBACK_HOST"/"$HCP_CALLBACK_PORT" 2>/dev/null || true
}
trap 'hcp_notify cancelled interrupted' TERM INT
trap 'hcp_notify failed command_error; exit 1' ERR
hcp_notify running started
set -e
echo "hcp job ${job.id} starting on $(hostname) at $(date -Is)"
${job.spec.command}
echo "hcp job ${job.id} finished at $(date -Is)"
trap - ERR
hcp_notify succeeded completed
`;
}

module.exports = { SlurmAdapter };
