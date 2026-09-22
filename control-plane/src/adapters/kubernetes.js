const k8s = require("@kubernetes/client-node");
const { PassThrough } = require("stream");
const { K8S_CONTEXT, K8S_NAMESPACE, KUBECONFIG } = require("../config");
const { JobStatus, SchedulerName, inventory } = require("../models");
const logger = require("../logger");

function isNotFound(err) {
  const status = err?.statusCode || err?.response?.statusCode || err?.body?.code;
  return status === 404;
}

function mapJobStatus(st = {}) {
  const succeeded = st.succeeded || 0;
  const failed = st.failed || 0;
  const active = st.active || 0;
  if (succeeded) return { status: JobStatus.SUCCEEDED, message: "job completed" };
  if (failed) return { status: JobStatus.FAILED, message: "job failed" };
  if (active) return { status: JobStatus.RUNNING, message: "pod running" };
  return { status: JobStatus.PENDING, message: "waiting for pod" };
}

class KubernetesAdapter {
  constructor() {
    this.name = SchedulerName.KUBERNETES;
    this.namespace = K8S_NAMESPACE;
    this.ready = false;
    this.error = "not initialized";
    this._load();
  }

  _load() {
    try {
      const kc = new k8s.KubeConfig();
      if (KUBECONFIG) {
        kc.loadFromFile(KUBECONFIG);
      } else {
        try {
          kc.loadFromCluster();
        } catch (_err) {
          kc.loadFromDefault();
        }
      }
      if (K8S_CONTEXT) kc.setCurrentContext(K8S_CONTEXT);
      this.kc = kc;
      this.batch = kc.makeApiClient(k8s.BatchV1Api);
      this.core = kc.makeApiClient(k8s.CoreV1Api);
      this.ready = true;
      this.error = "";
    } catch (err) {
      this.ready = false;
      this.error = String(err.message || err);
      logger.warn(`kubernetes adapter unavailable: ${this.error}`);
    }
  }

  async _ensureNamespace() {
    try {
      await this.core.readNamespace(this.namespace);
    } catch (err) {
      if (!isNotFound(err)) throw err;
      await this.core.createNamespace({ metadata: { name: this.namespace } });
    }
  }

  async health() {
    if (!this.ready) this._load();
    if (!this.ready) {
      return inventory({
        scheduler: SchedulerName.KUBERNETES,
        healthy: false,
        message: this.error || "kubeconfig not loaded",
      });
    }
    try {
      await this._ensureNamespace();
      const nodes = await this.core.listNode();
      let cpuAlloc = 0;
      let memAlloc = 0;
      for (const node of nodes.body.items) {
        const allocatable = node.status?.allocatable || {};
        cpuAlloc += parseCpu(allocatable.cpu);
        memAlloc += parseMemoryMb(allocatable.memory);
      }
      const pods = await this.core.listPodForAllNamespaces();
      let cpuUsed = 0;
      let memUsed = 0;
      let running = 0;
      let pending = 0;
      for (const pod of pods.body.items) {
        const phase = String(pod.status?.phase || "").toLowerCase();
        if (phase === "running") running += 1;
        else if (phase === "pending") pending += 1;
        if (phase === "succeeded" || phase === "failed") continue;
        for (const container of pod.spec?.containers || []) {
          const requests = container.resources?.requests || {};
          cpuUsed += parseCpu(requests.cpu);
          memUsed += parseMemoryMb(requests.memory);
        }
      }
      return inventory({
        scheduler: SchedulerName.KUBERNETES,
        healthy: true,
        cpu_idle: Math.max(cpuAlloc - cpuUsed, 0),
        memory_mb_idle: Math.max(memAlloc - memUsed, 0),
        running_jobs: running,
        pending_jobs: pending,
        message: `${nodes.body.items.length} nodes`,
      });
    } catch (err) {
      return inventory({
        scheduler: SchedulerName.KUBERNETES,
        healthy: false,
        message: String(err.message || err),
      });
    }
  }

  async submit(job) {
    if (!this.ready) this._load();
    if (!this.ready) throw new Error(this.error || "kubernetes unavailable");
    await this._ensureNamespace();
    const name = `hcp-${job.id}`;
    const cpu = String(job.spec.resources.cpu);
    const memory = `${job.spec.resources.memory_mb}Mi`;
    const body = {
      apiVersion: "batch/v1",
      kind: "Job",
      metadata: {
        name,
        namespace: this.namespace,
        labels: { "hcp.job-id": job.id, "hcp.managed": "true" },
      },
      spec: {
        backoffLimit: 1,
        ttlSecondsAfterFinished: 600,
        template: {
          metadata: { labels: { "hcp.job-id": job.id } },
          spec: {
            restartPolicy: "Never",
            containers: [
              {
                name: "main",
                image: job.spec.image,
                command: ["sh", "-lc", job.spec.command],
                resources: {
                  requests: { cpu, memory },
                  limits: { cpu, memory },
                },
              },
            ],
          },
        },
      },
    };
    await this.batch.createNamespacedJob(this.namespace, body);
    return name;
  }

  async status(job) {
    if (!job.native_id) return { status: JobStatus.UNKNOWN, message: "missing native id" };
    try {
      const kjob = await this.batch.readNamespacedJob(job.native_id, this.namespace);
      return mapJobStatus(kjob.body.status || {});
    } catch (err) {
      if (isNotFound(err)) return { status: JobStatus.UNKNOWN, message: "job not found on cluster" };
      return { status: JobStatus.UNKNOWN, message: String(err.message || err) };
    }
  }

  /**
   * Push Job status changes into the control plane via the Kubernetes Watch API.
   * Replaces the old reconcile poll loop for this backend.
   */
  startWatch(onEvent) {
    if (!this.ready) this._load();
    if (!this.ready) {
      logger.warn("kubernetes watch not started: adapter unavailable");
      return () => {};
    }

    let stopped = false;
    let request = null;
    let retryMs = 1000;
    const watch = new k8s.Watch(this.kc);

    const schedule = () => {
      if (stopped) return;
      setTimeout(connect, retryMs);
      retryMs = Math.min(retryMs * 2, 30000);
    };

    const connect = async () => {
      if (stopped) return;
      try {
        await this._ensureNamespace();
        request = await watch.watch(
          `/apis/batch/v1/namespaces/${this.namespace}/jobs`,
          { labelSelector: "hcp.managed=true" },
          (phase, obj) => {
            retryMs = 1000;
            if (!obj || !obj.metadata) return;
            const jobId = obj.metadata.labels && obj.metadata.labels["hcp.job-id"];
            if (!jobId) return;
            if (phase === "DELETED") {
              onEvent({
                job_id: jobId,
                scheduler: SchedulerName.KUBERNETES,
                native_id: obj.metadata.name,
                status: JobStatus.CANCELLED,
                message: "job deleted on cluster",
              }).catch((err) => logger.warn(`k8s watch handler: ${err.message}`));
              return;
            }
            const mapped = mapJobStatus(obj.status || {});
            onEvent({
              job_id: jobId,
              scheduler: SchedulerName.KUBERNETES,
              native_id: obj.metadata.name,
              status: mapped.status,
              message: mapped.message,
            }).catch((err) => logger.warn(`k8s watch handler: ${err.message}`));
          },
          (err) => {
            if (stopped) return;
            if (err) logger.warn(`kubernetes watch ended: ${err.message || err}`);
            schedule();
          },
        );
        logger.info("kubernetes job watch started");
      } catch (err) {
        logger.warn(`kubernetes watch connect failed: ${err.message}`);
        schedule();
      }
    };

    connect();
    return () => {
      stopped = true;
      try {
        if (request && typeof request.abort === "function") request.abort();
      } catch (_err) {
        /* ignore */
      }
    };
  }

  async cancel(job) {
    if (!job.native_id || !this.ready) return;
    try {
      await this.batch.deleteNamespacedJob(
        job.native_id,
        this.namespace,
        undefined,
        undefined,
        undefined,
        undefined,
        "Foreground",
      );
    } catch (err) {
      if (!isNotFound(err)) throw err;
    }
  }

  async logs(job) {
    if (!this.ready) return this.error;
    try {
      const pod = await this._latestPod(job.id);
      if (!pod) return "";
      const log = await this.core.readNamespacedPodLog(pod.metadata.name, this.namespace);
      return typeof log.body === "string" ? log.body : String(log.body || "");
    } catch (err) {
      return `(log error) ${err.message || err}`;
    }
  }

  async _latestPod(jobId) {
    const pods = await this.core.listNamespacedPod(
      this.namespace,
      undefined,
      undefined,
      undefined,
      undefined,
      `hcp.job-id=${jobId}`,
    );
    const items = pods.body.items || [];
    if (!items.length) return null;
    items.sort((a, b) => String(a.metadata?.creationTimestamp || "").localeCompare(String(b.metadata?.creationTimestamp || "")));
    return items[items.length - 1];
  }

  /**
   * Follow pod logs (kubectl logs -f). Calls onChunk(string) as data arrives.
   * Resolves when the follow stream ends or signal aborts.
   */
  async streamLogs(job, onChunk, { signal } = {}) {
    if (!this.ready) this._load();
    if (!this.ready) {
      onChunk(`(kubernetes unavailable) ${this.error}\n`);
      return;
    }

    let pod = null;
    const deadline = Date.now() + 120000;
    let waitingNoted = false;
    while (!pod) {
      if (signal?.aborted) return;
      pod = await this._latestPod(job.id);
      if (pod) break;
      if (!waitingNoted) {
        onChunk("(waiting for pod…)\n");
        waitingNoted = true;
      }
      await sleep(1500);
      if (Date.now() > deadline) {
        onChunk("(timed out waiting for pod)\n");
        return;
      }
    }

    const container = (pod.spec?.containers || [])[0]?.name || "main";
    const logApi = new k8s.Log(this.kc);
    const pass = new PassThrough();
    pass.setEncoding("utf8");
    pass.on("data", (chunk) => {
      try { onChunk(chunk); } catch (_err) { /* ignore sink errors */ }
    });

    let req;
    try {
      req = await logApi.log(this.namespace, pod.metadata.name, container, pass, {
        follow: true,
        tailLines: 200,
        timestamps: false,
      });
    } catch (err) {
      onChunk(`(log stream error) ${err.message || err}\n`);
      return;
    }

    const abort = () => {
      try {
        if (req && typeof req.abort === "function") req.abort();
      } catch (_err) { /* ignore */ }
      try { pass.destroy(); } catch (_err) { /* ignore */ }
    };
    if (signal) {
      if (signal.aborted) {
        abort();
        return;
      }
      signal.addEventListener("abort", abort, { once: true });
    }

    await new Promise((resolve) => {
      pass.on("end", resolve);
      pass.on("close", resolve);
      pass.on("error", resolve);
      if (req && typeof req.on === "function") {
        req.on("error", resolve);
        req.on("complete", resolve);
      }
    });
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseCpu(value) {
  if (!value) return 0;
  const raw = String(value);
  if (raw.endsWith("m")) return Number(raw.slice(0, -1)) / 1000;
  return Number(raw);
}

function parseMemoryMb(value) {
  if (!value) return 0;
  const raw = String(value);
  const multipliers = {
    Ki: 1 / 1024,
    Mi: 1,
    Gi: 1024,
    Ti: 1024 * 1024,
    K: 1000 / (1024 * 1024),
    M: 1000 / 1024,
    G: (1000 * 1000) / 1024,
  };
  for (const [suffix, mult] of Object.entries(multipliers)) {
    if (raw.endsWith(suffix)) return Math.trunc(Number(raw.slice(0, -suffix.length)) * mult);
  }
  if (raw.endsWith("i")) return Math.max(Math.trunc(Number(raw.slice(0, -1)) / (1024 * 1024)), 0);
  return Math.max(Math.trunc(Number(raw) / (1024 * 1024)), 0);
}

module.exports = { KubernetesAdapter };
