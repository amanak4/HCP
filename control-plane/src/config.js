const path = require("path");

const ROOT = path.resolve(__dirname, "..", "..");
const WEB_DIR = path.join(ROOT, "web");

const API_HOST = process.env.HCP_API_HOST || "0.0.0.0";
const API_PORT = Number(process.env.HCP_API_PORT || 8080);

const MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017/hcp";
const MONGODB_DB = process.env.MONGODB_DB || "hcp";

const SLURM_CONTAINER = process.env.SLURM_CONTAINER || "hcp-slurmctld";
const K8S_NAMESPACE = process.env.K8S_NAMESPACE || "hcp";
const K8S_CONTEXT = process.env.K8S_CONTEXT || "kind-hcp";
const KUBECONFIG = process.env.KUBECONFIG || "";

const ADAPTER_TIMEOUT_SECONDS = Number(process.env.HCP_ADAPTER_TIMEOUT || 20);

// Reachable from Slurm compute containers (see docker-compose host-gateway).
const CALLBACK_HOST = process.env.HCP_CALLBACK_HOST || "host.docker.internal";
const CALLBACK_PORT = Number(process.env.HCP_CALLBACK_PORT || API_PORT);
const EVENT_TOKEN = process.env.HCP_EVENT_TOKEN || "hcp-demo-event-token";

module.exports = {
  ROOT,
  WEB_DIR,
  API_HOST,
  API_PORT,
  MONGODB_URI,
  MONGODB_DB,
  SLURM_CONTAINER,
  K8S_NAMESPACE,
  K8S_CONTEXT,
  KUBECONFIG,
  ADAPTER_TIMEOUT_SECONDS,
  CALLBACK_HOST,
  CALLBACK_PORT,
  EVENT_TOKEN,
};
