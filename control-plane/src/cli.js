#!/usr/bin/env node
const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");

const DEFAULT_API = process.env.HCP_API || "http://127.0.0.1:8080";

function usage() {
  return `Hybrid Compute Platform CLI

Usage:
  node src/cli.js [--api URL] submit <file.yaml>
  node src/cli.js [--api URL] list
  node src/cli.js [--api URL] status <id>
  node src/cli.js [--api URL] logs <id>
  node src/cli.js [--api URL] cancel <id>
  node src/cli.js [--api URL] clusters
`;
}

function parseArgs(argv) {
  const args = { api: DEFAULT_API, cmd: null, rest: [] };
  const parts = argv.slice(2);
  while (parts.length) {
    const token = parts.shift();
    if (token === "--api") {
      args.api = parts.shift();
    } else if (token === "-h" || token === "--help") {
      args.cmd = "help";
    } else if (!args.cmd) {
      args.cmd = token;
    } else {
      args.rest.push(token);
    }
  }
  return args;
}

async function request(method, url, body) {
  const res = await fetch(url, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let payload = text;
  try {
    payload = text ? JSON.parse(text) : {};
  } catch (_err) {
    payload = { detail: text };
  }
  if (!res.ok) {
    const detail = typeof payload.detail === "string" ? payload.detail : JSON.stringify(payload);
    throw new Error(detail || `${res.status} ${res.statusText}`);
  }
  return payload;
}

function printJson(payload) {
  process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.cmd || args.cmd === "help") {
    process.stdout.write(usage());
    process.exit(args.cmd === "help" ? 0 : 1);
  }
  const api = args.api.replace(/\/$/, "");
  if (args.cmd === "submit") {
    const file = args.rest[0];
    if (!file) throw new Error("submit requires a YAML file");
    const spec = yaml.load(fs.readFileSync(path.resolve(file), "utf8"));
    printJson(await request("POST", `${api}/api/v1/jobs`, spec));
  } else if (args.cmd === "list") {
    printJson(await request("GET", `${api}/api/v1/jobs`));
  } else if (args.cmd === "status") {
    printJson(await request("GET", `${api}/api/v1/jobs/${args.rest[0]}`));
  } else if (args.cmd === "logs") {
    const payload = await request("GET", `${api}/api/v1/jobs/${args.rest[0]}/logs`);
    process.stdout.write(`${payload.logs || "(no logs yet)"}\n`);
  } else if (args.cmd === "cancel") {
    printJson(await request("DELETE", `${api}/api/v1/jobs/${args.rest[0]}`));
  } else if (args.cmd === "clusters") {
    printJson(await request("GET", `${api}/api/v1/clusters`));
  } else {
    throw new Error(`unknown command: ${args.cmd}`);
  }
}

main().catch((err) => {
  process.stderr.write(`${err.message}\n`);
  process.exit(1);
});
