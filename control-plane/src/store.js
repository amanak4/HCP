const { MongoClient } = require("mongodb");
const { MONGODB_URI, MONGODB_DB } = require("./config");
const { JobStatus, utcnow } = require("./models");

const TERMINAL = [JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED];

function toDoc(record) {
  return {
    _id: record.id,
    id: record.id,
    spec: record.spec,
    status: record.status,
    scheduler: record.scheduler,
    native_id: record.native_id,
    placement: record.placement,
    message: record.message,
    created_at: record.created_at,
    updated_at: record.updated_at,
  };
}

function fromDoc(doc) {
  if (!doc) return null;
  return {
    id: doc.id || doc._id,
    spec: doc.spec,
    status: doc.status,
    scheduler: doc.scheduler || null,
    native_id: doc.native_id || null,
    placement: doc.placement || null,
    message: doc.message || "",
    created_at: doc.created_at,
    updated_at: doc.updated_at,
  };
}

class JobStore {
  constructor(collection) {
    this.col = collection;
  }

  async upsert(record) {
    record.updated_at = utcnow();
    await this.col.replaceOne({ _id: record.id }, toDoc(record), { upsert: true });
    return record;
  }

  async get(jobId) {
    return fromDoc(await this.col.findOne({ _id: jobId }));
  }

  async list() {
    const docs = await this.col.find({}).sort({ updated_at: -1 }).toArray();
    return docs.map(fromDoc);
  }

  async active() {
    const docs = await this.col.find({ status: { $nin: TERMINAL } }).toArray();
    return docs.map(fromDoc);
  }

  /** Control-plane queue: oldest first so drain is FIFO with backfill skips. */
  async queued() {
    const docs = await this.col
      .find({ status: JobStatus.QUEUED })
      .sort({ created_at: 1 })
      .toArray();
    return docs.map(fromDoc);
  }
}

let store;
let client;

async function connectStore() {
  if (store) return store;
  client = new MongoClient(MONGODB_URI);
  await client.connect();
  const db = client.db(MONGODB_DB);
  const col = db.collection("jobs");
  await col.createIndex({ status: 1 });
  await col.createIndex({ updated_at: -1 });
  store = new JobStore(col);
  return store;
}

function getStore() {
  if (!store) throw new Error("MongoDB is not connected yet; call connectStore() first");
  return store;
}

module.exports = { JobStore, connectStore, getStore };
