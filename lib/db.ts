import { MongoClient, Db } from "mongodb";

declare global {
  var __accountsDb: Db | undefined;
  var __accountsMongoClientPromise: Promise<MongoClient> | undefined;
}

const MONGODB_URL = process.env.MONGODB_URL || "mongodb://localhost:27017/accounts-automation";
const SERVER_SELECTION_TIMEOUT_MS = Number(process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS || "5000");
const TXN_CODE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

export class DatabaseConnectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DatabaseConnectionError";
  }
}

function toDatabaseConnectionError(error: unknown): DatabaseConnectionError {
  const detail = error instanceof Error ? error.message : String(error);
  const lower = detail.toLowerCase();

  const isAtlasTlsError =
    lower.includes("tlsv1 alert internal error") ||
    lower.includes("err_ssl_tlsv1_alert_internal_error") ||
    lower.includes("replicasetnoprimary");

  if (isAtlasTlsError) {
    return new DatabaseConnectionError(
      "Database connection failed (Atlas TLS/cluster selection). Check that MONGODB_URL is valid, Atlas Network Access includes your current IP, and DB user credentials are correct."
    );
  }

  return new DatabaseConnectionError(`Database connection failed: ${detail}`);
}

async function getClient(): Promise<MongoClient> {
  if (global.__accountsMongoClientPromise) {
    return global.__accountsMongoClientPromise;
  }

  const client = new MongoClient(MONGODB_URL, {
    serverSelectionTimeoutMS: SERVER_SELECTION_TIMEOUT_MS,
    connectTimeoutMS: SERVER_SELECTION_TIMEOUT_MS,
  });

  global.__accountsMongoClientPromise = client.connect().catch((error) => {
    global.__accountsMongoClientPromise = undefined;
    throw toDatabaseConnectionError(error);
  });

  return global.__accountsMongoClientPromise;
}

async function initSchema(db: Db) {
  // Create collections if they don't exist
  const collections = await db.listCollections().toArray();
  const collectionNames = new Set(collections.map((c) => c.name));

  if (!collectionNames.has("vendors")) {
    await db.createCollection("vendors");
  }

  if (!collectionNames.has("transactions")) {
    await db.createCollection("transactions");
  }

  if (!collectionNames.has("audit_logs")) {
    await db.createCollection("audit_logs");
  }

  if (!collectionNames.has("pending_actions")) {
    await db.createCollection("pending_actions");
  }

  // Create indexes
  const vendorsCollection = db.collection("vendors");
  await vendorsCollection.createIndex({ name: 1 }, { unique: true }).catch(() => {});
  await vendorsCollection.createIndex({ type: 1 }).catch(() => {});

  const transactionsCollection = db.collection("transactions");
  await transactionsCollection.createIndex({ status: 1 }).catch(() => {});
  await transactionsCollection.createIndex({ txn_code: 1 }, { unique: true, sparse: true }).catch(() => {});
  await transactionsCollection.createIndex({ reference_id: 1 }).catch(() => {});
  await transactionsCollection.createIndex({ txn_hash: 1 }).catch(() => {});
  await transactionsCollection.createIndex({ image_hash: 1 }).catch(() => {});
  await transactionsCollection.createIndex({ vendor_name: 1, amount: 1, transaction_time: 1 }).catch(() => {});

  const auditLogsCollection = db.collection("audit_logs");
  await auditLogsCollection.createIndex({ txn_id: 1 }).catch(() => {});

  const pendingActionsCollection = db.collection("pending_actions");
  await pendingActionsCollection.createIndex({ session_id: 1 }, { unique: true }).catch(() => {});
  await pendingActionsCollection.createIndex({ created_at: 1 }).catch(() => {});

  // Backfill 4-character transaction codes for older transactions that don't have one yet.
  const missingCodeRows = await transactionsCollection
    .find({ $or: [{ txn_code: { $exists: false } }, { txn_code: null }, { txn_code: "" }] })
    .project({ _id: 1 })
    .toArray();

  for (const row of missingCodeRows) {
    let assigned = false;

    for (let i = 0; i < 40; i++) {
      let code = "";
      for (let j = 0; j < 4; j++) {
        code += TXN_CODE_ALPHABET[Math.floor(Math.random() * TXN_CODE_ALPHABET.length)];
      }

      const exists = await transactionsCollection.findOne({ txn_code: code }, { projection: { _id: 1 } });
      if (exists) {
        continue;
      }

      await transactionsCollection.updateOne({ _id: row._id }, { $set: { txn_code: code } });
      assigned = true;
      break;
    }

    if (!assigned) {
      throw new Error(`Could not assign txn_code for transaction ${String(row._id)}`);
    }
  }
}

export async function getDb(): Promise<Db> {
  if (global.__accountsDb) {
    return global.__accountsDb;
  }

  const client = await getClient();
  const db = client.db();

  try {
    // Initialize collections and indexes
    await initSchema(db);
  } catch (error) {
    throw toDatabaseConnectionError(error);
  }

  global.__accountsDb = db;
  return db;
}
