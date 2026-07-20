// =============================================================================
// P2 (CQRS) / P3 (Lambda Architecture) — MongoDB Initialisation Script
// =============================================================================
// LAYER: Data layer bootstrap (schema + index creation only, no data).
//
// EXECUTED BY: the mongo:7.0 Docker entrypoint, mounted read-only into
//   /docker-entrypoint-initdb.d and run automatically via mongosh the first
//   time the container starts against an empty data directory — shared
//   byte-identical by both docker-compose.p2.yml and docker-compose.p3.yml.
// WRITTEN TO:  the `iot_benchmark` database inside the P2 or P3 MongoDB
//   container. telemetry_aggregated is subsequently written by the Go
//   worker (P2: continuous Kafka consumer; P3: hourly batch job) — this
//   script only creates the collection and its indexes, never data.
//
// Creates:
//   - device_registry          (same as P1)
//   - telemetry_raw            (same as P1, same indexes)
//   - telemetry_aggregated     (pre-computed summaries, NOT present in P1)
//
// telemetry_aggregated holds multiple document types distinguished by a "type"
// field. Each type has its own partial index scoped to only matching documents:
//
//   type: "zone_daily"              → one doc per (zoneId, date)
//   type: "cultivation_daily"       → one doc per (cultivationId, date)
//   type: "cultivation_180d_rollup" → one doc per cultivationId
//   type: "latest_by_sensor"        → one doc per sensor serial (P2 only)
//                                     uses _id = serial → no extra index needed
//
// P2 vs P3 difference at this level: none. The same collections and indexes are
// created. The architectural difference is in what the Go worker writes at
// runtime (P2 continuous upserts including latest_by_sensor; P3 hourly batch
// rewrites without latest_by_sensor — that role belongs to Redis in P3).
//
// References:
//   - benchmark_schema_design_v41.pdf  §3–§6
//   - implementation_decisions.md      Decisions P0.1 through P0.5
//
// Run context:
//   Executed automatically by the mongo:7.0 Docker entrypoint on first startup
//   (data directory empty). Runs inside mongosh.
// =============================================================================

db = db.getSiblingDB("iot_benchmark");

// -----------------------------------------------------------------------------
// device_registry  (identical to P1)
// -----------------------------------------------------------------------------

db.createCollection("device_registry");

db.device_registry.createIndex(
  { serial: 1 },
  { unique: true, name: "idx_serial_unique" }
);

// -----------------------------------------------------------------------------
// telemetry_raw  (identical to P1 — same collection, same three indexes)
// -----------------------------------------------------------------------------

db.createCollection("telemetry_raw");

// Index A — Q1 (latest per sensor) + Q2 (24h range scan)
db.telemetry_raw.createIndex(
  { "sensor.serial": 1, timestamp: -1 },
  { name: "idx_sensor_serial_ts" }
);

// Index B — Q3 (7d zone aggregation)
db.telemetry_raw.createIndex(
  { "zone.zoneId": 1, timestamp: -1 },
  { name: "idx_zone_zoneId_ts" }
);

// Index C — Q4 (30d cultivation trend) + Q5 (180d VWC summary)
db.telemetry_raw.createIndex(
  { "cultivation.cultivationId": 1, timestamp: -1 },
  { name: "idx_cultivation_cultivationId_ts" }
);

// -----------------------------------------------------------------------------
// telemetry_aggregated  (P2/P3 only — not present in P1)
// -----------------------------------------------------------------------------
// A single collection holding multiple document types. Each type is served by
// a partial index so that:
//   (a) indexes only cover documents they're meant to serve (no wasted space
//       from indexing null fields on unrelated document types), and
//   (b) queries MUST include the type filter to use the index — which they
//       should do anyway to avoid returning wrong document types.
//
// All indexes carry unique: true to enforce the one-doc-per-bucket invariant
// at the database level. MongoDB's unique + partialFilterExpression scopes the
// constraint to only documents matching that type, so different document types
// in the same collection cannot interfere with each other.
//
// Schema reference: implementation_decisions.md Decision P0.3 (storage shape),
//                   Decision P0.5 (final document schemas)

db.createCollection("telemetry_aggregated");

// --- zone_daily ---
// One document per (zoneId, date). Serves Q3 in P2/P3.
// Written by the Go worker: P2 incremental upsert per Kafka event,
//                           P3 full rewrite per hourly batch run.
db.telemetry_aggregated.createIndex(
  { zoneId: 1, date: 1 },
  {
    unique: true,
    name: "idx_zone_daily",
    partialFilterExpression: { type: "zone_daily" },
  }
);

// --- cultivation_daily ---
// One document per (cultivationId, date). Serves Q4 and the daily portion
// of Q5 in P2/P3. Holds both temperature-per-depth and VWC-per-depth in
// the same document (Decision P0.3: avoids scanning telemetry_raw twice).
db.telemetry_aggregated.createIndex(
  { cultivationId: 1, date: 1 },
  {
    unique: true,
    name: "idx_cultivation_daily",
    partialFilterExpression: { type: "cultivation_daily" },
  }
);

// --- cultivation_180d_rollup ---
// One document per cultivationId, fully replaced on every worker run.
// Serves the 180-day rollup average (the "+1 value per depth" part of Q5).
db.telemetry_aggregated.createIndex(
  { cultivationId: 1 },
  {
    unique: true,
    name: "idx_cultivation_180d_rollup",
    partialFilterExpression: { type: "cultivation_180d_rollup" },
  }
);

// --- latest_by_sensor (P2 only) ---
// No index created here. Per Decision P0.4, these documents use
// _id = sensor.serial (a string, not ObjectId). MongoDB's built-in _id
// index handles lookups. The P2 worker upserts via:
//   replaceOne({ _id: serial }, newDoc, { upsert: true })
// The P3 worker never writes this type — Q1 in P3 reads Redis exclusively
// (Decision P0.2).

print(
  "P2/P3 init complete — device_registry (1 unique index) + " +
    "telemetry_raw (3 compound indexes) + " +
    "telemetry_aggregated (3 partial unique indexes)."
);
