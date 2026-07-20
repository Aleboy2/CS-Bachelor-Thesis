// =============================================================================
// P1 (Single-Store Baseline) — MongoDB Initialisation Script
// =============================================================================
// LAYER: Data layer bootstrap (schema + index creation only, no data).
//
// EXECUTED BY: the mongo:7.0 Docker entrypoint, mounted read-only into
//   /docker-entrypoint-initdb.d and run automatically via mongosh the first
//   time the container starts against an empty data directory (see
//   docker-compose.p1.yml). Nothing else in the repo calls this file.
// WRITTEN TO:  the `iot_benchmark` database inside the P1 MongoDB container.
//   The Go generator seeds device_registry data afterwards; the ingest API
//   (ingest-api/src/mongo.js) reads/writes telemetry_raw at runtime.
//
// Creates:
//   - device_registry   (static lookup table, one doc per sensor)
//   - telemetry_raw     (append-only time-series, one doc per reading)
//
// Does NOT create:
//   - telemetry_aggregated  (not used in P1 — all queries run live against
//                            telemetry_raw via aggregation pipelines)
//
// References:
//   - benchmark_schema_design_v41.pdf  §3 (Collections), §4 (device_registry),
//     §6 (telemetry_raw)
//   - implementation_decisions.md      Decision P0.1 (compound indexes)
//
// Run context:
//   Executed automatically by the mongo:7.0 Docker entrypoint on first startup
//   (data directory empty). Runs inside mongosh.
// =============================================================================

db = db.getSiblingDB("iot_benchmark");

// -----------------------------------------------------------------------------
// device_registry
// -----------------------------------------------------------------------------
// Static lookup table seeded once before each benchmark run by the Go generator.
// One document per physical sensor. The ingest API queries this on every MQTT
// message to retrieve zone, cultivation, GPS, and DCI for document enrichment.
//
// Schema reference: benchmark_schema_design_v41.pdf §4.3

db.createCollection("device_registry");

// serial is the lookup key used by the ingest API on every write.
// Unique constraint enforces one-document-per-sensor invariant.
db.device_registry.createIndex(
  { serial: 1 },
  { unique: true, name: "idx_serial_unique" }
);

// -----------------------------------------------------------------------------
// telemetry_raw
// -----------------------------------------------------------------------------
// Append-only time-series collection. One document per sensor reading.
// At 100K sensors × 96 readings/day × 30 days = ~288M documents at max scale.
//
// Schema reference: benchmark_schema_design_v41.pdf §6.4
// Index rationale: implementation_decisions.md Decision P0.1

db.createCollection("telemetry_raw");

// Index A — serves Q1 (latest reading per sensor: findOne + sort) and
//           Q2 (24h sensor range scan).
// Field order: equality filter first (sensor.serial), then sort key (timestamp
// descending). Lets MongoDB satisfy both filter and sort from the index with
// no in-memory sort step.
db.telemetry_raw.createIndex(
  { "sensor.serial": 1, timestamp: -1 },
  { name: "idx_sensor_serial_ts" }
);

// Index B — serves Q3 (7d zone aggregation).
db.telemetry_raw.createIndex(
  { "zone.zoneId": 1, timestamp: -1 },
  { name: "idx_zone_zoneId_ts" }
);

// Index C — serves Q4 (30d cultivation trend) and Q5 (180d VWC summary).
db.telemetry_raw.createIndex(
  { "cultivation.cultivationId": 1, timestamp: -1 },
  { name: "idx_cultivation_cultivationId_ts" }
);

print(
  "P1 init complete — device_registry (1 unique index) + " +
    "telemetry_raw (3 compound indexes: A, B, C)."
);
