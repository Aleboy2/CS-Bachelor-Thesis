# CLAUDE.md — IoT Telemetry Benchmark (BSc Thesis Implementation)

This file gives Claude Code the context needed to work on this codebase correctly.
It reflects **locked decisions** — do not silently deviate from anything marked 🔒.
If a change to a locked decision seems necessary, stop and ask before implementing it;
these decisions were made for specific methodological reasons tied to the thesis's
research questions, and an unreviewed change can invalidate the comparison the whole
benchmark exists to make.

---

## 1. What this project is

A BSc Computer Science thesis (VU Amsterdam) benchmarking three data architecture
patterns for agricultural IoT telemetry under synthetic sensor workloads at increasing
scale (1K, 10K, 50K sensors). The motivating context is a precision agriculture
IoT platform (soil sensors reporting moisture/temperature/conductivity), but **the
company behind it must never be named anywhere in code, comments, collection names,
schema, or documentation** — contributions must read as applicable to any precision
agriculture IoT platform. If you're about to write a comment justifying a design choice
by referencing "production" or "the company's system," stop — that justification must
be reframed in generic terms or removed.

The three architectures under comparison:

- **P1** — Single-store MongoDB baseline. No Kafka, no Redis, no background workers.
  All dashboard queries compute live from `telemetry_raw` via aggregation pipelines.
- **P2** — CQRS. MongoDB + Kafka (KRaft mode) + a continuous Go worker that
  incrementally materialises `telemetry_aggregated` from Kafka events.
- **P3** — Lambda Architecture. MongoDB + Kafka + a Go batch worker (hourly,
  full-history recompute of `telemetry_aggregated`) + Redis as the speed layer
  (most recent 60-minute window, per-sensor latest state only).

**Framing discipline:** never use "migration" language. This is proactive research
("at what scale does the architectural trade-off become meaningful") not reactive
problem-solving ("P1 has limitations, here's how we fixed them"). P1 showing strain
at scale is the empirical *finding*, not the project's premise.

**"MongoDB vs CQRS" is a conceptually muddled framing.** All three prototypes use
MongoDB. The comparison is between architectural patterns (when/where aggregation
happens), not between databases.

---

## 2. Locked implementation decisions (Phase 0)

Source of truth: `implementation_decisions.md` in the project knowledge base. Treat
this section as a summary — if there's ever a conflict between this file and that
document, that document wins, and the discrepancy should be flagged.

### P0.1 — `telemetry_raw` compound indexes 🔒
Three indexes, all following the pattern `{equality_field: 1, timestamp: -1}` so
MongoDB can serve both the filter and the sort from the index with no in-memory sort:

| Index | Definition | Serves |
|---|---|---|
| A | `{ "sensor.serial": 1, "timestamp": -1 }` | Q1, Q2 |
| B | `{ "zone.zoneId": 1, "timestamp": -1 }` | Q3 |
| C | `{ "cultivation.cultivationId": 1, "timestamp": -1 }` | Q4, Q5 |

These exist in P1 too — P1 must receive every reasonable single-store optimization
or the baseline is artificially slow and the comparison is indefensible. Write
overhead from maintaining these indexes is part of P1's *measured* cost (RQ1), not
a confound to be avoided.

### P0.2 — Q1 read source split between P2 and P3 🔒
- **P2:** Q1 reads from `telemetry_aggregated`, document type `latest_by_sensor`,
  upserted on every Kafka event by the continuous worker.
- **P3:** Q1 reads from **Redis only**. P3's `telemetry_aggregated` never tracks
  per-sensor latest state — that's the speed layer's job exclusively. No fallback
  to `telemetry_aggregated` if Redis misses.

This keeps the Lambda batch/speed split clean: batch layer owns Q3/Q4/Q5, speed
layer owns Q1. Don't blur this even if it seems like a convenient shortcut.

### P0.3 — `telemetry_aggregated` storage shape: Option A 🔒
One document per entity per time-bucket (e.g. one per cultivation per day) —
**not** one growing document per cultivation with embedded arrays. This keeps the
same per-bucket philosophy as `telemetry_raw`'s one-doc-per-reading, so the only
variable changing between P1/P2/P3 is *when* aggregation happens, not *how the data
is shaped*. A differently-shaped P2/P3 schema would let a critical reader argue the
performance gain partly comes from a smuggled-in schema advantage.

### P0.4 — P2 `latest_by_sensor` schema 🔒
Lives in `telemetry_aggregated`, P2 only. `_id = sensor.serial` (no secondary index
needed — natural key). Embeds zone/cultivation (mirrors `telemetry_raw`'s structure)
rather than being a lean reading-only doc — this keeps Q1 a true single-document read
in every prototype; a lean doc would force P2's Q1 to do a second lookup, reintroducing
the join cost P1 deliberately avoids via denormalisation.

### P0.5 — Final `telemetry_aggregated` schemas 🔒
- `cultivation_daily` and `cultivation_180d_rollup` are **separate document types**,
  not merged — different write patterns need different indexes.
- `date` / bucket fields are stored as **ISODate truncated to UTC midnight**, never
  as strings — avoids timezone-truncation mismatches that could silently corrupt
  daily bucket boundaries.
- `sensorCount` retained on bucket documents — cheap, gives a free sanity-check axis
  during evaluation.
- **Critical:** `telemetry_aggregated` documents always store **pre-computed**
  values written by the worker at write time, **never raw readings computed on
  demand at query time**. If this rule is violated, P2/P3 silently reimplement P1's
  read-time computation and the entire RQ2 comparison collapses. Worker writes the
  number; query layer reads the number — zero aggregation pipelines in the P2/P3
  query path.

### Infrastructure decisions (this session) 🔒
- **Two Mongo init scripts, not three.** `mongo-init/p1/` (no `telemetry_aggregated`)
  and `mongo-init/p2p3/` (shared by both P2 and P3 — they are byte-identical at the
  database-init level; the P2/P3 difference is in worker runtime behavior, not schema).
- **Partial indexes** on `telemetry_aggregated`, scoped via `partialFilterExpression`
  on the `type` field, with `unique: true` to enforce one-doc-per-bucket at the DB
  level. Query code **must** include the `type` filter or these indexes won't be used.
- **No MongoDB auth.** Per-operation auth overhead isn't part of the research
  question; this is a documented controlled simplification in the methodology.
- **Separate named Docker volume per prototype** (`mongodb-data-p1/p2/p3`). MongoDB's
  entrypoint only runs init scripts on an empty data directory — sharing a volume
  across prototypes would mean switching from P1 to P2 silently skips creating
  `telemetry_aggregated`.
- **MongoDB infra for P1 and P2 verified** (2026-06-21) — all collections and
  indexes confirmed via `getIndexes()` against the actual running containers,
  matching this file exactly (field order, `unique`, `partialFilterExpression`
  all present and correct). P3 not separately re-verified beyond the init-complete
  log line, since it shares the byte-identical P2/P3 init script.

### Ingest API decisions (this session) 🔒
- **One shared codebase** for the ingest API across all three prototypes — not
  duplicated per-prototype. The ingest path (§3 below) is byte-identical in P1,
  P2, and P3 per the schema doc; the only difference is whether it also publishes
  to Kafka after the MongoDB write. Duplicating identical logic three times would
  be pure redundancy with no methodological justification (unlike the Mongo init
  scripts, where P1 and P2/P3 actually have different schemas to create).
- **Prototype selection via `PROTOTYPE=p1|p2|p3` env var.** The ingest API reads
  this at startup and conditionally publishes to Kafka after the `telemetry_raw`
  insert only when `PROTOTYPE` is `p2` or `p3`. P1 never touches Kafka.
- **MQTT topic: single flat topic, `sensors/telemetry`.** Every sensor (real or
  simulated) publishes to this one topic; the ingest API subscribes once at
  startup. No per-sensor or per-zone topic hierarchy. Rationale: Mosquitto is
  explicitly "not a variable in the benchmark" (`benchmark_schema_design_v41.pdf`
  §9.1) and is identical across all three prototypes — a topic hierarchy would
  add complexity with zero benchmark payoff, since the ingest API must subscribe
  to all sensors regardless and the sensor's `serial` is already in the payload.
- **`device_registry` lookup miss → log and drop, never crash.** If a `serial`
  arrives with no matching `device_registry` document (shouldn't happen if the
  generator seeds correctly, but must be handled), the ingest API logs the event
  with a counter and drops the message — it does **not** crash or halt ingestion.
  A hard failure mid-50K-sensor benchmark run is far more disruptive than one
  dropped reading. If this counter is ever non-zero during a real benchmark run,
  that's a generator/seeding bug worth investigating, not an expected outcome.
- **No HTTP server.** The ingest API's only job is consume-from-MQTT,
  enrich-from-`device_registry`, write-to-`telemetry_raw` (and conditionally
  publish-to-Kafka). No health-check endpoint, no manual-trigger endpoint — kept
  minimal. Built as a plain Node.js MQTT consumer (`mqtt` npm package), no
  Express/Fastify.

### P0.6 — Kafka topic configuration 🔒
- **Topic name: `telemetry-raw-events`.** Single topic, published immediately after
  a successful `telemetry_raw` insert, carrying the same document that was just
  written. P2's continuous Go worker and P3's batch Go worker both consume from
  this topic. Consistent with the single-flat-MQTT-topic philosophy — Kafka topic
  hierarchy would add complexity with no benchmark payoff.
- **Partition key: `sensor.serial`.** Ensures all readings from a single sensor
  land on the same partition in order, which simplifies P2's per-sensor
  `latest_by_sensor` upsert logic (no cross-partition coordination needed).
- **Partition count: 1.** Single-node benchmark with a single Go consumer per
  prototype — there is no parallelism to exploit from multiple partitions.
  One partition guarantees total ordering and avoids Kafka bookkeeping overhead.
  The partition key (`sensor.serial`) remains meaningful for future-proofing:
  if partition count were ever increased, same-sensor readings would co-locate.

---

## 3. Collections and schemas

### `device_registry`
Static lookup table, seeded once before each benchmark run, never modified during it.
Source of truth for GPS and DCI (sensor-transmitted GPS/DCI are ignored — registered
values are authoritative, since sensors are assumed stationary for a growing season).

```json
{
  "_id": ObjectId,
  "serial": "A70302A02025",
  "dci": "7fc98f10-...",
  "gps": { "latitude": 51.612621, "longitude": 5.683513 },
  "zone": { "zoneId": "67bc...", "name": "Field 2" },
  "cultivation": {
    "cultivationId": "67bd...",
    "name": "Field 2 — Potatoes 2025",
    "crop": { "name": "Potatoes", "isInGreenHouse": false, "sowingDate": "2025-09-02T23:00:00Z",
               "bumpHeight": 0, "distanceToSeedling": 25, "sowingDepth": 0 },
    "irrigationSystem": { "irrigationProfile": "conservative", "dripperFlowRate": 0,
                           "minAmount": 2, "maxAmount": 50 }
  },
  "schemaVersion": "1.0"
}
```
Index: `{ serial: 1 }` unique.

### `telemetry_raw`
Append-only, one document per sensor reading. Fully denormalised (zone + cultivation
embedded — deliberately, to keep P1 a clean single-store baseline with no $lookup
joins; normalising this would partially replicate P2's architectural advantage inside
P1 itself). ~144M documents at 50K-sensor scale over 30 days.

```json
{
  "_id": ObjectId,
  "timestamp": ISODate,        // event time, device-generated
  "ingestedAt": ISODate,       // processing time, API-generated — diff = ingest lag (RQ1)
  "sensor": { "serial": "A70302A02025", "battery": 90 },  // battery: metadata only, never queried
  "zone": { "zoneId": "67bc...", "name": "Field 2" },
  "cultivation": { "cultivationId": "67bd...", "name": "...", "crop": {...}, "irrigationSystem": {...} },
  "depth15": { "vwc": 34.21, "temperature": 18.6, "ec": 0.42 },
  "depth30": { "vwc": 38.10, "temperature": 17.9, "ec": 0.51 },
  "depth45": { "vwc": 40.50, "temperature": 17.2, "ec": 0.48 },
  "depth60": { "vwc": 41.80, "temperature": 16.8, "ec": 0.45 },
  "schemaVersion": "1.0"
}
```
Indexes: A, B, C as in §2.

### `telemetry_aggregated` (P2/P3 only — not created in P1)
One collection, multiple document types distinguished by `type`. **Always
pre-computed values, never raw data** (see P0.5).

```json
// zone_daily — Q3
{ "_id": ObjectId, "type": "zone_daily", "zoneId": "67bc...",
  "date": ISODate (UTC midnight), "avgVwc": 34.21, "sensorCount": 3 }
// unique partial index: { zoneId: 1, date: 1 } where type == "zone_daily"

// cultivation_daily — Q4 + daily portion of Q5
{ "_id": ObjectId, "type": "cultivation_daily", "cultivationId": "67bd...",
  "date": ISODate (UTC midnight),
  "temperature": { "depth15": 18.6, "depth30": 17.9, "depth45": 17.2, "depth60": 16.8 },
  "vwc":         { "depth15": 34.21, "depth30": 38.10, "depth45": 40.50, "depth60": 41.80 },
  "sensorCount": 3 }
// unique partial index: { cultivationId: 1, date: 1 } where type == "cultivation_daily"

// cultivation_180d_rollup — 180-day rollup portion of Q5
{ "_id": ObjectId, "type": "cultivation_180d_rollup", "cultivationId": "67bd...",
  "windowEnd": ISODate, "avgVwc180d": { "depth15": 33.8, "depth30": 37.5, "depth45": 39.9, "depth60": 41.2 } }
// unique partial index: { cultivationId: 1 } where type == "cultivation_180d_rollup"

// latest_by_sensor — Q1, P2 ONLY (never written in P3)
{ "_id": "A70302A02025",   // sensor.serial as _id — natural key, no extra index
  "type": "latest_by_sensor", "timestamp": ISODate,
  "zone": {...}, "cultivation": {...},
  "depth15": {...}, "depth30": {...}, "depth45": {...}, "depth60": {...} }
```

### MQTT payload (what the sensor actually transmits — lean, not the stored shape)
```json
{
  "timestamp": "2025-02-26T11:58:17+01:00",
  "sensor": { "serial": "A70302A02025", "battery": 90 },
  "depth15": { "vwc": 34.21, "temperature": 18.6, "ec": 0.42 },
  "depth30": { "vwc": 38.10, "temperature": 17.9, "ec": 0.51 },
  "depth45": { "vwc": 40.50, "temperature": 17.2, "ec": 0.48 },
  "depth60": { "vwc": 41.80, "temperature": 16.8, "ec": 0.45 }
}
```
`timestamp` is the sensor's event time (ISO 8601 string). It is required: without
it, ingest lag (`ingestedAt - timestamp`) is unmeasurable and RQ1 loses a key
metric. **Note:** `benchmark_schema_design_v41.pdf` has an internal inconsistency
here — §5.2 omits `timestamp` from the payload example, but §6.3 explicitly states
that `timestamp` is "included in the MQTT payload." The §6.3 description is
correct; the §5.2 payload example is incomplete.

No GPS, no DCI, no zone/cultivation — all enrichment happens at ingest via
`device_registry` lookup keyed on `serial`. This is intentional (bandwidth + lets
metadata be updated centrally without firmware changes).

### Ingest path (identical across all three prototypes)
1. Sensor (or Go generator) publishes MQTT message to Mosquitto.
2. Node.js ingest API receives it, extracts `serial`.
3. API looks up `device_registry` by `serial` — retrieves the full registry document
   (including `zone`, `cultivation`, `gps`, `dci`).
4. API constructs the `telemetry_raw` document: `timestamp` + `sensor` from MQTT
   payload, `zone` + `cultivation` + `schemaVersion` from registry, `ingestedAt`
   stamped now. **`gps` and `dci` are deliberately not embedded** — no benchmark
   query uses them, and embedding GPS in every document at ~144M-document scale
   would be pure redundancy.
5. API inserts into `telemetry_raw`. This insert is the RQ1 write-throughput
   measurement point. `ingestedAt - timestamp` = ingest lag.

---

## 4. The five benchmark query types

Ordered cheapest → most expensive in P1, to make the P1-vs-P2/P3 performance gap
increasingly visible as query complexity grows.

| Query | Description | Window | Data volume (max) | P1 cost | P2/P3 cost |
|---|---|---|---|---|---|
| **Q1** | Latest reading, single sensor | none | 1 doc | minimal (indexed point lookup) | minimal (single-doc read) |
| **Q2** | All readings, single sensor | 24h | 96 docs | low (indexed range scan) | low |
| **Q3** | Avg VWC across zone | 7d | up to 672 docs × 1–5 sensors | moderate (aggregation pipeline) | low (pre-computed) |
| **Q4** | Daily avg soil temp at all 4 depths, single cultivation | 30d | up to 14,400 docs | high | low |
| **Q5** | Avg VWC at all 4 depths, full cultivation period | up to 180d | up to 86,400 docs | very high | minimal (single lookup) |

Q5 detail: returns 180 daily-average-VWC points + 1 overall 180-day rollup, per depth
— 4 × 181 = 724 values total. Both daily buckets and the rollup are computed
**server-side in every prototype** (P1 via `$facet` aggregation to get both outputs
in one pass; P2/P3 pre-computed by the worker) — deliberate, so the only fair
variable is *when* the work happens, not *where*.

Scale levels (sensor count is the single independent variable — everything else held
constant): 1K, 10K, 50K sensors. Sensors transmit every 15 minutes, staggered
offsets seeded deterministically from serial number.

---

## 5. Tech stack

| Component | Technology | Notes |
|---|---|---|
| MQTT broker | Mosquitto (Docker) | Identical across all 3 prototypes — not a benchmark variable |
| Message broker | Kafka, KRaft mode (Docker) | P2 + P3 only. No Zookeeper. |
| Ingest API | JavaScript / Node.js (CommonJS) | Shared across all 3 prototypes |
| Database | MongoDB 7.0 (Docker) | All 3 prototypes |
| Speed layer | Redis | P3 only |
| Background worker | Go | P2: continuous Kafka consumer. P3: hourly batch job. Absent in P1. |
| Benchmark harness | Go | Runs on host, **outside Docker**, to avoid measurement interference |
| Synthetic data generator | Go | Pre-generates full dataset before any timed run; deterministic seeding |
| Containerisation | Docker Compose | One compose file per prototype: `docker-compose.p1.yml`, `.p2.yml`, `.p3.yml` |

---

## 6. Repository layout (current state)

```
iot-benchmark/
├── CLAUDE.md
├── docker-compose.p1.yml      # MongoDB only
├── docker-compose.p2.yml      # MongoDB (+ Kafka placeholder)
├── docker-compose.p3.yml      # MongoDB (+ Kafka/Redis placeholders)
├── mongo-init/
│   ├── p1/01-init-p1.js       # device_registry + telemetry_raw (3 indexes)
│   └── p2p3/01-init-p2p3.js   # above + telemetry_aggregated (3 partial unique indexes)
└── ingest-api/                # IN PROGRESS — shared across all 3 prototypes
    ├── src/
    │   ├── index.js           # entrypoint: connects MQTT + Mongo (+ Kafka if P2/P3), subscribes
    │   ├── mongo.js            # Mongo client + device_registry lookup + telemetry_raw insert
    │   ├── kafka.js            # Kafka producer, only invoked when PROTOTYPE=p2|p3
    │   └── buildDocument.js    # constructs telemetry_raw doc from MQTT payload + registry data
    ├── package.json
    └── Dockerfile
```

Each compose file mounts its own data directory as a **separate named volume**
(`mongodb-data-p1/p2/p3`) and its own init-script folder. Run `docker compose`
commands from the `iot-benchmark/` root — paths are relative to the compose file location.

No auth on MongoDB (intentional — documented methodology simplification).
MongoDB infra verified for P1 and P2 (see §2 Infrastructure decisions). No
Mosquitto, Kafka, Redis, or workers yet. Ingest API is the current focus — see
§2 Ingest API decisions for the locked design (single flat MQTT topic, shared
codebase, `PROTOTYPE` env var, log-and-drop on registry-miss, no HTTP server).

---

## 7. What's NOT done yet (build order, most logical next first)

1. **Mosquitto + Node.js ingest API** — IN PROGRESS. Shared identically across
   all 3 prototypes (see §2 Ingest API decisions for locked design choices:
   single flat topic `sensors/telemetry`, `PROTOTYPE` env var, log-and-drop on
   registry-miss, no HTTP server, plain `mqtt` package).
2. **Go synthetic data generator** — needed to seed `device_registry` and produce
   replayable MQTT payloads. Natural next step once ingest API can receive them.
3. **Kafka (KRaft)** — P2/P3 only.
4. **Go background workers** — P2 continuous worker, P3 hourly batch worker.
5. **Redis** — P3 speed layer only.
6. **Go benchmark harness** — runs on host, outside Docker.

---

## 8. Working conventions for this project

- **Citation discipline applies to thesis text, not code** — but if Claude Code is
  ever asked to help write methodology/thesis prose alongside the implementation,
  the same citation-accuracy rules apply: verify claims against actual source PDFs,
  never invent attributions.
- **Don't add fields, collections, or indexes not listed above without flagging it
  first.** Every field in this schema exists for a specific, documented reason tied
  to a research question or query type. An "obviously useful" addition (e.g. a
  convenience field, a denormalised shortcut) can quietly become a confound.
- **Don't relax the "no Agurotech naming" rule** even in code comments, env var
  names, or test fixtures.
- **If a query implementation seems to need a schema change**, stop and flag it —
  schema changes have already been deliberated extensively and have second-order
  effects on the P1 vs P2/P3 comparison's validity.
- Alessandro reviews and tests everything himself iteratively; don't assume code
  is "done" just because it runs — flag edge cases, assumptions, and anything that
  deserves a second look rather than silently picking a default.
