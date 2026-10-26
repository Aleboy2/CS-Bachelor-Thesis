# Benchmarking Data Architecture Patterns for Precision Agriculture IoT Telemetry

**Bachelor's Thesis** — BSc Computer Science, Vrije Universiteit Amsterdam (X_400628)

**Author:** Alessandro Coatto (2770462)
**First Supervisor:** Jeewanie Jayasinghe Arachchillage, VU Amsterdam
**Daily Supervisor:** Qiuke Li, Agurotech
**Second Reader:** Shahab Ud Din, VU Amsterdam
**Defense Date:** August 24, 2026

---

## Overview

This repository contains the full source code, infrastructure definitions, and raw benchmark results for the thesis *Benchmarking Data Architecture Patterns for Precision Agriculture IoT Telemetry*.

The thesis empirically compares three MongoDB-based data architecture patterns under agricultural IoT telemetry workloads:

| Prototype | Pattern | Description |
|-----------|---------|-------------|
| **P1** | Single-Store Baseline | All queries run live aggregation pipelines against `telemetry_raw`. No background processing. |
| **P2** | CQRS | A Kafka-driven Go worker continuously materializes pre-computed aggregates into `telemetry_aggregated`. Queries become cheap single-document reads. |
| **P3** | Lambda Architecture | An hourly batch worker rewrites `telemetry_aggregated` from full history. A Redis speed layer serves per-sensor latest readings (Q1). |

Each prototype is benchmarked across three sensor scales (1K, 10K, 50K) on five representative dashboard queries and an ingest-lag measurement.

> **Key finding:** CQRS (P2) delivers the full read-latency benefit of the Lambda Architecture (P3) at lower operational complexity. Query latency for P2 and P3 stays flat across sensor scales (pre-computed aggregates), while P1 scales steeply for aggregation-heavy queries.

## Repository Structure

```
.
├── Thesis.pdf                  # Final thesis document
├── datagen/                    # Go CLI — data seeding and MQTT replay
│   ├── main.go                 #   Entry point (seed / replay subcommands)
│   ├── entities.go             #   Device registry generation (zones, cultivations, sensors)
│   ├── readings.go             #   Synthetic telemetry reading generation
│   ├── seed.go                 #   Bulk-insert 180 days of historical telemetry into MongoDB
│   └── replay.go               #   Real-time MQTT replay for ingest-lag measurement
├── ingest-api/                 # Node.js (CommonJS) MQTT-to-MongoDB ingest service
│   ├── src/
│   │   ├── index.js            #   Entry point — MQTT subscriber, routes to MongoDB/Kafka/Redis
│   │   ├── buildDocument.js    #   Transforms MQTT payload into telemetry_raw document
│   │   ├── mongo.js            #   MongoDB connection and insert logic
│   │   ├── kafka.js            #   Kafka producer (P2/P3 only)
│   │   └── redis.js            #   Redis writer (P3 only)
│   └── test-buildDocument.js   #   Unit tests for document builder
├── worker/                     # Go worker — CQRS continuous & Lambda batch processing
│   ├── main.go                 #   Entry point — mode selection (p2, p2-backfill, p3-batch, etc.)
│   ├── p2.go                   #   CQRS: continuous Kafka consumer → telemetry_aggregated upserts
│   ├── p3.go                   #   Lambda: hourly batch rewrite of telemetry_aggregated
│   ├── backfill.go             #   Backfill modes for aggregated views and Redis
│   └── types.go                #   Shared type definitions
├── harness/                    # Go benchmark harness — query latency & ingest-lag measurement
│   ├── main.go                 #   Entry point — prototype/scale/anchor configuration
│   ├── queries.go              #   Q1–Q5 implementations per prototype
│   ├── runner.go               #   Query iteration loop, CSV output
│   └── ingestlag.go            #   Ingest-lag collection logic
├── mongo-init/                 # MongoDB initialization scripts
│   ├── p1/init-p1.js           #   Indexes for P1 (single-store optimized)
│   └── p2p3/init-p2p3.js       #   Indexes for P2 and P3 (raw + aggregated)
├── mosquitto/
│   └── mosquitto.conf          # Mosquitto MQTT broker configuration
├── docker-compose.p1.yml       # Docker Compose for P1 (MongoDB + Mosquitto + Ingest API)
├── docker-compose.p2.yml       # Docker Compose for P2 (+ Kafka + Worker)
├── docker-compose.p3.yml       # Docker Compose for P3 (+ Kafka + Redis + Worker)
├── run-benchmark-50k.sh        # Automated, resumable benchmark pipeline (P1→P2→P3 at 50K)
└── results/                    # Raw benchmark CSV data (17 files)
    ├── queries-p{1,2,3}-{1k,10k,50k}.csv    # Query latency (500 iterations × 5 queries)
    └── ingest-lag-p{1,2,3}-{1k,10k,50k}.csv  # Ingest lag per message
```

## Benchmark Queries

| Query | Description | P1 Strategy | P2/P3 Strategy |
|-------|-------------|-------------|----------------|
| **Q1** | Latest reading for a single sensor | `findOne` sorted by timestamp (Index A) | P2: direct `_id` lookup on `telemetry_aggregated`; P3: Redis `GET` |
| **Q2** | All readings, single sensor, last 24h | Range scan on `telemetry_raw` (Index A) | Same as P1 (reads raw) |
| **Q3** | Avg VWC across zone, last 7 days | Aggregation pipeline on `telemetry_raw` (Index B) | Lookup `zone_daily` docs from `telemetry_aggregated` |
| **Q4** | Daily avg temperature (4 depths), single cultivation, 30 days | Aggregation pipeline on `telemetry_raw` (Index C) | Lookup `cultivation_daily` docs from `telemetry_aggregated` |
| **Q5** | Avg VWC (4 depths), full cultivation period (180 days + rollup) | `$facet` pipeline on `telemetry_raw` (Index C) | Read `cultivation_daily` + `cultivation_180d_rollup` from `telemetry_aggregated` |

## Technology Stack

- **Data Generator & Benchmark Harness:** Go 1.22
- **Ingest API:** Node.js (CommonJS)
- **Database:** MongoDB 7.0
- **Message Broker:** Apache Kafka 3.8.1 (KRaft mode, no Zookeeper)
- **Speed Layer Cache:** Redis (P3 only)
- **MQTT Broker:** Eclipse Mosquitto
- **Orchestration:** Docker Compose (one file per prototype)

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/) v2+
- [Go](https://go.dev/dl/) 1.22+
- [Node.js](https://nodejs.org/) 18+
- Sufficient disk space (~360 GB recommended for the 50K-sensor scale)

## Getting Started

### 1. Build the Go tools

```bash
# Data generator
cd datagen && go build -o datagen . && cd ..

# Benchmark harness
cd harness && go build -o harness . && cd ..

# Worker (used by P2 and P3)
cd worker && go build -o worker . && cd ..
```

### 2. Install Ingest API dependencies

```bash
cd ingest-api && npm install && cd ..
```

### 3. Run a single prototype (e.g., P1 at 1K sensors)

```bash
# Start the infrastructure
docker compose -f docker-compose.p1.yml up --build -d

# Wait for MongoDB to be healthy, then seed
./datagen/datagen seed --sensors 1000

# Run the query benchmark (500 iterations per query)
./harness/harness --prototype p1 --sensors 1000 --anchor 2026-06-01T00:00:00Z

# Run the ingest-lag measurement (MQTT replay)
./datagen/datagen replay --sensors 1000 --duration 5m
./harness/harness --prototype p1 --sensors 1000 --mode ingest-lag

# Tear down
docker compose -f docker-compose.p1.yml down -v
```

### 4. Run the full 50K-sensor pipeline (automated)

The `run-benchmark-50k.sh` script automates the full P1→P2→P3 pipeline with checkpointing for resumability:

```bash
bash run-benchmark-50k.sh
```

> **Note:** A full 50K run takes approximately 8–10 hours, dominated by seeding ~864M documents per prototype. If interrupted, re-run the script — completed steps are checkpointed and skipped on resume.

## Results

All raw benchmark data lives in `results/`. Each query-latency CSV has the format:

```
prototype,query,iteration,latency_ms
p1,Q1,1,0.267
p1,Q1,2,0.328
...
```

Each ingest-lag CSV has the format:

```
index,lag_ms
1,0.000
2,0.000
...
```

A total of 17 CSV files cover all prototype × scale combinations (9 query files + 8 ingest-lag files).

## Citation

If you reference this work, please cite:

```
Coatto, A. (2026). Benchmarking Data Architecture Patterns for Precision Agriculture
IoT Telemetry. Bachelor's Thesis, Vrije Universiteit Amsterdam.
```

## License

This repository is part of an academic thesis. Please contact the author for reuse permissions.
