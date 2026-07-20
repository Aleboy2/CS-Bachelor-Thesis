#!/bin/bash
set -e

# =============================================================================
# Benchmark runner — P2 (10K, 50K) + P3 (1K, 10K, 50K)
# Polls for completion instead of blind sleep timers.
# Run inside tmux: tmux new -s bench './run-benchmarks.sh'
# =============================================================================

mkdir -p results

# ---------------------------------------------------------------------------
# Helper: wait for a Docker container to exit (polls every 10s)
# ---------------------------------------------------------------------------
wait_for_container_exit() {
    local name=$1
    local label=$2
    echo "[wait] Waiting for $label to finish..."
    while docker ps --format '{{.Names}}' | grep -q "^${name}$"; do
        sleep 10
    done
    echo "[wait] $label finished."
}

# ---------------------------------------------------------------------------
# Helper: wait for worker rewrite by checking logs (polls every 15s)
# ---------------------------------------------------------------------------
wait_for_rewrite() {
    local container=$1
    local label=$2
    echo "[wait] Waiting for $label batch rewrite..."
    while true; do
        if docker logs "$container" 2>&1 | tail -20 | grep -q "rewrite complete"; then
            echo "[wait] $label rewrite complete."
            break
        fi
        sleep 15
    done
}

# ---------------------------------------------------------------------------
# Helper: cooldown (short fixed sleep — these are fine, just 2 min)
# ---------------------------------------------------------------------------
cooldown() {
    echo "[cooldown] Resting 2 minutes..."
    sleep 120
}

# ===========================================================================
# P2 @ 10K — ALREADY DONE, skipping
# ===========================================================================

# ===========================================================================
# P2 @ 50K
# ===========================================================================
echo ""
echo "=========================================="
echo "  P2 @ 50K — $(date)"
echo "=========================================="

docker compose -f docker-compose.p2.yml down -v 2>/dev/null
docker compose -f docker-compose.p2.yml up --build -d
sleep 40

./datagen/datagen seed --sensors 50000
cooldown

echo "[catch-up] Starting P2 50K catch-up..."
docker compose -f docker-compose.p2.yml run --rm --name p2-catchup worker \
    --mode p3 --mongo-uri mongodb://mongodb:27017/iot_benchmark --batch-interval 876000h &
CATCHUP_PID=$!
wait $CATCHUP_PID
echo "[catch-up] P2 50K catch-up complete."

echo "[latest_by_sensor] Populating from telemetry_raw..."
docker exec benchmark-mongodb-p2 mongosh --quiet --eval '
  db = db.getSiblingDB("iot_benchmark");
  db.telemetry_raw.aggregate([
    { $sort: { "sensor.serial": 1, timestamp: -1 } },
    { $group: {
        _id: "$sensor.serial",
        timestamp: { $first: "$timestamp" },
        zone: { $first: "$zone" },
        cultivation: { $first: "$cultivation" },
        depth15: { $first: "$depth15" },
        depth30: { $first: "$depth30" },
        depth45: { $first: "$depth45" },
        depth60: { $first: "$depth60" }
    }},
    { $addFields: { type: "latest_by_sensor" } },
    { $merge: { into: "telemetry_aggregated", whenMatched: "replace" } }
  ], { allowDiskUse: true });
  print("latest_by_sensor: " + db.telemetry_aggregated.countDocuments({type: "latest_by_sensor"}));
'
cooldown

./datagen/datagen replay --sensors 50000 --duration 16m

echo "[drain] Waiting 10 min for P2 worker to drain Kafka backlog..."
sleep 600

./harness/harness ingest-lag --window 16m --output results/ingest-lag-p2-50k.csv
./harness/harness query --prototype p2 --iterations 100 --output results/queries-p2-50k.csv

echo "=== P2 @ 50K — DONE $(date) ==="
docker compose -f docker-compose.p2.yml down
cooldown

# ===========================================================================
# P3 @ 1K
# ===========================================================================
echo ""
echo "=========================================="
echo "  P3 @ 1K — $(date)"
echo "=========================================="

docker compose -f docker-compose.p3.yml down -v 2>/dev/null
docker compose -f docker-compose.p3.yml up --build -d
sleep 40

./datagen/datagen seed --sensors 1000
cooldown

docker restart benchmark-worker-p3
wait_for_rewrite benchmark-worker-p3 "P3 1K"

./datagen/datagen replay --sensors 1000 --duration 16m
./harness/harness ingest-lag --window 16m --output results/ingest-lag-p3-1k.csv
./harness/harness query --prototype p3 --iterations 100 --output results/queries-p3-1k.csv

echo "=== P3 @ 1K — DONE $(date) ==="
docker compose -f docker-compose.p3.yml down
cooldown

# ===========================================================================
# P3 @ 10K
# ===========================================================================
echo ""
echo "=========================================="
echo "  P3 @ 10K — $(date)"
echo "=========================================="

docker compose -f docker-compose.p3.yml down -v 2>/dev/null
docker compose -f docker-compose.p3.yml up --build -d
sleep 40

./datagen/datagen seed --sensors 10000
cooldown

docker restart benchmark-worker-p3
wait_for_rewrite benchmark-worker-p3 "P3 10K"

./datagen/datagen replay --sensors 10000 --duration 16m
./harness/harness ingest-lag --window 16m --output results/ingest-lag-p3-10k.csv
./harness/harness query --prototype p3 --iterations 100 --output results/queries-p3-10k.csv

echo "=== P3 @ 10K — DONE $(date) ==="
docker compose -f docker-compose.p3.yml down
cooldown

# ===========================================================================
# P3 @ 50K
# ===========================================================================
echo ""
echo "=========================================="
echo "  P3 @ 50K — $(date)"
echo "=========================================="

docker compose -f docker-compose.p3.yml down -v 2>/dev/null
docker compose -f docker-compose.p3.yml up --build -d
sleep 40

./datagen/datagen seed --sensors 50000
cooldown

docker restart benchmark-worker-p3
wait_for_rewrite benchmark-worker-p3 "P3 50K"

./datagen/datagen replay --sensors 50000 --duration 16m
./harness/harness ingest-lag --window 16m --output results/ingest-lag-p3-50k.csv
./harness/harness query --prototype p3 --iterations 100 --output results/queries-p3-50k.csv

echo "=== P3 @ 50K — DONE $(date) ==="
docker compose -f docker-compose.p3.yml down

echo ""
echo "=========================================="
echo "  ALL BENCHMARKS COMPLETE — $(date)"
echo "=========================================="
ls -la results/
