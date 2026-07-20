#!/bin/bash

# =============================================================================
# P3 — all three scales (50K first, then 10K, then 1K)
#
# Bulletproof:
#   - No set -e (won't crash on non-zero exit codes)
#   - No wait $PID (can't block forever)
#   - Uses --since on docker logs (ignores stale "rewrite complete" messages)
#   - Prints progress every 30 seconds during long waits
#   - Verifies data at each step before proceeding
#   - 2-minute cooldowns between heavy operations
#
# Run with: tmux new -s bench 'caffeinate -dims ./run-p3-all.sh'
# =============================================================================

mkdir -p results

# ---------------------------------------------------------------------------
# Helper: poll for "rewrite complete" in worker logs, only after $since_time
# ---------------------------------------------------------------------------
wait_for_rewrite() {
    local container=$1
    local since_time=$2
    local label=$3
    echo "[wait] Waiting for $label batch rewrite (logs since $since_time)..."
    while true; do
        if docker logs "$container" --since "$since_time" 2>&1 | grep -q "rewrite complete"; then
            echo "[wait] $label batch rewrite complete!"
            return 0
        fi
        LATEST=$(docker logs "$container" --since "$since_time" 2>&1 | tail -1)
        echo "  [polling $(date +%H:%M:%S)] $LATEST"
        sleep 30
    done
}

# ---------------------------------------------------------------------------
# Helper: verify aggregated doc counts
# ---------------------------------------------------------------------------
verify_aggregated() {
    local mongo_container=$1
    docker exec "$mongo_container" mongosh --quiet --eval '
      db = db.getSiblingDB("iot_benchmark");
      print("  zone_daily:       " + db.telemetry_aggregated.countDocuments({type:"zone_daily"}));
      print("  cult_daily:       " + db.telemetry_aggregated.countDocuments({type:"cultivation_daily"}));
      print("  cult_180d_rollup: " + db.telemetry_aggregated.countDocuments({type:"cultivation_180d_rollup"}));
    '
}

# ---------------------------------------------------------------------------
# Helper: run one complete P3 benchmark at a given scale
# ---------------------------------------------------------------------------
run_p3() {
    local SENSORS=$1
    local SCALE_LABEL=$2

    echo ""
    echo "=========================================="
    echo "  P3 @ ${SCALE_LABEL} — STARTING $(date)"
    echo "=========================================="

    # --- Step 1: Clean start ---
    echo ""
    echo "[1/7] Starting fresh P3 stack..."
    docker compose -f docker-compose.p3.yml down -v 2>/dev/null
    docker compose -f docker-compose.p3.yml up --build -d
    echo "[1/7] Waiting 60s for healthchecks..."
    sleep 60
    docker compose -f docker-compose.p3.yml ps
    echo "[1/7] Stack is up."

    # --- Step 2: Seed ---
    echo ""
    echo "[2/7] Seeding ${SENSORS} sensors..."
    ./datagen/datagen seed --sensors "$SENSORS"
    if [ $? -ne 0 ]; then
        echo "!!! SEED FAILED for ${SCALE_LABEL} — skipping to next scale"
        docker compose -f docker-compose.p3.yml down
        return 1
    fi
    echo "[2/7] Seed complete. Cooling down 2 min..."
    sleep 120

    # --- Step 3: Restart worker and wait for batch rewrite ---
    echo ""
    echo "[3/7] Restarting worker for batch rewrite..."
    # Record time BEFORE restart — only look at logs after this moment.
    RESTART_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    sleep 3
    docker restart benchmark-worker-p3
    echo "[3/7] Worker restarted. Polling for rewrite completion..."

    wait_for_rewrite "benchmark-worker-p3" "$RESTART_TIME" "P3 ${SCALE_LABEL}"

    echo "[3/7] Verifying aggregated data..."
    verify_aggregated "benchmark-mongodb-p3"
    echo "[3/7] Done. Cooling down 2 min..."
    sleep 120

    # --- Step 4: Replay (fills Redis for Q1) ---
    echo ""
    echo "[4/7] Running replay (16 min, fills Redis for all ${SENSORS} sensors)..."
    ./datagen/datagen replay --sensors "$SENSORS" --duration 16m
    if [ $? -ne 0 ]; then
        echo "!!! REPLAY FAILED for ${SCALE_LABEL} — skipping to next scale"
        docker compose -f docker-compose.p3.yml down
        return 1
    fi

    echo "[4/7] Verifying Redis..."
    DBSIZE=$(docker exec benchmark-redis-p3 redis-cli DBSIZE 2>/dev/null | grep -o '[0-9]*')
    echo "  Redis keys: ${DBSIZE:-0} (expect ~${SENSORS})"
    echo "[4/7] Done."

    # --- Step 5: Ingest lag ---
    echo ""
    echo "[5/7] Running ingest lag analysis..."
    ./harness/harness ingest-lag --window 16m --output "results/ingest-lag-p3-${SCALE_LABEL}.csv"
    echo "[5/7] Done."

    # --- Step 6: Query benchmark ---
    echo ""
    echo "[6/7] Running query benchmark (100 iterations)..."
    ./harness/harness query --prototype p3 --iterations 100 --output "results/queries-p3-${SCALE_LABEL}.csv"
    echo "[6/7] Done."

    # --- Step 7: Verify result files ---
    echo ""
    echo "[7/7] Verifying result files..."
    ls -la "results/ingest-lag-p3-${SCALE_LABEL}.csv" "results/queries-p3-${SCALE_LABEL}.csv" 2>/dev/null
    if [ -f "results/queries-p3-${SCALE_LABEL}.csv" ]; then
        echo "[7/7] SUCCESS — result files exist."
    else
        echo "[7/7] WARNING — result files missing!"
    fi

    echo ""
    echo "=========================================="
    echo "  P3 @ ${SCALE_LABEL} — COMPLETE $(date)"
    echo "=========================================="

    # Clean up
    docker compose -f docker-compose.p3.yml down

    # Cooldown between scales
    echo "[cooldown] Resting 5 min before next scale..."
    sleep 300
}

# =============================================================================
# Run all three scales: 50K first (most important), then 10K, then 1K
# =============================================================================

run_p3 50000 "50k"
run_p3 10000 "10k"
run_p3 1000  "1k"

echo ""
echo "=========================================="
echo "  ALL P3 BENCHMARKS COMPLETE — $(date)"
echo "=========================================="
echo ""
echo "Result files:"
ls -la results/ingest-lag-p3-*.csv results/queries-p3-*.csv 2>/dev/null
