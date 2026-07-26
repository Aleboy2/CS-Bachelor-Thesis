#!/usr/bin/env bash
# =============================================================================
# run-benchmark-50k.sh
# Full benchmark pipeline for P1, P2, P3 at 50K sensor scale.
#
# Per prototype: teardown → bring up → seed → backfill (P2/P3) → query
#                benchmark → replay → ingest lag collection → teardown.
#
# No hardcoded sleep timers. Every step blocks until the previous one
# completes. Pipeline drain after replay is verified by polling MongoDB
# document count until it stabilises.
#
# Replay durations are FIXED per prototype (5m for P1/P2, 15m for P3),
# matching the convention already established at 1K scale, rather than
# computed from a target sample count.
#
# RESUMABLE: every expensive/completed step (seed, P2/P3 Mongo backfill,
# query benchmark, ingest-lag collection) is checkpointed under
# ./results/.checkpoints/. If this script crashes, is Ctrl+C'd, or the
# laptop sleeps mid-run, just run it again — already-completed steps are
# skipped (cross-verified against real MongoDB state, not just the
# checkpoint file), so a failure after seeding never forces a ~2h reseed.
# The one exception is P3's Redis backfill, which is intentionally never
# checkpointed (60-min TTL) and is always redone fresh immediately before
# querying, if the query step hasn't already succeeded.
#
# Estimated total time: ~8-10 hours on a first clean run (dominated by
# seeding ~864M docs per prototype at ~100K docs/sec ≈ 2h each). A resumed
# run only redoes whatever step it crashed on.
#
# Run from the repo root:  bash run-benchmark-50k.sh
# =============================================================================
set -euo pipefail

# =============================================================================
# CONFIGURATION
# =============================================================================
SENSORS=50000
TARGET_N=500            # target ingest-lag sample count (N=500 convention)
RESULTS_DIR="./results"

# Paths to the pre-compiled host binaries (run on host, not in Docker)
DATAGEN="./datagen/datagen"
HARNESS="./harness/harness"

# Compose files
P1_COMPOSE="docker-compose.p1.yml"
P2_COMPOSE="docker-compose.p2.yml"
P3_COMPOSE="docker-compose.p3.yml"

# Query benchmark anchor (must match datagen seed --benchmark-start)
ANCHOR="2026-06-01T00:00:00Z"

# Checkpoint markers live here. This is the actual fail-safe: every expensive
# or completed step is marked here, and re-running this script (after a crash,
# a Ctrl+C, a laptop sleep, anything) skips everything already done instead of
# starting over. Seeding is the expensive part (~2h/prototype at 50K) — a
# crash during query benchmarking or ingest-lag collection must NEVER force
# a reseed. Checkpoints are cross-verified against real MongoDB state (not
# trusted blindly), so a stale/incorrect checkpoint can't silently mask
# missing data either.
CHECKPOINT_DIR="${RESULTS_DIR}/.checkpoints"

# --- Fixed replay durations, matching the convention already used at 1K ---
# (inferred from ingestlagp*1k.csv sample counts: P1/P2 ≈5min, P3 ≈15min).
# Reused as-is rather than computed from a target sample count, so the 50K
# ingest-lag methodology stays consistent with what's already in the thesis
# for 1K, rather than introducing a third convention.
P1_REPLAY_DURATION="5m"
P2_REPLAY_DURATION="5m"
P3_REPLAY_DURATION="15m"

# Harness window = replay duration + 60s margin (captures all replayed docs
# even with a little end-of-replay drain lag).
P1_HARNESS_WINDOW="6m"
P2_HARNESS_WINDOW="6m"
P3_HARNESS_WINDOW="16m"

# =============================================================================
# SANITY CHECKS
# =============================================================================
for bin in "$DATAGEN" "$HARNESS"; do
    if [ ! -f "$bin" ]; then
        echo "ERROR: binary not found at $bin"
        echo "       Run this script from the repo root (CS-Bachelor-Thesis/)."
        exit 1
    fi
done

if [ ! -d "$RESULTS_DIR" ]; then
    echo "ERROR: results directory not found at $RESULTS_DIR"
    exit 1
fi

mkdir -p "$CHECKPOINT_DIR"

# Check that mongosh is available (needed for verification steps)
if ! command -v mongosh &>/dev/null; then
    echo "ERROR: mongosh not found. Install MongoDB Shell to continue."
    exit 1
fi

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

teardown_all() {
    echo ""
    echo "=== Tearing down ALL prototype stacks (with volumes) ==="
    docker compose -f "$P1_COMPOSE" down -v 2>/dev/null || true
    docker compose -f "$P2_COMPOSE" down -v 2>/dev/null || true
    docker compose -f "$P3_COMPOSE" down -v 2>/dev/null || true
    echo "[✓] All stacks torn down."
}

# Tear down every prototype stack EXCEPT the given one, destroying their
# volumes to free disk and release port 27017.
#
# CRITICAL: the target's own volume is deliberately left intact. A previous
# version of this script called teardown_all() here, which destroyed the
# target's volume too — meaning that resuming after a reboot / Docker restart
# / manual `down` wiped the seeded data the checkpoints were meant to protect
# and forced a full ~2h reseed. That completely defeated the fail-safe in the
# exact scenario it exists for. Leaving the target volume alone is safe:
# `datagen seed --drop` drops collections anyway on a genuine reseed, and
# is_seeded() cross-checks the real document count before deciding to skip.
teardown_others() {
    local keep="$1"
    echo ""
    echo "=== Tearing down other prototype stacks (freeing disk + port 27017) ==="
    for cf in "$P1_COMPOSE" "$P2_COMPOSE" "$P3_COMPOSE"; do
        if [ "$cf" != "$keep" ]; then
            docker compose -f "$cf" down -v 2>/dev/null || true
        fi
    done
    echo "[✓] Other stacks torn down (${keep} volume preserved)."
}

# --- Checkpoint helpers -----------------------------------------------------
checkpoint_exists() { [ -f "${CHECKPOINT_DIR}/$1" ]; }
mark_checkpoint()   { touch "${CHECKPOINT_DIR}/$1"; echo "[✓] checkpoint: $1"; }
clear_checkpoint()  { rm -f "${CHECKPOINT_DIR}/$1"; }

# Wipe all downstream checkpoints for a prototype. Called whenever a genuine
# reseed happens, since backfill/query/lag results from before a reseed are
# no longer valid and must never be skipped based on a stale checkpoint.
invalidate_downstream() {
    local proto="$1"
    echo "[…] Invalidating downstream checkpoints for $proto (reseed occurred)…"
    clear_checkpoint "${proto}-backfilled-${SENSORS}"
    clear_checkpoint "${proto}-query-done"
    clear_checkpoint "${proto}-lag-done"
}

# Bring up a prototype's stack, but ONLY tear down / rebuild if it isn't
# already the one currently running. This is what makes resuming after a
# crash cheap: if the script died during, say, the query benchmark, the
# stack (and its seeded data) is very likely still up and healthy — no need
# to tear anything down or wait through another Kafka/Mongo boot cycle.
ensure_stack_up() {
    local compose="$1"
    local label="$2"
    shift 2
    # Remaining "$@" is an optional explicit service subset (used by P3 to
    # exclude the batch worker). Kept as positional params rather than a
    # local array: expanding an empty array under `set -u` is an error on
    # bash 3.2, which is what macOS ships as /bin/bash.

    if docker compose -f "$compose" ps mongodb 2>/dev/null | grep -qi "healthy\|running"; then
        echo "[✓] $label stack already up — skipping teardown/rebuild."
        return 0
    fi

    teardown_others "$compose"
    echo ""
    echo "=== Bringing up $label stack ==="
    if [ $# -eq 0 ]; then
        docker compose -f "$compose" up --build -d
    else
        docker compose -f "$compose" up --build -d "$@"
    fi
    wait_for_healthy "$compose" "$label"
}

# True if this prototype's data is already correctly seeded at the target
# scale. Never trusts the checkpoint file alone — always cross-verifies the
# actual device_registry count against what's live in MongoDB right now, in
# case a volume was deleted or replaced outside this script's control.
is_seeded() {
    local proto="$1"
    local marker="${proto}-seeded-${SENSORS}"

    local count
    count=$(mongosh --quiet --eval 'db.device_registry.countDocuments({})' \
        mongodb://localhost:27017/iot_benchmark 2>/dev/null || echo "0")

    if [ "$count" = "$SENSORS" ]; then
        if ! checkpoint_exists "$marker"; then
            mark_checkpoint "$marker"   # data is fine, just missing its marker — restore it
        fi
        return 0
    fi

    if checkpoint_exists "$marker"; then
        echo "[!] Checkpoint '$marker' exists but device_registry has $count docs (expected $SENSORS) — treating as not seeded."
        clear_checkpoint "$marker"
    fi
    return 1
}

# Wait for all services in a compose file to report healthy/running.
# No hardcoded timer — polls every 5s until all services pass, or bails
# after 180s (enough for Kafka's slow startup at first boot).
wait_for_healthy() {
    local compose_file="$1"
    local prototype="$2"
    local max_wait=180
    local elapsed=0

    echo "[…] Waiting for $prototype services to become healthy…"
    while [ $elapsed -lt $max_wait ]; do
        # Count services that are NOT yet healthy/running.
        #
        # NOTE on the parsing logic below: `docker compose ps --format json`
        # always includes a "Health" key, even for services with no defined
        # healthcheck (as an empty string, not an absent key). A naive
        # `svc.get('Health', svc.get('Status', ''))` NEVER falls back to
        # Status in that case, since dict.get only uses its default when the
        # key is missing — not when it's present-but-empty. That would count
        # every healthcheck-less service (e.g. ingest-api, worker) as
        # permanently unhealthy and spin this loop for the full timeout on
        # every single stack bring-up. So: services WITH a healthcheck must
        # show Health containing "healthy"; services WITHOUT one just need
        # State == "running" (there's no better signal available for them).
        local unhealthy
        unhealthy=$(docker compose -f "$compose_file" ps --format json 2>/dev/null | \
            python3 -c "
import sys, json
unhealthy = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    svc = json.loads(line)
    health = (svc.get('Health') or '').lower()
    state = (svc.get('State') or '').lower()
    if health:
        ok = (health == 'healthy')
    else:
        ok = state == 'running'
    if not ok:
        unhealthy += 1
print(unhealthy)
" 2>/dev/null || echo "1")

        if [ "$unhealthy" = "0" ]; then
            echo "[✓] All $prototype services healthy."
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    echo "ERROR: $prototype services did not become healthy within ${max_wait}s."
    docker compose -f "$compose_file" ps
    exit 1
}

# Verify that seed produced the expected number of device_registry docs.
verify_seed() {
    local expected="$1"
    echo "[…] Verifying seed data…"

    local device_count
    device_count=$(mongosh --quiet --eval 'db.device_registry.countDocuments({})' \
        mongodb://localhost:27017/iot_benchmark 2>/dev/null || echo "0")
    local telemetry_count
    telemetry_count=$(mongosh --quiet --eval 'db.telemetry_raw.countDocuments({})' \
        mongodb://localhost:27017/iot_benchmark 2>/dev/null || echo "0")

    echo "    device_registry: $device_count (expected $expected)"
    echo "    telemetry_raw:   $telemetry_count"

    if [ "$device_count" != "$expected" ]; then
        echo "ERROR: device_registry count mismatch! Expected $expected, got $device_count"
        exit 1
    fi
    echo "[✓] Seed verified."
}

# Verify that telemetry_aggregated has been populated by a backfill.
verify_backfill() {
    local label="$1"
    echo "[…] Verifying $label backfill…"

    local agg_count
    agg_count=$(mongosh --quiet --eval 'db.telemetry_aggregated.countDocuments({})' \
        mongodb://localhost:27017/iot_benchmark 2>/dev/null || echo "0")

    echo "    telemetry_aggregated: $agg_count docs"

    if [ "$agg_count" = "0" ]; then
        echo "ERROR: telemetry_aggregated is empty after $label backfill!"
        exit 1
    fi
    echo "[✓] $label backfill verified."
}

# Poll telemetry_raw count until it stops increasing (pipeline fully drained).
# No hardcoded sleep — checks every 3s, requires 3 consecutive stable readings.
wait_for_pipeline_drain() {
    echo "[…] Waiting for ingest pipeline to drain (polling document count)…"
    local prev_count=-1
    local stable_checks=0
    local required_stable=3    # 3 consecutive unchanged readings = drained

    while true; do
        local current_count
        current_count=$(mongosh --quiet --eval 'db.telemetry_raw.countDocuments({})' \
            mongodb://localhost:27017/iot_benchmark 2>/dev/null || echo "-1")

        if [ "$current_count" = "$prev_count" ]; then
            stable_checks=$((stable_checks + 1))
            if [ $stable_checks -ge $required_stable ]; then
                echo "[✓] Pipeline drained (count stable at $current_count for $((required_stable * 3))s)."
                return 0
            fi
        else
            stable_checks=0
            prev_count="$current_count"
        fi
        sleep 3
    done
}

# Verify an output CSV file exists and has the expected number of data rows.
#
# mode="strict": row_count must EXACTLY equal expected_rows, or the script
#   aborts immediately. Use this for query benchmark CSVs — each of Q1-Q5
#   must produce exactly `iterations` rows; anything less means some
#   iterations silently failed (mongo.ErrNoDocuments, empty cursor, etc.) —
#   exactly the bug that invalidated the original 1K/10K/50K query data and
#   took an entire debugging session to find. This must never pass silently
#   again, especially unattended.
#
# mode="min": row_count must be >= expected_rows, else a warning only (not
#   fatal). Use this for ingest-lag CSVs, where sample count legitimately
#   varies with actual throughput and isn't a fixed target.
verify_output() {
    local filepath="$1"
    local expected_rows="$2"
    local label="$3"
    local mode="${4:-min}"

    if [ ! -f "$filepath" ]; then
        echo "ERROR: $label output file not created at $filepath"
        exit 1
    fi

    local row_count
    row_count=$(( $(wc -l < "$filepath" | tr -d ' ') - 1 ))  # subtract header

    echo "[✓] $label: $row_count data rows in $filepath"

    if [ "$mode" = "strict" ]; then
        if [ "$row_count" -ne "$expected_rows" ]; then
            echo "ERROR: $label has $row_count rows, expected exactly $expected_rows."
            echo "       This means some query iterations failed silently (empty result,"
            echo "       missing backfill coverage, etc.) — the exact class of bug that"
            echo "       invalidated the original query data. Aborting rather than"
            echo "       continuing with incomplete data."
            exit 1
        fi
    elif [ "$row_count" -lt "$expected_rows" ]; then
        echo "WARNING: Only $row_count rows (expected ≥$expected_rows). Check for pipeline issues."
    fi
}

# =============================================================================
# PROTOTYPE RUNNERS
# =============================================================================

run_p1() {
    local compose="$P1_COMPOSE"
    local proto="p1"
    local query_out="${RESULTS_DIR}/queries-p1-50k.csv"
    local lag_out="${RESULTS_DIR}/ingest-lag-p1-50k.csv"

    echo ""
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║  FULL BENCHMARK: P1 (Single-Store Baseline) @ 50K sensors       ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    echo ""

    if checkpoint_exists "p1-query-done" && checkpoint_exists "p1-lag-done"; then
        echo "[✓] P1 already fully complete (checkpoints found) — skipping entirely."
        return 0
    fi

    # 1-2. Bring up P1 stack (skips teardown/rebuild if already up)
    ensure_stack_up "$compose" "P1"

    # 3. Seed (skipped if already seeded at this scale)
    if is_seeded "$proto"; then
        echo "[✓] P1 already seeded at $SENSORS sensors — skipping (~2h saved)."
    else
        echo ""
        echo "=== Seeding $SENSORS sensors (this will take ~2 hours) ==="
        $DATAGEN seed --sensors "$SENSORS" --drop
        verify_seed "$SENSORS"
        mark_checkpoint "p1-seeded-${SENSORS}"
        invalidate_downstream "$proto"
    fi

    # 4. Query benchmark (no backfill needed — P1 queries telemetry_raw live)
    if checkpoint_exists "p1-query-done"; then
        echo "[✓] P1 query benchmark already done — skipping."
    else
        echo ""
        echo "=== Running query benchmark (P1) ==="
        $HARNESS query --prototype "$proto" --anchor "$ANCHOR" --output "$query_out"
        verify_output "$query_out" 500 "P1 queries" strict
        mark_checkpoint "p1-query-done"
    fi

    # 5-7. Replay + drain + ingest lag
    if checkpoint_exists "p1-lag-done"; then
        echo "[✓] P1 ingest lag already done — skipping."
    else
        echo ""
        echo "=== Running replay (${P1_REPLAY_DURATION}) ==="
        $DATAGEN replay --sensors "$SENSORS" --duration "$P1_REPLAY_DURATION"

        wait_for_pipeline_drain

        echo ""
        echo "=== Collecting ingest lag (window=${P1_HARNESS_WINDOW}) ==="
        $HARNESS ingest-lag --window "$P1_HARNESS_WINDOW" --output "$lag_out"
        verify_output "$lag_out" "$TARGET_N" "P1 ingest lag"
        mark_checkpoint "p1-lag-done"
    fi

    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
    echo "  P1 COMPLETE"
    echo "═══════════════════════════════════════════════════════════════════"
}

run_p2() {
    local compose="$P2_COMPOSE"
    local proto="p2"
    local query_out="${RESULTS_DIR}/queries-p2-50k.csv"
    local lag_out="${RESULTS_DIR}/ingest-lag-p2-50k.csv"

    echo ""
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║  FULL BENCHMARK: P2 (CQRS) @ 50K sensors                       ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    echo ""

    if checkpoint_exists "p2-query-done" && checkpoint_exists "p2-lag-done"; then
        echo "[✓] P2 already fully complete (checkpoints found) — skipping entirely."
        return 0
    fi

    # 1-2. Bring up P2 stack (skips teardown/rebuild if already up)
    ensure_stack_up "$compose" "P2"

    # 3. Seed (bypasses Kafka — goes directly into MongoDB; skipped if already seeded)
    if is_seeded "$proto"; then
        echo "[✓] P2 already seeded at $SENSORS sensors — skipping (~2h saved)."
    else
        echo ""
        echo "=== Seeding $SENSORS sensors (this will take ~2 hours) ==="
        $DATAGEN seed --sensors "$SENSORS" --drop
        verify_seed "$SENSORS"
        mark_checkpoint "p2-seeded-${SENSORS}"
        invalidate_downstream "$proto"
    fi

    # 4. Backfill telemetry_aggregated from seeded history (skipped if already done)
    #    The continuous P2 worker only sees live Kafka events, not seeded data.
    #    p2-backfill is a one-shot that runs the same aggregation pipelines over
    #    the full telemetry_raw contents and builds latest_by_sensor.
    if checkpoint_exists "p2-backfilled-${SENSORS}"; then
        echo "[✓] P2 backfill already done — skipping."
    else
        echo ""
        echo "=== Running P2 backfill (telemetry_aggregated from seed history) ==="
        docker compose -f "$compose" run --rm worker \
            --mode p2-backfill \
            --mongo-uri "mongodb://mongodb:27017/iot_benchmark"
        verify_backfill "P2"
        mark_checkpoint "p2-backfilled-${SENSORS}"
    fi

    # 5. Query benchmark
    if checkpoint_exists "p2-query-done"; then
        echo "[✓] P2 query benchmark already done — skipping."
    else
        echo ""
        echo "=== Running query benchmark (P2) ==="
        $HARNESS query --prototype "$proto" --anchor "$ANCHOR" --output "$query_out"
        verify_output "$query_out" 500 "P2 queries" strict
        mark_checkpoint "p2-query-done"
    fi

    # 6-8. Replay + drain + ingest lag
    if checkpoint_exists "p2-lag-done"; then
        echo "[✓] P2 ingest lag already done — skipping."
    else
        echo ""
        echo "=== Running replay (${P2_REPLAY_DURATION}) ==="
        $DATAGEN replay --sensors "$SENSORS" --duration "$P2_REPLAY_DURATION"

        wait_for_pipeline_drain

        echo ""
        echo "=== Collecting ingest lag (window=${P2_HARNESS_WINDOW}) ==="
        $HARNESS ingest-lag --window "$P2_HARNESS_WINDOW" --output "$lag_out"
        verify_output "$lag_out" "$TARGET_N" "P2 ingest lag"
        mark_checkpoint "p2-lag-done"
    fi

    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
    echo "  P2 COMPLETE"
    echo "═══════════════════════════════════════════════════════════════════"
}

run_p3() {
    local compose="$P3_COMPOSE"
    local proto="p3"
    local query_out="${RESULTS_DIR}/queries-p3-50k.csv"
    local lag_out="${RESULTS_DIR}/ingest-lag-p3-50k.csv"

    echo ""
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║  FULL BENCHMARK: P3 (Lambda Architecture) @ 50K sensors         ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    echo ""

    if checkpoint_exists "p3-query-done" && checkpoint_exists "p3-lag-done"; then
        echo "[✓] P3 already fully complete (checkpoints found) — skipping entirely."
        return 0
    fi

    # 1-2. Bring up P3 stack WITHOUT the batch worker (skips teardown/rebuild
    #    if already up). The worker does a full rewrite on startup — if it
    #    starts before seed finishes, it rewrites against empty/partial data.
    #    We skip it entirely and use the one-shot p2-backfill mode instead.
    ensure_stack_up "$compose" "P3" mongodb mosquitto kafka redis ingest-api

    # 3. Seed (skipped if already seeded at this scale)
    if is_seeded "$proto"; then
        echo "[✓] P3 already seeded at $SENSORS sensors — skipping (~2h saved)."
    else
        echo ""
        echo "=== Seeding $SENSORS sensors (this will take ~2 hours) ==="
        $DATAGEN seed --sensors "$SENSORS" --drop
        verify_seed "$SENSORS"
        mark_checkpoint "p3-seeded-${SENSORS}"
        invalidate_downstream "$proto"
    fi

    # 4. Backfill MongoDB aggregated data (zone_daily, cultivation_daily,
    #    cultivation_180d_rollup, latest_by_sensor) using the same one-shot
    #    p2-backfill worker mode. No TTL on this data, so it's safely
    #    skippable once done. latest_by_sensor is harmless extra data in P3
    #    (Q1 reads Redis exclusively, Q3-Q5 filter by their own doc types).
    if checkpoint_exists "p3-backfilled-${SENSORS}"; then
        echo "[✓] P3 MongoDB backfill already done — skipping."
    else
        echo ""
        echo "=== Running P3 MongoDB backfill (telemetry_aggregated) ==="
        docker compose -f "$compose" run --rm worker \
            --mode p2-backfill \
            --mongo-uri "mongodb://mongodb:27017/iot_benchmark"
        verify_backfill "P3 (MongoDB)"
        mark_checkpoint "p3-backfilled-${SENSORS}"
    fi

    # 5-6. Redis backfill + query benchmark. Deliberately NOT checkpointed —
    #    Redis keys carry a 60-min TTL, so if the query step hasn't already
    #    succeeded, any earlier Redis backfill may well have expired by now.
    #    Always redo it fresh, immediately before querying.
    if checkpoint_exists "p3-query-done"; then
        echo "[✓] P3 query benchmark already done — skipping (and skipping Redis backfill, not needed)."
    else
        echo ""
        echo "=== Running P3 Redis backfill (latest keys, 60-min TTL starts NOW) ==="
        docker compose -f "$compose" run --rm worker \
            --mode p3-redis-backfill \
            --mongo-uri "mongodb://mongodb:27017/iot_benchmark" \
            --redis-addr "redis:6379"

        echo ""
        echo "=== Running query benchmark (P3) — must finish within 60 min of Redis backfill ==="
        $HARNESS query --prototype "$proto" --anchor "$ANCHOR" \
            --redis-url "redis://localhost:6379" --output "$query_out"
        verify_output "$query_out" 500 "P3 queries" strict
        mark_checkpoint "p3-query-done"
    fi

    # 7-9. Replay + drain + ingest lag
    if checkpoint_exists "p3-lag-done"; then
        echo "[✓] P3 ingest lag already done — skipping."
    else
        echo ""
        echo "=== Running replay (${P3_REPLAY_DURATION}) ==="
        $DATAGEN replay --sensors "$SENSORS" --duration "$P3_REPLAY_DURATION"

        wait_for_pipeline_drain

        echo ""
        echo "=== Collecting ingest lag (window=${P3_HARNESS_WINDOW}) ==="
        $HARNESS ingest-lag --window "$P3_HARNESS_WINDOW" --output "$lag_out"
        verify_output "$lag_out" "$TARGET_N" "P3 ingest lag"
        mark_checkpoint "p3-lag-done"
    fi

    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
    echo "  P3 COMPLETE"
    echo "═══════════════════════════════════════════════════════════════════"
}

# =============================================================================
# MAIN
# =============================================================================

echo ""
echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║  FULL BENCHMARK — 50K Sensors — P1 → P2 → P3                   ║"
echo "╠═══════════════════════════════════════════════════════════════════╣"
echo "║  Sensors:          $SENSORS                                    ║"
echo "║  Replay durations: P1/P2=${P1_REPLAY_DURATION}, P3=${P3_REPLAY_DURATION} (matches 1K convention)   ║"
echo "║  Harness windows:  P1/P2=${P1_HARNESS_WINDOW}, P3=${P3_HARNESS_WINDOW}                              ║"
echo "║  Query anchor:     ${ANCHOR}                      ║"
echo "║  Estimated time:   8-10 hours on a clean run (mostly seeding)   ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""
echo "Resumable: if this run is interrupted, just run this script again."
echo ""
echo "Output files (will overwrite existing):"
echo "  ${RESULTS_DIR}/queries-p1-50k.csv"
echo "  ${RESULTS_DIR}/queries-p2-50k.csv"
echo "  ${RESULTS_DIR}/queries-p3-50k.csv"
echo "  ${RESULTS_DIR}/ingest-lag-p1-50k.csv"
echo "  ${RESULTS_DIR}/ingest-lag-p2-50k.csv"
echo "  ${RESULTS_DIR}/ingest-lag-p3-50k.csv"
echo ""
# Only prompt when stdin is an interactive terminal. Under `nohup`, a
# background job, or any redirected stdin, `read` fails immediately and
# `set -e` would kill the script before it did any work at all — precisely
# the unattended long-run case this script exists for. Set BENCH_YES=1 to
# skip the prompt even in an interactive shell.
if [ -t 0 ] && [ "${BENCH_YES:-}" != "1" ]; then
    read -r -p "Press Enter to start, or Ctrl+C to abort… "
else
    echo "(non-interactive stdin or BENCH_YES=1 — starting automatically)"
fi

SCRIPT_START=$(date +%s)

# --- P1 -----------------------------------------------------------------------
P1_START=$(date +%s)
run_p1
P1_END=$(date +%s)
echo "  P1 wall time: $(( (P1_END - P1_START) / 60 )) minutes"

# --- P2 -----------------------------------------------------------------------
P2_START=$(date +%s)
run_p2
P2_END=$(date +%s)
echo "  P2 wall time: $(( (P2_END - P2_START) / 60 )) minutes"

# --- P3 -----------------------------------------------------------------------
P3_START=$(date +%s)
run_p3
P3_END=$(date +%s)
echo "  P3 wall time: $(( (P3_END - P3_START) / 60 )) minutes"

# --- Final teardown and summary ------------------------------------------------
teardown_all

SCRIPT_END=$(date +%s)
TOTAL_MIN=$(( (SCRIPT_END - SCRIPT_START) / 60 ))

echo ""
echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║  ALL DONE                                                        ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""
echo "Total wall-clock time: ${TOTAL_MIN} minutes"
echo ""
echo "Results:"
for f in \
    "${RESULTS_DIR}/queries-p1-50k.csv" \
    "${RESULTS_DIR}/queries-p2-50k.csv" \
    "${RESULTS_DIR}/queries-p3-50k.csv" \
    "${RESULTS_DIR}/ingest-lag-p1-50k.csv" \
    "${RESULTS_DIR}/ingest-lag-p2-50k.csv" \
    "${RESULTS_DIR}/ingest-lag-p3-50k.csv"; do
    if [ -f "$f" ]; then
        rows=$(( $(wc -l < "$f" | tr -d ' ') - 1 ))
        echo "  ✓ $f  ($rows rows)"
    else
        echo "  ✗ $f  (MISSING)"
    fi
done
echo ""
echo "Next steps:"
echo "  1. Verify all query files have exactly 500 rows (5 × 100)"
echo "  2. Verify all ingest-lag files have ≥${TARGET_N} rows"
echo "  3. Upload all 6 CSVs to Claude for cross-prototype validation"
echo ""
