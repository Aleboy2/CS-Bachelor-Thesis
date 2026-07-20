#!/bin/bash
# =============================================================================
# Integration test for P1: Mosquitto → Ingest API → MongoDB
# =============================================================================
# LAYER: End-to-end smoke test, run manually from the host (not by CI, not
#   inside Docker) against an already-running P1 stack.
#
# Prerequisites: docker compose -f docker-compose.p1.yml up --build -d
# Wait for all services to be healthy before running this script.
#
# What it does:
#   1. Seeds one test device_registry document into MongoDB
#   2. Publishes one test MQTT message to sensors/telemetry
#   3. Waits briefly for the ingest API to process it
#   4. Queries telemetry_raw for the resulting document
#   5. Cleans up the test data
# =============================================================================

set -e

echo "=== P1 Integration Test ==="
echo ""

# --- Step 1: Seed a test device ---
echo "[1/5] Seeding test device_registry document..."
docker exec benchmark-mongodb-p1 mongosh --quiet iot_benchmark --eval '
  db.device_registry.insertOne({
    serial: "TEST-SENSOR-001",
    dci: "test-dci-001",
    gps: { latitude: 51.612621, longitude: 5.683513 },
    zone: { zoneId: "test-zone-1", name: "Test Zone" },
    cultivation: {
      cultivationId: "test-cult-1",
      name: "Test Cultivation",
      crop: { name: "Wheat", isInGreenHouse: false, sowingDate: "2025-03-01T00:00:00Z",
              bumpHeight: 0, distanceToSeedling: 25, sowingDepth: 0 },
      irrigationSystem: { irrigationProfile: "conservative", dripperFlowRate: 0,
                          minAmount: 2, maxAmount: 50 }
    },
    schemaVersion: "1.0"
  });
  print("  → device_registry: " + db.device_registry.countDocuments({ serial: "TEST-SENSOR-001" }) + " test doc(s)");
'

# --- Step 2: Publish a test MQTT message ---
echo ""
echo "[2/5] Publishing test MQTT message to sensors/telemetry..."
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")
docker exec benchmark-mosquitto-p1 mosquitto_pub \
  -t "sensors/telemetry" \
  -m "{
    \"timestamp\": \"${TIMESTAMP}\",
    \"sensor\": { \"serial\": \"TEST-SENSOR-001\", \"battery\": 85 },
    \"depth15\": { \"vwc\": 30.0, \"temperature\": 18.0, \"ec\": 0.40 },
    \"depth30\": { \"vwc\": 35.0, \"temperature\": 17.5, \"ec\": 0.45 },
    \"depth45\": { \"vwc\": 38.0, \"temperature\": 17.0, \"ec\": 0.42 },
    \"depth60\": { \"vwc\": 40.0, \"temperature\": 16.5, \"ec\": 0.44 }
  }"
echo "  → published (timestamp: ${TIMESTAMP})"

# --- Step 3: Wait for processing ---
echo ""
echo "[3/5] Waiting 3 seconds for ingest API to process..."
sleep 3

# --- Step 4: Verify the telemetry_raw document ---
echo ""
echo "[4/5] Checking telemetry_raw for the ingested document..."
RESULT=$(docker exec benchmark-mongodb-p1 mongosh --quiet iot_benchmark --eval '
  const doc = db.telemetry_raw.findOne({ "sensor.serial": "TEST-SENSOR-001" });
  if (!doc) {
    print("FAIL — no document found in telemetry_raw");
  } else {
    print("PASS — document found:");
    print("  timestamp:     " + doc.timestamp);
    print("  ingestedAt:    " + doc.ingestedAt);
    print("  sensor.serial: " + doc.sensor.serial);
    print("  sensor.battery:" + doc.sensor.battery);
    print("  zone.name:     " + doc.zone.name);
    print("  cultivation:   " + doc.cultivation.name);
    print("  depth15.vwc:   " + doc.depth15.vwc);
    print("  schemaVersion: " + doc.schemaVersion);
    const hasGps = doc.gps !== undefined;
    const hasDci = doc.dci !== undefined;
    print("  gps present:   " + hasGps + (hasGps ? " ← ERROR: should be absent!" : " (correct)"));
    print("  dci present:   " + hasDci + (hasDci ? " ← ERROR: should be absent!" : " (correct)"));
    if (doc.ingestedAt && doc.timestamp) {
      const lagMs = doc.ingestedAt - doc.timestamp;
      print("  ingest lag:    " + lagMs + "ms");
    }
  }
')
echo "$RESULT"

# --- Step 5: Cleanup ---
echo ""
echo "[5/5] Cleaning up test data..."
docker exec benchmark-mongodb-p1 mongosh --quiet iot_benchmark --eval '
  db.device_registry.deleteMany({ serial: "TEST-SENSOR-001" });
  db.telemetry_raw.deleteMany({ "sensor.serial": "TEST-SENSOR-001" });
  print("  → cleaned up");
'

echo ""
echo "=== Integration test complete ==="
