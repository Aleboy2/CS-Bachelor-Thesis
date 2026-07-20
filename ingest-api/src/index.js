'use strict';

const mqtt = require('mqtt');
const { MongoGateway }              = require('./mongo');
const { KafkaGateway }              = require('./kafka');
const { RedisGateway }              = require('./redis');
const { buildTelemetryRawDocument } = require('./buildDocument');

// =============================================================================
// Configuration — all from environment variables (see CLAUDE.md §2)
// =============================================================================

const PROTOTYPE        = (process.env.PROTOTYPE || '').toLowerCase();
const MONGO_URI        = process.env.MONGO_URI        || 'mongodb://localhost:27017/iot_benchmark';
const MQTT_BROKER_URL  = process.env.MQTT_BROKER_URL  || 'mqtt://localhost:1883';
const KAFKA_BROKER_URL = process.env.KAFKA_BROKER_URL;
const REDIS_URL        = process.env.REDIS_URL;

const MQTT_TOPIC = 'sensors/telemetry';  // locked — see CLAUDE.md §2

// --- Validation ---

if (!['p1', 'p2', 'p3'].includes(PROTOTYPE)) {
  console.error(`[fatal] PROTOTYPE must be p1, p2, or p3 (got: "${PROTOTYPE}")`);
  process.exit(1);
}

const useKafka = PROTOTYPE === 'p2' || PROTOTYPE === 'p3';
const useRedis = PROTOTYPE === 'p3';

if (useKafka && !KAFKA_BROKER_URL) {
  console.error('[fatal] KAFKA_BROKER_URL is required when PROTOTYPE is p2 or p3');
  process.exit(1);
}

if (useRedis && !REDIS_URL) {
  console.error('[fatal] REDIS_URL is required when PROTOTYPE is p3');
  process.exit(1);
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  console.log(`[init] prototype=${PROTOTYPE}, kafka=${useKafka}, redis=${useRedis}`);

  // --- MongoDB ---
  const mongo = new MongoGateway(MONGO_URI);
  await mongo.connect();

  // --- Kafka (P2/P3 only) ---
  let kafka = null;
  if (useKafka) {
    kafka = new KafkaGateway(KAFKA_BROKER_URL);
    await kafka.connect();
  }

  // --- Redis (P3 only — speed layer for Q1) ---
  let redis = null;
  if (useRedis) {
    redis = new RedisGateway(REDIS_URL);
    await redis.connect();
  }

  // --- MQTT ---
  const mqttClient = mqtt.connect(MQTT_BROKER_URL);

  mqttClient.on('connect', () => {
    console.log(`[mqtt] connected to ${MQTT_BROKER_URL}`);
    mqttClient.subscribe(MQTT_TOPIC, (err) => {
      if (err) {
        console.error('[mqtt] subscribe error:', err.message);
        process.exit(1);
      }
      console.log(`[mqtt] subscribed to "${MQTT_TOPIC}"`);
    });
  });

  mqttClient.on('error', (err) => {
    console.error('[mqtt] connection error:', err.message);
  });

  // --- Stats (periodic log every 30s) ---
  const stats = {
    received: 0,
    inserted: 0,
    registryMisses: 0,
    parseErrors: 0,
    insertErrors: 0,
  };

  function logStats() {
    console.log(
      `[stats] received=${stats.received} inserted=${stats.inserted} ` +
      `registryMisses=${stats.registryMisses} parseErrors=${stats.parseErrors} ` +
      `insertErrors=${stats.insertErrors}`
    );
  }

  const statsInterval = setInterval(logStats, 30_000);

  // --- Message handler (ingest path — see CLAUDE.md §3) ---
  mqttClient.on('message', async (_topic, message) => {
    stats.received++;

    // Step 2: parse the MQTT payload
    let payload;
    try {
      payload = JSON.parse(message.toString());
    } catch (err) {
      stats.parseErrors++;
      console.error('[ingest] JSON parse error:', err.message);
      return;
    }

    const serial = payload?.sensor?.serial;
    if (!serial) {
      stats.parseErrors++;
      console.error('[ingest] payload missing sensor.serial');
      return;
    }

    try {
      // Step 3: device_registry lookup
      const registryDoc = await mongo.findDeviceBySerial(serial);
      if (!registryDoc) {
        // Log and drop — never crash (locked decision, CLAUDE.md §2).
        // If this counter is non-zero during a real benchmark run, that's a
        // generator/seeding bug, not expected behavior.
        stats.registryMisses++;
        console.warn(`[ingest] registry miss (serial=${serial}) — dropped`);
        return;
      }

      // Step 4: document construction
      const ingestedAt = new Date();
      const doc = buildTelemetryRawDocument(payload, registryDoc, ingestedAt);

      // Step 5: write to MongoDB (this insert is the RQ1 measurement point)
      await mongo.insertTelemetryRaw(doc);

      // Step 5b (P2/P3 only): publish to Kafka
      // Order matters: Mongo write comes FIRST. If Kafka publish fails,
      // the document is still in telemetry_raw (raw store is the source of
      // truth; the worker can replay from it if needed). The reverse
      // (Kafka succeeds, Mongo fails) is the dangerous case.
      if (kafka) {
        await kafka.publishTelemetryEvent(doc);
      }

      // Step 5c (P3 only): write to Redis speed layer
      // Per Decision P0.2, Q1 in P3 reads Redis exclusively.
      // SET latest:{serial} with 60-minute TTL.
      if (redis) {
        await redis.setLatestReading(doc);
      }

      stats.inserted++;
    } catch (err) {
      stats.insertErrors++;
      console.error(`[ingest] write error (serial=${serial}):`, err.message);
    }
  });

  // --- Graceful shutdown ---
  async function shutdown(signal) {
    console.log(`\n[shutdown] received ${signal}`);
    clearInterval(statsInterval);
    logStats();
    mqttClient.end();
    if (kafka) await kafka.close();
    if (redis) await redis.close();
    await mongo.close();
    console.log('[shutdown] clean exit');
    process.exit(0);
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

main().catch((err) => {
  console.error('[fatal] startup error:', err);
  process.exit(1);
});
