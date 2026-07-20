'use strict';

const Redis = require('ioredis');

/**
 * Thin wrapper around the Redis client. Only instantiated and connected
 * when PROTOTYPE is p3 — P1 and P2 never touch this module.
 *
 * P3 speed layer (LOCKED — CLAUDE.md §2, P0.2):
 *   Redis holds per-sensor latest readings for the most recent 60-minute
 *   window. Q1 in P3 reads Redis exclusively — never falls back to
 *   telemetry_aggregated. A sensor silent for more than 60 minutes will
 *   have no entry in Redis (expected behavior, not a bug).
 *
 * Key format:  latest:{serial}    e.g. latest:0000001
 * Value:       JSON string matching the latest_by_sensor schema (P0.4),
 *              self-contained with zone + cultivation context.
 * TTL:         3600 seconds (60 minutes).
 */
class RedisGateway {
  /**
   * @param {string} redisUrl - e.g. redis://redis:6379
   */
  constructor(redisUrl) {
    this._client = new Redis(redisUrl, {
      // Reconnect automatically if Redis restarts.
      retryStrategy: (times) => Math.min(times * 200, 5000),
      maxRetriesPerRequest: 3,
    });
  }

  async connect() {
    // ioredis connects lazily on first command; ping to verify.
    await this._client.ping();
    console.log('[redis] connected');
  }

  /**
   * Write the latest reading for a sensor. Called on every ingest event
   * when PROTOTYPE=p3, after the MongoDB write and Kafka publish.
   *
   * The value mirrors the latest_by_sensor schema (P0.4) — same shape as
   * telemetry_raw minus sensor.battery. Includes zone and cultivation
   * context so Q1 is a single-read operation (no join to device_registry).
   *
   * @param {object} doc - the telemetry_raw document just inserted
   */
  async setLatestReading(doc) {
    const key = `latest:${doc.sensor.serial}`;
    const value = JSON.stringify({
      serial:    doc.sensor.serial,
      timestamp: doc.timestamp,
      zone: {
        zoneId: doc.zone.zoneId,
        name:   doc.zone.name,
      },
      cultivation: {
        cultivationId: doc.cultivation.cultivationId,
        name:          doc.cultivation.name,
        crop:          doc.cultivation.crop,
        irrigationSystem: doc.cultivation.irrigationSystem,
      },
      depth15: doc.depth15,
      depth30: doc.depth30,
      depth45: doc.depth45,
      depth60: doc.depth60,
    });

    // SET with EX (TTL in seconds). 3600 = 60 minutes.
    await this._client.set(key, value, 'EX', 3600);
  }

  async close() {
    await this._client.quit();
  }
}

module.exports = { RedisGateway };
