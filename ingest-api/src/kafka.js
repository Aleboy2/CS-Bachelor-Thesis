'use strict';

const { Kafka } = require('kafkajs');

/**
 * FILE:  kafka.js
 * LAYER: Ingest API — Kafka producer gateway (P2/P3 only).
 *
 * RECEIVES FROM: index.js, which calls publishTelemetryEvent() with the same
 *   document just inserted into telemetry_raw.
 * SENDS TO:      Kafka topic "telemetry-raw-events", consumed downstream by
 *   P2's continuous worker (upserts telemetry_aggregated, incl.
 *   latest_by_sensor) and P3's hourly batch worker.
 *
 * Thin wrapper around the Kafka producer. Only instantiated and connected
 * when PROTOTYPE is p2 or p3 — P1 never touches this module's connect/send
 * methods (see index.js for the conditional wiring).
 *
 * Topic name: 'telemetry-raw-events' — published immediately after a
 * successful telemetry_raw insert, carrying the same document that was just
 * written. This is what P2's continuous worker and P3's batch worker consume
 * from. Partitioned by sensor.serial (locked — CLAUDE.md §2, P0.6).
 */
class KafkaGateway {
  /**
   * @param {string} brokerUrl - e.g. kafka:9092
   */
  constructor(brokerUrl) {
    this._kafka = new Kafka({
      clientId: 'ingest-api',
      brokers: [brokerUrl],
    });
    this._producer = this._kafka.producer();
  }

  async connect() {
    await this._producer.connect();
    console.log('[kafka] producer connected');
  }

  async close() {
    await this._producer.disconnect();
  }

  /**
   * Publishes a telemetry event to Kafka after a successful MongoDB write.
   *
   * Called immediately after a successful telemetry_raw insert — document
   * shape is identical to what was just written to MongoDB.
   *
   * @param {object} doc - the complete telemetry_raw document just inserted.
   */
  async publishTelemetryEvent(doc) {
    // Topic: 'telemetry-raw-events', key: sensor.serial (locked — CLAUDE.md §2, P0.6).
    // Order matters: if Kafka publish fails, the document is still in
    // telemetry_raw — that's intentional (raw store is the source of truth;
    // the worker can replay from it if needed). The reverse (Kafka succeeds,
    // Mongo fails) is the dangerous case and is why Mongo write comes first.
    await this._producer.send({
      topic: 'telemetry-raw-events',
      messages: [
        {
          key:   doc.sensor.serial,          // partitioning key (locked)
          value: JSON.stringify(doc),
        },
      ],
    });
  }
}

module.exports = { KafkaGateway };
