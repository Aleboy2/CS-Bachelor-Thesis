'use strict';

/**
 * FILE:  buildDocument.js
 * LAYER: Ingest API — pure transformation, no I/O.
 *
 * CALLED BY: index.js, once per incoming MQTT message.
 * INPUT:     the lean MQTT payload (published by a sensor or the Go
 *            generator) + the matching device_registry document (already
 *            looked up by index.js via mongo.js).
 * OUTPUT:    a plain object shaped for telemetry_raw, handed back to
 *            index.js — which writes it to MongoDB (mongo.js) and, on
 *            P2/P3 only, publishes it to Kafka (kafka.js).
 *
 * Deliberately has no Mongo/MQTT dependencies so it can be unit-tested with
 * plain objects in, plain object out — no infrastructure required.
 *
 * Schema reference: benchmark_schema_design_v41.pdf §6.4 (Document structure)
 *
 * IMPORTANT: the MQTT payload MUST include a `timestamp` field (sensor's
 * event time, ISO 8601 string). Without it, ingest lag (ingestedAt - timestamp,
 * RQ1) is unmeasurable.
 *
 * Fields deliberately EXCLUDED from the output document:
 *   - gps (latitude/longitude) — sensors are assumed stationary for the
 *     growing season; embedding GPS in every reading would be pure redundancy
 *     at ~144M-document scale. Available via device_registry if ever needed.
 *   - dci — "not used in any benchmark query" per schema doc Decision 6.
 *     Stored in device_registry only.
 *
 * @param {object} mqttPayload  - the lean payload sent by the sensor.
 *   Must contain: timestamp (string), sensor.serial, sensor.battery,
 *   depth15, depth30, depth45, depth60.
 * @param {object} registryDoc  - the device_registry document for this sensor.
 *   Must contain: zone, cultivation, schemaVersion.
 * @param {Date}   ingestedAt   - processing time, stamped by the ingest API.
 * @returns {object} a plain object ready for MongoDB insertOne().
 */
function buildTelemetryRawDocument(mqttPayload, registryDoc, ingestedAt) {
  if (!mqttPayload.timestamp) {
    throw new Error('timestamp is required in MQTT payload — without it, ingest lag (RQ1) is unmeasurable');
  }

  return {
    timestamp:     new Date(mqttPayload.timestamp),
    ingestedAt,
    sensor: {
      serial:  mqttPayload.sensor.serial,
      battery: mqttPayload.sensor.battery,
    },
    zone:          registryDoc.zone,
    cultivation:   registryDoc.cultivation,
    depth15:       mqttPayload.depth15,
    depth30:       mqttPayload.depth30,
    depth45:       mqttPayload.depth45,
    depth60:       mqttPayload.depth60,
    schemaVersion: registryDoc.schemaVersion,
  };
}

module.exports = { buildTelemetryRawDocument };
