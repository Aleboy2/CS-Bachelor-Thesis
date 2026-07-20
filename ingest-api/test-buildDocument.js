#!/usr/bin/env node
'use strict';

/**
 * FILE:  test-buildDocument.js
 * LAYER: Ingest API — unit test, no infrastructure required.
 *
 * Exercises buildDocument.js in isolation using plain-object fixtures that
 * stand in for a real MQTT payload and device_registry document — does not
 * touch MQTT, MongoDB, or Kafka.
 *
 * Run: node test-buildDocument.js
 */

const { buildTelemetryRawDocument } = require('./src/buildDocument');

// --- Test 1: valid payload produces correct document shape ---

const payload = {
  timestamp: '2026-06-15T11:58:17.000Z',
  sensor: { serial: 'A70302A02025', battery: 90 },
  depth15: { vwc: 34.21, temperature: 18.6, ec: 0.42 },
  depth30: { vwc: 38.10, temperature: 17.9, ec: 0.51 },
  depth45: { vwc: 40.50, temperature: 17.2, ec: 0.48 },
  depth60: { vwc: 41.80, temperature: 16.8, ec: 0.45 },
};

const registry = {
  serial: 'A70302A02025',
  dci: '7fc98f10-test',
  gps: { latitude: 51.612621, longitude: 5.683513 },
  zone: { zoneId: '67bc1234', name: 'Field 2' },
  cultivation: { cultivationId: '67bd5678', name: 'Field 2 — Potatoes 2025' },
  schemaVersion: '1.0',
};

const ingestedAt = new Date('2026-06-15T11:58:17.050Z'); // 50ms after sensor timestamp

const doc = buildTelemetryRawDocument(payload, registry, ingestedAt);

console.assert(doc.timestamp instanceof Date, 'timestamp should be a Date');
console.assert(doc.timestamp.toISOString() === '2026-06-15T11:58:17.000Z', 'timestamp should match payload');
console.assert(doc.ingestedAt === ingestedAt, 'ingestedAt should be the passed-in Date');
console.assert(doc.sensor.serial === 'A70302A02025', 'serial correct');
console.assert(doc.sensor.battery === 90, 'battery correct');
console.assert(doc.zone.zoneId === '67bc1234', 'zone copied from registry');
console.assert(doc.cultivation.cultivationId === '67bd5678', 'cultivation copied from registry');
console.assert(doc.gps === undefined, 'gps must NOT be in telemetry_raw');
console.assert(doc.dci === undefined, 'dci must NOT be in telemetry_raw');
console.assert(doc.depth15.vwc === 34.21, 'depth15 vwc correct');
console.assert(doc.schemaVersion === '1.0', 'schemaVersion copied from registry');

const lagMs = doc.ingestedAt - doc.timestamp;
console.assert(lagMs === 50, 'ingest lag should be 50ms');
console.log('Test 1 PASSED — valid payload, lag=' + lagMs + 'ms, gps/dci absent');

// --- Test 2: missing timestamp throws ---

try {
  const badPayload = { sensor: { serial: 'X' }, depth15: {}, depth30: {}, depth45: {}, depth60: {} };
  buildTelemetryRawDocument(badPayload, registry, ingestedAt);
  console.error('Test 2 FAILED — should have thrown');
  process.exit(1);
} catch (e) {
  console.assert(e.message.includes('timestamp is required'), 'error mentions timestamp');
  console.log('Test 2 PASSED — missing timestamp throws correctly');
}

console.log('\nAll tests passed.');
