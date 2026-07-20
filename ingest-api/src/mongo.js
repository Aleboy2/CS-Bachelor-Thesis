'use strict';

const { MongoClient } = require('mongodb');

/**
 * FILE:  mongo.js
 * LAYER: Ingest API — MongoDB gateway (shared by all three prototypes).
 *
 * RECEIVES FROM: index.js — (1) a lookup-by-serial request before building
 *   each document, (2) the completed telemetry_raw document to insert.
 * SENDS TO:      MongoDB `iot_benchmark` database — reads device_registry,
 *   writes telemetry_raw. This module has no MQTT/Kafka awareness; it only
 *   knows Mongo.
 *
 * Thin wrapper around the MongoDB driver for the ingest API's two operations:
 * (1) look up a device_registry document by serial, (2) insert a telemetry_raw
 * document. No auth (locked decision — see CLAUDE.md §2, "No MongoDB auth").
 */
class MongoGateway {
  /**
   * @param {string} uri - e.g. mongodb://mongodb:27017/iot_benchmark
   */
  constructor(uri) {
    this._client = new MongoClient(uri);
    this._db = null;
  }

  async connect() {
    await this._client.connect();
    this._db = this._client.db(); // uses the db name embedded in the URI
    console.log('[mongo] connected');
  }

  async close() {
    await this._client.close();
  }

  /**
   * Looks up a device_registry document by serial.
   * @param {string} serial
   * @returns {Promise<object|null>} the matching document, or null if not found.
   */
  async findDeviceBySerial(serial) {
    return this._db.collection('device_registry').findOne({ serial });
  }

  /**
   * Inserts a fully-constructed telemetry_raw document.
   * @param {object} doc
   */
  async insertTelemetryRaw(doc) {
    await this._db.collection('telemetry_raw').insertOne(doc);
  }
}

module.exports = { MongoGateway };
