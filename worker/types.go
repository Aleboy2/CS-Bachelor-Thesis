package main

import (
	"encoding/json"
	"math"
	"time"
)

// ---------------------------------------------------------------------------
// TelemetryEvent is the JSON shape of a telemetry_raw document as serialised
// by kafka.js (JSON.stringify of the MongoDB document). Kafka message value.
// ---------------------------------------------------------------------------

type TelemetryEvent struct {
	Timestamp   string          `json:"timestamp"`
	IngestedAt  string          `json:"ingestedAt"`
	Sensor      SensorData      `json:"sensor"`
	Zone        ZoneData        `json:"zone"`
	Cultivation CultivationData `json:"cultivation"`
	Depth15     DepthData       `json:"depth15"`
	Depth30     DepthData       `json:"depth30"`
	Depth45     DepthData       `json:"depth45"`
	Depth60     DepthData       `json:"depth60"`
	SchemaVer   string          `json:"schemaVersion"`
}

type SensorData struct {
	Serial  string `json:"serial"`
	Battery int    `json:"battery"`
}

type ZoneData struct {
	ZoneID string `json:"zoneId"`
	Name   string `json:"name"`
}

type CultivationData struct {
	CultivationID    string          `json:"cultivationId"`
	Name             string          `json:"name"`
	Crop             json.RawMessage `json:"crop"`
	IrrigationSystem json.RawMessage `json:"irrigationSystem"`
}

type DepthData struct {
	VWC         float64 `json:"vwc"`
	Temperature float64 `json:"temperature"`
	EC          float64 `json:"ec"`
}

// ParseTimestamp parses the ISO 8601 timestamp from the Kafka event.
func (e *TelemetryEvent) ParseTimestamp() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, e.Timestamp)
}

// TotalVWC returns the sum of VWC across all four depths (for zone_daily avg).
func (e *TelemetryEvent) TotalVWC() float64 {
	return e.Depth15.VWC + e.Depth30.VWC + e.Depth45.VWC + e.Depth60.VWC
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// truncateToDay returns the given time truncated to UTC midnight.
// Stored as ISODate in MongoDB (LOCKED — implementation_decisions.md).
func truncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
