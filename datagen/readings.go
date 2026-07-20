package main

import (
	"hash/fnv"
	"math"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// ---------------------------------------------------------------------------
// Value ranges (LOCKED — confirmed in design discussion)
//   VWC:          15–55 %       (increases slightly with depth)
//   Temperature:   8–28 °C      (decreases with depth)
//   EC:          0.1–1.5 dS/m   (slight increase with depth)
//   Battery:     60–100 %       (linear drain over 180 days)
//
// Per-reading random walk steps:
//   VWC:   ±0.5    Temperature: ±0.3    EC: ±0.05
// ---------------------------------------------------------------------------

const (
	vwcMin  = 15.0
	vwcMax  = 55.0
	tempMin = 8.0
	tempMax = 28.0
	ecMin   = 0.1
	ecMax   = 1.5

	readingsPerDay = 96 // every 15 minutes
	readingInterval = 15 * time.Minute
)

// DepthReading holds the three agronomic measurements at one depth.
type DepthReading struct {
	VWC         float64
	Temperature float64
	EC          float64
}

// SensorState tracks the random-walk state for one sensor across readings.
type SensorState struct {
	rng          *rand.Rand
	depths       [4]DepthReading // indices 0–3 → depth15, depth30, depth45, depth60
	batteryStart float64
	batteryDrain float64 // total drain over 180 days
}

// NewSensorState creates a seeded state for a sensor. The per-sensor PRNG is
// derived from the serial number (FNV hash), making each sensor's time series
// independently reproducible regardless of generation order.
func NewSensorState(serial string) *SensorState {
	h := fnv.New64a()
	h.Write([]byte(serial))
	sensorSeed := int64(h.Sum64())
	rng := rand.New(rand.NewSource(sensorSeed))

	// Base values with per-depth gradient.
	// depth15 is shallowest: driest, warmest, lowest EC.
	baseVWC := 20.0 + rng.Float64()*20.0   // 20–40 at depth15
	baseTemp := 15.0 + rng.Float64()*10.0   // 15–25 at depth15
	baseEC := 0.2 + rng.Float64()*0.5       // 0.2–0.7 at depth15

	var depths [4]DepthReading
	for i := 0; i < 4; i++ {
		depthFactor := float64(i) // 0, 1, 2, 3
		depths[i] = DepthReading{
			VWC:         baseVWC + depthFactor*2.5,     // +2.5% per depth level
			Temperature: baseTemp - depthFactor*0.5,    // -0.5°C per depth level
			EC:          baseEC + depthFactor*0.03,      // +0.03 dS/m per depth level
		}
	}

	batteryStart := 85.0 + rng.Float64()*15.0  // 85–100%
	batteryDrain := 15.0 + rng.Float64()*15.0  // drains 15–30% over 180 days

	return &SensorState{
		rng:          rng,
		depths:       depths,
		batteryStart: batteryStart,
		batteryDrain: batteryDrain,
	}
}

// Step advances the random walk by one reading and returns the current values.
// dayIndex is 0-based from the start of the dataset (used for battery drain).
func (s *SensorState) Step(dayIndex int, readingIndex int) ([4]DepthReading, int) {
	for i := 0; i < 4; i++ {
		s.depths[i].VWC = clamp(s.depths[i].VWC+s.walkStep(0.5), vwcMin, vwcMax)
		s.depths[i].Temperature = clamp(s.depths[i].Temperature+s.walkStep(0.3), tempMin, tempMax)
		s.depths[i].EC = clamp(s.depths[i].EC+s.walkStep(0.05), ecMin, ecMax)
	}

	totalReadings := float64(dayIndex*readingsPerDay + readingIndex)
	maxReadings := 180.0 * readingsPerDay
	battery := s.batteryStart - (totalReadings/maxReadings)*s.batteryDrain
	battery = clamp(battery, 60.0, 100.0)

	return s.depths, int(math.Round(battery))
}

// walkStep returns a random step in [-magnitude, +magnitude].
func (s *SensorState) walkStep(magnitude float64) float64 {
	return (s.rng.Float64()*2 - 1) * magnitude
}

// ---------------------------------------------------------------------------
// Document construction
// ---------------------------------------------------------------------------

// BuildTelemetryRawBSON constructs a telemetry_raw document in the same shape
// as buildDocument.js in the ingest API. gps and dci are deliberately excluded
// (LOCKED — CLAUDE.md §3 ingest path Step 4).
func BuildTelemetryRawBSON(
	sensor *Sensor,
	timestamp time.Time,
	ingestedAt time.Time,
	depths [4]DepthReading,
	battery int,
) bson.D {
	return bson.D{
		{Key: "timestamp", Value: timestamp},
		{Key: "ingestedAt", Value: ingestedAt},
		{Key: "sensor", Value: bson.D{
			{Key: "serial", Value: sensor.Serial},
			{Key: "battery", Value: battery},
		}},
		{Key: "zone", Value: bson.D{
			{Key: "zoneId", Value: sensor.Zone.ZoneID},
			{Key: "name", Value: sensor.Zone.Name},
		}},
		{Key: "cultivation", Value: bson.D{
			{Key: "cultivationId", Value: sensor.Cultivation.CultivationID},
			{Key: "name", Value: sensor.Cultivation.Name},
			{Key: "crop", Value: bson.D{
				{Key: "name", Value: sensor.Cultivation.Crop.Name},
				{Key: "isInGreenHouse", Value: sensor.Cultivation.Crop.IsInGreenHouse},
				{Key: "sowingDate", Value: sensor.Cultivation.Crop.SowingDate},
				{Key: "bumpHeight", Value: sensor.Cultivation.Crop.BumpHeight},
				{Key: "distanceToSeedling", Value: sensor.Cultivation.Crop.DistanceToSeedling},
				{Key: "sowingDepth", Value: sensor.Cultivation.Crop.SowingDepth},
			}},
			{Key: "irrigationSystem", Value: bson.D{
				{Key: "irrigationProfile", Value: sensor.Cultivation.IrrigationSystem.IrrigationProfile},
				{Key: "dripperFlowRate", Value: sensor.Cultivation.IrrigationSystem.DripperFlowRate},
				{Key: "minAmount", Value: sensor.Cultivation.IrrigationSystem.MinAmount},
				{Key: "maxAmount", Value: sensor.Cultivation.IrrigationSystem.MaxAmount},
			}},
		}},
		{Key: "depth15", Value: depthBSON(depths[0])},
		{Key: "depth30", Value: depthBSON(depths[1])},
		{Key: "depth45", Value: depthBSON(depths[2])},
		{Key: "depth60", Value: depthBSON(depths[3])},
		{Key: "schemaVersion", Value: sensor.SchemaVersion},
	}
}

func depthBSON(d DepthReading) bson.D {
	return bson.D{
		{Key: "vwc", Value: round2(d.VWC)},
		{Key: "temperature", Value: round2(d.Temperature)},
		{Key: "ec", Value: round2(d.EC)},
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
