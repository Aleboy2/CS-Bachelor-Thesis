package main

import (
	"fmt"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// ---------------------------------------------------------------------------
// Sensor-per-cultivation distribution (LOCKED — benchmark_schema_design_v41 §13.2)
//   70% → 1 sensor,  20% → 2,  7% → 3,  3% → 5
// Weighted average ≈ 1.46 sensors/cultivation.
// ---------------------------------------------------------------------------

var sensorCountWeights = []struct {
	count  int
	cumPct float64 // cumulative probability
}{
	{1, 0.70},
	{2, 0.90},
	{3, 0.97},
	{5, 1.00},
}

// Crop pool — realistic Dutch precision agriculture crops.
var cropPool = []string{
	"Potatoes",
	"Wheat",
	"Sugar Beet",
	"Onion",
	"Maize",
	"Carrot",
	"Barley",
	"Lettuce",
}

// Irrigation profiles — rotated across cultivations for variety.
var irrigationProfiles = []string{
	"conservative",
	"balanced",
	"intensive",
}

// GPS bounding box — clustered around the Netherlands.
const (
	gpsLatMin = 51.5
	gpsLatMax = 52.1
	gpsLonMin = 5.2
	gpsLonMax = 6.0
)

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

type GPS struct {
	Latitude  float64
	Longitude float64
}

type Crop struct {
	Name               string
	IsInGreenHouse     bool
	SowingDate         time.Time
	BumpHeight         int
	DistanceToSeedling int
	SowingDepth        int
}

type IrrigationSystem struct {
	IrrigationProfile string
	DripperFlowRate   float64
	MinAmount         float64
	MaxAmount         float64
}

type Zone struct {
	ZoneID string
	Name   string
}

type Cultivation struct {
	CultivationID    string
	Name             string
	Crop             Crop
	IrrigationSystem IrrigationSystem
}

type Sensor struct {
	Serial        string
	DCI           string
	Gps           GPS
	Zone          Zone
	Cultivation   Cultivation
	SchemaVersion string
	// TransmitOffset is the per-sensor stagger (0–899 seconds within the
	// 15-minute window). Derived deterministically from serial hash.
	TransmitOffset int
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

// GenerateEntities builds the full entity hierarchy for the given sensor count.
// The returned slices are deterministic for a given (sensorCount, rng) pair.
func GenerateEntities(sensorCount int, benchmarkStart time.Time, rng *rand.Rand) []Sensor {
	sensors := make([]Sensor, 0, sensorCount)
	sensorIdx := 0
	cultIdx := 0

	for sensorIdx < sensorCount {
		// Determine how many sensors this cultivation gets.
		roll := rng.Float64()
		sensorsInCult := 1
		for _, w := range sensorCountWeights {
			if roll < w.cumPct {
				sensorsInCult = w.count
				break
			}
		}
		// Don't overshoot the target sensor count.
		if sensorIdx+sensorsInCult > sensorCount {
			sensorsInCult = sensorCount - sensorIdx
		}

		cultIdx++
		zoneID := fmt.Sprintf("zone-%06d", cultIdx)
		cultID := fmt.Sprintf("cult-%06d", cultIdx)

		// Pick a crop from the pool (deterministic rotation + some randomness).
		cropName := cropPool[cultIdx%len(cropPool)]
		isGreenhouse := rng.Float64() < 0.15 // ~15% greenhouse

		// Sowing date: staggered across 150 days ending 30 days before benchmark start.
		// This ensures cultivations are at different lifecycle points (30–180 days old).
		maxSowingOffset := 150 // days before benchmarkStart - 30
		sowingDaysBack := 30 + rng.Intn(maxSowingOffset+1)
		sowingDate := benchmarkStart.AddDate(0, 0, -sowingDaysBack)

		// Zone and cultivation names.
		zoneName := fmt.Sprintf("Zone %d", cultIdx)
		cultName := fmt.Sprintf("%s — %s %d", zoneName, cropName, sowingDate.Year())

		zone := Zone{ZoneID: zoneID, Name: zoneName}
		cult := Cultivation{
			CultivationID: cultID,
			Name:          cultName,
			Crop: Crop{
				Name:               cropName,
				IsInGreenHouse:     isGreenhouse,
				SowingDate:         sowingDate,
				BumpHeight:         0,
				DistanceToSeedling: 25,
				SowingDepth:        0,
			},
			IrrigationSystem: IrrigationSystem{
				IrrigationProfile: irrigationProfiles[cultIdx%len(irrigationProfiles)],
				DripperFlowRate:   0,
				MinAmount:         2,
				MaxAmount:         50,
			},
		}

		// GPS — all sensors in a cultivation share the same base GPS with slight offset.
		baseLat := gpsLatMin + rng.Float64()*(gpsLatMax-gpsLatMin)
		baseLon := gpsLonMin + rng.Float64()*(gpsLonMax-gpsLonMin)

		for j := 0; j < sensorsInCult; j++ {
			sensorIdx++
			serial := fmt.Sprintf("%07d", sensorIdx)

			// Per-sensor GPS offset (sensors in the same field, ~10–50m apart).
			lat := baseLat + (rng.Float64()-0.5)*0.001
			lon := baseLon + (rng.Float64()-0.5)*0.001

			// DCI — deterministic UUID-like string from serial.
			dci := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
				rng.Uint32(), rng.Uint32()&0xFFFF, rng.Uint32()&0xFFFF,
				rng.Uint32()&0xFFFF, rng.Int63()&0xFFFFFFFFFFFF)

			// Transmit offset: 0–899 seconds within the 15-min window.
			// Ensures not all sensors transmit simultaneously.
			transmitOffset := rng.Intn(900)

			sensors = append(sensors, Sensor{
				Serial:         serial,
				DCI:            dci,
				Gps:            GPS{Latitude: lat, Longitude: lon},
				Zone:           zone,
				Cultivation:    cult,
				SchemaVersion:  "1.0",
				TransmitOffset: transmitOffset,
			})
		}
	}

	return sensors
}

// ---------------------------------------------------------------------------
// BSON serialisation helpers
// ---------------------------------------------------------------------------

// DeviceRegistryBSON returns the bson.D for a device_registry document.
// Includes gps and dci (device_registry is the source of truth for these).
func (s *Sensor) DeviceRegistryBSON() bson.D {
	return bson.D{
		{Key: "serial", Value: s.Serial},
		{Key: "dci", Value: s.DCI},
		{Key: "gps", Value: bson.D{
			{Key: "latitude", Value: s.Gps.Latitude},
			{Key: "longitude", Value: s.Gps.Longitude},
		}},
		{Key: "zone", Value: bson.D{
			{Key: "zoneId", Value: s.Zone.ZoneID},
			{Key: "name", Value: s.Zone.Name},
		}},
		{Key: "cultivation", Value: bson.D{
			{Key: "cultivationId", Value: s.Cultivation.CultivationID},
			{Key: "name", Value: s.Cultivation.Name},
			{Key: "crop", Value: bson.D{
				{Key: "name", Value: s.Cultivation.Crop.Name},
				{Key: "isInGreenHouse", Value: s.Cultivation.Crop.IsInGreenHouse},
				{Key: "sowingDate", Value: s.Cultivation.Crop.SowingDate},
				{Key: "bumpHeight", Value: s.Cultivation.Crop.BumpHeight},
				{Key: "distanceToSeedling", Value: s.Cultivation.Crop.DistanceToSeedling},
				{Key: "sowingDepth", Value: s.Cultivation.Crop.SowingDepth},
			}},
			{Key: "irrigationSystem", Value: bson.D{
				{Key: "irrigationProfile", Value: s.Cultivation.IrrigationSystem.IrrigationProfile},
				{Key: "dripperFlowRate", Value: s.Cultivation.IrrigationSystem.DripperFlowRate},
				{Key: "minAmount", Value: s.Cultivation.IrrigationSystem.MinAmount},
				{Key: "maxAmount", Value: s.Cultivation.IrrigationSystem.MaxAmount},
			}},
		}},
		{Key: "schemaVersion", Value: s.SchemaVersion},
	}
}
