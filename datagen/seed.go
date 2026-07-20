package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SeedConfig holds all parameters for the seed subcommand.
type SeedConfig struct {
	SensorCount    int
	MongoURI       string
	Days           int    // total days of telemetry data (default 180)
	BatchSize      int    // documents per InsertMany call
	PRNGSeed       int64
	BenchmarkStart string // ISO 8601 timestamp
	DropFirst      bool
}

// RunSeed executes the seed workflow:
//  1. Connect to MongoDB.
//  2. Optionally drop existing data (clean run).
//  3. Generate entities (zones, cultivations, sensors).
//  4. Bulk-insert device_registry.
//  5. Drop telemetry_raw indexes (faster bulk insert).
//  6. Bulk-insert telemetry_raw (sensor by sensor, batched).
//  7. Recreate the three compound indexes on telemetry_raw.
func RunSeed(cfg SeedConfig) error {
	benchmarkStart, err := time.Parse(time.RFC3339, cfg.BenchmarkStart)
	if err != nil {
		return fmt.Errorf("invalid --benchmark-start: %w", err)
	}

	// Data spans [benchmarkStart - days .. benchmarkStart].
	dataStart := benchmarkStart.AddDate(0, 0, -cfg.Days)
	totalReadings := cfg.Days * readingsPerDay
	totalDocs := int64(cfg.SensorCount) * int64(totalReadings)

	fmt.Printf("=== SEED MODE ===\n")
	fmt.Printf("Sensors:          %d\n", cfg.SensorCount)
	fmt.Printf("Data window:      %s → %s (%d days)\n",
		dataStart.Format("2006-01-02"), benchmarkStart.Format("2006-01-02"), cfg.Days)
	fmt.Printf("Readings/sensor:  %d\n", totalReadings)
	fmt.Printf("Total documents:  %d\n", totalDocs)
	fmt.Printf("Batch size:       %d\n", cfg.BatchSize)
	fmt.Println()

	// --- Connect to MongoDB ---
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return fmt.Errorf("MongoDB connect: %w", err)
	}
	defer client.Disconnect(ctx)

	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("MongoDB ping: %w", err)
	}
	fmt.Println("[✓] Connected to MongoDB")

	db := client.Database("iot_benchmark")
	registryColl := db.Collection("device_registry")
	rawColl := db.Collection("telemetry_raw")

	// --- Optionally drop existing data ---
	if cfg.DropFirst {
		fmt.Println("[…] Dropping existing collections…")
		registryColl.Drop(ctx)
		rawColl.Drop(ctx)
		fmt.Println("[✓] Collections dropped")
	}

	// --- Generate entities ---
	fmt.Printf("[…] Generating %d sensors…\n", cfg.SensorCount)
	rng := rand.New(rand.NewSource(cfg.PRNGSeed))
	sensors := GenerateEntities(cfg.SensorCount, benchmarkStart, rng)
	fmt.Printf("[✓] Generated %d sensors across %d cultivations\n",
		len(sensors), countCultivations(sensors))

	// --- Insert device_registry ---
	fmt.Println("[…] Inserting device_registry…")
	if err := insertDeviceRegistry(ctx, registryColl, sensors, cfg.BatchSize); err != nil {
		return fmt.Errorf("device_registry insert: %w", err)
	}
	// Create the serial index on device_registry (needed by the ingest API).
	_, err = registryColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "serial", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("device_registry index: %w", err)
	}
	fmt.Printf("[✓] device_registry: %d documents + serial index\n", len(sensors))

	// --- Bulk-insert telemetry_raw (no indexes yet — much faster) ---
	fmt.Println("[…] Inserting telemetry_raw (this may take a while at scale)…")
	startInsert := time.Now()
	docsInserted, err := insertTelemetryRaw(ctx, rawColl, sensors, dataStart, cfg)
	if err != nil {
		return fmt.Errorf("telemetry_raw insert: %w", err)
	}
	insertDuration := time.Since(startInsert)
	rate := float64(docsInserted) / insertDuration.Seconds()
	fmt.Printf("[✓] telemetry_raw: %d documents in %s (%.0f docs/sec)\n",
		docsInserted, insertDuration.Round(time.Second), rate)

	// --- Create indexes on telemetry_raw ---
	fmt.Println("[…] Creating compound indexes on telemetry_raw…")
	startIdx := time.Now()
	if err := createTelemetryIndexes(ctx, rawColl); err != nil {
		return fmt.Errorf("telemetry_raw indexes: %w", err)
	}
	fmt.Printf("[✓] 3 indexes created in %s\n", time.Since(startIdx).Round(time.Second))

	fmt.Println()
	fmt.Println("=== SEED COMPLETE ===")
	return nil
}

// ---------------------------------------------------------------------------
// device_registry insertion
// ---------------------------------------------------------------------------

func insertDeviceRegistry(ctx context.Context, coll *mongo.Collection, sensors []Sensor, batchSize int) error {
	batch := make([]interface{}, 0, batchSize)
	for i := range sensors {
		batch = append(batch, sensors[i].DeviceRegistryBSON())
		if len(batch) >= batchSize {
			if _, err := coll.InsertMany(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := coll.InsertMany(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// telemetry_raw insertion
// ---------------------------------------------------------------------------

func insertTelemetryRaw(
	ctx context.Context,
	coll *mongo.Collection,
	sensors []Sensor,
	dataStart time.Time,
	cfg SeedConfig,
) (int64, error) {
	var totalInserted int64
	batch := make([]interface{}, 0, cfg.BatchSize)
	reportInterval := 100 // report progress every N sensors
	if cfg.SensorCount >= 10000 {
		reportInterval = 500
	}

	overallStart := time.Now()

	for sIdx, sensor := range sensors {
		state := NewSensorState(sensor.Serial)

		for day := 0; day < cfg.Days; day++ {
			dayStart := dataStart.AddDate(0, 0, day)

			for r := 0; r < readingsPerDay; r++ {
				// Timestamp: day start + reading slot + per-sensor offset.
				timestamp := dayStart.Add(
					time.Duration(r)*readingInterval +
						time.Duration(sensor.TransmitOffset)*time.Second,
				)

				// ingestedAt: timestamp + small synthetic offset (20–80ms).
				// Not measured — these are pre-populated historical documents.
				syntheticLag := time.Duration(20+state.rng.Intn(60)) * time.Millisecond
				ingestedAt := timestamp.Add(syntheticLag)

				depths, battery := state.Step(day, r)

				doc := BuildTelemetryRawBSON(&sensor, timestamp, ingestedAt, depths, battery)
				batch = append(batch, doc)

				if len(batch) >= cfg.BatchSize {
					if _, err := coll.InsertMany(ctx, batch); err != nil {
						return totalInserted, err
					}
					totalInserted += int64(len(batch))
					batch = batch[:0]
				}
			}
		}

		// Progress report.
		if (sIdx+1)%reportInterval == 0 || sIdx+1 == len(sensors) {
			elapsed := time.Since(overallStart)
			pct := float64(sIdx+1) / float64(len(sensors)) * 100
			rate := float64(totalInserted) / elapsed.Seconds()
			remaining := time.Duration(float64(elapsed) * (100.0/pct - 1))
			fmt.Printf("  [%5.1f%%] sensor %d/%d  |  %d docs  |  %.0f docs/sec  |  ETA %s\n",
				pct, sIdx+1, len(sensors), totalInserted, rate,
				remaining.Round(time.Second))
		}
	}

	// Flush remaining batch.
	if len(batch) > 0 {
		if _, err := coll.InsertMany(ctx, batch); err != nil {
			return totalInserted, err
		}
		totalInserted += int64(len(batch))
	}

	return totalInserted, nil
}

// ---------------------------------------------------------------------------
// Index creation (LOCKED — CLAUDE.md §2 P0.1)
// ---------------------------------------------------------------------------

func createTelemetryIndexes(ctx context.Context, coll *mongo.Collection) error {
	// Index names match the mongo-init scripts (01-init.js) exactly so that
	// the state is identical regardless of whether you arrive here via
	// datagen seed --drop or via the Docker entrypoint init scripts.
	indexes := []mongo.IndexModel{
		// Index A: serves Q1 (latest per sensor) and Q2 (sensor 24h range).
		{
			Keys:    bson.D{{Key: "sensor.serial", Value: 1}, {Key: "timestamp", Value: -1}},
			Options: options.Index().SetName("idx_sensor_serial_ts"),
		},
		// Index B: serves Q3 (zone 7-day aggregation).
		{
			Keys:    bson.D{{Key: "zone.zoneId", Value: 1}, {Key: "timestamp", Value: -1}},
			Options: options.Index().SetName("idx_zone_zoneId_ts"),
		},
		// Index C: serves Q4 (cultivation 30-day trend) and Q5 (full-period VWC).
		{
			Keys:    bson.D{{Key: "cultivation.cultivationId", Value: 1}, {Key: "timestamp", Value: -1}},
			Options: options.Index().SetName("idx_cultivation_cultivationId_ts"),
		},
	}

	_, err := coll.Indexes().CreateMany(ctx, indexes)
	return err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func countCultivations(sensors []Sensor) int {
	seen := make(map[string]bool)
	for i := range sensors {
		seen[sensors[i].Cultivation.CultivationID] = true
	}
	return len(seen)
}
