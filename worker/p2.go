package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// RunP2 starts the continuous Kafka consumer for P2 (CQRS).
//
// For each telemetry-raw-events message, it upserts four document types
// in telemetry_aggregated:
//   - latest_by_sensor  (replaceOne, _id = serial)         — serves Q1
//   - zone_daily        (incremental avg, read-modify-write) — serves Q3
//   - cultivation_daily (incremental avg, read-modify-write) — serves Q4, Q5 daily
//   - cultivation_180d_rollup (incremental avg)             — serves Q5 rollup
//
// Single partition + single consumer = sequential processing, no race conditions.
// The read-modify-write pattern is safe here.
func RunP2(mongoURI, kafkaBroker string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("[P2] shutting down…")
		cancel()
	}()

	// --- MongoDB ---
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("MongoDB connect: %w", err)
	}
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("MongoDB ping: %w", err)
	}
	aggColl := client.Database("iot_benchmark").Collection("telemetry_aggregated")
	log.Println("[P2] connected to MongoDB")

	// --- Kafka consumer ---
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       "telemetry-raw-events",
		GroupID:     "p2-worker",
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()
	log.Println("[P2] Kafka consumer started (group: p2-worker)")

	var processed int64
	start := time.Now()
	reportTicker := time.NewTicker(5 * time.Second)
	defer reportTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[P2] stopped after %d events\n", processed)
			return nil
		case <-reportTicker.C:
			elapsed := time.Since(start)
			rate := float64(processed) / elapsed.Seconds()
			log.Printf("[P2] %d events processed (%.1f events/sec)\n", processed, rate)
		default:
		}

		// ReadMessage blocks until a message is available or context is cancelled.
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			log.Printf("[P2] Kafka read error: %v\n", err)
			continue
		}

		var event TelemetryEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[P2] JSON unmarshal error: %v\n", err)
			continue
		}

		if err := processP2Event(ctx, aggColl, &event); err != nil {
			log.Printf("[P2] process error (serial=%s): %v\n", event.Sensor.Serial, err)
			continue
		}

		processed++
	}
}

// processP2Event upserts all four document types for a single event.
func processP2Event(ctx context.Context, coll *mongo.Collection, event *TelemetryEvent) error {
	ts, err := event.ParseTimestamp()
	if err != nil {
		return fmt.Errorf("parse timestamp: %w", err)
	}

	if err := upsertLatestBySensor(ctx, coll, event, ts); err != nil {
		return fmt.Errorf("latest_by_sensor: %w", err)
	}
	if err := upsertZoneDaily(ctx, coll, event, ts); err != nil {
		return fmt.Errorf("zone_daily: %w", err)
	}
	if err := upsertCultivationDaily(ctx, coll, event, ts); err != nil {
		return fmt.Errorf("cultivation_daily: %w", err)
	}
	if err := upsertCultivation180dRollup(ctx, coll, event); err != nil {
		return fmt.Errorf("cultivation_180d_rollup: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// latest_by_sensor — replaceOne({_id: serial}, doc, {upsert: true})
// LOCKED: CLAUDE.md §2 P0.4. _id = sensor.serial, no secondary index.
// Mirrors telemetry_raw shape minus sensor.battery.
// ---------------------------------------------------------------------------

func upsertLatestBySensor(ctx context.Context, coll *mongo.Collection, event *TelemetryEvent, ts time.Time) error {
	// Decode crop and irrigationSystem from raw JSON to bson for embedding.
	var crop, irrigation interface{}
	json.Unmarshal(event.Cultivation.Crop, &crop)
	json.Unmarshal(event.Cultivation.IrrigationSystem, &irrigation)

	doc := bson.D{
		{Key: "_id", Value: event.Sensor.Serial},
		{Key: "type", Value: "latest_by_sensor"},
		{Key: "timestamp", Value: ts},
		{Key: "zone", Value: bson.D{
			{Key: "zoneId", Value: event.Zone.ZoneID},
			{Key: "name", Value: event.Zone.Name},
		}},
		{Key: "cultivation", Value: bson.D{
			{Key: "cultivationId", Value: event.Cultivation.CultivationID},
			{Key: "name", Value: event.Cultivation.Name},
			{Key: "crop", Value: crop},
			{Key: "irrigationSystem", Value: irrigation},
		}},
		{Key: "depth15", Value: bson.D{{Key: "vwc", Value: event.Depth15.VWC}, {Key: "temperature", Value: event.Depth15.Temperature}, {Key: "ec", Value: event.Depth15.EC}}},
		{Key: "depth30", Value: bson.D{{Key: "vwc", Value: event.Depth30.VWC}, {Key: "temperature", Value: event.Depth30.Temperature}, {Key: "ec", Value: event.Depth30.EC}}},
		{Key: "depth45", Value: bson.D{{Key: "vwc", Value: event.Depth45.VWC}, {Key: "temperature", Value: event.Depth45.Temperature}, {Key: "ec", Value: event.Depth45.EC}}},
		{Key: "depth60", Value: bson.D{{Key: "vwc", Value: event.Depth60.VWC}, {Key: "temperature", Value: event.Depth60.Temperature}, {Key: "ec", Value: event.Depth60.EC}}},
	}

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: event.Sensor.Serial}}, doc, opts)
	return err
}

// ---------------------------------------------------------------------------
// zone_daily — incremental average via read-modify-write
// Internal fields: _readingCount, _sumVwc, _sensors (not used by queries).
// avgVwc = _sumVwc / (_readingCount × 4)   [4 depths per reading]
// ---------------------------------------------------------------------------

func upsertZoneDaily(ctx context.Context, coll *mongo.Collection, event *TelemetryEvent, ts time.Time) error {
	date := truncateToDay(ts)
	filter := bson.D{
		{Key: "type", Value: "zone_daily"},
		{Key: "zoneId", Value: event.Zone.ZoneID},
		{Key: "date", Value: date},
	}

	// Read current state (if exists).
	var existing bson.M
	err := coll.FindOne(ctx, filter).Decode(&existing)

	vwcSum := event.TotalVWC()
	var readingCount int64
	var totalSumVwc float64
	sensors := make(map[string]bool)

	if err == nil {
		// Document exists — accumulate.
		readingCount = toInt64(existing["_readingCount"]) + 1
		totalSumVwc = toFloat64(existing["_sumVwc"]) + vwcSum
		if arr, ok := existing["_sensors"].(bson.A); ok {
			for _, s := range arr {
				sensors[fmt.Sprint(s)] = true
			}
		}
	} else {
		readingCount = 1
		totalSumVwc = vwcSum
	}
	sensors[event.Sensor.Serial] = true

	sensorList := make([]string, 0, len(sensors))
	for s := range sensors {
		sensorList = append(sensorList, s)
	}

	avgVwc := totalSumVwc / (float64(readingCount) * 4)

	replacement := bson.D{
		{Key: "type", Value: "zone_daily"},
		{Key: "zoneId", Value: event.Zone.ZoneID},
		{Key: "date", Value: date},
		{Key: "avgVwc", Value: round2(avgVwc)},
		{Key: "sensorCount", Value: len(sensors)},
		{Key: "_readingCount", Value: readingCount},
		{Key: "_sumVwc", Value: totalSumVwc},
		{Key: "_sensors", Value: sensorList},
	}

	opts := options.Replace().SetUpsert(true)
	_, err = coll.ReplaceOne(ctx, filter, replacement, opts)
	return err
}

// ---------------------------------------------------------------------------
// cultivation_daily — incremental per-depth averages
// Internal fields: _readingCount, _sum (per-depth sums for temp + vwc)
// ---------------------------------------------------------------------------

func upsertCultivationDaily(ctx context.Context, coll *mongo.Collection, event *TelemetryEvent, ts time.Time) error {
	date := truncateToDay(ts)
	filter := bson.D{
		{Key: "type", Value: "cultivation_daily"},
		{Key: "cultivationId", Value: event.Cultivation.CultivationID},
		{Key: "date", Value: date},
	}

	var existing bson.M
	err := coll.FindOne(ctx, filter).Decode(&existing)

	var readingCount int64
	// Running sums: [temp15, temp30, temp45, temp60, vwc15, vwc30, vwc45, vwc60]
	sums := [8]float64{}
	sensors := make(map[string]bool)

	newValues := [8]float64{
		event.Depth15.Temperature, event.Depth30.Temperature,
		event.Depth45.Temperature, event.Depth60.Temperature,
		event.Depth15.VWC, event.Depth30.VWC,
		event.Depth45.VWC, event.Depth60.VWC,
	}

	if err == nil {
		readingCount = toInt64(existing["_readingCount"]) + 1
		if existingSums, ok := existing["_sums"].(bson.A); ok {
			for i, v := range existingSums {
				if i < 8 {
					sums[i] = toFloat64(v) + newValues[i]
				}
			}
		}
		if arr, ok := existing["_sensors"].(bson.A); ok {
			for _, s := range arr {
				sensors[fmt.Sprint(s)] = true
			}
		}
	} else {
		readingCount = 1
		sums = newValues
	}
	sensors[event.Sensor.Serial] = true

	sensorList := make([]string, 0, len(sensors))
	for s := range sensors {
		sensorList = append(sensorList, s)
	}

	rc := float64(readingCount)
	replacement := bson.D{
		{Key: "type", Value: "cultivation_daily"},
		{Key: "cultivationId", Value: event.Cultivation.CultivationID},
		{Key: "date", Value: date},
		{Key: "temperature", Value: bson.D{
			{Key: "depth15", Value: round2(sums[0] / rc)},
			{Key: "depth30", Value: round2(sums[1] / rc)},
			{Key: "depth45", Value: round2(sums[2] / rc)},
			{Key: "depth60", Value: round2(sums[3] / rc)},
		}},
		{Key: "vwc", Value: bson.D{
			{Key: "depth15", Value: round2(sums[4] / rc)},
			{Key: "depth30", Value: round2(sums[5] / rc)},
			{Key: "depth45", Value: round2(sums[6] / rc)},
			{Key: "depth60", Value: round2(sums[7] / rc)},
		}},
		{Key: "sensorCount", Value: len(sensors)},
		{Key: "_readingCount", Value: readingCount},
		{Key: "_sums", Value: bson.A{sums[0], sums[1], sums[2], sums[3], sums[4], sums[5], sums[6], sums[7]}},
		{Key: "_sensors", Value: sensorList},
	}

	opts := options.Replace().SetUpsert(true)
	_, err = coll.ReplaceOne(ctx, filter, replacement, opts)
	return err
}

// ---------------------------------------------------------------------------
// cultivation_180d_rollup — incremental per-depth VWC average over full period
// Internal fields: _readingCount, _sums (4 depth VWC sums)
// ---------------------------------------------------------------------------

func upsertCultivation180dRollup(ctx context.Context, coll *mongo.Collection, event *TelemetryEvent) error {
	filter := bson.D{
		{Key: "type", Value: "cultivation_180d_rollup"},
		{Key: "cultivationId", Value: event.Cultivation.CultivationID},
	}

	var existing bson.M
	err := coll.FindOne(ctx, filter).Decode(&existing)

	var readingCount int64
	sums := [4]float64{}
	newVwc := [4]float64{
		event.Depth15.VWC, event.Depth30.VWC,
		event.Depth45.VWC, event.Depth60.VWC,
	}

	if err == nil {
		readingCount = toInt64(existing["_readingCount"]) + 1
		if existingSums, ok := existing["_sums"].(bson.A); ok {
			for i, v := range existingSums {
				if i < 4 {
					sums[i] = toFloat64(v) + newVwc[i]
				}
			}
		}
	} else {
		readingCount = 1
		sums = newVwc
	}

	rc := float64(readingCount)
	replacement := bson.D{
		{Key: "type", Value: "cultivation_180d_rollup"},
		{Key: "cultivationId", Value: event.Cultivation.CultivationID},
		{Key: "windowEnd", Value: time.Now().UTC()},
		{Key: "avgVwc180d", Value: bson.D{
			{Key: "depth15", Value: round2(sums[0] / rc)},
			{Key: "depth30", Value: round2(sums[1] / rc)},
			{Key: "depth45", Value: round2(sums[2] / rc)},
			{Key: "depth60", Value: round2(sums[3] / rc)},
		}},
		{Key: "_readingCount", Value: readingCount},
		{Key: "_sums", Value: bson.A{sums[0], sums[1], sums[2], sums[3]}},
	}

	opts := options.Replace().SetUpsert(true)
	_, err = coll.ReplaceOne(ctx, filter, replacement, opts)
	return err
}

// ---------------------------------------------------------------------------
// BSON type coercion helpers (MongoDB numbers may arrive as int32 or float64)
// ---------------------------------------------------------------------------

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}
