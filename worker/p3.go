package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// RunP3 starts the batch rewriter for P3 (Lambda Architecture batch layer).
//
// On startup: runs a full rewrite immediately (so telemetry_aggregated is
// populated before any benchmark queries run).
// Then: sleeps for batchInterval, rewrites again, repeats.
//
// Each rewrite:
//  1. Deletes all zone_daily, cultivation_daily, cultivation_180d_rollup docs
//  2. Runs three aggregation pipelines against telemetry_raw
//  3. Inserts the results into telemetry_aggregated
//
// Does NOT write latest_by_sensor — that belongs to Redis in P3 (Phase 5).
// LOCKED: CLAUDE.md §2 P0.2.
func RunP3(mongoURI, intervalStr string) error {
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return fmt.Errorf("invalid --batch-interval: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("[P3] shutting down…")
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
	log.Println("[P3] connected to MongoDB")

	db := client.Database("iot_benchmark")

	// Run immediately on startup, then on interval.
	for {
		log.Println("[P3] starting batch rewrite…")
		start := time.Now()

		if err := runBatchRewrite(ctx, db); err != nil {
			log.Printf("[P3] rewrite error: %v\n", err)
		} else {
			log.Printf("[P3] rewrite complete in %s\n", time.Since(start).Round(time.Second))
		}

		log.Printf("[P3] sleeping %s until next rewrite…\n", interval)
		select {
		case <-ctx.Done():
			log.Println("[P3] stopped")
			return nil
		case <-time.After(interval):
			// continue to next rewrite
		}
	}
}

// runBatchRewrite deletes all aggregated documents and recomputes them from
// telemetry_raw using three aggregation pipelines.
func runBatchRewrite(ctx context.Context, db *mongo.Database) error {
	rawColl := db.Collection("telemetry_raw")
	aggColl := db.Collection("telemetry_aggregated")

	if err := rewriteZoneDaily(ctx, rawColl, aggColl); err != nil {
		return fmt.Errorf("zone_daily: %w", err)
	}
	if err := rewriteCultivationDaily(ctx, rawColl, aggColl); err != nil {
		return fmt.Errorf("cultivation_daily: %w", err)
	}
	if err := rewriteCultivation180dRollup(ctx, rawColl, aggColl); err != nil {
		return fmt.Errorf("cultivation_180d_rollup: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// zone_daily — recompute from scratch
// Q3: avg VWC across all sensors and all 4 depths, per zone per day.
// avgVwc = sum(all depth VWC values) / (readingCount × 4)
// ---------------------------------------------------------------------------

func rewriteZoneDaily(ctx context.Context, rawColl, aggColl *mongo.Collection) error {
	log.Println("[P3]   rewriting zone_daily…")

	// Delete existing.
	res, err := aggColl.DeleteMany(ctx, bson.D{{Key: "type", Value: "zone_daily"}})
	if err != nil {
		return err
	}
	log.Printf("[P3]   deleted %d old zone_daily docs\n", res.DeletedCount)

	// Aggregation pipeline.
	pipeline := mongo.Pipeline{
		// Group by (zoneId, day).
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "zoneId", Value: "$zone.zoneId"},
				{Key: "date", Value: bson.D{{Key: "$dateTrunc", Value: bson.D{
					{Key: "date", Value: "$timestamp"},
					{Key: "unit", Value: "day"},
				}}}},
			}},
			{Key: "sumVwc", Value: bson.D{{Key: "$sum", Value: bson.D{{Key: "$add", Value: bson.A{
				"$depth15.vwc", "$depth30.vwc", "$depth45.vwc", "$depth60.vwc",
			}}}}}},
			{Key: "readingCount", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "sensors", Value: bson.D{{Key: "$addToSet", Value: "$sensor.serial"}}},
		}}},
		// Project to final shape.
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "type", Value: bson.D{{Key: "$literal", Value: "zone_daily"}}},
			{Key: "zoneId", Value: "$_id.zoneId"},
			{Key: "date", Value: "$_id.date"},
			{Key: "avgVwc", Value: bson.D{{Key: "$round", Value: bson.A{
				bson.D{{Key: "$divide", Value: bson.A{
					"$sumVwc",
					bson.D{{Key: "$multiply", Value: bson.A{"$readingCount", 4}}},
				}}},
				2,
			}}}},
			{Key: "sensorCount", Value: bson.D{{Key: "$size", Value: "$sensors"}}},
		}}},
	}

	return runPipelineAndInsert(ctx, rawColl, aggColl, pipeline, "zone_daily")
}

// ---------------------------------------------------------------------------
// cultivation_daily — recompute per-depth temperature and VWC averages
// Q4: daily avg temperature at all 4 depths, per cultivation per day.
// Q5 (daily part): daily avg VWC at all 4 depths.
// ---------------------------------------------------------------------------

func rewriteCultivationDaily(ctx context.Context, rawColl, aggColl *mongo.Collection) error {
	log.Println("[P3]   rewriting cultivation_daily…")

	res, err := aggColl.DeleteMany(ctx, bson.D{{Key: "type", Value: "cultivation_daily"}})
	if err != nil {
		return err
	}
	log.Printf("[P3]   deleted %d old cultivation_daily docs\n", res.DeletedCount)

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "cultivationId", Value: "$cultivation.cultivationId"},
				{Key: "date", Value: bson.D{{Key: "$dateTrunc", Value: bson.D{
					{Key: "date", Value: "$timestamp"},
					{Key: "unit", Value: "day"},
				}}}},
			}},
			{Key: "avgTemp15", Value: bson.D{{Key: "$avg", Value: "$depth15.temperature"}}},
			{Key: "avgTemp30", Value: bson.D{{Key: "$avg", Value: "$depth30.temperature"}}},
			{Key: "avgTemp45", Value: bson.D{{Key: "$avg", Value: "$depth45.temperature"}}},
			{Key: "avgTemp60", Value: bson.D{{Key: "$avg", Value: "$depth60.temperature"}}},
			{Key: "avgVwc15", Value: bson.D{{Key: "$avg", Value: "$depth15.vwc"}}},
			{Key: "avgVwc30", Value: bson.D{{Key: "$avg", Value: "$depth30.vwc"}}},
			{Key: "avgVwc45", Value: bson.D{{Key: "$avg", Value: "$depth45.vwc"}}},
			{Key: "avgVwc60", Value: bson.D{{Key: "$avg", Value: "$depth60.vwc"}}},
			{Key: "readingCount", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "sensors", Value: bson.D{{Key: "$addToSet", Value: "$sensor.serial"}}},
		}}},
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "type", Value: bson.D{{Key: "$literal", Value: "cultivation_daily"}}},
			{Key: "cultivationId", Value: "$_id.cultivationId"},
			{Key: "date", Value: "$_id.date"},
			{Key: "temperature", Value: bson.D{
				{Key: "depth15", Value: bson.D{{Key: "$round", Value: bson.A{"$avgTemp15", 2}}}},
				{Key: "depth30", Value: bson.D{{Key: "$round", Value: bson.A{"$avgTemp30", 2}}}},
				{Key: "depth45", Value: bson.D{{Key: "$round", Value: bson.A{"$avgTemp45", 2}}}},
				{Key: "depth60", Value: bson.D{{Key: "$round", Value: bson.A{"$avgTemp60", 2}}}},
			}},
			{Key: "vwc", Value: bson.D{
				{Key: "depth15", Value: bson.D{{Key: "$round", Value: bson.A{"$avgVwc15", 2}}}},
				{Key: "depth30", Value: bson.D{{Key: "$round", Value: bson.A{"$avgVwc30", 2}}}},
				{Key: "depth45", Value: bson.D{{Key: "$round", Value: bson.A{"$avgVwc45", 2}}}},
				{Key: "depth60", Value: bson.D{{Key: "$round", Value: bson.A{"$avgVwc60", 2}}}},
			}},
			{Key: "sensorCount", Value: bson.D{{Key: "$size", Value: "$sensors"}}},
		}}},
	}

	return runPipelineAndInsert(ctx, rawColl, aggColl, pipeline, "cultivation_daily")
}

// ---------------------------------------------------------------------------
// cultivation_180d_rollup — recompute full-period VWC average per depth
// Q5 (rollup part): one doc per cultivation, avg VWC at each depth across
// the entire cultivation period.
// ---------------------------------------------------------------------------

func rewriteCultivation180dRollup(ctx context.Context, rawColl, aggColl *mongo.Collection) error {
	log.Println("[P3]   rewriting cultivation_180d_rollup…")

	res, err := aggColl.DeleteMany(ctx, bson.D{{Key: "type", Value: "cultivation_180d_rollup"}})
	if err != nil {
		return err
	}
	log.Printf("[P3]   deleted %d old cultivation_180d_rollup docs\n", res.DeletedCount)

	now := time.Now().UTC()
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$cultivation.cultivationId"},
			{Key: "avgVwc15", Value: bson.D{{Key: "$avg", Value: "$depth15.vwc"}}},
			{Key: "avgVwc30", Value: bson.D{{Key: "$avg", Value: "$depth30.vwc"}}},
			{Key: "avgVwc45", Value: bson.D{{Key: "$avg", Value: "$depth45.vwc"}}},
			{Key: "avgVwc60", Value: bson.D{{Key: "$avg", Value: "$depth60.vwc"}}},
		}}},
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "type", Value: bson.D{{Key: "$literal", Value: "cultivation_180d_rollup"}}},
			{Key: "cultivationId", Value: "$_id"},
			{Key: "windowEnd", Value: bson.D{{Key: "$literal", Value: now}}},
			{Key: "avgVwc180d", Value: bson.D{
				{Key: "depth15", Value: bson.D{{Key: "$round", Value: bson.A{"$avgVwc15", 2}}}},
				{Key: "depth30", Value: bson.D{{Key: "$round", Value: bson.A{"$avgVwc30", 2}}}},
				{Key: "depth45", Value: bson.D{{Key: "$round", Value: bson.A{"$avgVwc45", 2}}}},
				{Key: "depth60", Value: bson.D{{Key: "$round", Value: bson.A{"$avgVwc60", 2}}}},
			}},
		}}},
	}

	return runPipelineAndInsert(ctx, rawColl, aggColl, pipeline, "cultivation_180d_rollup")
}

// ---------------------------------------------------------------------------
// Shared: run a pipeline and batch-insert results into telemetry_aggregated
// ---------------------------------------------------------------------------

func runPipelineAndInsert(
	ctx context.Context,
	rawColl, aggColl *mongo.Collection,
	pipeline mongo.Pipeline,
	docType string,
) error {
	// AllowDiskUse is critical — at 50K sensors the intermediate results
	// exceed MongoDB's 100MB memory limit for aggregation.
	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := rawColl.Aggregate(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("aggregate: %w", err)
	}
	defer cursor.Close(ctx)

	const batchSize = 5000
	batch := make([]interface{}, 0, batchSize)
	var total int64

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		batch = append(batch, doc)

		if len(batch) >= batchSize {
			if _, err := aggColl.InsertMany(ctx, batch); err != nil {
				return fmt.Errorf("insert batch: %w", err)
			}
			total += int64(len(batch))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if _, err := aggColl.InsertMany(ctx, batch); err != nil {
			return fmt.Errorf("insert final batch: %w", err)
		}
		total += int64(len(batch))
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}

	log.Printf("[P3]   inserted %d %s docs\n", total, docType)
	return nil
}
