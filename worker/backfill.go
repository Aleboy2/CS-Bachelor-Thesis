package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// =============================================================================
// RunP2Backfill — one-shot bootstrap of telemetry_aggregated for P2.
//
// Why this exists: P2's continuous worker (p2.go) only ever processes events
// it consumes from Kafka. The 180-day historical dataset is written directly
// into telemetry_raw by `datagen seed`, bypassing MQTT/Kafka entirely — so
// without this backfill, P2's telemetry_aggregated only ever contains entries
// for whatever dates the live replay happened to cover, while P1 and P3 (whose
// batch worker already does a full unfiltered rewrite on startup, see p3.go)
// have the complete history available. That asymmetry silently invalidates
// the Q3/Q4/Q5 comparison for P2 once query windows are anchored to a fixed
// historical date rather than live wall-clock time.
//
// This reuses the exact same three aggregation pipelines P3's batch worker
// already runs (zone_daily, cultivation_daily, cultivation_180d_rollup) over
// the full current contents of telemetry_raw — seed data plus anything
// replayed so far — and additionally computes latest_by_sensor for every
// sensor, which P2 normally only builds incrementally from live Kafka events.
//
// Safe to run once, right after seeding and before/alongside the continuous
// worker: every write here is a replaceOne-based upsert against freshly
// recomputed values, so there is no double-counting. The continuous worker
// can keep incrementing on top of this baseline afterward exactly as before.
//
// NOT safe to run repeatedly while the continuous worker is also processing
// live events for the same window — the two would race on read-modify-write
// increments for zone_daily/cultivation_daily. For a one-time historical
// bootstrap immediately after seeding, this is not a concern.
// =============================================================================

func RunP2Backfill(mongoURI string) error {
	ctx := context.Background()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("MongoDB connect: %w", err)
	}
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("MongoDB ping: %w", err)
	}
	log.Println("[P2-backfill] connected to MongoDB")

	db := client.Database("iot_benchmark")
	rawColl := db.Collection("telemetry_raw")
	aggColl := db.Collection("telemetry_aggregated")

	start := time.Now()

	// Reuse P3's full-history rewrite for zone_daily, cultivation_daily,
	// cultivation_180d_rollup — identical shape, identical pipelines. The only
	// thing P2 needs on top is latest_by_sensor (P3 never writes this to Mongo
	// at all — see RunP3RedisBackfill below for P3's equivalent gap).
	log.Println("[P2-backfill] rewriting zone_daily…")
	if err := rewriteZoneDaily(ctx, rawColl, aggColl); err != nil {
		return fmt.Errorf("zone_daily: %w", err)
	}
	log.Println("[P2-backfill] rewriting cultivation_daily…")
	if err := rewriteCultivationDaily(ctx, rawColl, aggColl); err != nil {
		return fmt.Errorf("cultivation_daily: %w", err)
	}
	log.Println("[P2-backfill] rewriting cultivation_180d_rollup…")
	if err := rewriteCultivation180dRollup(ctx, rawColl, aggColl); err != nil {
		return fmt.Errorf("cultivation_180d_rollup: %w", err)
	}

	log.Println("[P2-backfill] rewriting latest_by_sensor…")
	n, err := backfillLatestBySensor(ctx, rawColl, aggColl)
	if err != nil {
		return fmt.Errorf("latest_by_sensor: %w", err)
	}
	log.Printf("[P2-backfill] wrote %d latest_by_sensor docs\n", n)

	log.Printf("[P2-backfill] complete in %s\n", time.Since(start).Round(time.Second))
	return nil
}

// RunP2BackfillLatestOnly runs ONLY the latest_by_sensor stage of the P2
// backfill, leaving zone_daily / cultivation_daily / cultivation_180d_rollup
// untouched. Use this to resume a backfill that already completed those three
// heavy aggregations but died before (or during) latest_by_sensor — e.g. the
// 50K run that ran out of disk on the old sort-based pipeline. Avoids redoing
// the multi-hour daily aggregations. Produces byte-identical latest_by_sensor
// docs to the full backfill, since it calls the exact same function.
func RunP2BackfillLatestOnly(mongoURI string) error {
	ctx := context.Background()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("MongoDB connect: %w", err)
	}
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("MongoDB ping: %w", err)
	}
	log.Println("[P2-backfill-latest] connected to MongoDB")

	db := client.Database("iot_benchmark")
	rawColl := db.Collection("telemetry_raw")
	aggColl := db.Collection("telemetry_aggregated")

	start := time.Now()
	log.Println("[P2-backfill-latest] rewriting latest_by_sensor only…")
	n, err := backfillLatestBySensor(ctx, rawColl, aggColl)
	if err != nil {
		return fmt.Errorf("latest_by_sensor: %w", err)
	}
	log.Printf("[P2-backfill-latest] wrote %d latest_by_sensor docs in %s\n",
		n, time.Since(start).Round(time.Second))
	return nil
}

// backfillLatestBySensor finds, for every sensor, the telemetry_raw document
// with the maximum timestamp, and upserts it into telemetry_aggregated as a
// latest_by_sensor document — same shape as the continuous worker produces
// per-event (p2.go upsertLatestBySensor), just computed for all sensors at
// once instead of incrementally.
func backfillLatestBySensor(ctx context.Context, rawColl, aggColl *mongo.Collection) (int64, error) {
	// Delete existing latest_by_sensor docs first — this is a full rewrite,
	// mirroring how P3's batch rewrite deletes-then-reinserts each doc type.
	if _, err := aggColl.DeleteMany(ctx, bson.D{{Key: "type", Value: "latest_by_sensor"}}); err != nil {
		return 0, fmt.Errorf("delete existing: %w", err)
	}

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$sensor.serial"},
			{Key: "doc", Value: bson.D{{Key: "$top", Value: bson.D{
				{Key: "sortBy", Value: bson.D{{Key: "timestamp", Value: -1}}},
				{Key: "output", Value: "$$ROOT"},
			}}}},
		}}},
		bson.D{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: "$doc"}}}},
	}

	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := rawColl.Aggregate(ctx, pipeline, opts)
	if err != nil {
		return 0, fmt.Errorf("aggregate: %w", err)
	}
	defer cursor.Close(ctx)

	const batchSize = 5000
	batch := make([]mongo.WriteModel, 0, batchSize)
	var total int64

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, err := aggColl.BulkWrite(ctx, batch); err != nil {
			return err
		}
		total += int64(len(batch))
		batch = batch[:0]
		return nil
	}

	for cursor.Next(ctx) {
		var raw struct {
			Timestamp   time.Time `bson:"timestamp"`
			Sensor      bson.M    `bson:"sensor"`
			Zone        bson.M    `bson:"zone"`
			Cultivation bson.M    `bson:"cultivation"`
			Depth15     bson.M    `bson:"depth15"`
			Depth30     bson.M    `bson:"depth30"`
			Depth45     bson.M    `bson:"depth45"`
			Depth60     bson.M    `bson:"depth60"`
		}
		if err := cursor.Decode(&raw); err != nil {
			return total, fmt.Errorf("decode: %w", err)
		}

		serial, _ := raw.Sensor["serial"].(string)
		if serial == "" {
			continue
		}

		doc := bson.D{
			{Key: "_id", Value: serial},
			{Key: "type", Value: "latest_by_sensor"},
			{Key: "timestamp", Value: raw.Timestamp},
			{Key: "zone", Value: raw.Zone},
			{Key: "cultivation", Value: raw.Cultivation},
			{Key: "depth15", Value: raw.Depth15},
			{Key: "depth30", Value: raw.Depth30},
			{Key: "depth45", Value: raw.Depth45},
			{Key: "depth60", Value: raw.Depth60},
		}

		model := mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: serial}}).
			SetReplacement(doc).
			SetUpsert(true)
		batch = append(batch, model)

		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return total, fmt.Errorf("bulk write: %w", err)
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return total, fmt.Errorf("cursor: %w", err)
	}
	if err := flush(); err != nil {
		return total, fmt.Errorf("bulk write final: %w", err)
	}

	return total, nil
}

// =============================================================================
// RunP3RedisBackfill — one-shot bootstrap of the Redis speed layer for P3.
//
// Why this exists: P3's telemetry_aggregated is fully covered by the batch
// worker's unfiltered rewrite (p3.go), but Redis is only ever written by the
// live ingest API on real-time inserts (ingest-api/src/index.js step 5c) —
// never from seed data, never by the batch worker. Per the locked P0.2
// decision, P3's Q1 reads Redis exclusively with no fallback, so without this
// backfill Q1 in P3 has the identical coverage gap P2 has for
// latest_by_sensor: only sensors that fired during a live replay have an
// entry, everyone else misses.
//
// Writes the same key shape and TTL the ingest API already uses
// (ingest-api/src/redis.js): key `latest:{serial}`, 3600s TTL. Because the
// TTL is real wall-clock time, these keys expire 60 minutes after this
// command runs — run it shortly before the query benchmark for the same
// scale, not as a one-time setup step done far in advance.
// =============================================================================

func RunP3RedisBackfill(mongoURI, redisAddr string) error {
	ctx := context.Background()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("MongoDB connect: %w", err)
	}
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("MongoDB ping: %w", err)
	}
	log.Println("[P3-redis-backfill] connected to MongoDB")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis ping: %w", err)
	}
	log.Println("[P3-redis-backfill] connected to Redis")

	rawColl := client.Database("iot_benchmark").Collection("telemetry_raw")

	start := time.Now()

	// Per-sensor indexed lookup instead of a collection-wide $group.
	//
	// A {$group, $top} over telemetry_raw does NOT use any index — MongoDB
	// must scan all 864M documents to build the groups, which hangs for hours
	// at 50K scale. Instead we read the serial list from device_registry
	// (50K tiny docs) and, for each, do a FindOne sorted by timestamp desc.
	// That lookup is served directly by the existing compound index
	// {sensor.serial:1, timestamp:-1} in ~1ms, so the whole backfill is
	// ~50K fast indexed reads that stream with steady, observable progress
	// rather than one monolithic multi-hour scan.
	regColl := client.Database("iot_benchmark").Collection("device_registry")

	regCursor, err := regColl.Find(ctx, bson.D{},
		options.Find().SetProjection(bson.D{{Key: "serial", Value: 1}, {Key: "_id", Value: 0}}))
	if err != nil {
		return fmt.Errorf("device_registry find: %w", err)
	}
	defer regCursor.Close(ctx)

	const ttl = 3600 * time.Second
	var total int64
	pipe := rdb.Pipeline()
	const flushEvery = 1000
	var pending int

	findOpts := options.FindOne().SetSort(bson.D{{Key: "timestamp", Value: -1}})

	for regCursor.Next(ctx) {
		var reg struct {
			Serial string `bson:"serial"`
		}
		if err := regCursor.Decode(&reg); err != nil {
			return fmt.Errorf("decode registry: %w", err)
		}
		if reg.Serial == "" {
			continue
		}

		var raw struct {
			Timestamp   time.Time `bson:"timestamp"`
			Sensor      bson.M    `bson:"sensor"`
			Zone        bson.M    `bson:"zone"`
			Cultivation bson.M    `bson:"cultivation"`
			Depth15     bson.M    `bson:"depth15"`
			Depth30     bson.M    `bson:"depth30"`
			Depth45     bson.M    `bson:"depth45"`
			Depth60     bson.M    `bson:"depth60"`
		}
		err := rawColl.FindOne(ctx,
			bson.D{{Key: "sensor.serial", Value: reg.Serial}}, findOpts).Decode(&raw)
		if err == mongo.ErrNoDocuments {
			continue // sensor has no readings (shouldn't happen with seeded data)
		}
		if err != nil {
			return fmt.Errorf("findOne serial=%s: %w", reg.Serial, err)
		}

		serial, _ := raw.Sensor["serial"].(string)
		if serial == "" {
			serial = reg.Serial
		}

		// Same value shape as ingest-api/src/redis.js setLatestReading.
		value := map[string]interface{}{
			"serial":      serial,
			"timestamp":   raw.Timestamp.Format(time.RFC3339Nano),
			"zone":        raw.Zone,
			"cultivation": raw.Cultivation,
			"depth15":     raw.Depth15,
			"depth30":     raw.Depth30,
			"depth45":     raw.Depth45,
			"depth60":     raw.Depth60,
		}
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("marshal serial=%s: %w", serial, err)
		}

		pipe.Set(ctx, "latest:"+serial, jsonBytes, ttl)
		pending++
		total++

		if pending >= flushEvery {
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("redis pipeline exec: %w", err)
			}
			pending = 0
			log.Printf("[P3-redis-backfill] %d keys written…\n", total)
		}
	}
	if err := regCursor.Err(); err != nil {
		return fmt.Errorf("registry cursor: %w", err)
	}
	if pending > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("redis pipeline exec final: %w", err)
		}
	}

	log.Printf("[P3-redis-backfill] wrote %d keys (TTL 60m) in %s\n", total, time.Since(start).Round(time.Second))
	return nil
}

