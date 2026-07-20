package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ---------------------------------------------------------------------------
// Connections bundle — passed to every query function
// ---------------------------------------------------------------------------

type Connections struct {
	DB        *mongo.Database
	RawColl   *mongo.Collection // telemetry_raw
	AggColl   *mongo.Collection // telemetry_aggregated
	Redis     *redis.Client     // nil for P1/P2
	Prototype string
	MaxTS     time.Time // max(telemetry_raw.timestamp) — all windows relative to this
}

// ---------------------------------------------------------------------------
// Target types — loaded from device_registry at startup
// ---------------------------------------------------------------------------

type QueryTargets struct {
	Serials        []string // sensor serial numbers
	ZoneIDs        []string // zone IDs
	CultivationIDs []string // cultivation IDs
}

// LoadTargets reads device_registry and extracts unique serials, zones, cultivations.
func LoadTargets(ctx context.Context, db *mongo.Database) (*QueryTargets, error) {
	coll := db.Collection("device_registry")
	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetProjection(bson.D{
		{Key: "serial", Value: 1},
		{Key: "zone.zoneId", Value: 1},
		{Key: "cultivation.cultivationId", Value: 1},
	}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	zoneSet := make(map[string]bool)
	cultSet := make(map[string]bool)
	t := &QueryTargets{}

	for cursor.Next(ctx) {
		var doc struct {
			Serial      string `bson:"serial"`
			Zone        struct{ ZoneID string `bson:"zoneId"` } `bson:"zone"`
			Cultivation struct{ CultivationID string `bson:"cultivationId"` } `bson:"cultivation"`
		}
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		t.Serials = append(t.Serials, doc.Serial)
		if !zoneSet[doc.Zone.ZoneID] {
			zoneSet[doc.Zone.ZoneID] = true
			t.ZoneIDs = append(t.ZoneIDs, doc.Zone.ZoneID)
		}
		if !cultSet[doc.Cultivation.CultivationID] {
			cultSet[doc.Cultivation.CultivationID] = true
			t.CultivationIDs = append(t.CultivationIDs, doc.Cultivation.CultivationID)
		}
	}

	return t, cursor.Err()
}

// FindMaxTimestamp returns the latest timestamp in telemetry_raw.
// All query time windows are computed relative to this value.
func FindMaxTimestamp(ctx context.Context, coll *mongo.Collection) (time.Time, error) {
	opts := options.FindOne().SetSort(bson.D{{Key: "timestamp", Value: -1}}).SetProjection(bson.D{{Key: "timestamp", Value: 1}})
	var doc struct{ Timestamp time.Time `bson:"timestamp"` }
	if err := coll.FindOne(ctx, bson.D{}, opts).Decode(&doc); err != nil {
		return time.Time{}, err
	}
	return doc.Timestamp, nil
}

// =========================================================================
// Q1 — Latest reading per sensor (point lookup)
// =========================================================================

func RunQ1(ctx context.Context, conn *Connections, serial string) error {
	switch conn.Prototype {
	case "p1":
		// findOne sorted by timestamp desc, using Index A.
		opts := options.FindOne().SetSort(bson.D{{Key: "timestamp", Value: -1}})
		var result bson.M
		return conn.RawColl.FindOne(ctx, bson.D{{Key: "sensor.serial", Value: serial}}, opts).Decode(&result)

	case "p2":
		// Direct _id lookup on telemetry_aggregated (latest_by_sensor).
		var result bson.M
		return conn.AggColl.FindOne(ctx, bson.D{{Key: "_id", Value: serial}}).Decode(&result)

	case "p3":
		// Redis GET — speed layer.
		val, err := conn.Redis.Get(ctx, "latest:"+serial).Result()
		if err != nil {
			return err
		}
		// Parse JSON to verify it's valid (included in measured latency).
		var result map[string]interface{}
		return json.Unmarshal([]byte(val), &result)

	default:
		return fmt.Errorf("unknown prototype: %s", conn.Prototype)
	}
}

// =========================================================================
// Q2 — All readings, single sensor, 24h range scan
// Same implementation for all prototypes (reads telemetry_raw).
// =========================================================================

func RunQ2(ctx context.Context, conn *Connections, serial string) error {
	since := conn.MaxTS.Add(-24 * time.Hour)
	filter := bson.D{
		{Key: "sensor.serial", Value: serial},
		{Key: "timestamp", Value: bson.D{{Key: "$gte", Value: since}}},
	}
	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: -1}})

	cursor, err := conn.RawColl.Find(ctx, filter, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	// Drain cursor — all ~96 docs must be read (included in measured latency).
	var doc bson.M
	for cursor.Next(ctx) {
		if err := cursor.Decode(&doc); err != nil {
			return err
		}
	}
	return cursor.Err()
}

// =========================================================================
// Q3 — Avg VWC across zone, last 7 days
// P1: aggregation pipeline on telemetry_raw
// P2/P3: lookup zone_daily docs from telemetry_aggregated
// =========================================================================

func RunQ3(ctx context.Context, conn *Connections, zoneID string) error {
	since := conn.MaxTS.Add(-7 * 24 * time.Hour)

	switch conn.Prototype {
	case "p1":
		pipeline := mongo.Pipeline{
			bson.D{{Key: "$match", Value: bson.D{
				{Key: "zone.zoneId", Value: zoneID},
				{Key: "timestamp", Value: bson.D{{Key: "$gte", Value: since}}},
			}}},
			bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: nil},
				{Key: "sumVwc", Value: bson.D{{Key: "$sum", Value: bson.D{{Key: "$add", Value: bson.A{
					"$depth15.vwc", "$depth30.vwc", "$depth45.vwc", "$depth60.vwc",
				}}}}}},
				{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			}}},
			bson.D{{Key: "$project", Value: bson.D{
				{Key: "_id", Value: 0},
				{Key: "avgVwc", Value: bson.D{{Key: "$round", Value: bson.A{
					bson.D{{Key: "$divide", Value: bson.A{
						"$sumVwc",
						bson.D{{Key: "$multiply", Value: bson.A{"$count", 4}}},
					}}},
					2,
				}}}},
			}}},
		}
		cursor, err := conn.RawColl.Aggregate(ctx, pipeline)
		if err != nil {
			return err
		}
		defer cursor.Close(ctx)
		var result bson.M
		if cursor.Next(ctx) {
			return cursor.Decode(&result)
		}
		return cursor.Err()

	default: // p2, p3
		sinceDay := truncateToDay(since)
		filter := bson.D{
			{Key: "type", Value: "zone_daily"},
			{Key: "zoneId", Value: zoneID},
			{Key: "date", Value: bson.D{{Key: "$gte", Value: sinceDay}}},
		}
		cursor, err := conn.AggColl.Find(ctx, filter)
		if err != nil {
			return err
		}
		defer cursor.Close(ctx)
		var doc bson.M
		for cursor.Next(ctx) {
			if err := cursor.Decode(&doc); err != nil {
				return err
			}
		}
		return cursor.Err()
	}
}

// =========================================================================
// Q4 — Daily avg temperature at all 4 depths, single cultivation, 30 days
// P1: aggregation pipeline
// P2/P3: lookup cultivation_daily docs
// =========================================================================

func RunQ4(ctx context.Context, conn *Connections, cultivationID string) error {
	since := conn.MaxTS.Add(-30 * 24 * time.Hour)

	switch conn.Prototype {
	case "p1":
		pipeline := mongo.Pipeline{
			bson.D{{Key: "$match", Value: bson.D{
				{Key: "cultivation.cultivationId", Value: cultivationID},
				{Key: "timestamp", Value: bson.D{{Key: "$gte", Value: since}}},
			}}},
			bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: bson.D{{Key: "$dateTrunc", Value: bson.D{
					{Key: "date", Value: "$timestamp"},
					{Key: "unit", Value: "day"},
				}}}},
				{Key: "avgTemp15", Value: bson.D{{Key: "$avg", Value: "$depth15.temperature"}}},
				{Key: "avgTemp30", Value: bson.D{{Key: "$avg", Value: "$depth30.temperature"}}},
				{Key: "avgTemp45", Value: bson.D{{Key: "$avg", Value: "$depth45.temperature"}}},
				{Key: "avgTemp60", Value: bson.D{{Key: "$avg", Value: "$depth60.temperature"}}},
			}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
		}
		cursor, err := conn.RawColl.Aggregate(ctx, pipeline)
		if err != nil {
			return err
		}
		defer cursor.Close(ctx)
		var doc bson.M
		for cursor.Next(ctx) {
			if err := cursor.Decode(&doc); err != nil {
				return err
			}
		}
		return cursor.Err()

	default: // p2, p3
		sinceDay := truncateToDay(since)
		filter := bson.D{
			{Key: "type", Value: "cultivation_daily"},
			{Key: "cultivationId", Value: cultivationID},
			{Key: "date", Value: bson.D{{Key: "$gte", Value: sinceDay}}},
		}
		opts := options.Find().SetSort(bson.D{{Key: "date", Value: 1}})
		cursor, err := conn.AggColl.Find(ctx, filter, opts)
		if err != nil {
			return err
		}
		defer cursor.Close(ctx)
		var doc bson.M
		for cursor.Next(ctx) {
			if err := cursor.Decode(&doc); err != nil {
				return err
			}
		}
		return cursor.Err()
	}
}

// =========================================================================
// Q5 — Avg VWC at all 4 depths, full cultivation period (up to 180 days)
//       Returns 180 daily points + 1 rollup = 4 × 181 = 724 values
// P1: $facet aggregation pipeline (daily + rollup in one pass)
// P2/P3: read cultivation_daily docs + cultivation_180d_rollup doc
// =========================================================================

func RunQ5(ctx context.Context, conn *Connections, cultivationID string) error {
	switch conn.Prototype {
	case "p1":
		pipeline := mongo.Pipeline{
			bson.D{{Key: "$match", Value: bson.D{
				{Key: "cultivation.cultivationId", Value: cultivationID},
			}}},
			bson.D{{Key: "$facet", Value: bson.D{
				{Key: "dailyAverages", Value: bson.A{
					bson.D{{Key: "$group", Value: bson.D{
						{Key: "_id", Value: bson.D{{Key: "$dateTrunc", Value: bson.D{
							{Key: "date", Value: "$timestamp"},
							{Key: "unit", Value: "day"},
						}}}},
						{Key: "avgVwc15", Value: bson.D{{Key: "$avg", Value: "$depth15.vwc"}}},
						{Key: "avgVwc30", Value: bson.D{{Key: "$avg", Value: "$depth30.vwc"}}},
						{Key: "avgVwc45", Value: bson.D{{Key: "$avg", Value: "$depth45.vwc"}}},
						{Key: "avgVwc60", Value: bson.D{{Key: "$avg", Value: "$depth60.vwc"}}},
					}}},
					bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
				}},
				{Key: "overallRollup", Value: bson.A{
					bson.D{{Key: "$group", Value: bson.D{
						{Key: "_id", Value: nil},
						{Key: "avgVwc15", Value: bson.D{{Key: "$avg", Value: "$depth15.vwc"}}},
						{Key: "avgVwc30", Value: bson.D{{Key: "$avg", Value: "$depth30.vwc"}}},
						{Key: "avgVwc45", Value: bson.D{{Key: "$avg", Value: "$depth45.vwc"}}},
						{Key: "avgVwc60", Value: bson.D{{Key: "$avg", Value: "$depth60.vwc"}}},
					}}},
				}},
			}}},
		}
		cursor, err := conn.RawColl.Aggregate(ctx, pipeline)
		if err != nil {
			return err
		}
		defer cursor.Close(ctx)
		var result bson.M
		if cursor.Next(ctx) {
			return cursor.Decode(&result)
		}
		return cursor.Err()

	default: // p2, p3
		// Read all cultivation_daily docs (up to 180).
		filter := bson.D{
			{Key: "type", Value: "cultivation_daily"},
			{Key: "cultivationId", Value: cultivationID},
		}
		opts := options.Find().SetSort(bson.D{{Key: "date", Value: 1}})
		cursor, err := conn.AggColl.Find(ctx, filter, opts)
		if err != nil {
			return err
		}
		var doc bson.M
		for cursor.Next(ctx) {
			if err := cursor.Decode(&doc); err != nil {
				cursor.Close(ctx)
				return err
			}
		}
		cursor.Close(ctx)
		if err := cursor.Err(); err != nil {
			return err
		}

		// Read the single rollup document.
		var rollup bson.M
		return conn.AggColl.FindOne(ctx, bson.D{
			{Key: "type", Value: "cultivation_180d_rollup"},
			{Key: "cultivationId", Value: cultivationID},
		}).Decode(&rollup)
	}
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func truncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
