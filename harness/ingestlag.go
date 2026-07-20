package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// IngestLagConfig holds parameters for the ingest-lag subcommand.
type IngestLagConfig struct {
	MongoURI  string
	Window    string // e.g. "5m"
	OutputCSV string
}

// RunIngestLag queries telemetry_raw for documents with ingestedAt within the
// given window, computes ingest lag statistics, and reports write throughput.
//
// Ingest lag = ingestedAt - timestamp (both set during the live replay,
// not during seed pre-population). This measures actual processing time
// through the MQTT → ingest API → MongoDB pipeline.
func RunIngestLag(cfg IngestLagConfig) error {
	window, err := time.ParseDuration(cfg.Window)
	if err != nil {
		return fmt.Errorf("invalid --window: %w", err)
	}

	ctx := context.Background()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return fmt.Errorf("MongoDB connect: %w", err)
	}
	defer client.Disconnect(ctx)

	coll := client.Database("iot_benchmark").Collection("telemetry_raw")

	// Find max ingestedAt to define the window end.
	opts := options.FindOne().SetSort(bson.D{{Key: "ingestedAt", Value: -1}}).SetProjection(bson.D{{Key: "ingestedAt", Value: 1}})
	var latest struct{ IngestedAt time.Time `bson:"ingestedAt"` }
	if err := coll.FindOne(ctx, bson.D{}, opts).Decode(&latest); err != nil {
		return fmt.Errorf("find max ingestedAt: %w", err)
	}

	windowStart := latest.IngestedAt.Add(-window)

	log.Printf("[ingest-lag] window: %s → %s (%s)\n",
		windowStart.Format(time.RFC3339), latest.IngestedAt.Format(time.RFC3339), window)

	// Query documents within the window.
	filter := bson.D{{Key: "ingestedAt", Value: bson.D{{Key: "$gte", Value: windowStart}}}}
	projection := bson.D{
		{Key: "timestamp", Value: 1},
		{Key: "ingestedAt", Value: 1},
	}
	findOpts := options.Find().SetProjection(projection)

	cursor, err := coll.Find(ctx, filter, findOpts)
	if err != nil {
		return fmt.Errorf("find: %w", err)
	}
	defer cursor.Close(ctx)

	var lags []float64 // milliseconds
	var count int
	var minIngest, maxIngest time.Time

	for cursor.Next(ctx) {
		var doc struct {
			Timestamp  time.Time `bson:"timestamp"`
			IngestedAt time.Time `bson:"ingestedAt"`
		}
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		lagMs := float64(doc.IngestedAt.Sub(doc.Timestamp).Microseconds()) / 1000.0

		// Filter out pre-populated documents (synthetic lag, typically 20-80ms,
		// with timestamps far in the past). Real replay documents have timestamp
		// ≈ ingestedAt (within seconds, not days).
		if lagMs > 60000 { // > 60 seconds = pre-populated, skip
			continue
		}
		if lagMs < 0 { // clock skew, skip
			continue
		}

		lags = append(lags, lagMs)
		count++

		if minIngest.IsZero() || doc.IngestedAt.Before(minIngest) {
			minIngest = doc.IngestedAt
		}
		if doc.IngestedAt.After(maxIngest) {
			maxIngest = doc.IngestedAt
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}

	if count == 0 {
		fmt.Println("No replay documents found in the window.")
		fmt.Println("Did you run 'datagen replay' before this command?")
		return nil
	}

	// --- Compute statistics ---
	sort.Float64s(lags)

	p50 := percentile(lags, 50)
	p95 := percentile(lags, 95)
	var sum float64
	for _, l := range lags {
		sum += l
	}
	mean := math.Round(sum/float64(count)*100) / 100

	// Write throughput.
	actualWindow := maxIngest.Sub(minIngest)
	var throughput float64
	if actualWindow > 0 {
		throughput = float64(count) / actualWindow.Seconds()
	}

	// --- Output ---
	fmt.Println()
	fmt.Printf("╔═══════════════════════════════════════════════════╗\n")
	fmt.Printf("║  INGEST LAG ANALYSIS                              ║\n")
	fmt.Printf("╠═══════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Documents analysed:   %6d                      ║\n", count)
	fmt.Printf("║  Actual window:        %s                   ║\n", actualWindow.Round(time.Second))
	fmt.Printf("║  Write throughput:     %6.1f docs/sec             ║\n", throughput)
	fmt.Printf("╠═══════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Ingest lag p50:       %7.2f ms                  ║\n", p50)
	fmt.Printf("║  Ingest lag p95:       %7.2f ms                  ║\n", p95)
	fmt.Printf("║  Ingest lag mean:      %7.2f ms                  ║\n", mean)
	fmt.Printf("║  Ingest lag min:       %7.2f ms                  ║\n", lags[0])
	fmt.Printf("║  Ingest lag max:       %7.2f ms                  ║\n", lags[count-1])
	fmt.Printf("╚═══════════════════════════════════════════════════╝\n")

	// --- Optional CSV ---
	if cfg.OutputCSV != "" {
		f, err := os.Create(cfg.OutputCSV)
		if err != nil {
			return err
		}
		defer f.Close()
		w := csv.NewWriter(f)
		defer w.Flush()

		w.Write([]string{"index", "lag_ms"})
		for i, l := range lags {
			w.Write([]string{strconv.Itoa(i + 1), fmt.Sprintf("%.3f", l)})
		}
		fmt.Printf("\nLag data written to %s\n", cfg.OutputCSV)
	}

	return nil
}
