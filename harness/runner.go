package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// QueryConfig holds all parameters for the query benchmark.
type QueryConfig struct {
	Prototype  string
	MongoURI   string
	RedisURL   string
	Iterations int
	Warmup     int
	OutputCSV  string
	Seed       int64
}

// QueryResult holds latency measurements for one query type.
type QueryResult struct {
	Name      string
	Latencies []time.Duration
}

// RunQueryBenchmark executes Q1–Q5 with warm-up and timed iterations.
func RunQueryBenchmark(cfg QueryConfig) error {
	ctx := context.Background()

	// --- Connect to MongoDB ---
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return fmt.Errorf("MongoDB connect: %w", err)
	}
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("MongoDB ping: %w", err)
	}
	log.Println("[harness] connected to MongoDB")

	db := client.Database("iot_benchmark")
	conn := &Connections{
		DB:        db,
		RawColl:   db.Collection("telemetry_raw"),
		AggColl:   db.Collection("telemetry_aggregated"),
		Prototype: cfg.Prototype,
	}

	// --- Connect to Redis (P3 only) ---
	if cfg.Prototype == "p3" {
		opt, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return fmt.Errorf("Redis URL parse: %w", err)
		}
		conn.Redis = redis.NewClient(opt)
		if err := conn.Redis.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("Redis ping: %w", err)
		}
		defer conn.Redis.Close()
		log.Println("[harness] connected to Redis")
	}

	// --- Find max timestamp (all query windows relative to this) ---
	maxTS, err := FindMaxTimestamp(ctx, conn.RawColl)
	if err != nil {
		return fmt.Errorf("find max timestamp: %w", err)
	}
	conn.MaxTS = maxTS
	log.Printf("[harness] max timestamp: %s\n", maxTS.Format(time.RFC3339))

	// --- Load query targets ---
	targets, err := LoadTargets(ctx, db)
	if err != nil {
		return fmt.Errorf("load targets: %w", err)
	}
	log.Printf("[harness] loaded targets: %d sensors, %d zones, %d cultivations\n",
		len(targets.Serials), len(targets.ZoneIDs), len(targets.CultivationIDs))

	// --- Validate we have enough targets ---
	needed := cfg.Warmup + cfg.Iterations
	if len(targets.Serials) < needed || len(targets.ZoneIDs) < needed || len(targets.CultivationIDs) < needed {
		log.Printf("[harness] warning: fewer targets than iterations — some targets will repeat")
	}

	rng := rand.New(rand.NewSource(cfg.Seed))

	fmt.Printf("\n=== QUERY BENCHMARK: %s ===\n", cfg.Prototype)
	fmt.Printf("Iterations: %d (+ %d warm-up)\n", cfg.Iterations, cfg.Warmup)
	fmt.Printf("Max timestamp: %s\n\n", maxTS.Format("2006-01-02 15:04:05"))

	// --- Define the query suite ---
	type queryDef struct {
		name    string
		targets []string // pool to pick from
		run     func(ctx context.Context, conn *Connections, target string) error
	}

	suite := []queryDef{
		{"Q1", targets.Serials, RunQ1},
		{"Q2", targets.Serials, RunQ2},
		{"Q3", targets.ZoneIDs, RunQ3},
		{"Q4", targets.CultivationIDs, RunQ4},
		{"Q5", targets.CultivationIDs, RunQ5},
	}

	var allResults []QueryResult

	for _, q := range suite {
		result := QueryResult{Name: q.name}

		// Shuffle targets for this query type.
		shuffled := make([]string, len(q.targets))
		copy(shuffled, q.targets)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

		// Warm-up (untimed).
		fmt.Printf("[%s] warming up (%d iterations)…\n", q.name, cfg.Warmup)
		for i := 0; i < cfg.Warmup; i++ {
			target := shuffled[i%len(shuffled)]
			if err := q.run(ctx, conn, target); err != nil {
				log.Printf("[%s] warm-up error (target=%s): %v\n", q.name, target, err)
			}
		}

		// Timed iterations.
		fmt.Printf("[%s] running %d timed iterations…\n", q.name, cfg.Iterations)
		for i := 0; i < cfg.Iterations; i++ {
			target := shuffled[(cfg.Warmup+i)%len(shuffled)]

			start := time.Now()
			err := q.run(ctx, conn, target)
			elapsed := time.Since(start)

			if err != nil {
				log.Printf("[%s] error (iteration=%d, target=%s): %v\n", q.name, i+1, target, err)
				continue
			}

			result.Latencies = append(result.Latencies, elapsed)
		}

		if len(result.Latencies) > 0 {
			stats := computeStats(result.Latencies)
			fmt.Printf("[%s] p50=%.2fms  p95=%.2fms  mean=%.2fms  min=%.2fms  max=%.2fms  (n=%d)\n",
				q.name, stats.P50, stats.P95, stats.Mean, stats.Min, stats.Max, stats.N)
		} else {
			fmt.Printf("[%s] no successful iterations\n", q.name)
		}

		allResults = append(allResults, result)
	}

	// --- CSV output ---
	if cfg.OutputCSV != "" {
		if err := writeCSV(cfg, allResults); err != nil {
			return fmt.Errorf("write CSV: %w", err)
		}
		fmt.Printf("\nResults written to %s\n", cfg.OutputCSV)
	}

	// --- Summary table ---
	fmt.Println()
	printSummaryTable(cfg.Prototype, allResults)

	return nil
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

type Stats struct {
	N    int
	P50  float64 // milliseconds
	P95  float64
	Mean float64
	Min  float64
	Max  float64
}

func computeStats(latencies []time.Duration) Stats {
	n := len(latencies)
	if n == 0 {
		return Stats{}
	}

	// Sort for percentiles.
	sorted := make([]float64, n)
	var sum float64
	for i, l := range latencies {
		ms := float64(l.Microseconds()) / 1000.0
		sorted[i] = ms
		sum += ms
	}
	sort.Float64s(sorted)

	return Stats{
		N:    n,
		P50:  percentile(sorted, 50),
		P95:  percentile(sorted, 95),
		Mean: math.Round(sum/float64(n)*100) / 100,
		Min:  sorted[0],
		Max:  sorted[n-1],
	}
}

func percentile(sorted []float64, pct int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := float64(pct) / 100.0 * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper {
		return math.Round(sorted[lower]*100) / 100
	}
	frac := idx - float64(lower)
	val := sorted[lower]*(1-frac) + sorted[upper]*frac
	return math.Round(val*100) / 100
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

func writeCSV(cfg QueryConfig, results []QueryResult) error {
	f, err := os.Create(cfg.OutputCSV)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write([]string{"prototype", "query", "iteration", "latency_ms"})

	for _, r := range results {
		for i, l := range r.Latencies {
			ms := fmt.Sprintf("%.3f", float64(l.Microseconds())/1000.0)
			w.Write([]string{cfg.Prototype, r.Name, strconv.Itoa(i + 1), ms})
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Summary table
// ---------------------------------------------------------------------------

func printSummaryTable(prototype string, results []QueryResult) {
	fmt.Printf("╔══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  BENCHMARK SUMMARY — %s                                    ║\n", prototype)
	fmt.Printf("╠═══════╦═══════════╦═══════════╦═══════════╦════════════════╣\n")
	fmt.Printf("║ Query ║   p50 ms  ║   p95 ms  ║  mean ms  ║  iterations    ║\n")
	fmt.Printf("╠═══════╬═══════════╬═══════════╬═══════════╬════════════════╣\n")

	for _, r := range results {
		if len(r.Latencies) == 0 {
			fmt.Printf("║  %s   ║     —     ║     —     ║     —     ║       0        ║\n", r.Name)
			continue
		}
		s := computeStats(r.Latencies)
		fmt.Printf("║  %s   ║ %7.2f   ║ %7.2f   ║ %7.2f   ║ %6d         ║\n",
			r.Name, s.P50, s.P95, s.Mean, s.N)
	}

	fmt.Printf("╚═══════╩═══════════╩═══════════╩═══════════╩════════════════╝\n")
}
