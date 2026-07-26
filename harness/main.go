// Package main is the benchmark harness for measuring query latency and ingest
// lag across the three prototypes (P1, P2, P3).
//
// Runs on the HOST machine, outside Docker, to avoid measurement interference
// (LOCKED — CLAUDE.md §5 tech stack). Connects to MongoDB on localhost:27017
// and Redis on localhost:6379 (P3 Q1 only).
//
// Two subcommands:
//
//	harness query       — run Q1–Q5 with warm-up + timed iterations, output CSV
//	harness ingest-lag  — compute ingest lag stats from recently written docs
package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "query":
		runQueryCmd(os.Args[2:])
	case "ingest-lag":
		runIngestLagCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func runQueryCmd(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	prototype := fs.String("prototype", "", "Prototype to benchmark: p1, p2, or p3")
	mongoURI := fs.String("mongo-uri", "mongodb://localhost:27017/iot_benchmark", "MongoDB URI")
	redisURL := fs.String("redis-url", "redis://localhost:6379", "Redis URL (P3 Q1 only)")
	iterations := fs.Int("iterations", 100, "Timed iterations per query type")
	warmup := fs.Int("warmup", 10, "Untimed warm-up iterations")
	output := fs.String("output", "", "Output CSV file path (default: stdout summary only)")
	seed := fs.Int64("seed", 42, "PRNG seed for target selection")
	anchor := fs.String("anchor", "2026-06-01T00:00:00Z",
		"Fixed reference point (RFC3339) for Q2-Q5 windows. Must match datagen "+
			"seed's --benchmark-start for that scale, so windows fall inside the "+
			"dense seeded history rather than a live-traffic gap. Do NOT default "+
			"this to live wall-clock time — see CLAUDE.md discussion on anchor date.")

	fs.Parse(args)

	if *prototype == "" {
		fmt.Fprintln(os.Stderr, "Error: --prototype is required (p1, p2, or p3)")
		os.Exit(1)
	}

	anchorTime, err := time.Parse(time.RFC3339, *anchor)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid --anchor %q: %v\n", *anchor, err)
		os.Exit(1)
	}

	cfg := QueryConfig{
		Prototype:  *prototype,
		MongoURI:   *mongoURI,
		RedisURL:   *redisURL,
		Iterations: *iterations,
		Warmup:     *warmup,
		OutputCSV:  *output,
		Seed:       *seed,
		Anchor:     anchorTime,
	}

	if err := RunQueryBenchmark(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "query benchmark failed: %v\n", err)
		os.Exit(1)
	}
}

func runIngestLagCmd(args []string) {
	fs := flag.NewFlagSet("ingest-lag", flag.ExitOnError)
	mongoURI := fs.String("mongo-uri", "mongodb://localhost:27017/iot_benchmark", "MongoDB URI")
	window := fs.String("window", "5m", "Time window to analyse (e.g. 5m, 10m)")
	output := fs.String("output", "", "Output CSV file path (optional)")

	fs.Parse(args)

	cfg := IngestLagConfig{
		MongoURI:  *mongoURI,
		Window:    *window,
		OutputCSV: *output,
	}

	if err := RunIngestLag(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "ingest-lag analysis failed: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: harness <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  query       Run Q1–Q5 benchmark (warm-up + timed iterations)")
	fmt.Fprintln(os.Stderr, "  ingest-lag  Compute ingest lag + write throughput from recent docs")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Run 'harness <subcommand> -help' for subcommand-specific flags.")
}
