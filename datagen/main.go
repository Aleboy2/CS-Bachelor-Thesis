// Package main is the CLI entry point for the synthetic data generator.
//
// Two subcommands:
//
//	datagen seed    — bulk-insert device_registry + 180 days of telemetry_raw
//	                  directly into MongoDB. Runs once before benchmarking.
//	datagen replay  — publish MQTT payloads at the target ingest rate for a
//	                  configurable duration. Measures write throughput and
//	                  ingest lag (RQ1).
//
// Both subcommands use the same deterministic entity generation (PRNG seed 42),
// so the sensor list, zone/cultivation assignments, and value distributions are
// identical across seed and replay, and across P1/P2/P3 runs.
//
// See CLAUDE.md §13 and §2 P0.6 for the locked design decisions this implements.
package main

import (
	"flag"
	"fmt"
	"os"
)

const defaultMongoURI = "mongodb://localhost:27017/iot_benchmark"
const defaultMQTTBroker = "tcp://localhost:1883"
const defaultPRNGSeed = 42

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "seed":
		runSeedCmd(os.Args[2:])
	case "replay":
		runReplayCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func runSeedCmd(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	sensors := fs.Int("sensors", 1000, "Number of sensors (1000, 10000, or 50000)")
	mongoURI := fs.String("mongo-uri", defaultMongoURI, "MongoDB connection URI")
	days := fs.Int("days", 180, "Total days of telemetry data to generate")
	batchSize := fs.Int("batch-size", 5000, "Documents per bulk-insert batch")
	prngSeed := fs.Int64("seed", int64(defaultPRNGSeed), "PRNG seed for deterministic generation")
	benchmarkStart := fs.String("benchmark-start", "2026-06-01T00:00:00Z",
		"ISO 8601 timestamp for the benchmark window start. Data spans [start-days .. start].")
	dropFirst := fs.Bool("drop", true, "Drop existing collections before seeding (recommended for clean runs)")

	fs.Parse(args)

	cfg := SeedConfig{
		SensorCount:    *sensors,
		MongoURI:       *mongoURI,
		Days:           *days,
		BatchSize:      *batchSize,
		PRNGSeed:       *prngSeed,
		BenchmarkStart: *benchmarkStart,
		DropFirst:      *dropFirst,
	}

	if err := RunSeed(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "seed failed: %v\n", err)
		os.Exit(1)
	}
}

func runReplayCmd(args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	sensors := fs.Int("sensors", 1000, "Number of sensors (must match seed run)")
	mqttBroker := fs.String("mqtt-broker", defaultMQTTBroker, "MQTT broker URL")
	duration := fs.String("duration", "5m", "Replay duration (e.g. 5m, 10m)")
	prngSeed := fs.Int64("seed", int64(defaultPRNGSeed), "PRNG seed (must match seed run)")

	fs.Parse(args)

	cfg := ReplayConfig{
		SensorCount: *sensors,
		MQTTBroker:  *mqttBroker,
		Duration:    *duration,
		PRNGSeed:    *prngSeed,
	}

	if err := RunReplay(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "replay failed: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: datagen <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  seed     Bulk-insert device_registry + telemetry_raw into MongoDB")
	fmt.Fprintln(os.Stderr, "  replay   Publish MQTT payloads at the target ingest rate")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Run 'datagen <subcommand> -help' for subcommand-specific flags.")
}
