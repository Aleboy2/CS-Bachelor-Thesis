// Package main is the Go background worker for P2 (CQRS) and P3 (Lambda).
//
// Two modes, selected via --mode flag:
//
//	worker --mode p2  — Continuous Kafka consumer. Consumes telemetry-raw-events,
//	                    incrementally upserts telemetry_aggregated on every event.
//	worker --mode p3  — Hourly batch rewriter. Reads full telemetry_raw history via
//	                    MongoDB aggregation pipelines, rewrites telemetry_aggregated
//	                    from scratch. Sleeps, repeats.
//
// Not used in P1 — P1 has no background worker (all queries run live against
// telemetry_raw).
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	mode := flag.String("mode", "", "Worker mode: p2 (continuous) or p3 (batch)")
	mongoURI := flag.String("mongo-uri", "mongodb://mongodb:27017/iot_benchmark", "MongoDB connection URI")
	kafkaBroker := flag.String("kafka-broker", "kafka:9092", "Kafka broker address (P2 only)")
	batchInterval := flag.String("batch-interval", "1h", "Interval between batch rewrites (P3 only)")

	flag.Parse()

	switch *mode {
	case "p2":
		if err := RunP2(*mongoURI, *kafkaBroker); err != nil {
			fmt.Fprintf(os.Stderr, "[fatal] P2 worker: %v\n", err)
			os.Exit(1)
		}
	case "p3":
		if err := RunP3(*mongoURI, *batchInterval); err != nil {
			fmt.Fprintf(os.Stderr, "[fatal] P3 worker: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Usage: worker --mode p2|p3 [flags]\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
}
