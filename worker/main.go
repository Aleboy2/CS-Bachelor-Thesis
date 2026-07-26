// Package main is the Go background worker for P2 (CQRS) and P3 (Lambda).
//
// Four modes, selected via --mode flag:
//
//	worker --mode p2                — Continuous Kafka consumer. Consumes
//	                                   telemetry-raw-events, incrementally
//	                                   upserts telemetry_aggregated on every event.
//	worker --mode p3                — Hourly batch rewriter. Reads full
//	                                   telemetry_raw history via MongoDB
//	                                   aggregation pipelines, rewrites
//	                                   telemetry_aggregated from scratch. Sleeps,
//	                                   repeats.
//	worker --mode p2-backfill       — One-shot. Bootstraps telemetry_aggregated
//	                                   (all four doc types, including
//	                                   latest_by_sensor) from the full current
//	                                   contents of telemetry_raw. Run once after
//	                                   seeding, before relying on Q1/Q3/Q4/Q5
//	                                   results for P2. See backfill.go for why
//	                                   this is needed.
//	worker --mode p3-redis-backfill — One-shot. Bootstraps the Redis speed layer
//	                                   (latest:{serial} keys, 60m TTL) from the
//	                                   full current contents of telemetry_raw.
//	                                   Run shortly before querying P3 Q1 — see
//	                                   backfill.go for the TTL caveat.
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
	mode := flag.String("mode", "", "Worker mode: p2, p3, p2-backfill, or p3-redis-backfill")
	mongoURI := flag.String("mongo-uri", "mongodb://mongodb:27017/iot_benchmark", "MongoDB connection URI")
	kafkaBroker := flag.String("kafka-broker", "kafka:9092", "Kafka broker address (P2 only)")
	batchInterval := flag.String("batch-interval", "1h", "Interval between batch rewrites (P3 only)")
	redisAddr := flag.String("redis-addr", "redis:6379", "Redis address (p3-redis-backfill only)")

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
	case "p2-backfill":
		if err := RunP2Backfill(*mongoURI); err != nil {
			fmt.Fprintf(os.Stderr, "[fatal] P2 backfill: %v\n", err)
			os.Exit(1)
		}
	case "p2-backfill-latest":
		if err := RunP2BackfillLatestOnly(*mongoURI); err != nil {
			fmt.Fprintf(os.Stderr, "[fatal] P2 backfill-latest: %v\n", err)
			os.Exit(1)
		}
	case "p3-redis-backfill":
		if err := RunP3RedisBackfill(*mongoURI, *redisAddr); err != nil {
			fmt.Fprintf(os.Stderr, "[fatal] P3 Redis backfill: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Usage: worker --mode p2|p3|p2-backfill|p3-redis-backfill [flags]\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
}
