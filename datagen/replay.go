package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// ReplayConfig holds all parameters for the replay subcommand.
type ReplayConfig struct {
	SensorCount int
	MQTTBroker  string
	Duration    string // parsed to time.Duration
	PRNGSeed    int64
}

// mqttPayload is the lean payload structure published to sensors/telemetry.
// Matches CLAUDE.md §3 MQTT payload definition (with timestamp).
type mqttPayload struct {
	Timestamp string       `json:"timestamp"`
	Sensor    mqttSensor   `json:"sensor"`
	Depth15   mqttDepth    `json:"depth15"`
	Depth30   mqttDepth    `json:"depth30"`
	Depth45   mqttDepth    `json:"depth45"`
	Depth60   mqttDepth    `json:"depth60"`
}

type mqttSensor struct {
	Serial  string `json:"serial"`
	Battery int    `json:"battery"`
}

type mqttDepth struct {
	VWC         float64 `json:"vwc"`
	Temperature float64 `json:"temperature"`
	EC          float64 `json:"ec"`
}

// RunReplay publishes MQTT payloads at the target ingest rate.
//
// Target rate = sensorCount / 900 messages per second.
// (Each sensor transmits every 15 min = 900 sec.)
//
// The replay round-robins through sensors, publishing one message per tick.
// Each message gets timestamp = time.Now(), so the ingest API's ingestedAt
// minus this timestamp gives the real processing lag for RQ1.
func RunReplay(cfg ReplayConfig) error {
	duration, err := time.ParseDuration(cfg.Duration)
	if err != nil {
		return fmt.Errorf("invalid --duration: %w", err)
	}

	targetRate := float64(cfg.SensorCount) / 900.0
	interval := time.Duration(float64(time.Second) / targetRate)

	fmt.Printf("=== REPLAY MODE ===\n")
	fmt.Printf("Sensors:       %d\n", cfg.SensorCount)
	fmt.Printf("Target rate:   %.1f msgs/sec\n", targetRate)
	fmt.Printf("Tick interval: %s\n", interval)
	fmt.Printf("Duration:      %s\n", duration)
	fmt.Printf("MQTT broker:   %s\n", cfg.MQTTBroker)
	fmt.Println()

	// --- Regenerate the same entity list (deterministic, same PRNG seed) ---
	// We don't need the full entity data for replay — just the serials and
	// per-sensor state for generating realistic values. The ingest API does
	// the enrichment from device_registry.
	rng := rand.New(rand.NewSource(cfg.PRNGSeed))
	// benchmarkStart doesn't affect serial generation, but we need it for
	// GenerateEntities. Use a fixed value — entity list is the same regardless.
	benchmarkStart := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	sensors := GenerateEntities(cfg.SensorCount, benchmarkStart, rng)
	fmt.Printf("[✓] Regenerated %d sensor serials\n", len(sensors))

	// Initialise per-sensor random walk states.
	states := make([]*SensorState, len(sensors))
	for i := range sensors {
		states[i] = NewSensorState(sensors[i].Serial)
	}

	// --- Connect to MQTT ---
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBroker).
		SetClientID("datagen-replay").
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("MQTT connect timed out")
	}
	if token.Error() != nil {
		return fmt.Errorf("MQTT connect: %w", token.Error())
	}
	defer client.Disconnect(250)
	fmt.Println("[✓] Connected to MQTT broker")
	fmt.Println()

	// --- Replay loop ---
	fmt.Println("[▶] Starting replay…")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.After(duration)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	var published int64
	sensorIdx := 0
	// Track a virtual day/reading index for the random walk state.
	// Starts at day 180, reading 0 — continuing from where seed left off.
	virtualReading := 0

	startTime := time.Now()
	reportTicker := time.NewTicker(5 * time.Second)
	defer reportTicker.Stop()

	for {
		select {
		case <-deadline:
			printReplaySummary(published, startTime)
			return nil

		case <-sigs:
			fmt.Println("\n[!] Interrupted")
			printReplaySummary(published, startTime)
			return nil

		case <-reportTicker.C:
			elapsed := time.Since(startTime)
			rate := float64(published) / elapsed.Seconds()
			fmt.Printf("  [REPLAY] %d msgs in %s (%.1f msgs/sec)\n",
				published, elapsed.Round(time.Second), rate)

		case <-ticker.C:
			sensor := &sensors[sensorIdx]
			state := states[sensorIdx]

			// Advance random walk.
			depths, battery := state.Step(180, virtualReading)

			// Build the lean MQTT payload — timestamp is NOW.
			payload := mqttPayload{
				Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
				Sensor:    mqttSensor{Serial: sensor.Serial, Battery: battery},
				Depth15:   toMqttDepth(depths[0]),
				Depth30:   toMqttDepth(depths[1]),
				Depth45:   toMqttDepth(depths[2]),
				Depth60:   toMqttDepth(depths[3]),
			}

			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				fmt.Fprintf(os.Stderr, "JSON marshal error: %v\n", err)
				continue
			}

			// Publish to the flat MQTT topic (QoS 0, no retain).
			token := client.Publish("sensors/telemetry", 0, false, jsonBytes)
			// Non-blocking — don't wait for ACK at QoS 0.
			_ = token

			published++
			sensorIdx = (sensorIdx + 1) % len(sensors)
			if sensorIdx == 0 {
				virtualReading++
			}
		}
	}
}

func toMqttDepth(d DepthReading) mqttDepth {
	return mqttDepth{
		VWC:         round2(d.VWC),
		Temperature: round2(d.Temperature),
		EC:          round2(d.EC),
	}
}

func printReplaySummary(published int64, startTime time.Time) {
	elapsed := time.Since(startTime)
	rate := float64(published) / elapsed.Seconds()
	fmt.Println()
	fmt.Println("=== REPLAY COMPLETE ===")
	fmt.Printf("Published:  %d messages\n", published)
	fmt.Printf("Duration:   %s\n", elapsed.Round(time.Second))
	fmt.Printf("Avg rate:   %.1f msgs/sec\n", rate)
}
