package stats

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/ports"
)

func TestInferenceHealthHonoursConfiguredThresholdsAndLatency(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		failures int
		latency  time.Duration
		want     string
	}{
		{name: "below thresholds", latency: 50 * time.Millisecond, want: "healthy"},
		{name: "degraded boundary", failures: 1, latency: 50 * time.Millisecond, want: "degraded"},
		{name: "critical boundary", failures: 2, latency: 50 * time.Millisecond, want: "critical"},
		{name: "latency objective", latency: 150 * time.Millisecond, want: "degraded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultInferenceHealthConfig()
			cfg.MinObservations = 4
			cfg.DegradedFailureRate, cfg.CriticalFailureRate = 0.25, 0.5
			cfg.LatencyObjective = 100 * time.Millisecond
			collector := newInferenceHealthCollector(cfg)
			for i := range 4 {
				status := 200
				if i < tc.failures {
					status = 503
				}
				collector.record(ports.InferenceRequestOutcome{Model: "chat", At: now, StatusCode: status, Latency: tc.latency})
			}
			health := collector.snapshot(map[string]bool{"chat": true}, now)
			require.Equal(t, tc.want, health.Status)
			require.Equal(t, tc.want, health.Models["chat"].Status)
		})
	}
}

func TestInferenceHealthConcurrentRecordingAndSnapshots(t *testing.T) {
	collector := newInferenceHealthCollector(config.DefaultInferenceHealthConfig())
	now := time.Now()
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 100 {
				collector.record(ports.InferenceRequestOutcome{Model: "chat", At: now, StatusCode: 200})
				collector.snapshot(map[string]bool{"chat": true}, now)
			}
		}()
	}
	workers.Wait()
	health := collector.snapshot(map[string]bool{"chat": true}, now)
	require.Equal(t, 800, health.Observations)
	require.Equal(t, 800, health.Models["chat"].Successes)
}

func TestInferenceHealthCountsFinalRequestOutcomes(t *testing.T) {
	cfg := config.DefaultInferenceHealthConfig()
	cfg.MinObservations = 1
	c := newInferenceHealthCollector(cfg)
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	inputs := []ports.InferenceRequestOutcome{
		{Model: "chat", At: now, StatusCode: 200, Latency: 120 * time.Millisecond},
		{Model: "chat", At: now, StatusCode: 503},
		{Model: "chat", At: now, StatusCode: 429},
		{Model: "chat", At: now, StatusCode: 408, TimedOut: true},
		{Model: "chat", At: now, StatusCode: 503, NoRouting: true},
		{Model: "chat", At: now, StatusCode: 200, StreamError: true},
		{Model: "chat", At: now, StatusCode: 400},
		{Model: "chat", At: now, StatusCode: 499},
	}
	for _, input := range inputs {
		c.record(input)
	}
	health := c.snapshot(map[string]bool{"chat": true}, now)
	if health.Observations != 6 || health.Successes != 1 || health.ServerErrors != 1 || health.StreamErrors != 1 || health.Timeouts != 1 || health.NoRouting != 1 || health.Upstream429 != 1 {
		t.Fatalf("unexpected outcome counts: %+v", health)
	}
	model := health.Models["chat"]
	if model.Availability != "available" || model.Observations != health.Observations {
		t.Fatalf("unexpected per-model health: %+v", model)
	}
}

func TestInferenceHealthKeepsPerModelSignalAndAvailability(t *testing.T) {
	cfg := config.DefaultInferenceHealthConfig()
	cfg.MinObservations = 10
	c := newInferenceHealthCollector(cfg)
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	for range 100 {
		c.record(ports.InferenceRequestOutcome{Model: "healthy", At: now, StatusCode: 200})
	}
	for range 10 {
		c.record(ports.InferenceRequestOutcome{Model: "failing", At: now, StatusCode: 503})
	}
	health := c.snapshot(map[string]bool{"healthy": true, "failing": false}, now)
	if health.Status == "critical" {
		t.Fatalf("aggregate unexpectedly critical: %+v", health)
	}
	if got := health.Models["failing"]; got.Status != "unavailable" || got.State != "unavailable" || got.RequestStatus != "critical" || got.Availability != "unavailable" {
		t.Fatalf("failing model signal/availability lost: %+v", got)
	}
	if health.Availability.Known != 2 || health.Availability.Available != 1 || health.Availability.Unavailable != 1 {
		t.Fatalf("unexpected aggregate availability: %+v", health.Availability)
	}
	if health.Availability.Status != "partial" {
		t.Fatalf("unexpected aggregate availability status: %+v", health.Availability)
	}
}

func TestInferenceHealthInsufficientAndIdleUseInjectedTime(t *testing.T) {
	cfg := config.DefaultInferenceHealthConfig()
	cfg.MinObservations = 2
	c := newInferenceHealthCollector(cfg)
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	first := c.snapshot(nil, now)
	if first.State != "insufficient_data" {
		t.Fatalf("fresh collector state = %q", first.State)
	}
	c.record(ports.InferenceRequestOutcome{Model: "idle-model", At: now.Add(-10 * time.Minute), StatusCode: 200})
	health := c.snapshot(map[string]bool{"idle-model": true}, now)
	model := health.Models["idle-model"]
	if model.State != "idle" || model.Status != "idle" {
		t.Fatalf("expired model state = %+v", model)
	}
	if model.Availability != "available" {
		t.Fatalf("availability changed with worker activity: %+v", model)
	}
}

func TestInferenceHealthDistinguishesUnknownFromAuthoritativeEmptyAvailability(t *testing.T) {
	cfg := config.DefaultInferenceHealthConfig()
	c := newInferenceHealthCollector(cfg)
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	c.record(ports.InferenceRequestOutcome{Model: "previous", ModelKnown: true, At: now, StatusCode: 200})

	unknown := c.snapshot(nil, now)
	if unknown.Availability.Status != "unknown" || unknown.Models["previous"].Availability != "unknown" {
		t.Fatalf("discovery-unknown state became unavailable: %+v", unknown)
	}
	empty := c.snapshot(map[string]bool{}, now)
	if empty.Availability.Status != "unavailable" || empty.Status != "unavailable" || empty.Models["previous"].Status != "unavailable" {
		t.Fatalf("authoritative empty catalogue not reflected: %+v", empty)
	}
}
