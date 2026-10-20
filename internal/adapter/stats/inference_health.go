package stats

import (
	"sync"
	"time"

	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/ports"
)

const (
	maxInferenceModels             = 500
	targetInferenceBuckets         = 300
	minInferenceBucket             = time.Second
	maxInferenceBucket             = time.Minute
	inferenceStatusAvailable       = "available"
	inferenceStatusUnavailable     = "unavailable"
	inferenceStatusUnknown         = "unknown"
	inferenceStatusPartial         = "partial"
	inferenceStateIdle             = "idle"
	inferenceStateInsufficientData = "insufficient_data"
	inferenceStateObserved         = "observed"
	inferenceStatusCritical        = "critical"
)

type inferenceCounts struct {
	observations int
	successes    int
	serverErrors int
	streamErrors int
	timeouts     int
	noRouting    int
	upstream429  int
	latencyMs    int64
}

type inferenceModelBuckets struct {
	lastSeen time.Time
	buckets  map[int64]inferenceCounts
	known    bool
}

type inferenceHealthCollector struct {
	lastAny          time.Time
	global           map[int64]inferenceCounts
	models           map[string]*inferenceModelBuckets
	config           config.InferenceHealthConfig
	lastPrunedBucket int64
	mu               sync.Mutex
	modelsTruncated  bool
	hasPrunedBucket  bool
}

func newInferenceHealthCollector(cfg config.InferenceHealthConfig) *inferenceHealthCollector {
	if cfg.Window <= 0 && cfg.Enabled {
		cfg = config.DefaultInferenceHealthConfig()
	}
	return &inferenceHealthCollector{config: cfg, global: make(map[int64]inferenceCounts), models: make(map[string]*inferenceModelBuckets)}
}

func (c *inferenceHealthCollector) record(outcome ports.InferenceRequestOutcome) {
	if !c.config.Enabled || !inferenceObservation(outcome) {
		return
	}
	if outcome.At.IsZero() {
		outcome.At = time.Now()
	}
	bucketDuration := inferenceBucketDuration(c.config.Window)
	bucket := outcome.At.UnixNano() / int64(bucketDuration)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hasPrunedBucket && bucket < c.lastPrunedBucket {
		return
	}
	c.global[bucket] = addOutcome(c.global[bucket], outcome)
	if outcome.At.After(c.lastAny) {
		c.lastAny = outcome.At
	}
	if outcome.Model != "" {
		c.recordModelLocked(outcome.Model, bucket, outcome)
	}
	cutoffBucket := outcome.At.Add(-c.config.Window).UnixNano() / int64(bucketDuration)
	c.pruneLocked(cutoffBucket)
}

func (c *inferenceHealthCollector) recordModelLocked(name string, bucket int64, outcome ports.InferenceRequestOutcome) {
	model := c.models[name]
	if model == nil {
		model = &inferenceModelBuckets{buckets: make(map[int64]inferenceCounts)}
		c.models[name] = model
	}
	model.buckets[bucket] = addOutcome(model.buckets[bucket], outcome)
	if outcome.At.After(model.lastSeen) {
		model.lastSeen = outcome.At
	}
	model.known = model.known || outcome.ModelKnown
	if len(c.models) <= maxInferenceModels {
		return
	}
	var oldest string
	var oldestAt time.Time
	for modelName, candidate := range c.models {
		if oldestAt.IsZero() || candidate.lastSeen.Before(oldestAt) {
			oldest, oldestAt = modelName, candidate.lastSeen
		}
	}
	delete(c.models, oldest)
	c.modelsTruncated = true
}

func (c *inferenceHealthCollector) pruneLocked(cutoffBucket int64) {
	if c.hasPrunedBucket && cutoffBucket <= c.lastPrunedBucket {
		return
	}
	for index := range c.global {
		if index < cutoffBucket {
			delete(c.global, index)
		}
	}
	for _, model := range c.models {
		for index := range model.buckets {
			if index < cutoffBucket {
				delete(model.buckets, index)
			}
		}
	}
	c.lastPrunedBucket = cutoffBucket
	c.hasPrunedBucket = true
}

func inferenceBucketDuration(window time.Duration) time.Duration {
	bucket := window / targetInferenceBuckets
	if bucket < minInferenceBucket {
		return minInferenceBucket
	}
	if bucket > maxInferenceBucket {
		return maxInferenceBucket
	}
	return bucket
}

func inferenceObservation(e ports.InferenceRequestOutcome) bool {
	return e.NoRouting || e.TimedOut || e.StatusCode == 429 || e.StatusCode >= 500 || e.StreamError || (e.StatusCode >= 200 && e.StatusCode < 300)
}

func addOutcome(counts inferenceCounts, e ports.InferenceRequestOutcome) inferenceCounts {
	switch {
	case e.NoRouting:
		counts.observations++
		counts.noRouting++
	case e.TimedOut:
		counts.observations++
		counts.timeouts++
	case e.StatusCode == 429:
		counts.observations++
		counts.upstream429++
	case e.StatusCode >= 500:
		counts.observations++
		counts.serverErrors++
	case e.StreamError:
		counts.observations++
		counts.streamErrors++
	case e.StatusCode >= 200 && e.StatusCode < 300:
		counts.observations++
		counts.successes++
		counts.latencyMs += e.Latency.Milliseconds()
	}
	return counts
}

func (c *inferenceHealthCollector) snapshot(knownModels map[string]bool, now time.Time) ports.InferenceHealth {
	if now.IsZero() {
		now = time.Now()
	}
	bucketDuration := inferenceBucketDuration(c.config.Window)
	cutoffBucket := now.Add(-c.config.Window).UnixNano() / int64(bucketDuration)
	c.mu.Lock()
	allCounts := countsSince(c.global, cutoffBucket)
	lastAny := c.lastAny
	modelsTruncated := c.modelsTruncated
	modelCopy := make(map[string]inferenceModelBuckets, len(c.models))
	for name, model := range c.models {
		buckets := make(map[int64]inferenceCounts, len(model.buckets))
		for key, counts := range model.buckets {
			if key >= cutoffBucket {
				buckets[key] = counts
			}
		}
		modelCopy[name] = inferenceModelBuckets{lastSeen: model.lastSeen, known: model.known, buckets: buckets}
	}
	c.mu.Unlock()

	all := healthFromCounts(allCounts, c.config, lastAny, now)
	all.ModelsTruncated = modelsTruncated
	all.Models = make(map[string]ports.InferenceModelHealth, len(modelCopy)+len(knownModels))
	for name, model := range modelCopy {
		counts := countsSince(model.buckets, cutoffBucket)
		health := healthFromCounts(counts, c.config, model.lastSeen, now)
		available, currentKnown := knownModels[name]
		known := currentKnown || model.known
		if knownModels == nil {
			available = false
		}
		all.Models[name] = modelHealth(health, known, available, knownModels != nil)
		if currentKnown || (knownModels != nil && model.known) {
			all.Availability.Known++
			if available {
				all.Availability.Available++
			} else {
				all.Availability.Unavailable++
			}
		}
	}
	for name, available := range knownModels {
		if _, exists := all.Models[name]; exists {
			continue
		}
		health := healthFromCounts(inferenceCounts{}, c.config, time.Time{}, now)
		all.Models[name] = modelHealth(health, true, available, true)
		all.Availability.Known++
		if available {
			all.Availability.Available++
		} else {
			all.Availability.Unavailable++
		}
	}
	switch {
	case knownModels == nil:
		all.Availability.Status = inferenceStatusUnknown
	case all.Availability.Known == 0:
		all.Availability.Status = inferenceStatusUnavailable
	case all.Availability.Available == 0:
		all.Availability.Status = inferenceStatusUnavailable
	case all.Availability.Unavailable == 0:
		all.Availability.Status = inferenceStatusAvailable
	default:
		all.Availability.Status = inferenceStatusPartial
	}
	if all.Availability.Status == inferenceStatusUnavailable {
		all.Status, all.State = inferenceStatusUnavailable, inferenceStatusUnavailable
	}
	return all
}

func countsSince(buckets map[int64]inferenceCounts, cutoff int64) inferenceCounts {
	var total inferenceCounts
	for index, current := range buckets {
		if index < cutoff {
			continue
		}
		total.observations += current.observations
		total.successes += current.successes
		total.serverErrors += current.serverErrors
		total.streamErrors += current.streamErrors
		total.timeouts += current.timeouts
		total.noRouting += current.noRouting
		total.upstream429 += current.upstream429
		total.latencyMs += current.latencyMs
	}
	return total
}

func healthFromCounts(counts inferenceCounts, cfg config.InferenceHealthConfig, lastSeen, now time.Time) ports.InferenceHealth {
	health := ports.InferenceHealth{
		Observations: counts.observations, Successes: counts.successes,
		ServerErrors: counts.serverErrors, StreamErrors: counts.streamErrors,
		Timeouts: counts.timeouts, NoRouting: counts.noRouting, Upstream429: counts.upstream429,
	}
	if counts.observations > 0 {
		health.FailureRate = float64(counts.observations-counts.successes) / float64(counts.observations)
	}
	if counts.successes > 0 {
		health.AverageLatencyMs = counts.latencyMs / int64(counts.successes)
	}
	switch {
	case counts.observations == 0 && !lastSeen.IsZero() && now.Sub(lastSeen) > cfg.Window:
		health.State, health.Status = inferenceStateIdle, inferenceStateIdle
	case counts.observations < cfg.MinObservations:
		health.State, health.Status = inferenceStateInsufficientData, inferenceStateInsufficientData
	case health.FailureRate >= cfg.CriticalFailureRate:
		health.State, health.Status = inferenceStateObserved, inferenceStatusCritical
	case health.FailureRate >= cfg.DegradedFailureRate:
		health.State, health.Status = inferenceStateObserved, "degraded"
	case cfg.LatencyObjective > 0 && time.Duration(health.AverageLatencyMs)*time.Millisecond > cfg.LatencyObjective:
		health.State, health.Status = inferenceStateObserved, "degraded"
	default:
		health.State, health.Status = inferenceStateObserved, "healthy"
	}
	return health
}

func modelHealth(health ports.InferenceHealth, known, available, authoritative bool) ports.InferenceModelHealth {
	availability := inferenceStatusUnknown
	if known && authoritative {
		availability = inferenceStatusUnavailable
		if available {
			availability = inferenceStatusAvailable
		}
	}
	state := health.State
	if known && authoritative && !available {
		state = inferenceStatusUnavailable
	}
	status := health.Status
	if known && authoritative && !available {
		status = inferenceStatusUnavailable
	}
	return ports.InferenceModelHealth{
		Status: status, State: state, RequestStatus: health.Status, Availability: availability,
		Observations: health.Observations, Successes: health.Successes, ServerErrors: health.ServerErrors,
		StreamErrors: health.StreamErrors, Timeouts: health.Timeouts, NoRouting: health.NoRouting,
		Upstream429: health.Upstream429, FailureRate: health.FailureRate, AverageLatencyMs: health.AverageLatencyMs,
	}
}
