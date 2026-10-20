package config

import (
	"errors"
	"math"
	"time"
)

// InferenceHealthConfig controls request-oriented inference health summaries.
// Failure rates are fractions (0.05 means five per cent).
type InferenceHealthConfig struct {
	Enabled             bool          `yaml:"enabled"`
	Window              time.Duration `yaml:"window"`
	MinObservations     int           `yaml:"min_observations"`
	DegradedFailureRate float64       `yaml:"degraded_failure_rate"`
	CriticalFailureRate float64       `yaml:"critical_failure_rate"`
	LatencyObjective    time.Duration `yaml:"latency_objective"`
}

// DefaultInferenceHealthConfig sets conservative defaults for a five-minute
// request window. Ten observations are required before the rate is actionable.
func DefaultInferenceHealthConfig() InferenceHealthConfig {
	return InferenceHealthConfig{
		Enabled:             true,
		Window:              5 * time.Minute,
		MinObservations:     10,
		DegradedFailureRate: 0.05,
		CriticalFailureRate: 0.10,
	}
}

// Validate checks thresholds and bounds that could otherwise produce
// misleading health classifications or unbounded request history.
func (c InferenceHealthConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Window < time.Second || c.Window > 24*time.Hour {
		return errors.New("inference_health.window must be between 1s and 24h")
	}
	if c.MinObservations < 1 || c.MinObservations > 100_000 {
		return errors.New("inference_health.min_observations must be between 1 and 100000")
	}
	if math.IsNaN(c.DegradedFailureRate) || math.IsInf(c.DegradedFailureRate, 0) || c.DegradedFailureRate < 0 || c.DegradedFailureRate > 1 {
		return errors.New("inference_health.degraded_failure_rate must be between 0 and 1")
	}
	if math.IsNaN(c.CriticalFailureRate) || math.IsInf(c.CriticalFailureRate, 0) || c.CriticalFailureRate < c.DegradedFailureRate || c.CriticalFailureRate > 1 {
		return errors.New("inference_health.critical_failure_rate must be between degraded_failure_rate and 1")
	}
	if c.LatencyObjective < 0 || c.LatencyObjective > 24*time.Hour {
		return errors.New("inference_health.latency_objective must be between 0 and 24h")
	}
	return nil
}
