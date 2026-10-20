package config

import (
	"math"
	"testing"
	"time"
)

func TestInferenceHealthConfigValidation(t *testing.T) {
	cfg := DefaultInferenceHealthConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default configuration must validate: %v", err)
	}
	cfg.Window = time.Second - 1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected sub-second window to fail validation")
	}
	cfg = DefaultInferenceHealthConfig()
	cfg.CriticalFailureRate = math.NaN()
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected NaN threshold to fail validation")
	}
	cfg = DefaultInferenceHealthConfig()
	cfg.Enabled = false
	cfg.Window = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("disabled health collection must not require active settings: %v", err)
	}
}
