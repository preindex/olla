package config

import (
	"testing"

	"github.com/thushan/olla/internal/core/domain"
)

func TestSwarmDiscoveryConfigValidate(t *testing.T) {
	if err := DefaultSwarmDiscoveryConfig().Validate(); err != nil {
		t.Fatalf("disabled default should validate: %v", err)
	}
	cfg := DefaultSwarmDiscoveryConfig()
	cfg.Enabled = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("enabled Swarm discovery must require service and network")
	}
	cfg.Service, cfg.Network = "ollama", "llm-overlay"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid Swarm settings rejected: %v", err)
	}
	cfg.APIVersion = "v1.-1"
	if err := cfg.Validate(); err == nil {
		t.Fatal("non-positive API version component should be rejected")
	}
	cfg.APIVersion = ""
	cfg.Socket = `C:\docker.sock`
	if err := cfg.Validate(); err == nil {
		t.Fatal("Windows path should not be accepted as a Unix socket path")
	}
	cfg.Socket = defaultSwarmSocket
	cfg.Endpoint.ModelFilter = &domain.FilterConfig{ParameterRange: &domain.ParameterRange{MinB: floatPointer(-1)}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("invalid model parameter range should fail Swarm config validation")
	}
}

func floatPointer(value float64) *float64 { return &value }
