package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestExtensionConfiguration(t *testing.T) {
	cfg := DefaultConfig()
	err := yaml.Unmarshal([]byte(`
discovery:
  static:
    endpoints:
      - name: small-worker
        url: http://worker:11434
        type: ollama
        model_filter:
          exclude: ["gpt-oss:latest"]
          parameter_range:
            min_b: 0
            max_b: 9
inference_health:
  window: 10m
  min_observations: 5
  degraded_failure_rate: 0.1
  critical_failure_rate: 0.25
`), cfg)
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	require.False(t, cfg.Discovery.Swarm.Enabled)
	filter := cfg.Discovery.Static.Endpoints[0].ModelFilter
	require.Equal(t, []string{"gpt-oss:latest"}, filter.Exclude)
	require.NotNil(t, filter.ParameterRange.MinB)
	require.Equal(t, 0.0, *filter.ParameterRange.MinB)
	require.Equal(t, 9.0, *filter.ParameterRange.MaxB)
	require.True(t, cfg.InferenceHealth.Enabled)
	require.Equal(t, 10*time.Minute, cfg.InferenceHealth.Window)
	require.Equal(t, 5, cfg.InferenceHealth.MinObservations)
}

func TestInvalidExtensionConfigurationFailsStartup(t *testing.T) {
	for name, source := range map[string]string{
		"reversed range":      `discovery: {static: {endpoints: [{name: worker, model_filter: {parameter_range: {min_b: 10, max_b: 9}}}]}}`,
		"negative range":      `discovery: {static: {endpoints: [{name: worker, model_filter: {parameter_range: {max_b: -1}}}]}}`,
		"range on profiles":   `proxy: {profile_filter: {parameter_range: {max_b: 9}}}`,
		"nonfinite threshold": `inference_health: {degraded_failure_rate: .nan}`,
		"reversed thresholds": `inference_health: {degraded_failure_rate: 0.3, critical_failure_rate: 0.1}`,
		"missing service":     `discovery: {swarm: {enabled: true, network: ai}}`,
		"missing network":     `discovery: {swarm: {enabled: true, service: ollama}}`,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			require.NoError(t, yaml.Unmarshal([]byte(source), cfg))
			require.Error(t, cfg.Validate())
		})
	}
}
