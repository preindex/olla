package config

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
)

// SwarmDiscoveryConfig discovers replicas of one explicitly selected Swarm
// service over a shared overlay network.
type SwarmDiscoveryConfig struct {
	NodeEndpoints   map[string]EndpointConfig `yaml:"node_endpoints,omitempty"`
	Socket          string                    `yaml:"socket"`
	Service         string                    `yaml:"service"`
	Network         string                    `yaml:"network"`
	APIVersion      string                    `yaml:"api_version"`
	Endpoint        EndpointConfig            `yaml:"endpoint"`
	RefreshInterval time.Duration             `yaml:"refresh_interval"`
	Port            uint32                    `yaml:"port"`
	Enabled         bool                      `yaml:"enabled"`
}

func DefaultSwarmDiscoveryConfig() SwarmDiscoveryConfig {
	priority := 100
	return SwarmDiscoveryConfig{
		Socket:          defaultSwarmSocket,
		Port:            11434,
		RefreshInterval: 30 * time.Second,
		Endpoint: EndpointConfig{
			Type:          "ollama",
			Priority:      &priority,
			CheckInterval: 5 * time.Second,
			CheckTimeout:  2 * time.Second,
		},
	}
}

const defaultSwarmSocket = "/var/run/docker.sock"

func (c SwarmDiscoveryConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Service == "" {
		return errors.New("discovery.swarm.service is required when Swarm discovery is enabled")
	}
	if c.Network == "" {
		return errors.New("discovery.swarm.network is required when Swarm discovery is enabled")
	}
	if c.Port == 0 {
		return errors.New("discovery.swarm.port is required when Swarm discovery is enabled")
	}
	if c.Socket == "" || !path.IsAbs(c.Socket) {
		return errors.New("discovery.swarm.socket must be an absolute Unix socket path")
	}
	if c.Port > 65535 {
		return fmt.Errorf("discovery.swarm.port must be between 1 and 65535, got %d", c.Port)
	}
	if c.APIVersion != "" {
		apiVersion := strings.TrimPrefix(c.APIVersion, "v")
		parts := strings.Split(apiVersion, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(apiVersion, "/?#") {
			return fmt.Errorf("discovery.swarm.api_version must look like v1.44, got %q", c.APIVersion)
		}
		for _, part := range parts {
			value, err := strconv.Atoi(part)
			if err != nil || value <= 0 {
				return fmt.Errorf("discovery.swarm.api_version must look like v1.44, got %q", c.APIVersion)
			}
		}
	}
	if c.RefreshInterval < time.Second {
		return fmt.Errorf("discovery.swarm.refresh_interval must be at least 1s, got %v", c.RefreshInterval)
	}
	if c.Endpoint.ModelFilter != nil {
		if err := c.Endpoint.ModelFilter.Validate(); err != nil {
			return fmt.Errorf("discovery.swarm.endpoint.model_filter: %w", err)
		}
	}
	for node, endpoint := range c.NodeEndpoints {
		if strings.TrimSpace(node) == "" {
			return errors.New("discovery.swarm.node_endpoints keys must be non-empty node names")
		}
		if endpoint.ModelFilter != nil {
			if err := endpoint.ModelFilter.Validate(); err != nil {
				return fmt.Errorf("discovery.swarm.node_endpoints[%q].model_filter: %w", node, err)
			}
		}
	}
	return nil
}
