package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"sync"

	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/domain"
)

// SwarmEndpointRepository holds the current task snapshot. The task ID is the
// endpoint incarnation, which prevents health results and affinity entries
// from an old task affecting a replacement that reuses its IP address.
type SwarmEndpointRepository struct {
	modelStore     domain.ModelRegistry
	endpoints      map[string]*domain.Endpoint
	builder        *StaticEndpointRepository
	pendingCleanup map[string]struct{}
	config         config.SwarmDiscoveryConfig
	mu             sync.RWMutex
	// lifecycleMu serialises discovery result publication against task removal,
	// while mu remains free so readers see snapshot changes immediately.
	lifecycleMu sync.RWMutex
	builderMu   sync.Mutex
}

func NewSwarmEndpointRepository(cfg config.SwarmDiscoveryConfig, modelStore domain.ModelRegistry) *SwarmEndpointRepository {
	return &SwarmEndpointRepository{
		endpoints: make(map[string]*domain.Endpoint), modelStore: modelStore,
		config: cfg, builder: NewStaticEndpointRepository(), pendingCleanup: make(map[string]struct{}),
	}
}

func (r *SwarmEndpointRepository) GetAll(ctx context.Context) ([]*domain.Endpoint, error) {
	return r.list(func(*domain.Endpoint) bool { return true })
}

func (r *SwarmEndpointRepository) GetHealthy(ctx context.Context) ([]*domain.Endpoint, error) {
	return r.list(func(endpoint *domain.Endpoint) bool { return endpoint.Status == domain.StatusHealthy })
}

func (r *SwarmEndpointRepository) GetRoutable(ctx context.Context) ([]*domain.Endpoint, error) {
	return r.list(func(endpoint *domain.Endpoint) bool { return endpoint.Status.IsRoutable() })
}

func (r *SwarmEndpointRepository) list(include func(*domain.Endpoint) bool) ([]*domain.Endpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*domain.Endpoint, 0, len(r.endpoints))
	for _, endpoint := range r.endpoints {
		if include(endpoint) {
			endpointCopy := *endpoint
			result = append(result, &endpointCopy)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key() < result[j].Key() })
	return result, nil
}

func (r *SwarmEndpointRepository) UpdateEndpoint(ctx context.Context, endpoint *domain.Endpoint) error {
	if endpoint == nil || endpoint.URL == nil {
		return errors.New("endpoint URL is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok := r.endpoints[endpoint.URLString]
	if !ok || existing.DiscoveryIdentity != endpoint.DiscoveryIdentity {
		return &domain.EndpointNotFoundError{URL: endpoint.URLString}
	}
	copyRuntimeState(existing, endpoint)
	return nil
}

func (r *SwarmEndpointRepository) Exists(ctx context.Context, endpointURL *url.URL) bool {
	if endpointURL == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.endpoints[endpointURL.String()]
	return ok
}

// Reconcile atomically replaces the task snapshot. Health state survives only
// when the same task incarnation remains present. Removed model data is cleared
// while lifecycleMu fences an in-flight model discovery commit.
func (r *SwarmEndpointRepository) Reconcile(ctx context.Context, desired []*domain.Endpoint) ([]*domain.Endpoint, []*domain.Endpoint, error) {
	newEndpoints := make(map[string]*domain.Endpoint, len(desired))
	for _, endpoint := range desired {
		if endpoint == nil || endpoint.URL == nil || endpoint.DiscoveryIdentity == "" {
			return nil, nil, errors.New("Swarm endpoint requires URL and discovery identity")
		}
		if _, duplicate := newEndpoints[endpoint.URLString]; duplicate {
			return nil, nil, fmt.Errorf("duplicate Swarm endpoint URL %q", endpoint.URLString)
		}
		newEndpoints[endpoint.URLString] = endpoint
	}

	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.mu.Lock()
	var added, removed []*domain.Endpoint
	oldByIdentity := make(map[string]*domain.Endpoint, len(r.endpoints))
	for _, endpoint := range r.endpoints {
		oldByIdentity[endpoint.DiscoveryIdentity] = endpoint
	}
	for _, endpoint := range newEndpoints {
		if old := oldByIdentity[endpoint.DiscoveryIdentity]; old != nil {
			copyRuntimeState(endpoint, old)
		}
	}
	for oldURL, oldEndpoint := range r.endpoints {
		newEndpoint, present := newEndpoints[oldURL]
		if present && newEndpoint.DiscoveryIdentity == oldEndpoint.DiscoveryIdentity {
			continue
		}
		removedCopy := *oldEndpoint
		removed = append(removed, &removedCopy)
		if r.modelStore != nil {
			r.pendingCleanup[oldURL] = struct{}{}
		}
	}
	for endpointURL, endpoint := range newEndpoints {
		old, present := r.endpoints[endpointURL]
		if !present || old.DiscoveryIdentity != endpoint.DiscoveryIdentity {
			addedCopy := *endpoint
			added = append(added, &addedCopy)
		}
	}
	r.endpoints = newEndpoints
	r.mu.Unlock()
	var cleanupErrors []error
	for endpointURL := range r.pendingCleanup {
		if err := r.modelStore.RemoveEndpoint(context.WithoutCancel(ctx), endpointURL); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove models for Swarm endpoint %q: %w", endpointURL, err))
			continue
		}
		delete(r.pendingCleanup, endpointURL)
	}
	if len(cleanupErrors) > 0 {
		return added, removed, errors.Join(cleanupErrors...)
	}
	return added, removed, nil
}

// WithCurrentEndpoint serialises model publication with task replacement.
// A stale worker cannot register results after its task has been removed or
// its address has been assigned to another task.
func (r *SwarmEndpointRepository) WithCurrentEndpoint(endpoint *domain.Endpoint, publish func() error) error {
	if endpoint == nil || publish == nil {
		return nil
	}
	r.lifecycleMu.RLock()
	defer r.lifecycleMu.RUnlock()
	r.mu.RLock()
	current, ok := r.endpoints[endpoint.URLString]
	_, cleanupPending := r.pendingCleanup[endpoint.URLString]
	r.mu.RUnlock()
	if !ok || current.DiscoveryIdentity != endpoint.DiscoveryIdentity || cleanupPending {
		return nil
	}
	return publish()
}

func copyRuntimeState(destination, source *domain.Endpoint) {
	destination.Status = source.Status
	destination.LastChecked = source.LastChecked
	destination.NextCheckTime = source.NextCheckTime
	destination.RateLimitedUntil = source.RateLimitedUntil
	destination.LastLatency = source.LastLatency
	destination.ConsecutiveFailures = source.ConsecutiveFailures
	destination.BackoffMultiplier = source.BackoffMultiplier
}

func (r *SwarmEndpointRepository) BuildEndpoints(ctx context.Context, tasks []SwarmTask) ([]*domain.Endpoint, error) {
	r.builderMu.Lock()
	defer r.builderMu.Unlock()
	configs := make([]config.EndpointConfig, 0, len(tasks))
	identities := make(map[string]string, len(tasks))
	seenURLs := make(map[string]struct{}, len(tasks))
	for _, task := range tasks {
		endpointConfig := r.config.Endpoint
		if override, ok := r.config.NodeEndpoints[task.NodeName]; ok {
			endpointConfig = mergeEndpointConfig(endpointConfig, override)
		}
		if endpointConfig.Name == "" {
			endpointConfig.Name = fmt.Sprintf("%s-%s", r.config.Service, task.NodeName)
		}
		endpointConfig.Name = fmt.Sprintf("%s-%s", endpointConfig.Name, task.ID)
		endpointConfig.URL = "http://" + net.JoinHostPort(task.Address, strconv.FormatUint(uint64(task.Port), 10))
		if _, exists := seenURLs[endpointConfig.URL]; exists {
			return nil, fmt.Errorf("multiple Swarm tasks resolved to endpoint URL %q", endpointConfig.URL)
		}
		seenURLs[endpointConfig.URL] = struct{}{}
		configs = append(configs, endpointConfig)
		identities[endpointConfig.URL] = "swarm:" + task.ID
	}
	if err := r.builder.LoadFromConfig(ctx, configs); err != nil {
		return nil, fmt.Errorf("build Swarm endpoint snapshot: %w", err)
	}
	result, err := r.builder.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	for _, endpoint := range result {
		endpoint.DiscoveryIdentity = identities[endpoint.URLString]
	}
	return result, nil
}

func mergeEndpointConfig(base, override config.EndpointConfig) config.EndpointConfig {
	if override.Name != "" {
		base.Name = override.Name
	}
	if override.Type != "" {
		base.Type = override.Type
	}
	if override.HealthCheckURL != "" {
		base.HealthCheckURL = override.HealthCheckURL
	}
	if override.ModelURL != "" {
		base.ModelURL = override.ModelURL
	}
	if override.CheckInterval != 0 {
		base.CheckInterval = override.CheckInterval
	}
	if override.CheckTimeout != 0 {
		base.CheckTimeout = override.CheckTimeout
	}
	if override.Priority != nil {
		base.Priority = override.Priority
	}
	if override.PreservePath {
		base.PreservePath = true
	}
	if override.ModelFilter != nil {
		base.ModelFilter = override.ModelFilter
	}
	if override.Auth != nil {
		base.Auth = override.Auth
	}
	if len(override.Headers) > 0 {
		base.Headers = override.Headers
	}
	return base
}

// CompositeEndpointRepository combines static and dynamic endpoints while
// preserving each repository's health lifecycle.
type CompositeEndpointRepository struct {
	static domain.EndpointRepository
	swarm  *SwarmEndpointRepository
}

func NewCompositeEndpointRepository(static domain.EndpointRepository, swarm *SwarmEndpointRepository) *CompositeEndpointRepository {
	return &CompositeEndpointRepository{static: static, swarm: swarm}
}

func (r *CompositeEndpointRepository) GetAll(ctx context.Context) ([]*domain.Endpoint, error) {
	return r.combine(func(repo domain.EndpointRepository) ([]*domain.Endpoint, error) { return repo.GetAll(ctx) })
}
func (r *CompositeEndpointRepository) GetHealthy(ctx context.Context) ([]*domain.Endpoint, error) {
	return r.combine(func(repo domain.EndpointRepository) ([]*domain.Endpoint, error) { return repo.GetHealthy(ctx) })
}
func (r *CompositeEndpointRepository) GetRoutable(ctx context.Context) ([]*domain.Endpoint, error) {
	return r.combine(func(repo domain.EndpointRepository) ([]*domain.Endpoint, error) { return repo.GetRoutable(ctx) })
}
func (r *CompositeEndpointRepository) combine(get func(domain.EndpointRepository) ([]*domain.Endpoint, error)) ([]*domain.Endpoint, error) {
	result, err := get(r.static)
	if err != nil {
		return nil, err
	}
	dynamic, err := get(r.swarm)
	if err != nil {
		return nil, err
	}
	staticURLs := make(map[string]struct{}, len(result))
	for _, endpoint := range result {
		staticURLs[endpoint.URLString] = struct{}{}
	}
	for _, endpoint := range dynamic {
		if _, duplicate := staticURLs[endpoint.URLString]; duplicate {
			return nil, fmt.Errorf("static and Swarm discovery both contain endpoint URL %q", endpoint.URLString)
		}
	}
	result = append(result, dynamic...)
	return result, nil
}
func (r *CompositeEndpointRepository) UpdateEndpoint(ctx context.Context, endpoint *domain.Endpoint) error {
	if endpoint != nil && endpoint.DiscoveryIdentity != "" {
		return r.swarm.UpdateEndpoint(ctx, endpoint)
	}
	return r.static.UpdateEndpoint(ctx, endpoint)
}
func (r *CompositeEndpointRepository) Exists(ctx context.Context, endpointURL *url.URL) bool {
	return r.static.Exists(ctx, endpointURL) || r.swarm.Exists(ctx, endpointURL)
}
func (r *CompositeEndpointRepository) WithCurrentEndpoint(endpoint *domain.Endpoint, publish func() error) error {
	if endpoint != nil && endpoint.DiscoveryIdentity != "" {
		return r.swarm.WithCurrentEndpoint(endpoint, publish)
	}
	return publish()
}
