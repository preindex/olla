package discovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/domain"
)

func TestSwarmEndpointRepositoryReconcileAdditionRemovalAndReplacement(t *testing.T) {
	ctx := context.Background()
	repo := NewSwarmEndpointRepository(structSwarmConfig(), nil)
	first := swarmEndpoint("http://10.0.0.2:11434", "task-a")
	added, removed, err := repo.Reconcile(ctx, []*domain.Endpoint{first})
	if err != nil || len(added) != 1 || len(removed) != 0 {
		t.Fatalf("initial reconcile = added %d removed %d err %v", len(added), len(removed), err)
	}

	first.Status = domain.StatusHealthy
	first.LastChecked = time.Now()
	if err := repo.UpdateEndpoint(ctx, first); err != nil {
		t.Fatalf("update health state: %v", err)
	}
	// A snapshot with the same task and URL preserves its health state.
	added, removed, err = repo.Reconcile(ctx, []*domain.Endpoint{swarmEndpoint(first.URLString, "task-a")})
	if err != nil || len(added) != 0 || len(removed) != 0 {
		t.Fatalf("same-task reconcile = added %d removed %d err %v", len(added), len(removed), err)
	}
	all, _ := repo.GetAll(ctx)
	if len(all) != 1 || all[0].Status != domain.StatusHealthy {
		t.Fatalf("same task lost health state: %+v", all)
	}

	// A new task reusing the same IP and port starts fresh and fences stale updates.
	replacement := swarmEndpoint(first.URLString, "task-b")
	added, removed, err = repo.Reconcile(ctx, []*domain.Endpoint{replacement})
	if err != nil || len(added) != 1 || len(removed) != 1 {
		t.Fatalf("replacement reconcile = added %d removed %d err %v", len(added), len(removed), err)
	}
	all, _ = repo.GetAll(ctx)
	if all[0].Status != domain.StatusUnknown || all[0].Key() != "swarm:task-b" {
		t.Fatalf("replacement inherited old state: %+v", all[0])
	}
	if err := repo.UpdateEndpoint(ctx, first); err == nil {
		t.Fatal("stale health update from replaced task was accepted")
	}

	// A successful empty snapshot is authoritative and removes the task.
	added, removed, err = repo.Reconcile(ctx, nil)
	if err != nil || len(added) != 0 || len(removed) != 1 {
		t.Fatalf("empty reconcile = added %d removed %d err %v", len(added), len(removed), err)
	}
	all, _ = repo.GetAll(ctx)
	if len(all) != 0 {
		t.Fatalf("expected empty endpoint snapshot, got %d", len(all))
	}
}

func TestSwarmEndpointRepositoryReschedulePreservesTaskHealth(t *testing.T) {
	ctx := context.Background()
	repo := NewSwarmEndpointRepository(structSwarmConfig(), nil)
	old := swarmEndpoint("http://10.0.0.2:11434", "task-a")
	old.Status = domain.StatusHealthy
	old.LastChecked = time.Now()
	if _, _, err := repo.Reconcile(ctx, []*domain.Endpoint{old}); err != nil {
		t.Fatal(err)
	}
	newAddress := swarmEndpoint("http://10.0.0.9:11434", "task-a")
	added, removed, err := repo.Reconcile(ctx, []*domain.Endpoint{newAddress})
	if err != nil || len(added) != 1 || len(removed) != 1 {
		t.Fatalf("reschedule reconcile = added %d removed %d err %v", len(added), len(removed), err)
	}
	all, _ := repo.GetAll(ctx)
	if len(all) != 1 || all[0].Status != domain.StatusHealthy {
		t.Fatalf("same task lost health during address change: %+v", all)
	}
}

func TestSwarmRepositoryFencesModelPublicationDuringReplacement(t *testing.T) {
	ctx := context.Background()
	repo := NewSwarmEndpointRepository(structSwarmConfig(), nil)
	old := swarmEndpoint("http://10.0.0.2:11434", "task-a")
	if _, _, err := repo.Reconcile(ctx, []*domain.Endpoint{old}); err != nil {
		t.Fatal(err)
	}
	published := false
	if err := repo.WithCurrentEndpoint(old, func() error { published = true; return nil }); err != nil || !published {
		t.Fatalf("current endpoint publication was rejected: published=%v err=%v", published, err)
	}
	if _, _, err := repo.Reconcile(ctx, []*domain.Endpoint{swarmEndpoint(old.URLString, "task-b")}); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := repo.WithCurrentEndpoint(old, func() error { called = true; return nil }); err != nil || called {
		t.Fatalf("stale endpoint publication was accepted: called=%v err=%v", called, err)
	}
}

func TestSwarmRepositorySerializesPublicationAndReplacement(t *testing.T) {
	ctx := context.Background()
	repo := NewSwarmEndpointRepository(structSwarmConfig(), nil)
	old := swarmEndpoint("http://10.0.0.2:11434", "task-a")
	if _, _, err := repo.Reconcile(ctx, []*domain.Endpoint{old}); err != nil {
		t.Fatal(err)
	}
	entered, publish := make(chan struct{}), make(chan struct{})
	publicationDone := make(chan error, 1)
	go func() {
		publicationDone <- repo.WithCurrentEndpoint(old, func() error {
			close(entered)
			<-publish
			return nil
		})
	}()
	<-entered
	reconcileDone := make(chan error, 1)
	go func() {
		_, _, err := repo.Reconcile(ctx, []*domain.Endpoint{swarmEndpoint(old.URLString, "task-b")})
		reconcileDone <- err
	}()
	select {
	case err := <-reconcileDone:
		t.Fatalf("replacement overtook an active publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(publish)
	if err := <-publicationDone; err != nil {
		t.Fatal(err)
	}
	if err := <-reconcileDone; err != nil {
		t.Fatal(err)
	}
	endpoints, _ := repo.GetAll(ctx)
	if len(endpoints) != 1 || endpoints[0].DiscoveryIdentity != "swarm:task-b" {
		t.Fatalf("replacement snapshot was not committed: %+v", endpoints)
	}
}

func TestSwarmEndpointRepositorySnapshotCopiesPermitActiveRequestsToFinish(t *testing.T) {
	ctx := context.Background()
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "finished")
	}))
	defer server.Close()
	repo := NewSwarmEndpointRepository(structSwarmConfig(), nil)
	endpoint := swarmEndpoint(server.URL, "task-a")
	if _, _, err := repo.Reconcile(ctx, []*domain.Endpoint{endpoint}); err != nil {
		t.Fatal(err)
	}
	activeRequestEndpoint, _ := repo.GetAll(ctx)
	requestDone := make(chan error, 1)
	go func() {
		response, err := http.Get(activeRequestEndpoint[0].URLString)
		if err != nil {
			requestDone <- err
			return
		}
		defer response.Body.Close()
		_, err = io.ReadAll(response.Body)
		requestDone <- err
	}()
	<-started
	if _, _, err := repo.Reconcile(ctx, nil); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-requestDone; err != nil {
		t.Fatalf("in-flight request did not finish after removal: %v", err)
	}
}

func TestSwarmDiscoveryReconcileKeepsLastSnapshotOnManagerFailure(t *testing.T) {
	ctx := context.Background()
	manager := &testSwarmManager{tasks: []SwarmTask{{ID: "task-a", Address: "10.0.0.2", Port: 11434}}}
	cfg := config.DefaultSwarmDiscoveryConfig()
	cfg.Enabled, cfg.Service, cfg.Network = true, "llm", "overlay"
	repo := NewSwarmEndpointRepository(cfg, nil)
	service := NewSwarmDiscoveryService(manager, repo, cfg.Service, cfg.Network, cfg.Port, time.Second, nil, nil)
	if err := service.Reconcile(ctx); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	manager.err = errors.New("manager unavailable")
	if err := service.Reconcile(ctx); err == nil {
		t.Fatal("expected manager error")
	}
	endpoints, _ := repo.GetAll(ctx)
	if len(endpoints) != 1 || endpoints[0].DiscoveryIdentity != "swarm:task-a" {
		t.Fatalf("manager failure replaced last snapshot: %+v", endpoints)
	}
	manager.err = nil
	manager.tasks = nil
	if err := service.Reconcile(ctx); err != nil {
		t.Fatalf("empty authoritative reconcile: %v", err)
	}
	endpoints, _ = repo.GetAll(ctx)
	if len(endpoints) != 0 {
		t.Fatalf("successful empty snapshot did not remove task: %+v", endpoints)
	}
}

func TestSwarmEventReconnectReconcilesChangesMissedDuringDisconnect(t *testing.T) {
	manager := &lifecycleSwarmManager{tasks: []SwarmTask{{ID: "task-a", NodeName: "gpu", Address: "10.0.0.2", Port: 11434}}, streamStopped: make(chan struct{}), firstDisconnect: make(chan struct{})}
	cfg := config.DefaultSwarmDiscoveryConfig()
	cfg.Enabled, cfg.Service, cfg.Network = true, "llm", "overlay"
	repo := NewSwarmEndpointRepository(cfg, nil)
	service := NewSwarmDiscoveryService(manager, repo, cfg.Service, cfg.Network, cfg.Port, time.Hour, nil, nil)
	if err := service.Reconcile(context.Background()); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	service.Start(context.Background())
	defer service.Stop(context.Background())
	select {
	case <-manager.firstDisconnect:
	case <-time.After(time.Second):
		t.Fatal("first event stream did not disconnect")
	}
	manager.setTasks([]SwarmTask{{ID: "task-b", NodeName: "gpu", Address: "10.0.0.3", Port: 11434}})
	waitFor(t, func() bool {
		endpoints, _ := repo.GetAll(context.Background())
		return len(endpoints) == 1 && endpoints[0].DiscoveryIdentity == "swarm:task-b" && manager.eventCalls.Load() >= 2
	})
}

func TestSwarmPeriodicSnapshotFindsChangesWithoutEventsAndStopCancelsStream(t *testing.T) {
	manager := &lifecycleSwarmManager{tasks: []SwarmTask{{ID: "task-a", NodeName: "gpu", Address: "10.0.0.2", Port: 11434}}, streamStopped: make(chan struct{}), firstDisconnect: make(chan struct{})}
	cfg := config.DefaultSwarmDiscoveryConfig()
	cfg.Enabled, cfg.Service, cfg.Network = true, "llm", "overlay"
	repo := NewSwarmEndpointRepository(cfg, nil)
	service := NewSwarmDiscoveryService(manager, repo, cfg.Service, cfg.Network, cfg.Port, 20*time.Millisecond, nil, nil)
	if err := service.Reconcile(context.Background()); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	manager.setTasks([]SwarmTask{{ID: "task-b", NodeName: "gpu", Address: "10.0.0.3", Port: 11434}})
	service.Start(context.Background())
	waitFor(t, func() bool {
		endpoints, _ := repo.GetAll(context.Background())
		return len(endpoints) == 1 && endpoints[0].DiscoveryIdentity == "swarm:task-b" && manager.eventCalls.Load() >= 2
	})
	if err := service.Stop(context.Background()); err != nil {
		t.Fatalf("stop discovery: %v", err)
	}
	select {
	case <-manager.streamStopped:
	case <-time.After(time.Second):
		t.Fatal("stopping discovery did not cancel the blocked event stream")
	}
	callsAfterStop := manager.snapshotCalls.Load()
	time.Sleep(60 * time.Millisecond)
	if got := manager.snapshotCalls.Load(); got != callsAfterStop {
		t.Fatalf("stopped service continued requesting snapshots: before %d, after %d", callsAfterStop, got)
	}
}

func TestSwarmStopWaitsForCanceledEndpointChangeCallbackAndStartIsOneShot(t *testing.T) {
	manager := &lifecycleSwarmManager{streamStopped: make(chan struct{}), firstDisconnect: make(chan struct{})}
	cfg := config.DefaultSwarmDiscoveryConfig()
	cfg.Enabled, cfg.Service, cfg.Network = true, "llm", "overlay"
	repo := NewSwarmEndpointRepository(cfg, nil)
	if _, _, err := repo.Reconcile(context.Background(), []*domain.Endpoint{swarmEndpoint("http://10.0.0.2:11434", "task-a")}); err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}
	callbackStarted, callbackFinished := make(chan struct{}), make(chan struct{})
	var callbackCount atomic.Int32
	service := NewSwarmDiscoveryService(manager, repo, cfg.Service, cfg.Network, cfg.Port, time.Hour, nil, func(ctx context.Context, _ bool) error {
		if callbackCount.Add(1) == 1 {
			close(callbackStarted)
			<-ctx.Done()
			close(callbackFinished)
		}
		return nil
	})
	service.Start(context.Background())
	service.Start(context.Background())
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("endpoint change callback did not start")
	}
	if err := service.Stop(context.Background()); err != nil {
		t.Fatalf("stop discovery: %v", err)
	}
	select {
	case <-callbackFinished:
	default:
		t.Fatal("Stop returned before the in-flight endpoint callback finished")
	}
	if got := manager.eventCalls.Load(); got != 1 {
		t.Fatalf("Start spawned %d event watchers; duplicate Start should be ignored", got)
	}
	if got := callbackCount.Load(); got != 1 {
		t.Fatalf("endpoint callback ran %d times", got)
	}
	service.Start(context.Background())
	if got := manager.eventCalls.Load(); got != 1 {
		t.Fatalf("Start after Stop spawned an event watcher: calls=%d", got)
	}
}

func TestSwarmCleanupFailureRemovesRoutingAndRetriesRegistryCleanup(t *testing.T) {
	ctx := context.Background()
	registry := &flakyCleanupRegistry{}
	registry.failures.Store(1)
	repo := NewSwarmEndpointRepository(config.DefaultSwarmDiscoveryConfig(), registry)
	endpoint := swarmEndpoint("http://10.0.0.2:11434", "task-a")
	if _, _, err := repo.Reconcile(ctx, []*domain.Endpoint{endpoint}); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	if _, _, err := repo.Reconcile(ctx, nil); err == nil {
		t.Fatal("expected model cleanup failure")
	}
	endpoints, _ := repo.GetRoutable(ctx)
	if len(endpoints) != 0 {
		t.Fatalf("removed endpoint remained routable after cleanup failure: %+v", endpoints)
	}
	if _, _, err := repo.Reconcile(ctx, nil); err != nil {
		t.Fatalf("pending cleanup retry failed: %v", err)
	}
	if got := registry.calls.Load(); got != 2 {
		t.Fatalf("registry cleanup called %d times, want failure plus retry", got)
	}
}

func TestSwarmDiscoveryExcludesTaskThatConflictsWithStaticEndpoint(t *testing.T) {
	ctx := context.Background()
	cfg := config.DefaultSwarmDiscoveryConfig()
	cfg.Enabled, cfg.Service, cfg.Network = true, "ollama", "overlay"
	repo := NewSwarmEndpointRepository(cfg, nil)
	manager := &testSwarmManager{tasks: []SwarmTask{{ID: "task-a", NodeName: "gpu-01", Address: "10.0.0.2", Port: cfg.Port}}}
	service := NewSwarmDiscoveryService(manager, repo, cfg.Service, cfg.Network, cfg.Port, time.Second, nil, nil)
	static := swarmEndpoint("http://10.0.0.2:11434", "static")
	service.ExcludeStaticEndpoints([]*domain.Endpoint{static})
	if err := service.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	endpoints, _ := repo.GetAll(ctx)
	if len(endpoints) != 0 {
		t.Fatalf("task conflicting with static endpoint was included: %+v", endpoints)
	}
}

func TestSwarmBuildEndpointsUsesNodeOverrideAndTaskUniqueNames(t *testing.T) {
	cfg := config.DefaultSwarmDiscoveryConfig()
	cfg.Service = "ollama"
	cfg.Endpoint.ModelFilter = &domain.FilterConfig{Exclude: []string{"*-unwanted"}}
	cfg.NodeEndpoints = map[string]config.EndpointConfig{
		"gpu-01": {ModelFilter: &domain.FilterConfig{Exclude: []string{"*-large"}}},
	}
	repo := NewSwarmEndpointRepository(cfg, nil)
	endpoints, err := repo.BuildEndpoints(context.Background(), []SwarmTask{
		{ID: "task-a", NodeName: "gpu-01", Address: "10.0.0.2", Port: cfg.Port},
		{ID: "task-b", NodeName: "gpu-02", Address: "10.0.0.3", Port: cfg.Port},
	})
	if err != nil {
		t.Fatal(err)
	}
	filters := make(map[string]*domain.FilterConfig)
	for _, endpoint := range endpoints {
		filters[endpoint.DiscoveryIdentity] = endpoint.ModelFilter
		if !strings.Contains(endpoint.Name, endpoint.DiscoveryIdentity[len("swarm:"):]) {
			t.Fatalf("endpoint name is not task-unique: %q", endpoint.Name)
		}
	}
	if len(filters["swarm:task-a"].Exclude) != 1 || filters["swarm:task-a"].Exclude[0] != "*-large" {
		t.Fatalf("node-specific filter did not replace the template: %+v", filters["swarm:task-a"])
	}
	if len(filters["swarm:task-b"].Exclude) != 1 || filters["swarm:task-b"].Exclude[0] != "*-unwanted" {
		t.Fatalf("template filter was not inherited: %+v", filters["swarm:task-b"])
	}
}

func TestReadSwarmEventsSignalsHintsAndReturnsStreamInterruption(t *testing.T) {
	changed := make(chan struct{}, 1)
	err := readSwarmEvents(context.Background(), strings.NewReader("{}\n"), changed)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("Docker event did not signal reconciliation")
	}
	broken := &failingReader{err: errors.New("stream reset")}
	if err := readSwarmEvents(context.Background(), broken, changed); err == nil {
		t.Fatal("expected interrupted stream error")
	}
}

func TestSwarmEventWatcherReconnectsAfterStreamInterruption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &reconnectingSwarmManager{}
	service := NewSwarmDiscoveryService(manager, NewSwarmEndpointRepository(structSwarmConfig(), nil), "ollama", "overlay", 11434, time.Hour, nil, nil)
	changed := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() { service.watchEvents(ctx, changed); close(done) }()
	select {
	case <-changed:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("event stream did not request reconciliation")
	}
	deadline := time.After(2 * time.Second)
	for manager.calls.Load() < 2 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("event watcher did not reconnect after EOF")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event watcher did not stop after context cancellation")
	}
}

func TestSwarmManagerClientUsesUnversionedPathByDefault(t *testing.T) {
	client := &SwarmManagerClient{baseURL: "http://docker"}
	if got := client.apiPath("/services"); got != "/services" {
		t.Fatalf("default API path = %q, want unversioned /services", got)
	}
	client.api = "v1.44"
	if got := client.apiPath("/services"); got != "/v1.44/services" {
		t.Fatalf("explicit API path = %q, want /v1.44/services", got)
	}
}

func TestSwarmManagerSelectsOnlyRunningTasksOnConfiguredNetwork(t *testing.T) {
	responses := map[string]string{
		"/v1.41/services": `[{"ID":"service-id","Spec":{"Name":"ollama","TaskTemplate":{"Networks":[{"Target":"network-id"}]}},"Endpoint":{"Spec":{"Ports":[{"Protocol":"tcp","TargetPort":11434}]}}}]`,
		"/v1.41/networks": `[{"Id":"network-id","Name":"llm-overlay"}]`,
		"/v1.41/nodes":    `[{"ID":"node-id","Description":{"Hostname":"gpu-01"}}]`,
		"/v1.41/tasks": `[
			{"ID":"running","ServiceID":"service-id","NodeID":"node-id","DesiredState":"running","Status":{"State":"running"},"NetworksAttachments":[{"Network":{"ID":"network-id"},"Addresses":["10.0.0.7/24"]}]},
			{"ID":"unresolved-node","ServiceID":"service-id","NodeID":"new-node","DesiredState":"running","Status":{"State":"running"},"NetworksAttachments":[{"Network":{"ID":"network-id"},"Addresses":["10.0.0.10/24"]}]},
			{"ID":"starting","ServiceID":"service-id","NodeID":"node-id","DesiredState":"running","Status":{"State":"preparing"},"NetworksAttachments":[{"Network":{"ID":"network-id"},"Addresses":["10.0.0.8/24"]}]},
			{"ID":"wrong-network","ServiceID":"service-id","NodeID":"node-id","DesiredState":"running","Status":{"State":"running"},"NetworksAttachments":[{"Network":{"ID":"other"},"Addresses":["10.0.0.9/24"]}]}
		]`,
	}
	client := &SwarmManagerClient{baseURL: "http://docker", api: "v1.41", timeout: time.Second, client: &http.Client{Transport: swarmRoundTripper(func(req *http.Request) (*http.Response, error) {
		for path, body := range responses {
			if strings.HasPrefix(req.URL.Path, path) {
				return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: req}, nil
			}
		}
		return nil, fmt.Errorf("unexpected Docker API path %s", req.URL.Path)
	})}}
	tasks, err := client.ListRunningTasks(context.Background(), "ollama", "llm-overlay", 11434)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].ID != "running" || tasks[0].NodeName != "gpu-01" || tasks[0].Address != "10.0.0.7" || tasks[0].Port != 11434 {
		t.Fatalf("unexpected task selection: %+v", tasks)
	}
}

type testSwarmManager struct {
	tasks []SwarmTask
	err   error
}

type reconnectingSwarmManager struct{ calls atomic.Int32 }

type lifecycleSwarmManager struct {
	mu              sync.RWMutex
	tasks           []SwarmTask
	snapshotCalls   atomic.Int32
	eventCalls      atomic.Int32
	streamStopped   chan struct{}
	firstDisconnect chan struct{}
}

func (m *lifecycleSwarmManager) ListRunningTasks(context.Context, string, string, uint32) ([]SwarmTask, error) {
	m.snapshotCalls.Add(1)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]SwarmTask(nil), m.tasks...), nil
}

func (m *lifecycleSwarmManager) setTasks(tasks []SwarmTask) {
	m.mu.Lock()
	m.tasks = append([]SwarmTask(nil), tasks...)
	m.mu.Unlock()
}

func (m *lifecycleSwarmManager) Events(ctx context.Context, _ string) (*http.Response, error) {
	if m.eventCalls.Add(1) == 1 {
		close(m.firstDisconnect)
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	}
	body := &blockingSwarmReader{ctx: ctx, stopped: m.streamStopped}
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: body, Header: make(http.Header)}, nil
}

type blockingSwarmReader struct {
	ctx     context.Context
	stopped chan struct{}
	once    sync.Once
}

func (r *blockingSwarmReader) Read([]byte) (int, error) {
	<-r.ctx.Done()
	r.once.Do(func() { close(r.stopped) })
	return 0, r.ctx.Err()
}

func (r *blockingSwarmReader) Close() error { return nil }

type flakyCleanupRegistry struct {
	domain.ModelRegistry
	failures atomic.Int32
	calls    atomic.Int32
}

func (r *flakyCleanupRegistry) RemoveEndpoint(context.Context, string) error {
	r.calls.Add(1)
	if r.failures.Add(-1) >= 0 {
		return errors.New("transient registry cleanup failure")
	}
	return nil
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func (m *reconnectingSwarmManager) ListRunningTasks(context.Context, string, string, uint32) ([]SwarmTask, error) {
	return nil, nil
}
func (m *reconnectingSwarmManager) Events(context.Context, string) (*http.Response, error) {
	m.calls.Add(1)
	body := ""
	if m.calls.Load() == 1 {
		body = "{}\n"
	}
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
}

func (m *testSwarmManager) ListRunningTasks(context.Context, string, string, uint32) ([]SwarmTask, error) {
	return m.tasks, m.err
}
func (m *testSwarmManager) Events(context.Context, string) (*http.Response, error) {
	return nil, io.EOF
}

type failingReader struct{ err error }

func (r *failingReader) Read([]byte) (int, error) { return 0, r.err }

type swarmRoundTripper func(*http.Request) (*http.Response, error)

func (f swarmRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func structSwarmConfig() config.SwarmDiscoveryConfig { return config.SwarmDiscoveryConfig{} }

func swarmEndpoint(rawURL, taskID string) *domain.Endpoint {
	parsed, _ := url.Parse(rawURL)
	return &domain.Endpoint{URL: parsed, URLString: rawURL, Name: taskID, DiscoveryIdentity: "swarm:" + taskID, Status: domain.StatusUnknown}
}
