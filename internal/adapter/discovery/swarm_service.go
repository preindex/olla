package discovery

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/logger"
)

// SwarmDiscoveryService maintains the task snapshot. Docker events only wake
// reconciliation early; periodic snapshots recover missed events and API
// outages without treating incomplete event streams as authoritative.
type SwarmDiscoveryService struct {
	client       SwarmManagerAPI
	logger       logger.StyledLogger
	repo         *SwarmEndpointRepository
	onChange     func(context.Context, bool) error
	stop         chan struct{}
	cancel       context.CancelFunc
	done         chan struct{}
	excludedURLs map[string]struct{}
	service      string
	network      string
	interval     time.Duration
	stateMu      sync.Mutex
	reconcileMu  sync.Mutex
	port         uint32
	started      bool
	stopped      bool
}

type SwarmManagerAPI interface {
	ListRunningTasks(context.Context, string, string, uint32) ([]SwarmTask, error)
	Events(context.Context, string) (*http.Response, error)
}

func NewSwarmDiscoveryService(client SwarmManagerAPI, repo *SwarmEndpointRepository, service, network string, port uint32, interval time.Duration, log logger.StyledLogger, onChange func(context.Context, bool) error) *SwarmDiscoveryService {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &SwarmDiscoveryService{client: client, repo: repo, service: service, network: network, port: port, interval: interval, logger: log, onChange: onChange, stop: make(chan struct{}), done: make(chan struct{}), excludedURLs: make(map[string]struct{})}
}

// ExcludeStaticEndpoints gives static configuration precedence if it names the
// same URL as a discovered task. This prevents duplicate routing and protects
// static model ownership when task snapshots change.
func (s *SwarmDiscoveryService) ExcludeStaticEndpoints(endpoints []*domain.Endpoint) {
	for _, endpoint := range endpoints {
		if endpoint != nil && endpoint.URLString != "" {
			s.excludedURLs[endpoint.URLString] = struct{}{}
		}
	}
}

func (s *SwarmDiscoveryService) Reconcile(ctx context.Context) error {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	tasks, err := s.client.ListRunningTasks(ctx, s.service, s.network, s.port)
	if err != nil {
		return err
	}
	endpoints, err := s.repo.BuildEndpoints(ctx, tasks)
	if err != nil {
		return err
	}
	filtered := endpoints[:0]
	for _, endpoint := range endpoints {
		if _, exists := s.excludedURLs[endpoint.URLString]; exists {
			continue
		}
		filtered = append(filtered, endpoint)
	}
	added, removed, err := s.repo.Reconcile(ctx, filtered)
	changed := len(added) > 0 || len(removed) > 0
	if changed && s.onChange != nil {
		return errors.Join(err, s.onChange(ctx, len(added) > 0))
	}
	return err
}

func (s *SwarmDiscoveryService) Start(ctx context.Context) {
	s.stateMu.Lock()
	if s.started || s.stopped {
		s.stateMu.Unlock()
		return
	}
	s.started = true
	serviceCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.stateMu.Unlock()
	go s.run(serviceCtx)
}

func (s *SwarmDiscoveryService) Stop(ctx context.Context) error {
	s.stateMu.Lock()
	if !s.stopped {
		s.stopped = true
		close(s.stop)
		if s.cancel != nil {
			s.cancel()
		}
	}
	started := s.started
	s.stateMu.Unlock()
	if closer, ok := s.client.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
	if !started {
		return nil
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *SwarmDiscoveryService) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	events := make(chan struct{}, 1)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		s.watchEvents(ctx, events)
	}()
	defer func() { <-watchDone }()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-ticker.C:
		case <-events:
		}
		if err := s.Reconcile(ctx); err != nil && ctx.Err() == nil {
			// A manager/API failure never replaces the last known good snapshot.
			s.logger.Warn("Swarm reconciliation failed; retaining last endpoint snapshot", "error", err)
		}
	}
}

func (s *SwarmDiscoveryService) watchEvents(ctx context.Context, changed chan<- struct{}) {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		default:
		}
		response, err := s.client.Events(ctx, s.service)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("Swarm event stream unavailable; periodic reconciliation remains active", "error", err)
			if !waitContext(ctx, s.stop, backoff) {
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		// Reconcile after reconnect so any events missed during the gap are caught.
		select {
		case changed <- struct{}{}:
		default:
		}
		streamErr := readSwarmEvents(ctx, response.Body, changed)
		_ = response.Body.Close()
		if ctx.Err() != nil {
			return
		}
		if streamErr != nil && !errors.Is(streamErr, io.EOF) {
			s.logger.Warn("Swarm event stream interrupted; reconnecting", "error", streamErr)
		}
		if !waitContext(ctx, s.stop, backoff) {
			return
		}
	}
}

func readSwarmEvents(ctx context.Context, body io.Reader, changed chan<- struct{}) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var event map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("decode Docker event: %w", err)
		}
		select {
		case changed <- struct{}{}:
		default:
		}
	}
	return scanner.Err()
}

func waitContext(ctx context.Context, stop <-chan struct{}, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	case <-timer.C:
		return true
	}
}
