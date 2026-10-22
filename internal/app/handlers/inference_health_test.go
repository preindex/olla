package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thushan/olla/internal/adapter/inspector"
	"github.com/thushan/olla/internal/adapter/translator"
	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/core/ports"
	"github.com/thushan/olla/internal/logger"
)

type inferenceStatusStats struct {
	*mockStatusStatsCollector
	health ports.InferenceHealth
}

func (s *inferenceStatusStats) GetInferenceHealth(known map[string]bool, _ time.Time) ports.InferenceHealth {
	return s.health
}

func (s *inferenceStatusStats) RecordInferenceRequest(ports.InferenceRequestOutcome) {}

func TestStatusHandlerIncludesInferenceHealthAdditively(t *testing.T) {
	stats := &inferenceStatusStats{
		mockStatusStatsCollector: &mockStatusStatsCollector{},
		health: ports.InferenceHealth{
			Status: "degraded", State: "observed", Observations: 12,
			Models: map[string]ports.InferenceModelHealth{
				"chat": {Status: "critical", State: "observed", Availability: "available", Observations: 10},
			},
		},
	}
	app := buildSystemStatusApp(t, allHealthyEndpoints(), ports.ProxyStats{})
	app.statsCollector = stats
	app.Config.InferenceHealth = config.DefaultInferenceHealthConfig()

	request := httptest.NewRequest(http.MethodGet, "/internal/status", nil)
	response := httptest.NewRecorder()
	app.statusHandler(response, request)
	require.Equal(t, http.StatusOK, response.Code)

	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Contains(t, body, "inference")
	var inference ports.InferenceHealth
	require.NoError(t, json.Unmarshal(body["inference"], &inference))
	require.Equal(t, "degraded", inference.Status)
	require.Equal(t, "critical", inference.Models["chat"].Status)
}

func TestStatusInferenceETagTracksSemanticChanges(t *testing.T) {
	stats := &inferenceStatusStats{
		mockStatusStatsCollector: &mockStatusStatsCollector{},
		health: ports.InferenceHealth{
			Status: "insufficient_data", State: "insufficient_data",
			Availability: ports.InferenceModelAvailability{Status: "partial", Known: 2, Available: 1, Unavailable: 1},
			Models: map[string]ports.InferenceModelHealth{
				"chat": {Status: "insufficient_data", State: "insufficient_data", Availability: "available"},
				"code": {Status: "unavailable", State: "unavailable", Availability: "unavailable"},
			},
		},
	}
	app := buildSystemStatusApp(t, allHealthyEndpoints(), ports.ProxyStats{})
	app.statsCollector = stats
	app.Config.InferenceHealth = config.DefaultInferenceHealthConfig()
	poll := func(etag string, wantStatus int) string {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, "/internal/status", nil)
		request.Header.Set("If-None-Match", etag)
		response := httptest.NewRecorder()
		app.statusHandler(response, request)
		require.Equal(t, wantStatus, response.Code)
		return response.Header().Get("ETag")
	}
	etag := poll("", http.StatusOK)
	require.Equal(t, etag, poll(etag, http.StatusNotModified))

	// Window expiry changes inference state without changing legacy counters.
	stats.health.Status, stats.health.State = "idle", "idle"
	next := poll(etag, http.StatusOK)
	require.NotEqual(t, etag, next)
	etag = next

	// Swap model availability while keeping aggregate availability unchanged.
	stats.health.Models["chat"], stats.health.Models["code"] = stats.health.Models["code"], stats.health.Models["chat"]
	next = poll(etag, http.StatusOK)
	require.NotEqual(t, etag, next)
	etag = next

	stats.health.Observations, stats.health.ServerErrors = 1, 1
	next = poll(etag, http.StatusOK)
	require.NotEqual(t, etag, next)
	require.Equal(t, next, poll(next, http.StatusNotModified))

	first := hashStatusResponse(&StatusResponse{Inference: &stats.health})
	stats.health.Models = map[string]ports.InferenceModelHealth{
		"code": stats.health.Models["code"], "chat": stats.health.Models["chat"],
	}
	require.Equal(t, first, hashStatusResponse(&StatusResponse{Inference: &stats.health}))
	app.Config.InferenceHealth.Enabled = false
	poll(next, http.StatusOK)
}

func TestInferenceOutcomeDoesNotInventSuccessOnEarlyReturn(t *testing.T) {
	stats := &inferenceCaptureStats{mockStatsCollector: &mockStatsCollector{}}
	app := &Application{statsCollector: stats}
	app.recordInferenceOutcome(context.Background(), &proxyRequest{
		method: http.MethodPost, model: "chat", stats: &ports.RequestStats{StartTime: time.Now()},
	}, newInferenceOutcomeWriter(httptest.NewRecorder()))
	require.Empty(t, stats.outcomes)
}

type inferenceFlushErrorWriter struct {
	*httptest.ResponseRecorder
}

func (*inferenceFlushErrorWriter) FlushError() error { return errors.New("client disconnected") }

func TestInferenceOutcomeExcludesClientFlushFailure(t *testing.T) {
	stats := &inferenceCaptureStats{mockStatsCollector: &mockStatsCollector{}}
	app := &Application{statsCollector: stats}
	writer := newInferenceOutcomeWriter(&inferenceFlushErrorWriter{httptest.NewRecorder()})
	require.Error(t, http.NewResponseController(writer).Flush())
	app.recordInferenceOutcome(context.Background(), &proxyRequest{
		method: http.MethodPost, model: "chat", stats: &ports.RequestStats{StartTime: time.Now()},
	}, writer)
	require.Empty(t, stats.outcomes)
}

type inferenceHeaderWriter struct {
	*httptest.ResponseRecorder
	statuses []int
}

func (w *inferenceHeaderWriter) WriteHeader(status int) {
	w.statuses = append(w.statuses, status)
}

func TestInferenceOutcomeWriterPassesInformationalHeaders(t *testing.T) {
	underlying := &inferenceHeaderWriter{ResponseRecorder: httptest.NewRecorder()}
	writer := newInferenceOutcomeWriter(underlying)
	writer.WriteHeader(http.StatusEarlyHints)
	require.Zero(t, writer.status)
	writer.WriteHeader(http.StatusServiceUnavailable)
	require.Equal(t, http.StatusServiceUnavailable, writer.status)
	require.Equal(t, []int{http.StatusEarlyHints, http.StatusServiceUnavailable}, underlying.statuses)
}

func TestTranslationUncommittedErrorWithContentTypeRecordsFailure(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprintf("streaming=%t", streaming), func(t *testing.T) {
			stats := &inferenceCaptureStats{mockStatsCollector: &mockStatsCollector{}}
			log := &mockStyledLogger{}
			trans := &mockTranslator{
				name: "test-translator", implementsErrorWriter: true,
				writeErrorFunc: func(w http.ResponseWriter, _ error, status int) { w.WriteHeader(status) },
				transformRequestFunc: func(context.Context, *http.Request) (*translator.TransformedRequest, error) {
					return &translator.TransformedRequest{OpenAIRequest: map[string]interface{}{"model": "chat"}, IsStreaming: streaming}, nil
				},
				transformResponseFunc: func(context.Context, interface{}, *http.Request) (interface{}, error) {
					return map[string]interface{}{"unsupported": make(chan int)}, nil
				},
				transformStreamingFunc: func(_ context.Context, upstream io.Reader, w http.ResponseWriter, _ *http.Request) error {
					_, err := io.Copy(io.Discard, upstream)
					if err != nil {
						return err
					}
					w.Header().Set(constants.HeaderContentType, "text/event-stream")
					return errors.New("translation failed before writing")
				},
			}
			app := &Application{
				logger: log, statsCollector: stats, Config: &config.Config{},
				repository: &mockEndpointRepository{}, inspectorChain: inspector.NewChain(log),
				profileFactory: &mockProfileFactory{}, discoveryService: &mockDiscoveryServiceForTranslation{},
				proxyService: &mockProxyService{proxyFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ []*domain.Endpoint, _ *ports.RequestStats, _ logger.StyledLogger) error {
					_, err := w.Write([]byte(`{"id":"ok"}`))
					return err
				}},
			}
			request := httptest.NewRequest(http.MethodPost, "/olla/anthropic/v1/messages", strings.NewReader(`{"model":"chat"}`))
			response := httptest.NewRecorder()
			app.translationHandler(trans).ServeHTTP(response, request)
			require.Equal(t, http.StatusBadGateway, response.Code)
			require.Len(t, stats.outcomes, 1)
			require.Equal(t, http.StatusBadGateway, stats.outcomes[0].StatusCode)
		})
	}
}

func TestInferenceCancelledTranslationWaitsForProxyStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	finish := make(chan struct{})
	returned := make(chan error, 1)
	log := &mockStyledLogger{}
	pr := &proxyRequest{model: "chat", requestLogger: log, stats: &ports.RequestStats{StartTime: time.Now()}}
	app := &Application{proxyService: &mockProxyService{proxyFunc: func(ctx context.Context, _ http.ResponseWriter, _ *http.Request, _ []*domain.Endpoint, stats *ports.RequestStats, _ logger.StyledLogger) error {
		close(started)
		<-ctx.Done()
		<-finish
		stats.InferenceStreamError = true
		return ctx.Err()
	}}}
	go func() {
		returned <- app.executeTranslatedStreamingRequest(ctx, httptest.NewRecorder(),
			httptest.NewRequest(http.MethodPost, "/test", nil), []*domain.Endpoint{{Name: "ep"}}, pr, &mockTranslator{})
	}()
	<-started
	cancel()
	select {
	case err := <-returned:
		close(finish)
		t.Fatalf("returned before proxy completed its stats updates: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	require.ErrorIs(t, <-returned, context.Canceled)
	require.True(t, pr.stats.InferenceStreamError)
}

func TestInferenceOutcomeWriterPreservesStreamingFlush(t *testing.T) {
	underlying := httptest.NewRecorder()
	writer := newInferenceOutcomeWriter(underlying)
	writer.WriteHeader(http.StatusOK)
	writer.Flush()
	require.Equal(t, http.StatusOK, underlying.Code)
	require.True(t, underlying.Flushed)
}

type failedWriteResponseWriter struct {
	header http.Header
}

func (w *failedWriteResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (*failedWriteResponseWriter) WriteHeader(int) {}
func (*failedWriteResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("client disconnected")
}

func TestInferenceOutcomeWriterExcludesClientWriteFailure(t *testing.T) {
	stats := &inferenceCaptureStats{mockStatsCollector: &mockStatsCollector{}}
	app := &Application{statsCollector: stats}
	writer := newInferenceOutcomeWriter(&failedWriteResponseWriter{})
	_, err := writer.Write([]byte("response"))
	require.Error(t, err)
	app.recordInferenceOutcome(context.Background(), &proxyRequest{
		method: http.MethodPost, model: "chat", stats: &ports.RequestStats{StartTime: time.Now()},
	}, writer)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	require.Empty(t, stats.outcomes, "a client disconnect must not be counted as an inference failure")
}

type inferenceCaptureStats struct {
	*mockStatsCollector
	mu       sync.Mutex
	attempts int
	outcomes []ports.InferenceRequestOutcome
}

func (s *inferenceCaptureStats) RecordRequest(*domain.Endpoint, string, time.Duration, int64) {
	s.mu.Lock()
	s.attempts++
	s.mu.Unlock()
}

func (s *inferenceCaptureStats) RecordInferenceRequest(outcome ports.InferenceRequestOutcome) {
	s.mu.Lock()
	s.outcomes = append(s.outcomes, outcome)
	s.mu.Unlock()
}

func (s *inferenceCaptureStats) GetInferenceHealth(map[string]bool, time.Time) ports.InferenceHealth {
	return ports.InferenceHealth{}
}

func TestProxyHandlerRecordsOneFinalInferenceOutcome(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		path        string
		status      int
		attempts    int
		streamError bool
		cancel      bool
		wantCount   int
		wantClass   string
	}{
		{name: "retry attempts finish once", status: http.StatusOK, attempts: 2, wantCount: 1, wantClass: "success"},
		{name: "backend 5xx", status: http.StatusServiceUnavailable, attempts: 3, wantCount: 1, wantClass: "backend_5xx"},
		{name: "client 4xx excluded by collector", status: http.StatusBadRequest, attempts: 1, wantCount: 1, wantClass: "client_4xx"},
		{name: "stream error after 200", status: http.StatusOK, attempts: 1, streamError: true, wantCount: 1, wantClass: "stream_errors"},
		{name: "cancelled request excluded", status: http.StatusOK, attempts: 1, cancel: true},
		{name: "model listing excluded", method: http.MethodGet, path: "/olla/proxy/v1/models", status: http.StatusOK, attempts: 1},
		{name: "version check excluded", method: http.MethodGet, path: "/olla/proxy/api/version", status: http.StatusOK, attempts: 1},
		{name: "process listing excluded", method: http.MethodGet, path: "/olla/proxy/api/ps", status: http.StatusOK, attempts: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := &inferenceCaptureStats{mockStatsCollector: &mockStatsCollector{}}
			log := &mockStyledLogger{}
			chain := inspector.NewChain(log)
			bodyInspector, err := inspector.NewBodyInspector(log)
			require.NoError(t, err)
			chain.AddInspector(bodyInspector)
			endpoint := &domain.Endpoint{Name: "ep", Type: "openai", URLString: "http://localhost:8080", Status: domain.StatusHealthy}
			app := &Application{
				Config: &config.Config{}, logger: log, inspectorChain: chain,
				discoveryService: &mockDiscoveryServiceForTranslation{}, statsCollector: stats,
				proxyService: &mockProxyService{proxyFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request, endpoints []*domain.Endpoint, requestStats *ports.RequestStats, _ logger.StyledLogger) error {
					for range tt.attempts {
						stats.RecordRequest(endpoint, "failure", time.Millisecond, 0)
					}
					if tt.streamError {
						requestStats.InferenceStreamError = true
					}
					w.WriteHeader(tt.status)
					return nil
				}},
			}
			if _, ok := app.statsCollector.(ports.InferenceHealthCollector); !ok {
				t.Fatal("capture collector does not satisfy optional inference-health seam")
			}

			method, path := tt.method, tt.path
			if method == "" {
				method = http.MethodPost
			}
			if path == "" {
				path = "/olla/proxy/v1/chat/completions"
			}
			var body io.Reader
			if method == http.MethodPost {
				body = strings.NewReader(`{"model":"chat"}`)
			}
			request := httptest.NewRequest(method, path, body)
			request.Header.Set(constants.HeaderContentType, constants.ContentTypeJSON)
			request = request.WithContext(context.WithValue(request.Context(), constants.ContextRoutePrefixKey, "/olla/proxy/"))
			if tt.cancel {
				cancelled, cancel := context.WithCancel(request.Context())
				cancel()
				request = request.WithContext(cancelled)
			}
			response := httptest.NewRecorder()
			app.proxyHandler(response, request)

			stats.mu.Lock()
			defer stats.mu.Unlock()
			require.Len(t, stats.outcomes, tt.wantCount, "response status=%d body=%q legacy attempts=%d", response.Code, response.Body.String(), stats.attempts)
			if tt.wantClass == "" {
				return
			}
			outcome := stats.outcomes[0]
			switch tt.wantClass {
			case "success":
				require.Equal(t, 200, outcome.StatusCode)
			case "backend_5xx":
				require.Equal(t, 503, outcome.StatusCode)
			case "client_4xx":
				require.Equal(t, 400, outcome.StatusCode)
			case "stream_errors":
				require.True(t, outcome.StreamError)
				require.Equal(t, 200, outcome.StatusCode)
			}
			require.Equal(t, tt.attempts, stats.attempts, "attempt-oriented legacy telemetry should remain unchanged")
		})
	}
}

func TestTranslationHandlerRecordsOneFinalInferenceOutcome(t *testing.T) {
	stats := &inferenceCaptureStats{mockStatsCollector: &mockStatsCollector{}}
	log := &mockStyledLogger{}
	trans := &mockTranslator{name: "test-translator"}
	app := &Application{
		logger: log, proxyService: &mockProxyService{}, statsCollector: stats,
		repository: &mockEndpointRepository{}, inspectorChain: inspector.NewChain(log),
		profileFactory: &mockProfileFactory{}, discoveryService: &mockDiscoveryServiceForTranslation{},
		Config: &config.Config{},
	}
	body := `{"model":"test-model","messages":[{"role":"user","content":"hello"}]}`
	request := httptest.NewRequest(http.MethodPost, "/olla/anthropic/v1/messages", strings.NewReader(body))
	request.Header.Set(constants.HeaderContentType, constants.ContentTypeJSON)
	response := httptest.NewRecorder()
	app.translationHandler(trans).ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	require.Len(t, stats.outcomes, 1)
	require.Equal(t, "test-model", stats.outcomes[0].Model)
	require.Equal(t, http.StatusOK, stats.outcomes[0].StatusCode)
}

func TestTranslationHandlerRecordsTranslatedBackend503(t *testing.T) {
	stats := &inferenceCaptureStats{mockStatsCollector: &mockStatsCollector{}}
	log := &mockStyledLogger{}
	trans := &mockTranslator{
		name:                  "test-translator",
		implementsErrorWriter: true,
		writeErrorFunc: func(w http.ResponseWriter, _ error, status int) {
			w.WriteHeader(status)
		},
	}
	app := &Application{
		logger: log,
		proxyService: &mockProxyService{proxyFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ []*domain.Endpoint, _ *ports.RequestStats, _ logger.StyledLogger) error {
			w.Header().Set(constants.HeaderContentType, "text/plain")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, err := w.Write([]byte("backend unavailable"))
			return err
		}},
		statsCollector: stats, repository: &mockEndpointRepository{},
		inspectorChain: inspector.NewChain(log), profileFactory: &mockProfileFactory{},
		discoveryService: &mockDiscoveryServiceForTranslation{}, Config: &config.Config{},
	}
	body := `{"model":"test-model","messages":[{"role":"user","content":"hello"}]}`
	request := httptest.NewRequest(http.MethodPost, "/olla/anthropic/v1/messages", strings.NewReader(body))
	request.Header.Set(constants.HeaderContentType, constants.ContentTypeJSON)
	response := httptest.NewRecorder()
	app.translationHandler(trans).ServeHTTP(response, request)

	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	require.Len(t, stats.outcomes, 1)
	require.Equal(t, http.StatusServiceUnavailable, stats.outcomes[0].StatusCode)
}
