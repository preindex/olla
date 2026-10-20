package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thushan/olla/internal/adapter/inspector"
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
		model: "chat", stats: &ports.RequestStats{StartTime: time.Now()},
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

			request := httptest.NewRequest(http.MethodPost, "/olla/proxy/v1/chat/completions", strings.NewReader(`{"model":"chat"}`))
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
