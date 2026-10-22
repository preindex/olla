package handlers

import (
	"context"
	"errors"
	"net/http"
	"time"

	proxycore "github.com/thushan/olla/internal/adapter/proxy/core"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/core/ports"
)

type inferenceOutcomeWriter struct {
	http.ResponseWriter
	status      int
	writeFailed bool
}

func newInferenceOutcomeWriter(w http.ResponseWriter) *inferenceOutcomeWriter {
	return &inferenceOutcomeWriter{ResponseWriter: w}
}

func (w *inferenceOutcomeWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	if status >= 100 && status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *inferenceOutcomeWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	if err != nil || n != len(p) {
		w.writeFailed = true
	}
	return n, err
}

func (w *inferenceOutcomeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *inferenceOutcomeWriter) Flush() {
	_ = w.FlushError()
}

func (w *inferenceOutcomeWriter) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if !errors.Is(err, http.ErrNotSupported) {
		if w.status == 0 {
			w.status = http.StatusOK
		}
		if err != nil {
			w.writeFailed = true
		}
	}
	return err
}

func (a *Application) recordInferenceOutcome(ctx context.Context, pr *proxyRequest, w *inferenceOutcomeWriter) {
	collector, ok := a.statsCollector.(ports.InferenceHealthCollector)
	if !ok || pr == nil || pr.stats == nil || (w != nil && w.writeFailed) {
		return
	}
	// Supported inference APIs use POST. Proxied model listings, version checks
	// and other management reads must not turn idle inference into healthy traffic.
	if pr.method != http.MethodPost {
		return
	}
	if ctx != nil && ctx.Err() == context.Canceled {
		return
	}
	// An early return without a committed response is not evidence of success.
	status := 0
	if w != nil {
		if w.status != 0 {
			status = w.status
		}
	}
	if status == 0 && !pr.stats.InferenceTimedOut && !pr.stats.InferenceNoRouting && !pr.stats.InferenceStreamError {
		return
	}
	modelKnown := a.inferenceModelKnown(ctx, pr.model)
	if status >= http.StatusOK && status < http.StatusMultipleChoices && a.Config != nil && a.Config.Discovery.ModelDiscovery.Enabled {
		modelKnown = true
	}
	collector.RecordInferenceRequest(ports.InferenceRequestOutcome{
		Model: pr.model, ModelKnown: modelKnown, At: time.Now(), Latency: time.Since(pr.stats.StartTime), StatusCode: status,
		TimedOut: pr.stats.InferenceTimedOut, NoRouting: pr.stats.InferenceNoRouting, StreamError: pr.stats.InferenceStreamError,
	})
}

func isInferenceTimeout(err error) bool {
	return proxycore.IsTimeoutError(err)
}

func (a *Application) inferenceModelKnown(ctx context.Context, model string) bool {
	if model == "" || a.modelRegistry == nil || a.Config == nil {
		return false
	}
	if !a.Config.Discovery.ModelDiscovery.Enabled {
		return false
	}
	if a.modelRegistry.IsModelAvailable(ctx, model) {
		return true
	}
	if endpoints, err := a.modelRegistry.GetEndpointsForModel(ctx, model); err == nil && len(endpoints) > 0 {
		return true
	}
	if _, ok := a.Config.ModelAliases[model]; ok {
		return true
	}
	return a.aliasResolver != nil && a.aliasResolver.IsAlias(model)
}

func (a *Application) inferenceAvailability(ctx context.Context, endpoints []*domain.Endpoint) map[string]bool {
	if a.modelRegistry == nil || a.Config == nil || !a.Config.Discovery.ModelDiscovery.Enabled {
		return nil
	}
	all, err := a.modelRegistry.GetAllModels(ctx)
	if err != nil {
		return nil
	}
	// A successful empty catalogue is authoritative: any previously observed
	// models have no current route. A nil map is reserved for unavailable or
	// disabled discovery.
	known := make(map[string]bool)
	for _, models := range all {
		for _, info := range models {
			if info != nil && info.Name != "" {
				known[info.Name] = modelHasRoutableEndpoint(ctx, a.modelRegistry, info.Name, endpoints)
			}
		}
	}
	for alias, actual := range a.Config.ModelAliases {
		available := false
		for _, model := range actual {
			if modelHasRoutableEndpoint(ctx, a.modelRegistry, model, endpoints) {
				available = true
				break
			}
		}
		known[alias] = available
	}
	return known
}

func modelHasRoutableEndpoint(ctx context.Context, registry domain.ModelRegistry, model string, endpoints []*domain.Endpoint) bool {
	modelEndpoints, err := registry.GetEndpointsForModel(ctx, model)
	if err != nil || len(modelEndpoints) == 0 {
		return false
	}
	registered := make(map[string]struct{}, len(modelEndpoints))
	for _, endpoint := range modelEndpoints {
		registered[endpoint] = struct{}{}
	}
	for _, endpoint := range endpoints {
		if endpoint == nil || !endpoint.Status.IsRoutable() {
			continue
		}
		if _, ok := registered[endpoint.GetURLString()]; ok {
			return true
		}
	}
	return false
}
