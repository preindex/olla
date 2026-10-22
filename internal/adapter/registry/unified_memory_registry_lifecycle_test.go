package registry

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/thushan/olla/internal/adapter/unifier"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/core/ports"
)

// Gate the real unifier so tests exercise its catalogue mutations as well as
// the registry's publication, without depending on goroutine scheduling.
type gatedCatalogueUnifier struct {
	ports.ModelUnifier
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

type gatedMergeUnifier struct {
	ports.ModelUnifier
	started chan struct{}
	release chan struct{}
}

func (u *gatedMergeUnifier) MergeUnifiedModels(ctx context.Context, models []*domain.UnifiedModel) (*domain.UnifiedModel, error) {
	close(u.started)
	<-u.release
	return u.ModelUnifier.MergeUnifiedModels(ctx, models)
}

func (u *gatedCatalogueUnifier) UnifyModels(ctx context.Context, models []*domain.ModelInfo, endpoint *domain.Endpoint) ([]*domain.UnifiedModel, error) {
	if len(models) > 0 {
		u.once.Do(func() {
			close(u.started)
			<-u.release
		})
	}
	return u.ModelUnifier.UnifyModels(ctx, models, endpoint)
}

func lifecycleToken(r *UnifiedMemoryModelRegistry, url string) *unificationToken {
	r.generationMu.Lock()
	defer r.generationMu.Unlock()
	return r.replaceUnificationTokenLocked(url)
}

func assertLifecycleCatalogues(t *testing.T, r *UnifiedMemoryModelRegistry, expected ...string) {
	t.Helper()
	r.unificationMutex.Lock()
	defer r.unificationMutex.Unlock()
	ctx := context.Background()
	internal, err := r.unifier.(interface {
		GetAllModels(context.Context) ([]*domain.UnifiedModel, error)
	}).GetAllModels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	global, err := r.GetUnifiedModels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]string{}, expected...)
	sort.Strings(want)
	for name, models := range map[string][]*domain.UnifiedModel{"internal": internal, "unified": global} {
		got := []string{}
		for _, model := range models {
			for _, source := range model.SourceEndpoints {
				got = append(got, model.ID+"@"+source.EndpointURL)
			}
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s catalogue = %v, want %v", name, got, want)
		}
	}
}

func waitLifecycle(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for lifecycle transition")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAsyncUnificationRemovalBeforeCallback(t *testing.T) {
	r := createTestUnifiedRegistry()
	ctx := context.Background()
	url := "http://removed:11434"
	token := lifecycleToken(r, url)
	if err := r.RemoveEndpoint(ctx, url); err != nil {
		t.Fatal(err)
	}
	// Replay a queued callback after removal has already returned.
	r.unifyModelsAsync(ctx, url, []*domain.ModelInfo{{Name: "stale"}}, token)
	assertLifecycleCatalogues(t, r)
	if _, err := r.GetUnifiedModel(ctx, "stale"); err == nil {
		t.Fatal("removed callback restored an alias")
	}
}

func TestAsyncUnificationStaleCallbackAfterReplacement(t *testing.T) {
	r := createTestUnifiedRegistry()
	ctx := context.Background()
	url := "http://replacement:11434"
	stale := lifecycleToken(r, url)
	current := lifecycleToken(r, url)
	r.unifyModelsAsync(ctx, url, []*domain.ModelInfo{{Name: "current"}}, current)
	r.unifyModelsAsync(ctx, url, []*domain.ModelInfo{{Name: "stale"}}, stale)
	assertLifecycleCatalogues(t, r, "current@"+url)
	if _, err := r.unifier.ResolveAlias(ctx, "stale"); err == nil {
		t.Fatal("stale callback mutated the alias catalogue")
	}
}

func TestAsyncUnificationRemovalDuringCallback(t *testing.T) {
	r := createTestUnifiedRegistry()
	real := unifier.NewDefaultUnifier()
	gate := &gatedCatalogueUnifier{ModelUnifier: real, started: make(chan struct{}), release: make(chan struct{})}
	r.unifier = gate
	ctx := context.Background()
	url := "http://in-flight:11434"
	if err := r.RegisterModels(ctx, url, []*domain.ModelInfo{{Name: "stale"}}); err != nil {
		t.Fatal(err)
	}
	<-gate.started
	removed := make(chan error, 1)
	go func() { removed <- r.RemoveEndpoint(ctx, url) }()
	waitLifecycle(t, func() bool {
		r.generationMu.Lock()
		defer r.generationMu.Unlock()
		return r.unificationTokens[url] == nil
	})
	close(gate.release)
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	r.unifier = real
	assertLifecycleCatalogues(t, r)
}

func TestAsyncUnificationConcurrentRegistration(t *testing.T) {
	r := createTestUnifiedRegistry()
	real := unifier.NewDefaultUnifier()
	gate := &gatedCatalogueUnifier{ModelUnifier: real, started: make(chan struct{}), release: make(chan struct{})}
	r.unifier = gate
	ctx := context.Background()
	url := "http://concurrent:11434"
	if err := r.RegisterModels(ctx, url, []*domain.ModelInfo{{Name: "stale"}}); err != nil {
		t.Fatal(err)
	}
	<-gate.started
	// Registration must remain possible while an older callback is in flight.
	if err := r.RegisterModels(ctx, url, []*domain.ModelInfo{{Name: "current"}}); err != nil {
		t.Fatal(err)
	}
	close(gate.release)
	waitLifecycle(t, func() bool {
		_, found := r.globalUnified.Load("current")
		return found
	})
	r.unificationMutex.Lock()
	r.unifier = real
	r.unificationMutex.Unlock()
	assertLifecycleCatalogues(t, r, "current@"+url)
	if _, err := r.unifier.ResolveAlias(ctx, "stale"); err == nil {
		t.Fatal("superseded in-flight callback restored an alias")
	}
}

func TestAsyncUnificationRegistrationDuringRemoval(t *testing.T) {
	r := createTestUnifiedRegistry()
	real := unifier.NewDefaultUnifier()
	gate := &gatedCatalogueUnifier{ModelUnifier: real, started: make(chan struct{}), release: make(chan struct{})}
	r.unifier = gate
	ctx := context.Background()
	url := "http://reused:11434"
	if err := r.RegisterModels(ctx, url, []*domain.ModelInfo{{Name: "stale"}}); err != nil {
		t.Fatal(err)
	}
	<-gate.started
	removed := make(chan error, 1)
	go func() { removed <- r.RemoveEndpoint(ctx, url) }()
	waitLifecycle(t, func() bool {
		r.generationMu.Lock()
		defer r.generationMu.Unlock()
		return r.unificationTokens[url] == nil
	})
	if err := r.RegisterModels(ctx, url, []*domain.ModelInfo{{Name: "current"}}); err != nil {
		t.Fatal(err)
	}
	close(gate.release)
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	waitLifecycle(t, func() bool {
		_, found := r.globalUnified.Load("current")
		return found
	})
	r.unificationMutex.Lock()
	r.unifier = real
	r.unificationMutex.Unlock()
	assertLifecycleCatalogues(t, r, "current@"+url)
	if _, err := r.GetUnifiedModel(ctx, "stale"); err == nil {
		t.Fatal("removed registration remains available after URL reuse")
	}
}

func TestAsyncUnificationReplacement(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "changed list", true: "empty list"}[empty], func(t *testing.T) {
			r := createTestUnifiedRegistry()
			ctx := context.Background()
			url, other := "http://replaced:11434", "http://survivor:11434"
			r.unifyModelsAsync(ctx, url, []*domain.ModelInfo{{Name: "old"}, {Name: "shared"}}, lifecycleToken(r, url))
			r.unifyModelsAsync(ctx, other, []*domain.ModelInfo{{Name: "shared"}}, lifecycleToken(r, other))
			// Retained snapshots must survive subsequent updates unchanged.
			snapshot, err := r.GetUnifiedModel(ctx, "shared")
			if err != nil {
				t.Fatal(err)
			}
			models := []*domain.ModelInfo{{Name: "new"}}
			want := []string{"shared@" + other, "new@" + url}
			if empty {
				models = nil
				want = want[:1]
			}
			r.unifyModelsAsync(ctx, url, models, lifecycleToken(r, url))
			assertLifecycleCatalogues(t, r, want...)
			if len(snapshot.SourceEndpoints) != 2 {
				t.Fatal("replacement mutated a published snapshot")
			}
			if _, err := r.GetUnifiedModel(ctx, "old"); err == nil {
				t.Fatal("omitted model remains available")
			}
			if _, found := r.GetEndpointSet("old"); found {
				t.Fatal("omitted model remains cached")
			}
		})
	}
}

func TestRemoveEndpointInvalidatesUnaliasedNativeNameCache(t *testing.T) {
	r := createTestUnifiedRegistry()
	ctx := context.Background()
	removed, survivor := "http://removed:11434", "http://survivor:11434"
	digest := "shared-digest"
	model := func(name string) *domain.ModelInfo {
		return &domain.ModelInfo{Name: name, Details: &domain.ModelDetails{Digest: &digest}}
	}
	// A second name for the same digest on one endpoint replaces its native
	// name without adding an alias. Its native-name cache must still be cleared.
	r.unifyModelsAsync(ctx, removed, []*domain.ModelInfo{model("first"), model("oldNative")}, lifecycleToken(r, removed))
	r.unifyModelsAsync(ctx, survivor, []*domain.ModelInfo{model("survivor")}, lifecycleToken(r, survivor))
	set, found := r.GetEndpointSet("oldNative")
	if !found {
		t.Fatal("native-name cache was not populated")
	}
	if _, found := set.Load(removed); !found {
		t.Fatal("native-name cache did not contain the endpoint before removal")
	}
	if err := r.RemoveEndpoint(ctx, removed); err != nil {
		t.Fatal(err)
	}
	assertLifecycleCatalogues(t, r, "first@"+survivor)
	if _, found := r.GetEndpointSet("oldNative"); found {
		t.Fatal("removed unaliased native name remains cached")
	}
	for _, name := range []string{"first", "survivor"} {
		set, found := r.GetEndpointSet(name)
		if !found {
			t.Fatalf("surviving cache %q is missing", name)
		}
		if _, found := set.Load(removed); found {
			t.Errorf("cache %q retains the removed endpoint", name)
		}
		if _, found := set.Load(survivor); !found {
			t.Errorf("cache %q lost the surviving endpoint", name)
		}
	}
}

func TestAsyncUnificationRefreshKeepsPublishedEndpoints(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "shared"}[shared], func(t *testing.T) {
			r := createTestUnifiedRegistry()
			ctx := context.Background()
			a := &domain.Endpoint{URLString: "http://refreshed:11434", Status: domain.StatusHealthy}
			b := &domain.Endpoint{URLString: "http://other:11434", Status: domain.StatusHealthy}
			models := []*domain.ModelInfo{{Name: "model", Size: 10}}
			r.unifyModelsAsync(ctx, a.URLString, models, lifecycleToken(r, a.URLString))
			endpoints := []*domain.Endpoint{a}
			want := []string{a.URLString}
			catalogue := []string{"model@" + a.URLString}
			if shared {
				r.unifyModelsAsync(ctx, b.URLString, models, lifecycleToken(r, b.URLString))
				endpoints = append(endpoints, b)
				want = append(want, b.URLString)
				catalogue = append(catalogue, "model@"+b.URLString)
			}
			sort.Strings(want)
			real := r.unifier
			gate := &gatedMergeUnifier{ModelUnifier: real, started: make(chan struct{}), release: make(chan struct{})}
			r.unifier = gate
			token := lifecycleToken(r, a.URLString)
			done := make(chan struct{})
			go func() {
				r.unifyModelsAsync(ctx, a.URLString, models, token)
				close(done)
			}()
			defer func() {
				close(gate.release)
				<-done
				r.unifier = real
				assertLifecycleCatalogues(t, r, catalogue...)
			}()
			select {
			case <-gate.started:
			case <-time.After(5 * time.Second):
				t.Fatal("refresh did not reach merge")
			}
			assertURLs := func(label string, got []string) {
				t.Helper()
				sort.Strings(got)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("%s during refresh = %v, want %v", label, got, want)
				}
			}
			published, err := r.GetUnifiedModels(ctx)
			if err != nil {
				t.Fatal(err)
			}
			urls := []string{}
			for _, model := range published {
				for _, source := range model.SourceEndpoints {
					urls = append(urls, source.EndpointURL)
				}
			}
			assertURLs("unified catalogue", urls)
			urls = []string{}
			if set, found := r.GetEndpointSet("model"); found {
				set.Range(func(url string, _ struct{}) bool { urls = append(urls, url); return true })
			}
			assertURLs("endpoint cache", urls)
			healthy, err := r.GetHealthyEndpointsForModel(ctx, "model", &mockEndpointRepository{endpoints: endpoints})
			if err != nil {
				t.Fatal(err)
			}
			urls = []string{}
			for _, endpoint := range healthy {
				urls = append(urls, endpoint.GetURLString())
			}
			assertURLs("healthy endpoints", urls)
			routable, _, err := r.GetRoutableEndpointsForModel(ctx, "model", endpoints)
			if err != nil {
				t.Fatal(err)
			}
			urls = []string{}
			for _, endpoint := range routable {
				urls = append(urls, endpoint.GetURLString())
			}
			assertURLs("routable endpoints", urls)
		})
	}
}

func TestAsyncUnificationRefreshInvalidatesObsoleteNativeNameCache(t *testing.T) {
	r := createTestUnifiedRegistry()
	ctx := context.Background()
	url := "http://renamed:11434"
	digest := "shared-digest"
	model := func(name string) *domain.ModelInfo {
		return &domain.ModelInfo{Name: name, Details: &domain.ModelDetails{Digest: &digest}}
	}
	r.unifyModelsAsync(ctx, url, []*domain.ModelInfo{model("first"), model("oldNative")}, lifecycleToken(r, url))
	if _, found := r.GetEndpointSet("oldNative"); !found {
		t.Fatal("old native name was not cached")
	}
	r.unifyModelsAsync(ctx, url, []*domain.ModelInfo{model("first"), model("newNative")}, lifecycleToken(r, url))
	assertLifecycleCatalogues(t, r, "first@"+url)
	if _, found := r.GetEndpointSet("oldNative"); found {
		t.Fatal("refresh retained an obsolete native-name cache")
	}
	set, found := r.GetEndpointSet("newNative")
	if !found {
		t.Fatal("refreshed native-name cache is missing")
	}
	if _, found := set.Load(url); !found {
		t.Fatal("refreshed native-name cache lost its endpoint")
	}
}
