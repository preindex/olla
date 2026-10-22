package unifier

import (
	"context"
	"testing"

	"github.com/thushan/olla/internal/core/domain"
)

func TestDefaultUnifierEndpointUpdatesPreserveSnapshots(t *testing.T) {
	u := NewDefaultUnifier().(*DefaultUnifier)
	ctx := context.Background()
	a := &domain.Endpoint{URLString: "http://first:11434", Name: "first"}
	b := &domain.Endpoint{URLString: "http://second:11434", Name: "second"}
	models := []*domain.ModelInfo{{Name: "shared", Size: 10}}
	if _, err := u.UnifyModels(ctx, models, a); err != nil {
		t.Fatal(err)
	}
	first, err := u.ResolveAlias(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.UnifyModels(ctx, models, b); err != nil {
		t.Fatal(err)
	}
	if len(first.SourceEndpoints) != 1 || first.DiskSize != 10 {
		t.Fatal("merging an endpoint mutated a retained catalogue snapshot")
	}
	merged, err := u.ResolveAlias(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.UnifyModels(ctx, nil, a); err != nil {
		t.Fatal(err)
	}
	if len(merged.SourceEndpoints) != 2 || merged.DiskSize != 20 {
		t.Fatal("removing an endpoint mutated a retained catalogue snapshot")
	}
	remaining, err := u.ResolveAlias(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining.SourceEndpoints) != 1 || remaining.SourceEndpoints[0].EndpointURL != b.URLString || remaining.DiskSize != 10 {
		t.Fatalf("unexpected remaining catalogue entry: %+v", remaining)
	}
}

func TestDefaultUnifierMergePreservesSnapshotPlatforms(t *testing.T) {
	u := NewDefaultUnifier().(*DefaultUnifier)
	ctx := context.Background()
	u.store.PutModel(&domain.UnifiedModel{
		ID:       "shared",
		Metadata: map[string]interface{}{"platforms": map[string]bool{"first": true}},
	})
	snapshot, err := u.ResolveAlias(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}
	u.mergeModel(snapshot, &Model{Name: "shared", Metadata: map[string]interface{}{"platform": "second"}},
		&domain.Endpoint{URLString: "http://second:11434", Name: "second"})
	platforms := snapshot.Metadata["platforms"].(map[string]bool)
	if len(platforms) != 1 || !platforms["first"] {
		t.Fatalf("merge mutated retained snapshot platforms: %v", platforms)
	}
	current, err := u.ResolveAlias(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}
	platforms = current.Metadata["platforms"].(map[string]bool)
	if len(platforms) != 2 || !platforms["first"] || !platforms["second"] {
		t.Fatalf("merge lost platform metadata: %v", platforms)
	}
}
