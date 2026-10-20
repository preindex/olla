package stats

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskConnectionsDoNotCrossReusedAddresses(t *testing.T) {
	collector := NewCollector(createTestLogger())
	old := createTestEndpoint("http://10.0.0.2:11434", "old")
	old.DiscoveryIdentity = "swarm:old-task"
	replacement := *old
	replacement.DiscoveryIdentity = "swarm:new-task"
	collector.RecordConnection(old, 1)
	collector.RecordConnection(&replacement, 1)
	collector.RecordConnection(&replacement, 1)
	collector.RecordConnection(old, -1)
	require.Zero(t, collector.GetConnectionCount(old.Key()))
	require.EqualValues(t, 2, collector.GetConnectionCount(replacement.Key()))
	require.EqualValues(t, 2, collector.GetConnectionStats()[old.URLString])
	collector.RecordConnection(&replacement, -1)
	collector.RecordConnection(&replacement, -1)
	require.Empty(t, collector.taskConnections)
}

func TestTaskConnectionsConcurrentCompletion(t *testing.T) {
	collector := NewCollector(createTestLogger())
	endpoint := createTestEndpoint("http://10.0.0.2:11434", "worker")
	endpoint.DiscoveryIdentity = "swarm:task"
	var requests sync.WaitGroup
	for range 50 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			collector.RecordConnection(endpoint, 1)
			collector.RecordConnection(endpoint, -1)
		}()
	}
	requests.Wait()
	require.Zero(t, collector.GetConnectionCount(endpoint.Key()))
	require.Zero(t, collector.GetConnectionStats()[endpoint.URLString])
	require.Empty(t, collector.taskConnections)
}
