package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultDockerSocket = "/var/run/docker.sock"
)

// SwarmTask describes the routable address and node identity of one running
// service task, using its task ID as its incarnation identity.
type SwarmTask struct {
	ID       string
	NodeID   string
	NodeName string
	Address  string
	Port     uint32
}

// SwarmManagerClient reads Docker Engine's local API over its Unix socket.
// Event streams are treated as hints by the service; snapshots remain
// authoritative and are always reconciled after a stream reconnect.
type SwarmManagerClient struct {
	client  *http.Client
	baseURL string
	api     string
	timeout time.Duration
}

func NewSwarmManagerClient(socket, apiVersion string, timeout time.Duration) (*SwarmManagerClient, error) {
	if socket == "" {
		socket = defaultDockerSocket
	}
	if !strings.HasPrefix(socket, "/") {
		return nil, errors.New("Docker socket must be an absolute Unix socket path")
	}
	apiVersion = strings.TrimPrefix(strings.Trim(apiVersion, "/"), "v")
	if apiVersion != "" {
		apiVersion = "v" + apiVersion
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
		DisableKeepAlives:     false,
		ResponseHeaderTimeout: timeout,
	}
	return &SwarmManagerClient{
		baseURL: "http://docker",
		api:     apiVersion,
		client:  &http.Client{Transport: transport},
		timeout: timeout,
	}, nil
}

func (c *SwarmManagerClient) CloseIdleConnections() {
	c.client.CloseIdleConnections()
}

type swarmService struct {
	ID   string `json:"ID"`
	Spec struct {
		Name         string `json:"Name"`
		TaskTemplate struct {
			Networks []struct {
				Target string `json:"Target"`
			} `json:"Networks"`
		} `json:"TaskTemplate"`
	} `json:"Spec"`
	Endpoint struct {
		Spec struct {
			Ports []struct {
				Protocol   string `json:"Protocol"`
				TargetPort uint32 `json:"TargetPort"`
			} `json:"Ports"`
		} `json:"Spec"`
	} `json:"Endpoint"`
}

type swarmTask struct {
	ID           string `json:"ID"`
	ServiceID    string `json:"ServiceID"`
	NodeID       string `json:"NodeID"`
	DesiredState string `json:"DesiredState"`
	Status       struct {
		State string `json:"State"`
	} `json:"Status"`
	NetworksAttachments []struct {
		Network struct {
			ID string `json:"ID"`
		} `json:"Network"`
		Addresses []string `json:"Addresses"`
	} `json:"NetworksAttachments"`
}

type swarmNetwork struct {
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

type swarmNode struct {
	ID          string `json:"ID"`
	Description struct {
		Hostname string `json:"Hostname"`
	} `json:"Description"`
}

// ListRunningTasks resolves exactly one service and returns only tasks that
// Docker reports as both desired and currently running.
func (c *SwarmManagerClient) ListRunningTasks(ctx context.Context, serviceName, networkName string, port uint32) ([]SwarmTask, error) {
	if serviceName == "" || networkName == "" || port == 0 {
		return nil, errors.New("Swarm service, network, and port are required")
	}
	var services []swarmService
	if err := c.getJSON(ctx, "/services", &services); err != nil {
		return nil, fmt.Errorf("list Swarm services: %w", err)
	}
	selected, err := selectSwarmService(services, serviceName)
	if err != nil {
		return nil, err
	}
	if selected == nil {
		// A successful empty service response is authoritative: the selected
		// service has been removed, so return an empty snapshot.
		return []SwarmTask{}, nil
	}

	var networks []swarmNetwork
	if err := c.getJSON(ctx, "/networks", &networks); err != nil {
		return nil, fmt.Errorf("list Docker networks: %w", err)
	}
	networkIDs := swarmNetworkIDs(networks, networkName)
	if len(networkIDs) == 0 {
		return []SwarmTask{}, nil
	}
	if !serviceUsesNetwork(selected, networkIDs) {
		return []SwarmTask{}, nil
	}

	var nodes []swarmNode
	if err := c.getJSON(ctx, "/nodes", &nodes); err != nil {
		return nil, fmt.Errorf("list Swarm nodes: %w", err)
	}
	nodeNames := make(map[string]string, len(nodes))
	for _, node := range nodes {
		nodeNames[node.ID] = node.Description.Hostname
	}

	var tasks []swarmTask
	filter, _ := json.Marshal(map[string][]string{"service": {selected.ID}})
	if err := c.getJSON(ctx, "/tasks?filters="+url.QueryEscape(string(filter)), &tasks); err != nil {
		return nil, fmt.Errorf("list Swarm tasks: %w", err)
	}
	return mapRunningSwarmTasks(tasks, selected.ID, networkIDs, nodeNames, port), nil
}

func selectSwarmService(services []swarmService, name string) (*swarmService, error) {
	var selected *swarmService
	for i := range services {
		if services[i].ID != name && services[i].Spec.Name != name {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("Swarm service selection %q is ambiguous", name)
		}
		selected = &services[i]
	}
	return selected, nil
}

func swarmNetworkIDs(networks []swarmNetwork, name string) map[string]struct{} {
	ids := make(map[string]struct{})
	for _, network := range networks {
		if network.Name == name || network.ID == name {
			ids[network.ID] = struct{}{}
		}
	}
	return ids
}

func serviceUsesNetwork(service *swarmService, networkIDs map[string]struct{}) bool {
	for _, network := range service.Spec.TaskTemplate.Networks {
		if _, ok := networkIDs[network.Target]; ok {
			return true
		}
	}
	return false
}

func mapRunningSwarmTasks(tasks []swarmTask, serviceID string, networkIDs map[string]struct{}, nodeNames map[string]string, port uint32) []SwarmTask {
	var result []SwarmTask
	for _, task := range tasks {
		if task.ServiceID != serviceID || task.DesiredState != "running" || task.Status.State != "running" {
			continue
		}
		nodeName := strings.TrimSpace(nodeNames[task.NodeID])
		if task.ID == "" || nodeName == "" {
			continue
		}
		address := taskAddressOnNetwork(task, networkIDs)
		if address != "" {
			result = append(result, SwarmTask{ID: task.ID, NodeID: task.NodeID, NodeName: nodeName, Address: address, Port: port})
		}
	}
	return result
}

func taskAddressOnNetwork(task swarmTask, networkIDs map[string]struct{}) string {
	for _, attachment := range task.NetworksAttachments {
		if _, ok := networkIDs[attachment.Network.ID]; !ok || len(attachment.Addresses) == 0 {
			continue
		}
		return strings.SplitN(attachment.Addresses[0], "/", 2)[0]
	}
	return ""
}

// Events returns a streaming response from Docker. Callers must close it and
// reconnect with backoff; every successful reconnect should trigger a snapshot.
func (c *SwarmManagerClient) Events(ctx context.Context, _ string) (*http.Response, error) {
	// Docker's Swarm event filters support services and nodes. Task state is
	// derived from the authoritative snapshot after either event arrives.
	filters, _ := json.Marshal(map[string][]string{"type": {"service", "node"}, "scope": {"swarm"}})
	path := "/events?filters=" + url.QueryEscape(string(filters))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+c.apiPath(path), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		response.Body.Close()
		return nil, fmt.Errorf("Docker events returned %s", response.Status)
	}
	return response, nil
}

func (c *SwarmManagerClient) getJSON(ctx context.Context, path string, target any) error {
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, c.baseURL+c.apiPath(path), nil)
	if err != nil {
		return err
	}
	response, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Docker API returned %s", response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return err
	}
	return nil
}

func (c *SwarmManagerClient) apiPath(path string) string {
	if c.api == "" {
		return path
	}
	return "/" + c.api + path
}
