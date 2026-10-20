---
title: Docker Swarm Discovery
description: Discover and route to running Docker Swarm tasks from Olla.
keywords: olla, docker swarm, endpoint discovery, llm proxy, overlay network
---

# Docker Swarm Discovery

Olla can discover running tasks for one explicitly selected Swarm service. Swarm
discovery is additive: configured static endpoints remain available alongside
the discovered tasks. Olla reads the Docker Engine API through its Unix socket,
then connects directly to each task's address on the selected overlay network
and configured container port. The Olla host must be attached to that network
and have access to a manager's Docker Unix socket. When Olla runs in a container,
mount the socket or provide a local Unix-socket proxy.

Set `discovery.static.endpoints: []` for a Swarm-only deployment. When migrating
existing workers, move their filters into `node_endpoints` and remove their old
static definitions to avoid registering the same worker through a hostname and
its task address.

Only tasks whose desired and current states are both `running` are routable.
Olla waits to publish a task until the manager can resolve its node hostname, so
the matching `node_endpoints` override and model filter are applied from the
first snapshot. The next snapshot retries tasks whose node details are not ready.
Docker service and node events trigger an early refresh. Periodic task snapshots
recover missed events and manager interruptions. A failed API request leaves the
last successful snapshot in place; a successful empty snapshot removes all
discovered tasks for the selected service.

```yaml
discovery:
  type: "static"
  static:
    endpoints:
      - url: "http://backup-ollama:11434"
        name: "backup-ollama"
        type: "ollama"
  swarm:
    enabled: true
    socket: "/var/run/docker.sock"
    service: "ollama"
    network: "llm-overlay"
    port: 11434 # default target port; not the published host port
    refresh_interval: 30s
    endpoint:
      type: "ollama"
      health_check_url: "/"
      model_url: "/api/tags"
      model_filter:
        exclude:
          - "*-unwanted"
    node_endpoints:
      legion-server-1:
        model_filter:
          exclude:
            - "gpt-oss:latest"
            - "*20b*"
            - "*32b*"
            - "*70b*"
      legion-server-2:
        model_filter:
          exclude:
            - "gpt-oss:latest"
            - "*20b*"
            - "*32b*"
            - "*70b*"
```

`endpoint` supplies the default endpoint settings for every task. Each value in
`node_endpoints` is an override keyed by the Docker node hostname; omitted fields
inherit from `endpoint`. Both use the regular endpoint settings, including the
backend `type`, health and model paths, auth, headers, and `model_filter`. A
node's `model_filter` replaces the template filter for that node. This lets
operators exclude different models on different nodes using the same filter
rules as static endpoints.

The node binding is needed because task names and addresses change during
replacement. The filter itself remains the existing endpoint `model_filter`;
there is no separate Swarm filtering policy.

| Field | Description |
|-------|-------------|
| `swarm.enabled` | Enable discovery alongside static endpoints; defaults to `false`. |
| `swarm.socket` | Absolute Docker Unix socket path; defaults to `/var/run/docker.sock`. |
| `swarm.service` | Required exact service name or ID. |
| `swarm.network` | Required overlay network name or ID shared with Olla. |
| `swarm.port` | Container target port used by each task; defaults to `11434`. |
| `swarm.api_version` | Optional Docker Engine API version prefix. Omit it to use the Engine's current API path. |
| `swarm.refresh_interval` | Full snapshot interval; defaults to `30s`. |
| `swarm.endpoint` | Default endpoint configuration applied to discovered tasks. |
| `swarm.node_endpoints` | Endpoint overrides keyed by Docker node hostname. |

Generated endpoint names include the full task ID. Task identity also fences
health checks, discovery state, registry ownership, and sticky sessions, so a
replacement task does not inherit the old task's state when Docker reuses its IP.
If a static endpoint and task resolve to the same URL, the static endpoint takes
precedence and that task is skipped.

The Docker socket grants broad control over the Docker daemon. Use a
read-restricted socket proxy or another least-privilege access mechanism when
Olla should only inspect Swarm resources. Do not expose an unauthenticated Docker
socket on a network.
