# LLM Service Discovery & Dynamic Registry Engine

High-performance, lightweight Go-based service discovery, heartbeat lease manager, active health prober, and Traefik dynamic configuration reconciler for LLM observability topologies.

## Key Features

- **In-Memory Registry**: Concurrent, thread-safe instance registration with TTL lease sweeping.
- **Active Probing**: HTTP (`/ping`, `/health`) and TCP socket probing for non-heartbeating external dependencies.
- **Traefik Reconciler**: Reconciles cluster state into atomic `discovery.yml` files for Traefik dynamic routing.
- **Seed Catalog**: Automatically boots and registers seed infrastructure services from a catalog file.
- **W3C Trace Context**: Distributed tracing context propagation across service resolution requests.

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Service registry health check |
| `GET` | `/v1/services` | List all registered service instances |
| `GET` | `/v1/resolve?service=<name>` | Resolve an active healthy service instance |
| `POST` | `/v1/register` | Register or update a service instance |
| `POST` | `/v1/heartbeat` | Send a heartbeat to maintain instance lease |

## Run Locally

```bash
go test -v ./tests/...
go run main.go
```

## Docker

```bash
docker build -t chiefj/llmobs-service-registry:latest .
docker run -p 31426:31426 chiefj/llmobs-service-registry:latest
```
