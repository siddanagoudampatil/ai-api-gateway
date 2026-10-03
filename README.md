# ai-api-gateway

A high-performance API Gateway and Anomaly Detector in Go, designed for high-concurrency microservices and real-time AI/LLM token streaming workloads.

---

## Architecture Overview

```
                      ┌──────────────────────────────────────────┐
                      │              Client Traffic              │
                      └────────────────────┬─────────────────────┘
                                           │ :8080
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ ai-api-gateway                                                                  │
│                                                                                 │
│  ├── GET /healthz ───────────► Instant Orchestrator Liveness (bypasses upstream)│
│  └── /* (Reverse Proxy Engine)                                                  │
│        ├── Go 1.22 ProxyRequest.Rewrite (Sanitizes hop-by-hop headers)          │
│        ├── Request Correlation ID Injection (X-Request-ID)                      │
│        ├── Immediate Flush Buffer (FlushInterval: -1ms for low-latency SSE)    │
│        ├── Tuned Transport (MaxIdleConns: 1000, MaxIdleConnsPerHost: 100)       │
│        └── Structured Observability (log/slog JSON logging)                     │
└───────────────────────┬─────────────────────────────────┬───────────────────────┘
                        │                                 │
                        │ :80                             │ :6379 (Phase 2+)
                        ▼                                 ▼
             ┌─────────────────────┐           ┌────────────────────┐
             │ Upstream AI Backend │           │    Redis Cache     │
             │   (Mock Service)    │           │ (Distributed State)│
             └─────────────────────┘           └────────────────────┘
```

### Key Engineering Decisions

- **Modern Rewrite Hook (`httputil.ProxyRequest`):** Uses Go 1.20+ `Rewrite` semantics instead of legacy `Director`. Sanitizes hop-by-hop headers, standardizes `X-Forwarded-*` forwarding without internal network disclosure, and generates `X-Request-ID` correlation identifiers.
- **Connection Pool Tuning:** Standard Go `http.DefaultTransport` limits `MaxIdleConnsPerHost` to `2`. Under high-concurrency gateway workloads targeting concentrated backends, this triggers TCP connection thrashing and ephemeral port exhaustion. Default connection pooling is configured with `MaxIdleConns: 1000` and `MaxIdleConnsPerHost: 100`.
- **Low-Latency Streaming:** Setting `FlushInterval: -1ms` forces immediate flush on received chunks. This minimizes Time-To-First-Token (TTFT) for Server-Sent Events (SSE) and streamed LLM responses.
- **Orchestrator Health Isolation:** The `/healthz` probe terminates directly at the gateway multiplexer without touching upstream backends, preventing cascading health check failures if downstreams experience transient degradation.
- **Graceful Shutdown:** Intercepts `SIGINT` and `SIGTERM` with a 15-second draining window to let in-flight upstream transactions complete cleanly before listener teardown.

---

## Directory Structure

```
ai-api-gateway/
├── cmd/
│   └── gateway/
│       └── main.go           # Application entrypoint, config loading, and HTTP lifecycle
├── internal/
│   └── proxy/
│       ├── handler.go        # Reverse proxy implementation and connection pool setup
│       └── handler_test.go   # Integration unit tests (timeout, failure, and proxy assertions)
├── Dockerfile                # Multi-stage static build using alpine runtime and non-root user
├── docker-compose.yml        # Orchestration for gateway, mock backend, and Redis
├── go.mod                    # Go 1.22+ module definition
└── go.sum                    # Dependency checksums
```

---

## Configuration

All configuration parameters are driven through environment variables:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` / `GATEWAY_PORT` | `8080` | Port on which the gateway listens. |
| `BACKEND_URL` / `TARGET_URL` | `http://localhost:8081` | Upstream target service URL (`http://backend:80` in Compose). |
| `LOG_LEVEL` | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`). |
| `READ_TIMEOUT` | `15s` | Maximum duration for reading the entire incoming request. |
| `WRITE_TIMEOUT` | `60s` | Maximum duration before timing out writes of the response. |
| `IDLE_TIMEOUT` | `120s` | Maximum amount of time to keep an idle keep-alive connection open. |

---

## Getting Started

### Prerequisites

- Go 1.22+
- Docker & Docker Compose

### Running with Docker Compose

Spin up the gateway, mock upstream backend, and Redis instance:

```bash
docker compose up -d --build
```

Verify service status:

```bash
docker compose ps
```

### Running Bare-Metal Locally

1. Start upstream dependencies:
   ```bash
   docker compose up -d backend redis
   ```

2. Run the gateway:
   ```bash
   BACKEND_URL=http://localhost:8081 PORT=8080 go run ./cmd/gateway
   ```

---

## Verification & Testing

### 1. Verify Proxy Forwarding
Send a request to the gateway to confirm traffic routing to upstream:
```bash
curl -i http://localhost:8080
```
Expected output contains the upstream response and the gateway verification header:
```http
HTTP/1.1 200 OK
Via: 1.1 ai-api-gateway
...
```

### 2. Verify Gateway Health Probe
```bash
curl -i http://localhost:8080/healthz
```
Response:
```json
{"status":"healthy","time":"2026-10-03T04:40:51Z"}
```

### 3. Run Unit Tests
```bash
go test -v ./...
```

---

## Roadmap

- [x] **Phase 1:** Core High-Throughput Reverse Proxy & Container Infrastructure
- [ ] **Phase 2:** Distributed Token Bucket Rate Limiting (Redis-backed)
- [ ] **Phase 3:** Streaming Anomaly Detection Engine (Inference latency & payload pattern analysis)
- [ ] **Phase 4:** OpenTelemetry Tracing & Prometheus Metric Exporting
