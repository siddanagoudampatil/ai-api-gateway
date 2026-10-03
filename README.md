# 🚀 AI-Assisted API Gateway (`ai-api-gateway`)

An intelligent, highly concurrent API Gateway and Reverse Proxy built to route incoming traffic, protect backend microservices from resource exhaustion, and automatically mitigate abuse. 

Designed with a focus on **distributed systems architecture** and **operational excellence**, this project splits the critical path (routing) from asynchronous heavy lifting (AI telemetry analysis). The gateway is written in **Go** for blazing-fast concurrent networking, while a decoupled **Python** worker analyzes traffic streams in real-time to detect anomalies.

---

## ✨ Key Features & Architecture

* **High-Throughput Routing (Go):** Utilizes lightweight Go goroutines and `httputil.ReverseProxy` to efficiently multiplex incoming HTTP traffic to downstream backend services with minimal latency overhead.
* **Distributed Rate Limiting (Redis + Lua):** Implements the **Token Bucket algorithm** via Redis. Uses atomic Lua scripts to prevent race conditions across concurrent requests, ensuring strict, distributed rate-limiting across multiple gateway instances.
* **AI Anomaly Detection (Python):** The Go gateway streams asynchronous request metadata to Redis. A decoupled Python ML worker consumes this stream, calculating rolling averages and standard deviations to detect layer 7 DDoS patterns and automatically ban malicious IPs in real-time.
* **Operational Observability (Prometheus & Grafana):** Fully instrumented with Prometheus metrics. Includes a provisioned Grafana dashboard to track P99 API latency, total request throughput, and real-time rejection rates (`HTTP 429` and `HTTP 403`).

```
                      ┌──────────────────────────────────────────┐
                      │              Client Traffic              │
                      └────────────────────┬─────────────────────┘
                                           │ :8080
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ ai-api-gateway (Go Core)                                                        │
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
                        │ :80                             │ :6379 (Redis Streams)
                        ▼                                 ▼
             ┌─────────────────────┐           ┌────────────────────┐
             │ Upstream AI Backend │           │    Redis State     │
             │   (Mock Service)    │           │ (Tokens & Streams) │
             └─────────────────────┘           └─────────┬──────────┘
                                                         │
                                                         ▼
                                               ┌────────────────────┐
                                               │ Python ML Worker   │
                                               │ (Anomaly Detector) │
                                               └────────────────────┘
```

## 🛠️ Tech Stack
* **Core Gateway:** Go (Golang 1.22+)
* **AI / Worker:** Python
* **State & Caching:** Redis 7 (Streams, Sets, Lua Scripting)
* **Observability:** Prometheus, Grafana
* **Infrastructure:** Docker & Docker Compose

---

## ⚙️ Configuration

Environment variables drive runtime configuration:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` / `GATEWAY_PORT` | `8080` | Port on which the gateway listens. |
| `BACKEND_URL` / `TARGET_URL` | `http://localhost:8081` | Upstream target service URL (`http://backend:80` in Compose). |
| `LOG_LEVEL` | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`). |
| `READ_TIMEOUT` | `15s` | Maximum duration for reading incoming requests. |
| `WRITE_TIMEOUT` | `60s` | Maximum duration before timing out response writes. |
| `IDLE_TIMEOUT` | `120s` | Maximum duration to keep idle keep-alive connections alive. |

---

## 🚦 Getting Started

### Local Multi-Container Environment

Spin up the entire stack using Docker Compose:

```bash
docker compose up -d --build
```

Verify service status:
```bash
docker compose ps
```

### Verification

1. **Test Proxy Routing:**
   ```bash
   curl -i http://localhost:8080
   ```
   *Expected response includes `Via: 1.1 ai-api-gateway` and upstream metadata.*

2. **Liveness Probe:**
   ```bash
   curl -i http://localhost:8080/healthz
   ```

3. **Run Unit Tests:**
   ```bash
   go test -v ./...
   ```

---

## 🗺️ Project Roadmap

- [x] **Phase 1: Core Routing Infrastructure** (Go Reverse Proxy, Docker Compose, Redis setup)
- [ ] **Phase 2: Distributed Rate Limiting** (Redis + Lua Token Bucket middleware)
- [ ] **Phase 3: Python ML Anomaly Detection** (Async Redis stream consumer, dynamic IP blacklisting)
- [ ] **Phase 4: Full Observability Suite** (Prometheus metric exporter & Grafana dashboard)
