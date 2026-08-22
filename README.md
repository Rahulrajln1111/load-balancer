# Load Balancer (Go)

A production-oriented **Layer 7 HTTP Load Balancer** built from scratch using Go's standard library (`net/http` + `httputil.ReverseProxy`).

> **Goal:** Understand how modern load balancers like Envoy, HAProxy, NGINX, and cloud L7 load balancers are engineered internally instead of relying on web frameworks.

---

## 🚀 Current Status

### Milestone 1 — Completed ✅

* HTTP reverse proxy using `httputil.ReverseProxy`
* Round Robin scheduler
* Concurrent health checker
* Atomic backend metrics (`Alive`, `InFlight`, `Requests`)
* Dependency Injection architecture
* Context-based background worker
* Multi-backend routing verified
* Concurrent request testing (`curl` + `xargs`)

---

## Architecture

```text
                    Client
                       │
                 HTTP Request
                       │
                net/http Server
                       │
                 Server Package
                       │
             Scheduler Interface
                       │
      ┌────────────────┴───────────────┐
      │                                │
 Round Robin                  Future Algorithms
 (Implemented)                (P2C / EWMA / WLR)
                       │
               Healthy Backend
                       │
        httputil.ReverseProxy
                       │
          Backend 1 / 2 / 3
```

---

## Project Structure

```text
internal/
├── backend/
│   └── backend.go
│
├── proxy/
│   └── proxy.go
│
├── scheduler/
│   ├── interface.go
│   └── round_robin.go
│
├── server/
│   └── server.go
│
└── health/
    └── checker.go

cmd/
└── lb/
    └── main.go
```

Each package owns a **single responsibility**, making scheduler implementations pluggable.

---

## Implemented Features

### Reverse Proxy

* Transparent request forwarding
* Backend isolation
* Header preservation

### Round Robin

Current scheduler rotates requests fairly across healthy backends.

Example:

```text
1 → Backend-1
2 → Backend-2
3 → Backend-3
4 → Backend-1
```

### Health Checking

Every backend exposes:

```http
GET /health
```

A concurrent health worker periodically updates backend availability without blocking request handling.

### Atomic Metrics

Every backend maintains thread-safe runtime state:

| Metric     | Description           |
| ---------- | --------------------- |
| `Alive`    | Health status         |
| `InFlight` | Active requests       |
| `Requests` | Total served requests |

---

## Testing

### Sequential

```bash
curl http://localhost:8080/1
```

### Concurrent

```bash
seq 30 | xargs -P30 -I{} curl -s http://localhost:8080/1
```

### Fairness Verification

```bash
seq 300 \
| xargs -P50 -I{} curl -s http://localhost:8080/1 \
| sort \
| uniq -c
```

Expected distribution:

```text
100 Backend-1
100 Backend-2
100 Backend-3
```

---

# 🎯 Modern Load Balancing Roadmap

This project will evolve beyond Round Robin into **three production-grade scheduling algorithms** commonly used in modern service meshes and cloud load balancers.

## 1. Power of Two Choices (Least Request) ⭐

**Priority:** Next Implementation

Instead of scanning every backend, randomly sample **two healthy servers** and choose the one with fewer active requests.

```text
Random Pick
     │
 ┌───┴────┐
 │        │
B1(12)  B3(4)
 │        │
 └──► Choose B3
```

### Why it's modern

* O(1) scheduling
* Excellent load distribution
* Handles uneven traffic much better than Round Robin
* Foundation of modern Envoy deployments

---

## 2. Weighted Least Request

Real production clusters contain machines with different capacities.

Example:

```text
Backend-A : 4 CPU
Backend-B : 16 CPU
Backend-C : 32 CPU
```

Instead of treating them equally, scheduling considers **capacity weight** together with active requests.

```text
Higher Weight
      +
Lower Active Requests
      │
      ▼
Best Backend
```

This enables heterogeneous infrastructure without wasting larger machines.

---

## 3. Peak EWMA (Latency-Aware Scheduling)

The most advanced scheduler in this project.

Rather than choosing the least busy server, it continuously learns backend latency using an **Exponentially Weighted Moving Average (EWMA)**.

```text
Observed Latency
      │
      ▼
 EWMA Calculator
      │
      ▼
Latency Score
      │
      ▼
Select Fastest Healthy Backend
```

Advantages:

* Automatically avoids slow servers
* Adapts to latency spikes
* Better tail-latency under real workloads
* Used in modern microservice environments

---

## Development Timeline

| Phase                  | Status      |
| ---------------------- | ----------- |
| Reverse Proxy          | ✅ Completed |
| Round Robin            | ✅ Completed |
| Health Checker         | ✅ Completed |
| Power of Two Choices   | 🔜 Next     |
| Weighted Least Request | ⏳ Planned   |
| Peak EWMA              | ⏳ Planned   |
| Graceful Shutdown      | ⏳ Planned   |
| Metrics Endpoint       | ⏳ Planned   |
| Prometheus Integration | ⏳ Planned   |
| Kubernetes Ready       | ⏳ Planned   |

---

## Technologies

* **Go**
* `net/http`
* `httputil.ReverseProxy`
* `context`
* `sync/atomic`
* Goroutines & WaitGroup

---

## Vision

The objective is **not** to clone an existing load balancer, but to progressively engineer one by implementing modern scheduling algorithms, concurrency primitives, health monitoring, graceful shutdown, observability, and production-ready networking from first principles.
