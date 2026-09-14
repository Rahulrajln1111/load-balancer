# Load Balancer (Go)

A production-oriented **Layer 7 HTTP Load Balancer** built from scratch with Go's standard library
(`net/http` + `httputil.ReverseProxy`). It fronts the [chitchat](https://github.com/Rahulrajln1111/chitchat)
secure group-chat application: three Go backends on three VMs, one shared PostgreSQL, and a single
public endpoint clients ever need to know.

**Public endpoint:** `http://10.1.75.51:3289`

---

## What it does

```text
                 Clients (browser / load generators)
                              |
                    Load Balancer  :3289 (published via NAT from VM :2289)
                              |
        +---------------------+---------------------+
        |                     |                     |
   Backend 1             Backend 2             Backend 3 + PostgreSQL
 VM :2290 -> :3000     VM :2291 -> :3000      VM :2292 -> :3000 / :5432
  172.17.0.91           172.17.0.92              172.17.0.93
```

- `POST /message` — accepts `client-name` and `msg`, submits a message (exact assignment route)
- `GET /feed` — retrieves all messages (exact assignment route)
- `GET /health`, `GET /status` — LB and backend observability
- `/ws` and all chat-app routes — proxied with WebSocket upgrade support

---

## Implemented Features

### Performance-based dynamic scheduling (no plain round-robin)

`internal/scheduler/performance.go` implements **power-of-two-choices least-load with
epsilon-greedy exploration**:

1. Global in-flight cap (2700): at capacity, new work drains to the least-loaded backend.
2. Candidate filter: alive backends whose active requests are below the per-backend
   threshold (900). If none qualify, fall back to least-loaded-alive.
3. With 5% probability pick a random healthy backend (explores and corrects drift).
4. Otherwise sample **two** candidates and pick the one with the lower load score
   (active requests + capped EWMA latency penalty).

EWMA samples are capped so a single slow response (e.g. a stalled probe) can never poison
the score, and **WebSocket sessions are excluded** from in-flight/latency metrics — a long-lived
WS connection must not look like a hanging HTTP request.

### Health checking and instant circuit breaking

- Active probes every 1 s (2 s timeout); 3 consecutive failures take a backend out of rotation.
- Passive breaker: a connection-refused/reset marks a backend dead **immediately**, so the
  retry lands on a live backend instead of a corpse.

### Failover with body-replay retry

`/message` and `/feed` requests are retried on the next backend if the first attempt fails
at the connection level. The request body is buffered and replayed byte-identical; duplicate
storage is impossible because the backend deduplicates on message ID (`ON CONFLICT DO NOTHING`).
Non-idempotent chat-app routes are never retried.

### Memory governance (512 MB cgroups)

- LB: `GOMEMLIMIT` from `lb.env` plus in-code `GCPercent(200)` and a 220 MiB soft memory
  limit — GC acts long before the cgroup OOM wall.
- Backends: `GOMEMLIMIT 260/180 MiB`, capped pgx pool, per-VM watchdogs restart any dead
  process in ~2 s with the correct environment.

### Transport tuning

`MaxIdleConns 4000 / MaxIdleConnsPerHost 1200 / MaxConnsPerHost 1600`, no server-side
Read/Write timeouts (WebSocket streams and slow clients must not be cut mid-flight;
per-request deadlines live in the transport).

---

## Configuration

| Env var       | Default                  | Meaning                                  |
| ------------- | ------------------------ | ---------------------------------------- |
| `LB_PORT`     | `3000`                   | Listen port (deployed as `3289`)         |
| `LB_BACKENDS` | `http://10.1.75.51:3290/91/92` | Comma-separated backend URLs       |
| `GOMEMLIMIT`  | unset                    | Go soft memory limit (set in `lb.env`)   |

IPs and ports of the allotted systems are fixed; no configuration changes them.

---

## Build and Run

```bash
go build -o lb ./cmd/lb
set -a; . ./lb.env; set +a   # optional env (GOMEMLIMIT, LB_PORT)
./lb
```

---

## Testing

```bash
# single request through the LB
curl http://localhost:3289/health

# post a message and read the feed
curl -X POST http://10.1.75.51:3289/message \
     -H 'Content-Type: application/json' \
     -d '{"client-name":"alice","msg":"hello"}'
curl http://10.1.75.51:3289/feed

# concurrent burst
seq 2000 | xargs -P200 -I{} curl -s -o /dev/null -w "%{http_code}\n" \
     -X POST http://10.1.75.51:3289/message \
     -H 'Content-Type: application/json' \
     -d '{"client-name":"burst","msg":"load test"}' | sort | uniq -c
```

### Load generator

`load_generator.py` (this repo) and the more full-featured
[`tools/loadgen/loadgen.py`](https://github.com/Rahulrajln1111/chitchat) in the app repo
implement the assignment's own-generator requirement: variable users, random message
lengths, random intervals, latency percentiles, and 4-VM utilization sampling.

```bash
python3 tools/loadgen/loadgen.py --url http://10.1.75.51:3289 \
    --ramp 100,500,1000,2500 --duration 20 \
    --msg-min 10 --msg-max 500 --think-min 0 --think-max 50 --sample-util
```

---

## Project Structure

```text
cmd/lb/main.go            entrypoint: backends, scheduler, health checker, GC tuning
internal/backend/         backend state: alive, in-flight, EWMA, load score
internal/proxy/           tuned httputil.ReverseProxy transport
internal/scheduler/       performance scheduler (P2C + epsilon-greedy), round-robin (legacy)
internal/server/          routing, /message + /feed retry, circuit breaker, WS proxy
internal/health/          active health checker
load_generator.py         simple load generator
```

---

## Results (see chitchat repo `report/REPORT.pdf`)

- Official harness final run: **feed persistence 100%, correctness 100%, 0 lost messages**
- Own-generator validation: 60,000 requests across static + breakpoint boards, 0 errors,
  byte-exact random samples after every stage; backend SIGKILL mid-flood with
  **0 client-visible errors** (failover + watchdog revival + journal replay)

---

## Technologies

Go · `net/http` · `httputil.ReverseProxy` · `sync/atomic` · goroutines · pgx (backends)
