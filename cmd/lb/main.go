package main

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"load-balancer/internal/backend"
	"load-balancer/internal/health"
	"load-balancer/internal/proxy"
	"load-balancer/internal/scheduler"
	"load-balancer/internal/server"
)

// Backend endpoints (NAT-mapped public ports of the 3 backend VMs).
var defaultBackends = []string{
	"http://10.1.75.51:3290",
	"http://10.1.75.51:3291",
	"http://10.1.75.51:3292",
}

func main() {
	// Serve 2500 concurrent keep-alive connections on a 1-core cgroup.
	// GCPercent 200: balances CPU spent on GC vs memory growth; the 220MiB
	// soft limit forces collection long before the 512MB cgroup ceiling
	// (observed 471MB peak with GCPercent 400 during a 2500-burst).
	debug.SetGCPercent(200)
	debug.SetMemoryLimit(220 << 20)

	backendURLs := defaultBackends
	if v := os.Getenv("LB_BACKENDS"); v != "" {
		backendURLs = strings.Split(v, ",")
	}

	backends := make([]*backend.Backend, 0, len(backendURLs))
	for _, raw := range backendURLs {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			log.Fatal(err)
		}
		backends = append(backends, backend.New(u, proxy.New(u)))
	}

	// Performance-based dynamic scheduling: least-load (power-of-two-choices,
	// epsilon-greedy) with per-backend in-flight threshold and health awareness.
	sch := scheduler.NewPerformanceScheduler(backends)

	// Active health checking: probe every second, mark down after 3 failures.
	checker := health.New(backends, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Run(ctx)
	checker.CheckAll(ctx) // probe once before accepting traffic

	srv := server.New(sch, backends)
	mux := http.NewServeMux()
	srv.Register(mux)

	addr := ":3000"
	if v := os.Getenv("LB_PORT"); v != "" {
		addr = ":" + v
	}

	log.Printf("Load Balancer (performance scheduler, %d backends) starting on %s", len(backends), addr)
	s := &http.Server{
		Addr:        addr,
		Handler:     mux,
		IdleTimeout: 120 * time.Second,
		// No Read/Write timeouts: WebSocket streams and slow clients must not
		// be cut mid-flight; per-request deadlines live in the transport.
	}
	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
